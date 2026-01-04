import logging
from datetime import datetime, time as dtime, timedelta
from typing import Dict, Optional, Tuple, List
from zoneinfo import ZoneInfo
from collections import deque

from pymongo import UpdateOne

from app.core.config import SETTINGS
from app.core.database import get_mongo_db
from app.services.data_sources.manager import DataSourceManager

logger = logging.getLogger(__name__)


class QuotesIngestionService:
    """Regularly capture market-wide near real-time patterns from the data source adaptation layer, and enter the database into the MongoDB collection `market quotes ' .

    Core characteristics:
    - Schedule frequency: controlled by settings. QUOTES INGEST INTERVAL SECONDS (default 360 seconds = 6 minutes)
    - Interface rotation: Tushare → AKShare Eastern Wealth → AKShare New Wave (to avoid a single interface being restricted)
    - Intelligent limit flow: Tushare free users up to 2 times an hour, pay users automatically switch to HF mode (5 seconds)
    - Break time: Skip the task, keep last round data; implement one-time refills if necessary
    - Fields: code (6), close, pct chg, amount, open, high, low, pre close, mode date, updated at
    """

    def __init__(self, collection_name: str = "market_quotes") -> None:
        from collections import deque

        self.collection_name = collection_name
        self.status_collection_name = "quotes_ingestion_status"  #Status Record Pool
        self.tz = ZoneInfo(SETTINGS.TIMEZONE)

        #Tushare Permission Detect Related Properties
        self._tushare_permission_checked = False  #Have you tested permissions?
        self._tushare_has_premium = False  #Is there a right to pay?
        self._tushare_last_call_time = None  #Last call time (for free user limit)
        self._tushare_hourly_limit = 2  #Maximum number of calls per hour per free user
        self._tushare_call_count = 0  #Number of calls within the current hour
        self._tushare_call_times = deque()  #Queue to record call times (for flow limit)

        #Interface rotation properties
        self._rotation_sources = ["tushare", "akshare_eastmoney", "akshare_sina"]
        self._rotation_index = 0  #Current rotation index

    @staticmethod
    def _normalize_stock_code(code: str) -> str:
        """Standardised stock code is 6 digit

        Address the following:
        - Sz00001.
        - Sh600036 - 600036.
        - 000001 - > 000001
        - > 000001

        Args:
            Code: Original stock code

        Returns:
            str: Standardized 6-bit stock code
        """
        if not code:
            return ""

        code_str = str(code).strip()

        #If the code length exceeds 6 bits, remove the front prefix (e.g. sz, sh)
        if len(code_str) > 6:
            #Extract all numeric characters
            code_str = ''.join(filter(str.isdigit, code_str))

        #If it's a pure number, make it six.
        if code_str.isdigit():
            code_clean = code_str.lstrip('0') or '0'  #Remove pilot 0, and if all zeros, keep a zero
            return code_clean.zfill(6)  #We got six.

        #If it's not a pure number, try to extract it.
        code_digits = ''.join(filter(str.isdigit, code_str))
        if code_digits:
            return code_digits.zfill(6)

        #Could not extract valid code, return empty string
        return ""

    async def ensure_indexes(self) -> None:
        db = get_mongo_db()
        coll = db[self.collection_name]
        try:
            await coll.create_index("code", unique=True)
            await coll.create_index("updated_at")
        except Exception as e:
            logger.warning(f"Could not close temporary folder: %s{e}")

    async def _record_sync_status(
        self,
        success: bool,
        source: Optional[str] = None,
        records_count: int = 0,
        error_msg: Optional[str] = None
    ) -> None:
        """Record Sync Status

        Args:
            Success
            source name
            records counts: number of records
            error message
        """
        try:
            db = get_mongo_db()
            status_coll = db[self.status_collection_name]

            now = datetime.now(self.tz)

            status_doc = {
                "job": "quotes_ingestion",
                "last_sync_time": now,
                "last_sync_time_iso": now.isoformat(),
                "success": success,
                "data_source": source,
                "records_count": records_count,
                "interval_seconds": SETTINGS.QUOTES_INGEST_INTERVAL_SECONDS,
                "error_message": error_msg,
                "updated_at": now,
            }

            await status_coll.update_one(
                {"job": "quotes_ingestion"},
                {"$set": status_doc},
                upsert=True
            )

        except Exception as e:
            logger.warning(f"Recording synchronization failed (negative):{e}")

    async def get_sync_status(self) -> Dict[str, any]:
        """Get Sync Status

        Returns:
            FMT 0 
        """
        try:
            db = get_mongo_db()
            status_coll = db[self.status_collection_name]

            doc = await status_coll.find_one({"job": "quotes_ingestion"})

            if not doc:
                return {
                    "last_sync_time": None,
                    "last_sync_time_iso": None,
                    "interval_seconds": SETTINGS.QUOTES_INGEST_INTERVAL_SECONDS,
                    "interval_minutes": SETTINGS.QUOTES_INGEST_INTERVAL_SECONDS / 60,
                    "data_source": None,
                    "success": None,
                    "records_count": 0,
                    "error_message": "尚未执行过同步"
                }

            #Remove  id field
            doc.pop("_id", None)
            doc.pop("job", None)

            #Add minutes
            doc["interval_minutes"] = doc.get("interval_seconds", 0) / 60

            #🔥 Formatting Time (to ensure conversion to local time zone)
            if "last_sync_time" in doc and doc["last_sync_time"]:
                dt = doc["last_sync_time"]
                #MongoDB returns UTC time datetime objects (aware or naive)
                #Add UTC time zone if naive; convert to local time zone if ware
                if dt.tzinfo is None:
                    #Let's say UTC.
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))
                #Convert to Local Timezone
                dt_local = dt.astimezone(self.tz)
                doc["last_sync_time"] = dt_local.strftime("%Y-%m-%d %H:%M:%S")

            return doc

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return {
                "last_sync_time": None,
                "last_sync_time_iso": None,
                "interval_seconds": SETTINGS.QUOTES_INGEST_INTERVAL_SECONDS,
                "interval_minutes": SETTINGS.QUOTES_INGEST_INTERVAL_SECONDS / 60,
                "data_source": None,
                "success": None,
                "records_count": 0,
                "error_message": f"获取状态失败: {str(e)}"
            }

    def _check_tushare_permission(self) -> bool:
        """Detecting Tushare rt k interface privileges

        Returns:
            True: Payable access (HF)
            False: Free users (up to 2 times an hour)
        """
        if self._tushare_permission_checked:
            return self._tushare_has_premium or False

        try:
            from app.services.data_sources.tushare_adapter import TushareAdapter
            adapter = TushareAdapter()

            if not adapter.is_available():
                logger.info("Tushare is not available, skip permission testing")
                self._tushare_has_premium = False
                self._tushare_permission_checked = True
                return False

            #Try to call rt k interface test privileges
            try:
                df = adapter._provider.api.rt_k(ts_code='000001.SZ')
                if df is not None and not getattr(df, 'empty', True):
                    logger.info("Tushare rt k interface privileges detected (paying user)")
                    self._tushare_has_premium = True
                else:
                    logger.info("⚠️Tushare rt k interface returns empty data (possibly free user or interface limit)")
                    self._tushare_has_premium = False
            except Exception as e:
                error_msg = str(e).lower()
                if "权限" in error_msg or "permission" in error_msg or "没有访问" in error_msg:
                    logger.info("⚠️Tushare rt k interface is not allowed (free user)")
                    self._tushare_has_premium = False
                else:
                    logger.warning(f"Tushare rt k interface test failed:{e}")
                    self._tushare_has_premium = False

            self._tushare_permission_checked = True
            return self._tushare_has_premium or False

        except Exception as e:
            logger.warning(f"Tushare permission check failed:{e}")
            self._tushare_has_premium = False
            self._tushare_permission_checked = True
            return False

    def _can_call_tushare(self) -> bool:
        """Judge whether Tushare rt k interface can be called

        Returns:
            True: Callable
            False: beyond limit, no callable
        """
        #In the case of paid users, no limit on the number of calls
        if self._tushare_has_premium:
            return True

        #Free users: check the number of calls per hour
        now = datetime.now(self.tz)
        one_hour_ago = now - timedelta(hours=1)

        #Clean up one hour ago.
        while self._tushare_call_times and self._tushare_call_times[0] < one_hour_ago:
            self._tushare_call_times.popleft()

        #Check if the limit is exceeded
        if len(self._tushare_call_times) >= self._tushare_hourly_limit:
            logger.warning(
                f"The Tushare rrt k interface has reached the hourly call limit (⚠️){self._tushare_hourly_limit}I don't know."
                f"Skip this call, use the AKShare backup interface"
            )
            return False

        return True

    def _record_tushare_call(self) -> None:
        """Record Tushare call time"""
        self._tushare_call_times.append(datetime.now(self.tz))

    def _get_next_source(self) -> Tuple[str, Optional[str]]:
        """Get the next data source (rotation mechanism)

        Returns:
            (source  type, akshare api):
            - "Tushare"
            - kshare api: "east money" | "sina"
        """
        if not SETTINGS.QUOTES_ROTATION_ENABLED:
            #Rotation not enabled, default priority used
            return "tushare", None

        #Rotation logic: 0 = Tushare, 1 = Akshare East Wealth, 2 = AKShare New Wave
        current_source = self._rotation_sources[self._rotation_index]

        #Update rotation index (next time use next interface)
        self._rotation_index = (self._rotation_index + 1) % len(self._rotation_sources)

        if current_source == "tushare":
            return "tushare", None
        elif current_source == "akshare_eastmoney":
            return "akshare", "eastmoney"
        else:  # akshare_sina
            return "akshare", "sina"

    def _is_trading_time(self, now: Optional[datetime] = None) -> bool:
        """Determination of whether it is a buffer period after trading time or closing

        Transaction time:
        - 9.30-11.30 a.m.
        - 15:00 to 15:00
        - Post-disclose buffer period: 15:00-15:30 (ensure that closing prices are obtained)

        Description of the buffer period after closing:
        - 30 minutes after the transaction.
        - Three additional opportunities for synchronization, assuming one in six minutes (15:06, 15:12, 15:18)
        - significantly reduce the risk of missing closing prices
        """
        now = now or datetime.now(self.tz)
        #Chile
        if now.weekday() > 4:
            return False
        t = now.time()
        #Regular period of transactions at the point of surrender/deep exchange
        morning = dtime(9, 30)
        noon = dtime(11, 30)
        afternoon_start = dtime(13, 0)
        #Post-disclose buffer period (extended 30 minutes to 15:30)
        buffer_end = dtime(15, 30)

        return (morning <= t <= noon) or (afternoon_start <= t <= buffer_end)

    async def _collection_empty(self) -> bool:
        db = get_mongo_db()
        coll = db[self.collection_name]
        try:
            count = await coll.estimated_document_count()
            return count == 0
        except Exception:
            return True

    async def _collection_stale(self, latest_trade_date: Optional[str]) -> bool:
        if not latest_trade_date:
            return False
        db = get_mongo_db()
        coll = db[self.collection_name]
        try:
            cursor = coll.find({}, {"trade_date": 1}).sort("trade_date", -1).limit(1)
            docs = await cursor.to_list(length=1)
            if not docs:
                return True
            doc_td = str(docs[0].get("trade_date") or "")
            return doc_td < str(latest_trade_date)
        except Exception:
            return True

    async def _bulk_upsert(self, quotes_map: Dict[str, Dict], trade_date: str, source: Optional[str] = None) -> None:
        db = get_mongo_db()
        coll = db[self.collection_name]
        ops = []
        updated_at = datetime.now(self.tz)
        for code, q in quotes_map.items():
            if not code:
                continue
            #Use of standardized methods to process stock codes (delegate exchange prefixes, e.g. sz00001 - > 000001)
            code6 = self._normalize_stock_code(code)
            if not code6:
                continue

            #Log: The trade value written in the record
            volume = q.get("volume")
            if code6 in ["300750", "000001", "600000"]:  #Only a few examples of stocks are recorded
                logger.info(f"I'm sorry.{code6} - volume={volume}, amount={q.get('amount')}, source={source}")

            ops.append(
                UpdateOne(
                    {"code": code6},
                    {"$set": {
                        "code": code6,
                        "symbol": code6,  #Add symbol field, consistent with code
                        "close": q.get("close"),
                        "pct_chg": q.get("pct_chg"),
                        "amount": q.get("amount"),
                        "volume": volume,
                        "open": q.get("open"),
                        "high": q.get("high"),
                        "low": q.get("low"),
                        "pre_close": q.get("pre_close"),
                        "trade_date": trade_date,
                        "updated_at": updated_at,
                    }},
                    upsert=True,
                )
            )
        if not ops:
            logger.info("Unwritten data, skip")
            return
        result = await coll.bulk_write(ops, ordered=False)
        logger.info(
            f"It's done.{source}, matched={result.matched_count}, upserted={len(result.upserted_ids) if result.upserted_ids else 0}, modified={result.modified_count}"
        )

    async def backfill_from_historical_data(self) -> None:
        """Importing data from historical data set to previous day's closing data to market quotes
        - Import all data if market quotes is empty
        - If markt quotes is not empty, check and fix missing barter fields
        """
        try:
            #Check if market quates is empty
            is_empty = await self._collection_empty()

            if not is_empty:
                #The collection is not empty. Check if there's any missing records of the trade.
                logger.info("The collection of market quotes is not empty, checking if the exchange needs to be repaired...")
                await self._fix_missing_volume()
                return

            logger.info("📊 market quotes collect empty and start importing from historical data")

            db = get_mongo_db()
            manager = DataSourceManager()

            #Get the latest transaction date
            try:
                latest_trade_date = manager.find_latest_trade_date_with_fallback()
                if not latest_trade_date:
                    logger.warning("Could not close temporary folder: %s")
                    return
            except Exception as e:
                logger.warning(f"The latest trading day failed:{e}Skip history data import")
                return

            logger.info(f"Import from the historical data set{latest_trade_date}Other Organiser")

            #Ask for data on the latest transaction day from the stock daily quotes collection
            daily_quotes_collection = db["stock_daily_quotes"]
            cursor = daily_quotes_collection.find({
                "trade_date": latest_trade_date,
                "period": "daily"
            })

            docs = await cursor.to_list(length=None)

            if not docs:
                logger.warning(f"Not found in historical data set{latest_trade_date}Data")
                logger.warning("⚠️ market quotes and historical data sets are empty, please sync historical data or real time patterns")
                return

            logger.info(f"I found it from the historical data collection.{len(docs)}Notes")

            #Convert to quates map format
            quotes_map = {}
            for doc in docs:
                code = doc.get("symbol") or doc.get("code")
                if not code:
                    continue
                code6 = str(code).zfill(6)

                #🔥 Get the barter, give priority to the volume field
                volume_value = doc.get("volume") or doc.get("vol")
                data_source = doc.get("data_source", "")

                #Log: Record the original barter value
                if code6 in ["300750", "000001", "600000"]:  #Only a few examples of stocks are recorded
                    logger.info(f"[Refilling ]{code6} - volume={doc.get('volume')}, vol={doc.get('vol')}, data_source={data_source}")

                quotes_map[code6] = {
                    "close": doc.get("close"),
                    "pct_chg": doc.get("pct_chg"),
                    "amount": doc.get("amount"),
                    "volume": volume_value,
                    "open": doc.get("open"),
                    "high": doc.get("high"),
                    "low": doc.get("low"),
                    "pre_close": doc.get("pre_close"),
                }

            if quotes_map:
                await self._bulk_upsert(quotes_map, latest_trade_date, "historical_data")
                logger.info(f"Successfully imported from historical data{len(quotes_map)}Disk data to market quotes")
            else:
                logger.warning("History data converted to empty, cannot be imported")

        except Exception as e:
            logger.error(f"Failed to import from historical data:{e}")
            import traceback
            logger.error(f"Stack tracking: \n{traceback.format_exc()}")

    async def backfill_last_close_snapshot(self) -> None:
        """A one-time closing snapshot (for cold start-up or data obsolescence). Allows calls during the break."""
        try:
            manager = DataSourceManager()
            #Using near real-time snapshots as the bottom, the data returned during the market break is the final closing
            quotes_map, source = manager.get_realtime_quotes_with_fallback()
            if not quotes_map:
                logger.warning("Backfill: No line data obtained, Skip")
                return
            try:
                trade_date = manager.find_latest_trade_date_with_fallback() or datetime.now(self.tz).strftime("%Y%m%d")
            except Exception:
                trade_date = datetime.now(self.tz).strftime("%Y%m%d")
            await self._bulk_upsert(quotes_map, trade_date, source)
        except Exception as e:
            logger.error(f"I'm sorry.{e}")

    async def backfill_last_close_snapshot_if_needed(self) -> None:
        """If the collection is empty or track date is behind the latest transaction date, backfill"""
        try:
            is_empty = await self._collection_empty()

            #If the collection is empty, first import from historical data
            if is_empty:
                logger.info("🔁 market quates collects empty and attempts to import from historical data")
                await self.backfill_from_historical_data()
                return

            #Update with real-time interface if the collection is not empty but old
            manager = DataSourceManager()
            latest_td = manager.find_latest_trade_date_with_fallback()
            if await self._collection_stale(latest_td):
                logger.info("🔁 Trigger market break/startback to fill in the latest disk data")
                await self.backfill_last_close_snapshot()
        except Exception as e:
            logger.warning(f"Backfill trigger check failed (neglect):{e}")

    def _fetch_quotes_from_source(self, source_type: str, akshare_api: Optional[str] = None) -> Tuple[Optional[Dict], Optional[str]]:
        """Fetch lines from specified data sources

        Args:
            "tushare"
            Akshare api: "east money"

        Returns:
            (quates map, source name)
        """
        try:
            if source_type == "tushare":
                #Check for callable Tushare
                if not self._can_call_tushare():
                    return None, None

                from app.services.data_sources.tushare_adapter import TushareAdapter
                adapter = TushareAdapter()

                if not adapter.is_available():
                    logger.warning("Tushare not available")
                    return None, None

                logger.info("Use the Tushare rt k interface to get real-time lines")
                quotes_map = adapter.get_realtime_quotes()

                if quotes_map:
                    self._record_tushare_call()
                    return quotes_map, "tushare"
                else:
                    logger.warning("Tushare rt k returns empty data")
                    return None, None

            elif source_type == "akshare":
                from app.services.data_sources.akshare_adapter import AKShareAdapter
                adapter = AKShareAdapter()

                if not adapter.is_available():
                    logger.warning("AKShare not available")
                    return None, None

                api_name = akshare_api or "eastmoney"
                logger.info(f"Use AKShare{api_name}Interface to get real-time lines")
                quotes_map = adapter.get_realtime_quotes(source=api_name)

                if quotes_map:
                    return quotes_map, f"akshare_{api_name}"
                else:
                    logger.warning(f"AKShare {api_name}Return empty data")
                    return None, None

            else:
                logger.error(f"Unknown data source type:{source_type}")
                return None, None

        except Exception as e:
            logger.error(f"From{source_type}Could not close temporary folder: %s{e}")
            return None, None

    async def run_once(self) -> None:
        """Implementation of a collection and inventory

        Core logic:
        1. Test Tushare Permissions (first run)
        2. Attempts to obtain a business by rotation: Tushare → AKShare’s Eastern Wealth → AKShare’s New Wave
        3. Any interface successfully enters the library and fails to skip this collection
        """
        #Non-trading periods
        if not self._is_trading_time():
            if SETTINGS.QUOTES_BACKFILL_ON_OFFHOURS:
                await self.backfill_last_close_snapshot_if_needed()
            else:
                logger.info("{\\cHFFFFFF}{\\cH00FF00} Non-trading time, skipping line collection")
            return

        try:
            #First run: Test Tushare Permissions
            if SETTINGS.QUOTES_AUTO_DETECT_TUSHARE_PERMISSION and not self._tushare_permission_checked:
                logger.info("🔍 Initial running, detection of Tushare rt k interface privileges...")
                has_premium = self._check_tushare_permission()

                if has_premium:
                    logger.info(
                        "✅Tushare Pay Permissions! It is recommended that QUOTES INGEST INTERVAL SECONDS be set to 5-60 seconds to make full use of permissions"
                    )
                else:
                    logger.info(
                        f"Tushare, free of charge, with maximum call per hour{self._tushare_hourly_limit}Minor rt k interface."
                        f"Current collection interval:{SETTINGS.QUOTES_INGEST_INTERVAL_SECONDS}sec"
                    )

            #Get Next Data Source
            source_type, akshare_api = self._get_next_source()

            #Try to get the line
            quotes_map, source_name = self._fetch_quotes_from_source(source_type, akshare_api)

            if not quotes_map:
                logger.warning(f"⚠️ {source_name or source_type}Could not close temporary folder: %s")
                #Record failed status
                await self._record_sync_status(
                    success=False,
                    source=source_name or source_type,
                    records_count=0,
                    error_msg="未获取到行情数据"
                )
                return

            #Get Trade Day
            try:
                manager = DataSourceManager()
                trade_date = manager.find_latest_trade_date_with_fallback() or datetime.now(self.tz).strftime("%Y%m%d")
            except Exception:
                trade_date = datetime.now(self.tz).strftime("%Y%m%d")

            #Library
            await self._bulk_upsert(quotes_map, trade_date, source_name)

            #Record success status
            await self._record_sync_status(
                success=True,
                source=source_name,
                records_count=len(quotes_map),
                error_msg=None
            )

        except Exception as e:
            logger.error(f"It's not working.{e}")
            #Record failed status
            await self._record_sync_status(
                success=False,
                source=None,
                records_count=0,
                error_msg=str(e)
            )

