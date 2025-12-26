"""Tushare Data Sync Service
To synchronise Tushare data to the MongoDB Standard Collection - Yeah.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import logging

from tradingagents.dataflows.providers.china.tushare import TushareProvider
from app.services.stock_data_service import get_stock_data_service
from app.services.historical_data_service import get_historical_data_service
from app.services.news_data_service import get_news_data_service
from app.core.database import get_mongo_db
from app.core.config import settings
from app.core.rate_limiter import get_tushare_rate_limiter
from app.utils.timezone import now_tz

logger = logging.getLogger(__name__)

#UTC+8 Timezone
UTC_8 = timezone(timedelta(hours=8))


def get_utc8_now():
    """Fetch UTC+8 Current Time

Note: returns a given datetime (without time zone information), MongoDB stores local time values as they are
This allows the frontend to directly add +08:00 suffix display
"""
    return now_tz().replace(tzinfo=None)


class TushareSyncService:
    """Tushare Data Sync Service
To synchronise Tushare data to the MongoDB Standard Collection - Yeah.
"""
    
    def __init__(self):
        self.provider = TushareProvider()
        self.stock_service = get_stock_data_service()
        self.historical_service = None  #Delay Initialization
        self.news_service = None  #Delay Initialization
        self.db = get_mongo_db()
        self.settings = settings

        #Sync Configuration
        self.batch_size = 100  #Batch size
        self.rate_limit_delay = 0.1  #API call interval (sec) - disabled, using rate list
        self.max_retries = 3  #Maximum number of retries

        #Speed Limiter (read configuration from environmental variables)
        tushare_tier = getattr(settings, "TUSHARE_TIER", "standard")  # free/basic/standard/premium/vip
        safety_margin = float(getattr(settings, "TUSHARE_RATE_LIMIT_SAFETY_MARGIN", "0.8"))
        self.rate_limiter = get_tushare_rate_limiter(tier=tushare_tier, safety_margin=safety_margin)
    
    async def initialize(self):
        """Initializing Sync Service"""
        success = await self.provider.connect()
        if not success:
            raise RuntimeError("❌ Tushare连接失败，无法启动同步服务")

        #Initialization of historical data services
        self.historical_service = await get_historical_data_service()

        #Initialization of news data services
        self.news_service = await get_news_data_service()

        logger.info("Initialization of Tushare Synchronization Service completed")
    
    #== sync, corrected by elderman == @elder man
    
    async def sync_stock_basic_info(self, force_update: bool = False, job_id: str = None) -> Dict[str, Any]:
        """Sync Equation Basic Information

Args:
force update: whether all data is mandatory update
job id: Task ID (for progress tracking)

Returns:
Sync Results Statistics
"""
        logger.info("Start syncing stock base information...")

        stats = {
            "total_processed": 0,
            "success_count": 0,
            "error_count": 0,
            "skipped_count": 0,
            "start_time": datetime.utcnow(),
            "errors": []
        }
        
        try:
            #1. Taking stock lists from Tushare
            stock_list = await self.provider.get_stock_list(market="CN")
            if not stock_list:
                logger.error("Could not close temporary folder: %s")
                return stats
            
            stats["total_processed"] = len(stock_list)
            logger.info(f"Other Organiser{len(stock_list)}Stock information only")

            #2. Batch processing
            for i in range(0, len(stock_list), self.batch_size):
                #Check for exit
                if job_id and await self._should_stop(job_id):
                    logger.warning(f"Mission{job_id}We've got a stop signal.")
                    stats["stopped"] = True
                    break

                batch = stock_list[i:i + self.batch_size]
                batch_stats = await self._process_basic_info_batch(batch, force_update)

                #Update statistics
                stats["success_count"] += batch_stats["success_count"]
                stats["error_count"] += batch_stats["error_count"]
                stats["skipped_count"] += batch_stats["skipped_count"]
                stats["errors"].extend(batch_stats["errors"])

                #Progress log and progress update
                progress = min(i + self.batch_size, len(stock_list))
                progress_percent = int((progress / len(stock_list)) * 100)
                logger.info(f"📈Sync progress of basic information:{progress}/{len(stock_list)} ({progress_percent}%) "
                           f"(success:{stats['success_count']}, Error:{stats['error_count']})")

                #Update Task Progress
                if job_id:
                    await self._update_progress(
                        job_id,
                        progress_percent,
                        f"已处理 {progress}/{len(stock_list)} 只股票"
                    )

                #API limit flow
                if i + self.batch_size < len(stock_list):
                    await asyncio.sleep(self.rate_limit_delay)
            
            #3. Completion of statistics
            stats["end_time"] = datetime.utcnow()
            stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()
            
            logger.info(f"✅Equal basic information synchronized:"
                       f"Total{stats['total_processed']}Only,"
                       f"Success{stats['success_count']}Only,"
                       f"Error{stats['error_count']}Only,"
                       f"Skip{stats['skipped_count']}Only,"
                       f"Time-consuming{stats['duration']:.2f}sec")
            
            return stats
            
        except Exception as e:
            logger.error(f"❌SystemSync failed:{e}")
            stats["errors"].append({"error": str(e), "context": "sync_stock_basic_info"})
            return stats
    
    async def _process_basic_info_batch(self, batch: List[Dict[str, Any]], force_update: bool) -> Dict[str, Any]:
        """Process basic information batch"""
        batch_stats = {
            "success_count": 0,
            "error_count": 0,
            "skipped_count": 0,
            "errors": []
        }
        
        for stock_info in batch:
            try:
                #Conversion to dictionary format (if Pydantic model)
                if hasattr(stock_info, 'model_dump'):
                    stock_data = stock_info.model_dump()
                elif hasattr(stock_info, 'dict'):
                    stock_data = stock_info.dict()
                else:
                    stock_data = stock_info

                code = stock_data["code"]

                #Check for updates
                if not force_update:
                    existing = await self.stock_service.get_stock_basic_info(code)
                    if existing:
                        #It's probably a Pydantic model that requires secure access to properties.
                        existing_dict = existing.model_dump() if hasattr(existing, 'model_dump') else (existing.dict() if hasattr(existing, 'dict') else existing)
                        if self._is_data_fresh(existing_dict.get("updated_at"), hours=24):
                            batch_stats["skipped_count"] += 1
                            continue

                #Update to database (specify data source as Tushare)
                success = await self.stock_service.update_stock_basic_info(code, stock_data, source="tushare")
                if success:
                    batch_stats["success_count"] += 1
                else:
                    batch_stats["error_count"] += 1
                    batch_stats["errors"].append({
                        "code": code,
                        "error": "数据库更新失败",
                        "context": "update_stock_basic_info"
                    })

            except Exception as e:
                batch_stats["error_count"] += 1
                #🔥 Secure access code
                try:
                    if hasattr(stock_info, 'code'):
                        code = stock_info.code
                    elif hasattr(stock_info, 'model_dump'):
                        code = stock_info.model_dump().get("code", "unknown")
                    elif hasattr(stock_info, 'dict'):
                        code = stock_info.dict().get("code", "unknown")
                    else:
                        code = stock_info.get("code", "unknown")
                except:
                    code = "unknown"

                batch_stats["errors"].append({
                    "code": code,
                    "error": str(e),
                    "context": "_process_basic_info_batch"
                })
        
        return batch_stats
    
    #== sync, corrected by elderman == @elder man
    
    async def sync_realtime_quotes(self, symbols: List[str] = None, force: bool = False) -> Dict[str, Any]:
        """Sync Real Time Line Data

Policy:
- Automatically switch to AKShare interface (avoid waste of Tushare rt k quota) if a small number of shares (1010) are specified
- One-time acquisition of a Tushare batch interface if a large number of shares or a full market are specified

Args:
symbols: specify a list of stock codes and synchronize all stocks if empty; if list of shares is specified, save only data on these stocks
force: enforcement ( Skip transaction time check), default False

Returns:
Sync Results Statistics
"""
        stats = {
            "total_processed": 0,
            "success_count": 0,
            "error_count": 0,
            "start_time": datetime.utcnow(),
            "errors": [],
            "stopped_by_rate_limit": False,
            "skipped_non_trading_time": False,
            "switched_to_akshare": False  #Switch to AKShare
        }

        try:
            #Check to see if the transaction is timed (to skip the check while manually synchronized)
            if not force and not self._is_trading_time():
                logger.info("⏸️ Not at current transaction time, skip real-time line sync (using force=True enforceable)")
                stats["skipped_non_trading_time"] = True
                return stats

            #🔥 strategy selection: small stock switching to AKShare, large stock or market-wide Tushare batch interface
            USE_AKSHARE_THRESHOLD = 10  #Switch to AKShare when less than or equal to 10 shares

            if symbols and len(symbols) <= USE_AKSHARE_THRESHOLD:
                #🔥 Automatically switch to AKShare (avoid waste of Tushare rt k quota, only 2 calls per hour)
                logger.info(
                    f"Number of shares{USE_AKSHARE_THRESHOLD}Only, automatically switch to AKShare interface"
                    f"(avoid waste of Tushare rt k quota, only 2 calls per hour)"
                )
                logger.info(f"Synchronise{len(symbols)}In real time:{symbols}")

                #Call AKShare Service
                from app.worker.akshare_sync_service import get_akshare_sync_service
                akshare_service = await get_akshare_sync_service()

                if not akshare_service:
                    logger.error("AKShare service is not available, back to Tushare batch interface")
                    #Back to Tushare Batch Interface
                    quotes_map = await self.provider.get_realtime_quotes_batch()
                    if quotes_map and symbols:
                        quotes_map = {symbol: quotes_map[symbol] for symbol in symbols if symbol in quotes_map}
                else:
                    #Sync with AKShare
                    akshare_result = await akshare_service.sync_realtime_quotes(
                        symbols=symbols,
                        force=force
                    )
                    stats["switched_to_akshare"] = True
                    stats["success_count"] = akshare_result.get("success_count", 0)
                    stats["error_count"] = akshare_result.get("error_count", 0)
                    stats["total_processed"] = akshare_result.get("total_processed", 0)
                    stats["errors"] = akshare_result.get("errors", [])
                    stats["end_time"] = datetime.utcnow()
                    stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

                    logger.info(
                        f"AKShare's real-time sync is complete:"
                        f"Total{stats['total_processed']}Only,"
                        f"Success{stats['success_count']}Only,"
                        f"Error{stats['error_count']}Only,"
                        f"Time-consuming{stats['duration']:.2f}sec"
                    )
                    return stats
            else:
                #A one-time market-wide situation using the Tushare batch interface
                if symbols:
                    logger.info(f"Sync with Tushare Batch Interface{len(symbols)}Real-time equity only (screened from market-wide data)")
                else:
                    logger.info("Synchronize market-wide real-time patterns with Tushare batch interfaces...")

                logger.info("📡 Call rrt k batch interface for market-wide real-time business...")
                quotes_map = await self.provider.get_realtime_quotes_batch()

                if not quotes_map:
                    logger.warning("No real-time status data obtained")
                    return stats

                logger.info(f"Other Organiser{len(quotes_map)}Real-time equity only.")

                #If the list of shares is specified, only these stocks are handled
                if symbols:
                    #Filter out specified shares
                    filtered_quotes_map = {symbol: quotes_map[symbol] for symbol in symbols if symbol in quotes_map}

                    #Check for stock not found
                    missing_symbols = [s for s in symbols if s not in quotes_map]
                    if missing_symbols:
                        logger.warning(f"The following stocks were not found in real-time situations:{missing_symbols}")

                    quotes_map = filtered_quotes_map
                    logger.info(f"Keep after filtering{len(quotes_map)}Only for stock")

            if not quotes_map:
                logger.warning("No real-time status data obtained")
                return stats

            stats["total_processed"] = len(quotes_map)

            #Batch to Database
            success_count = 0
            error_count = 0

            for symbol, quote_data in quotes_map.items():
                try:
                    #Save to Database
                    result = await self.stock_service.update_market_quotes(symbol, quote_data)
                    if result:
                        success_count += 1
                    else:
                        error_count += 1
                        stats["errors"].append({
                            "code": symbol,
                            "error": "更新数据库失败",
                            "context": "sync_realtime_quotes"
                        })
                except Exception as e:
                    error_count += 1
                    stats["errors"].append({
                        "code": symbol,
                        "error": str(e),
                        "context": "sync_realtime_quotes"
                    })

            stats["success_count"] = success_count
            stats["error_count"] = error_count

            #Completion of statistics
            stats["end_time"] = datetime.utcnow()
            stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

            logger.info(f"✅ Timeline sync completed:"
                      f"Total{stats['total_processed']}Only,"
                      f"Success{stats['success_count']}Only,"
                      f"Error{stats['error_count']}Only,"
                      f"Time-consuming{stats['duration']:.2f}sec")

            return stats

        except Exception as e:
            #Check for limit error
            error_msg = str(e)
            if self._is_rate_limit_error(error_msg):
                stats["stopped_by_rate_limit"] = True
                logger.error(f"Real-time line sync failed (API limit):{e}")
            else:
                logger.error(f"Real-time line sync failed:{e}")

            stats["errors"].append({"error": str(e), "context": "sync_realtime_quotes"})
            return stats

    #Abandoned: no more Tushare single interface (rt k only calls twice an hour, too valuable)
    #Automatically switch a small number of shares (≤10) to the AKShare interface
    # async def _get_quotes_individually(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    #     """
    #Retrieving real-time equities using single interfaces (disused)
    #
    #     Args:
    #symbols: list of stock codes
    #
    #     Returns:
    #         Dict[symbol, quote_data]
    #     """
    #     quotes_map = {}
    #
    #     for symbol in symbols:
    #         try:
    #             quote_data = await self.provider.get_stock_quotes(symbol)
    #             if quote_data:
    #                 quotes_map[symbol] = quote_data
    #(f"✅for   FMT 0 realtimelinesuccess")
    #             else:
    #logger.warning (f "⚠️ Unretributed Real-Time Line   FT 0>)
    #         except Exception as e:
    #Logger.error(f"❌ get FMT 0 realtimelinefailure: FMT 1"")
    #             continue
    #
    #logger.info (f "✅ single interface acquired successful   FMT 0/  FMT 1 1  only")
    #     return quotes_map

    async def _process_quotes_batch(self, batch: List[str]) -> Dict[str, Any]:
        """Processing line batches"""
        batch_stats = {
            "success_count": 0,
            "error_count": 0,
            "errors": [],
            "rate_limit_hit": False
        }

        #Also send to get movement data
        tasks = []
        for symbol in batch:
            task = self._get_and_save_quotes(symbol)
            tasks.append(task)

        #Waiting for all tasks to be completed
        results = await asyncio.gather(*tasks, return_exceptions=True)

        #Statistical results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                error_msg = str(result)
                batch_stats["error_count"] += 1
                batch_stats["errors"].append({
                    "code": batch[i],
                    "error": error_msg,
                    "context": "_process_quotes_batch"
                })

                #Test API limit error
                if self._is_rate_limit_error(error_msg):
                    batch_stats["rate_limit_hit"] = True
                    logger.warning(f"API limit error detected:{error_msg}")

            elif result:
                batch_stats["success_count"] += 1
            else:
                batch_stats["error_count"] += 1
                batch_stats["errors"].append({
                    "code": batch[i],
                    "error": "获取行情数据失败",
                    "context": "_process_quotes_batch"
                })

        return batch_stats

    def _is_rate_limit_error(self, error_msg: str) -> bool:
        """Test for API limit error"""
        rate_limit_keywords = [
            "每分钟最多访问",
            "每分钟最多",
            "rate limit",
            "too many requests",
            "访问频率",
            "请求过于频繁"
        ]
        error_msg_lower = error_msg.lower()
        return any(keyword in error_msg_lower for keyword in rate_limit_keywords)

    def _is_trading_time(self) -> bool:
        """Determines whether the current transaction time is
Unit A trading time:
Monday to Friday.
- 9.30-11.30 a.m.
- 15:00 to 15:00

Note: This method does not check holidays and only check periods
"""
        from datetime import datetime
        import pytz

        #Use Shanghai Time Zone
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)

        #Check for weekends.
        if now.weekday() >= 5:  #5 = Saturday, 6 = Sunday
            return False

        #Check period
        current_time = now.time()

        #Morning transactions: 9.30 - 11.30 a.m.
        morning_start = datetime.strptime("09:30", "%H:%M").time()
        morning_end = datetime.strptime("11:30", "%H:%M").time()

        #Afternoon to 13:15
        afternoon_start = datetime.strptime("13:00", "%H:%M").time()
        afternoon_end = datetime.strptime("15:00", "%H:%M").time()

        #To determine whether or not it's within the transaction time frame
        is_morning = morning_start <= current_time <= morning_end
        is_afternoon = afternoon_start <= current_time <= afternoon_end

        return is_morning or is_afternoon

    async def _get_and_save_quotes(self, symbol: str) -> bool:
        """Get and save individual stock lines"""
        try:
            quotes = await self.provider.get_stock_quotes(symbol)
            if quotes:
                #Convert to dictionary format (if Pydantic model)
                if hasattr(quotes, 'model_dump'):
                    quotes_data = quotes.model_dump()
                elif hasattr(quotes, 'dict'):
                    quotes_data = quotes.dict()
                else:
                    quotes_data = quotes

                return await self.stock_service.update_market_quotes(symbol, quotes_data)
            return False
        except Exception as e:
            error_msg = str(e)
            #Detecting flow-limit error. Throw it directly into the upper layer.
            if self._is_rate_limit_error(error_msg):
                logger.error(f"Access{symbol}Passage failed (restricted):{e}")
                raise  #Drop limit error
            logger.error(f"Access{symbol}Project failure:{e}")
            return False

    #== sync, corrected by elderman == @elder man

    async def sync_historical_data(
        self,
        symbols: List[str] = None,
        start_date: str = None,
        end_date: str = None,
        incremental: bool = True,
        all_history: bool = False,
        period: str = "daily",
        job_id: str = None
    ) -> Dict[str, Any]:
        """Sync Historical Data

Args:
symbols: list of stock codes
Start date: Start date
End date: End date
increment: Incremental sync
All history: Sync all historical data
period: data cycle (daily/weekly/montly)
job id: Task ID (for progress tracking)

Returns:
Sync Results Statistics
"""
        period_name = {"daily": "日线", "weekly": "周线", "monthly": "月线"}.get(period, period)
        logger.info(f"Synchronize{period_name}Historical Data...")

        stats = {
            "total_processed": 0,
            "success_count": 0,
            "error_count": 0,
            "total_records": 0,
            "start_time": datetime.utcnow(),
            "errors": []
        }

        try:
            #1. Taking stock lists (exclusion of re-marketed shares)
            if symbols is None:
                #Query all A stock (compatible with different data structures) and exclude de-marketed shares
                #Prioritize market info.market, downgrade to classgory field
                cursor = self.db.stock_basic_info.find(
                    {
                        "$and": [
                            {
                                "$or": [
                                    {"market_info.market": "CN"},  #New data structure
                                    {"category": "stock_cn"},      #Old data structure
                                    {"market": {"$in": ["主板", "创业板", "科创板", "北交所"]}}  #By market type
                                ]
                            },
                            #Dismissed shares
                            {
                                "$or": [
                                    {"status": {"$ne": "D"}},  #Status is not D.
                                    {"status": {"$exists": False}}  #Or the status field does not exist
                                ]
                            }
                        ]
                    },
                    {"code": 1}
                )
                symbols = [doc["code"] async for doc in cursor]
                logger.info(f"From stock basic info{len(symbols)}Stock only (releases excluded)")

            stats["total_processed"] = len(symbols)

            #2. Determination of global end date
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')

            #3. Determination of global start date (for log display only)
            global_start_date = start_date
            if not global_start_date:
                if all_history:
                    global_start_date = "1990-01-01"
                elif incremental:
                    global_start_date = "各股票最后日期"
                else:
                    global_start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

            logger.info(f"Historical data sync: End date={end_date}, stock ={len(symbols)}mode ={'Incremental' if incremental else 'Full'}")

            #4. Batch processing
            for i, symbol in enumerate(symbols):
                #Record single stock start time
                stock_start_time = datetime.now()

                try:
                    #Check for exit
                    if job_id and await self._should_stop(job_id):
                        logger.warning(f"Mission{job_id}We've got a stop signal.")
                        stats["stopped"] = True
                        break

                    #Rate limit
                    await self.rate_limiter.acquire()

                    #Determine the start date of the stock
                    symbol_start_date = start_date
                    if not symbol_start_date:
                        if all_history:
                            symbol_start_date = "1990-01-01"
                        elif incremental:
                            #Incremental sync: due date for acquisition of the stock
                            symbol_start_date = await self._get_last_sync_date(symbol)
                            logger.debug(f"📅 {symbol}From:{symbol_start_date}Start Synchronization")
                        else:
                            symbol_start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

                    #Record requested parameters
                    logger.debug(
                        f"🔍 {symbol}: Request{period_name}Data"
                        f"start={symbol_start_date}, end={end_date}, period={period}"
                    )

                    #Performance monitor: API call
                    api_start = datetime.now()
                    df = await self.provider.get_historical_data(symbol, symbol_start_date, end_date, period=period)
                    api_duration = (datetime.now() - api_start).total_seconds()

                    if df is not None and not df.empty:
                        #Performance monitoring: data preservation
                        save_start = datetime.now()
                        records_saved = await self._save_historical_data(symbol, df, period=period)
                        save_duration = (datetime.now() - save_start).total_seconds()

                        stats["success_count"] += 1
                        stats["total_records"] += records_saved

                        #Time-consuming calculation of individual stocks
                        stock_duration = (datetime.now() - stock_start_time).total_seconds()
                        logger.info(
                            f"✅ {symbol}: Save{records_saved}Article{period_name}Records,"
                            f"Total time-consuming{stock_duration:.2f}sec"
                            f"(API: {api_duration:.2f}Seconds, save:{save_duration:.2f}sec)"
                        )
                    else:
                        stock_duration = (datetime.now() - stock_start_time).total_seconds()
                        logger.warning(
                            f"⚠️ {symbol}: None{period_name}Data"
                            f"(start={symbol_start_date}, end={end_date}) Time-consuming{stock_duration:.2f}sec"
                        )

                    #Every stock update.
                    progress_percent = int(((i + 1) / len(symbols)) * 100)

                    #Update Task Progress
                    if job_id:
                        await self._update_progress(
                            job_id,
                            progress_percent,
                            f"正在同步 {symbol} ({i + 1}/{len(symbols)})"
                        )

                    #A detailed log for every 50 stocks
                    if (i + 1) % 50 == 0 or (i + 1) == len(symbols):
                        logger.info(f"📈 {period_name}Data Sync Progress:{i + 1}/{len(symbols)} ({progress_percent}%) "
                                   f"(success:{stats['success_count']}, Records:{stats['total_records']})")

                        #Output Rate Limiter Statistics
                        limiter_stats = self.rate_limiter.get_stats()
                        logger.info(f"Speed limit:{limiter_stats['current_calls']}/{limiter_stats['max_calls']}I don't know."
                                   f"Waiting:{limiter_stats['total_waits']}, "
                                   f"Total waiting time:{limiter_stats['total_wait_time']:.1f}sec")

                except Exception as e:
                    import traceback
                    error_details = traceback.format_exc()
                    stats["error_count"] += 1
                    stats["errors"].append({
                        "code": symbol,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "context": f"sync_historical_data_{period}",
                        "traceback": error_details
                    })
                    logger.error(
                        f"❌ {symbol} {period_name}Synchronising data failed\n"
                        f"Parameter: start={symbol_start_date if 'symbol_start_date' in locals() else 'N/A'}, "
                        f"end={end_date}, period={period}\n"
                        f"Error type:{type(e).__name__}\n"
                        f"Cannot initialise Evolution's mail component.{str(e)}\n"
                        f"Stack tracking: \n{error_details}"
                    )

            #4. Completion of statistics
            stats["end_time"] = datetime.utcnow()
            stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

            logger.info(f"✅ {period_name}Data sync complete:"
                       f"Equities{stats['success_count']}/{stats['total_processed']}, "
                       f"Records{stats['total_records']}Article,"
                       f"Error{stats['error_count']}Yeah."
                       f"Time-consuming{stats['duration']:.2f}sec")

            return stats

        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            logger.error(
                f"❌History Data Synchronization Failed (Four Layer Abnormal)\n"
                f"Error type:{type(e).__name__}\n"
                f"Cannot initialise Evolution's mail component.{str(e)}\n"
                f"Stack tracking: \n{error_details}"
            )
            stats["errors"].append({
                "error": str(e),
                "error_type": type(e).__name__,
                "context": "sync_historical_data",
                "traceback": error_details
            })
            return stats

    async def _save_historical_data(self, symbol: str, df, period: str = "daily") -> int:
        """Save historical data to database"""
        try:
            if self.historical_service is None:
                self.historical_service = await get_historical_data_service()

            #Save with Unified Historical Data Service (specify cycle)
            saved_count = await self.historical_service.save_historical_data(
                symbol=symbol,
                data=df,
                data_source="tushare",
                market="CN",
                period=period
            )

            return saved_count

        except Exception as e:
            logger.error(f"Save{period}Data Failed{symbol}: {e}")
            return 0

    async def _get_last_sync_date(self, symbol: str = None) -> str:
        """Get Last Sync Date

Args:
symbol: stock code, due date to return the stock if provided + 1 day

Returns:
Date string (YYYY-MM-DD)
"""
        try:
            if self.historical_service is None:
                self.historical_service = await get_historical_data_service()

            if symbol:
                #Recent date of acquisition of specific stocks
                latest_date = await self.historical_service.get_latest_date(symbol, "tushare")
                if latest_date:
                    #Return to the next day of the final date (duplicate)
                    try:
                        last_date_obj = datetime.strptime(latest_date, '%Y-%m-%d')
                        next_date = last_date_obj + timedelta(days=1)
                        return next_date.strftime('%Y-%m-%d')
                    except:
                        #If the date is not formatted correctly, return directly
                        return latest_date
                else:
                    #Full sync from listing date when no historical data are available
                    stock_info = await self.db.stock_basic_info.find_one(
                        {"code": symbol},
                        {"list_date": 1}
                    )
                    if stock_info and stock_info.get("list_date"):
                        list_date = stock_info["list_date"]
                        #Deal with different date formats
                        if isinstance(list_date, str):
                            #The format could be "201001011" or "2010-01-01."
                            if len(list_date) == 8 and list_date.isdigit():
                                return f"{list_date[:4]}-{list_date[4:6]}-{list_date[6:]}"
                            else:
                                return list_date
                        else:
                            return list_date.strftime('%Y-%m-%d')

                    #If no listing date, starting in 1990
                    logger.warning(f"⚠️ {symbol}: No listing date found, synchronized from 1990-01-01")
                    return "1990-01-01"

            #Default returns 30 days ago (ensure that data are not missing)
            return (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{symbol}: {e}")
            #Returns 30 days before error to ensure that data is not missing
            return (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    #== sync, corrected by elderman == @elder man

    async def sync_financial_data(self, symbols: List[str] = None, limit: int = 20, job_id: str = None) -> Dict[str, Any]:
        """Sync Financial Data

Args:
Symbols: list of stock codes. None means sync all stocks
Limited: Obtain financial reporting periods, default 20 issues (approximately 5 years of data)
job id: Task ID (for progress tracking)
"""
        logger.info(f"Synchronization of financial data (access to latest){limit}Period")

        stats = {
            "total_processed": 0,
            "success_count": 0,
            "error_count": 0,
            "start_time": datetime.utcnow(),
            "errors": []
        }

        try:
            #Get Stock List
            if symbols is None:
                cursor = self.db.stock_basic_info.find(
                    {
                        "$or": [
                            {"market_info.market": "CN"},  #New data structure
                            {"category": "stock_cn"},      #Old data structure
                            {"market": {"$in": ["主板", "创业板", "科创板", "北交所"]}}  #By market type
                        ]
                    },
                    {"code": 1}
                )
                symbols = [doc["code"] async for doc in cursor]
                logger.info(f"From stock basic info{len(symbols)}Only stocks")

            stats["total_processed"] = len(symbols)
            logger.info(f"We need to sync.{len(symbols)}Equities only")

            #Batch processing
            for i, symbol in enumerate(symbols):
                try:
                    #Rate limit
                    await self.rate_limiter.acquire()

                    #Access to financial data (described acquisition periods)
                    financial_data = await self.provider.get_financial_data(symbol, limit=limit)

                    if financial_data:
                        #Keep financial data
                        success = await self._save_financial_data(symbol, financial_data)
                        if success:
                            stats["success_count"] += 1
                        else:
                            stats["error_count"] += 1
                    else:
                        logger.warning(f"⚠️ {symbol}: No financial data available")

                    #Progress log and progress tracking
                    if (i + 1) % 20 == 0:
                        progress = int((i + 1) / len(symbols) * 100)
                        logger.info(f"Synchronization of financial data:{i + 1}/{len(symbols)} ({progress}%) "
                                   f"(success:{stats['success_count']}, Error:{stats['error_count']})")
                        #Output Rate Limiter Statistics
                        limiter_stats = self.rate_limiter.get_stats()
                        logger.info(f"Speed limit:{limiter_stats['current_calls']}/{limiter_stats['max_calls']}Minor")

                        #Update Task Progress
                        if job_id:
                            from app.services.scheduler_service import update_job_progress, TaskCancelledException
                            try:
                                await update_job_progress(
                                    job_id=job_id,
                                    progress=progress,
                                    message=f"正在同步 {symbol} 财务数据",
                                    current_item=symbol,
                                    total_items=len(symbols),
                                    processed_items=i + 1
                                )
                            except TaskCancelledException:
                                #Mission cancelled, recorded and withdrawn
                                logger.warning(f"⚠️ Financial Data Synchronization Job Canceled by User (processed{i + 1}/{len(symbols)})")
                                stats["end_time"] = datetime.utcnow()
                                stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()
                                stats["cancelled"] = True
                                raise

                except Exception as e:
                    stats["error_count"] += 1
                    stats["errors"].append({
                        "code": symbol,
                        "error": str(e),
                        "context": "sync_financial_data"
                    })
                    logger.error(f"❌ {symbol}Could not close temporary folder: %s{e}")

            #Completion of statistics
            stats["end_time"] = datetime.utcnow()
            stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

            logger.info(f"Synchronization of financial data:"
                       f"Success{stats['success_count']}/{stats['total_processed']}, "
                       f"Error{stats['error_count']}Yeah."
                       f"Time-consuming{stats['duration']:.2f}sec")

            return stats

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            stats["errors"].append({"error": str(e), "context": "sync_financial_data"})
            return stats

    async def _save_financial_data(self, symbol: str, financial_data: Dict[str, Any]) -> bool:
        """Keep financial data"""
        try:
            #Use of harmonized financial data services
            from app.services.financial_data_service import get_financial_data_service

            financial_service = await get_financial_data_service()

            #Keep financial data
            saved_count = await financial_service.save_financial_data(
                symbol=symbol,
                financial_data=financial_data,
                data_source="tushare",
                market="CN",
                report_period=financial_data.get("report_period"),
                report_type=financial_data.get("report_type", "quarterly")
            )

            return saved_count > 0

        except Exception as e:
            logger.error(f"Save{symbol}Financial data failed:{e}")
            return False

    #== sync, corrected by elderman == @elder man

    def _is_data_fresh(self, updated_at: datetime, hours: int = 24) -> bool:
        """Check if the data is fresh."""
        if not updated_at:
            return False

        threshold = datetime.utcnow() - timedelta(hours=hours)
        return updated_at > threshold

    async def get_sync_status(self) -> Dict[str, Any]:
        """Get Sync Status"""
        try:
            #Statistics of the amount of data collected
            basic_info_count = await self.db.stock_basic_info.count_documents({})
            quotes_count = await self.db.market_quotes.count_documents({})

            #Get Update Time
            latest_basic = await self.db.stock_basic_info.find_one(
                {},
                sort=[("updated_at", -1)]
            )
            latest_quotes = await self.db.market_quotes.find_one(
                {},
                sort=[("updated_at", -1)]
            )

            return {
                "provider_connected": self.provider.is_available(),
                "collections": {
                    "stock_basic_info": {
                        "count": basic_info_count,
                        "latest_update": latest_basic.get("updated_at") if (latest_basic and isinstance(latest_basic, dict)) else None
                    },
                    "market_quotes": {
                        "count": quotes_count,
                        "latest_update": latest_quotes.get("updated_at") if (latest_quotes and isinstance(latest_quotes, dict)) else None
                    }
                },
                "status_time": datetime.utcnow()
            }

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return {"error": str(e)}

    #== sync, corrected by elderman == @elder man

    async def sync_news_data(
        self,
        symbols: List[str] = None,
        hours_back: int = 24,
        max_news_per_stock: int = 20,
        force_update: bool = False,
        job_id: str = None
    ) -> Dict[str, Any]:
        """Sync News Data

Args:
symbols: list of stock codes to capture all stocks on the net
Hours back: Backtrace hours, default 24 hours
Max news per stock: Maximum number of news per stock
Force update
job id: Task ID (for progress tracking)

Returns:
Sync Results Statistics
"""
        logger.info("Commencing news data...")

        stats = {
            "total_processed": 0,
            "success_count": 0,
            "error_count": 0,
            "news_count": 0,
            "start_time": datetime.utcnow(),
            "errors": []
        }

        try:
            #1. Taking stock lists
            if symbols is None:
                stock_list = await self.stock_service.get_all_stocks()
                symbols = [stock["code"] for stock in stock_list]

            if not symbols:
                logger.warning("No shares have been found that need to synchronize news.")
                return stats

            stats["total_processed"] = len(symbols)
            logger.info(f"We need to sync.{len(symbols)}Only stock news.")

            #2. Batch processing
            for i in range(0, len(symbols), self.batch_size):
                #Check for exit
                if job_id and await self._should_stop(job_id):
                    logger.warning(f"Mission{job_id}We've got a stop signal.")
                    stats["stopped"] = True
                    break

                batch = symbols[i:i + self.batch_size]
                batch_stats = await self._process_news_batch(
                    batch, hours_back, max_news_per_stock
                )

                #Update statistics
                stats["success_count"] += batch_stats["success_count"]
                stats["error_count"] += batch_stats["error_count"]
                stats["news_count"] += batch_stats["news_count"]
                stats["errors"].extend(batch_stats["errors"])

                #Progress log and progress update
                progress = min(i + self.batch_size, len(symbols))
                progress_percent = int((progress / len(symbols)) * 100)
                logger.info(f"NewsSync:{progress}/{len(symbols)} ({progress_percent}%) "
                           f"(success:{stats['success_count']}News:{stats['news_count']})")

                #Update Task Progress
                if job_id:
                    await self._update_progress(
                        job_id,
                        progress_percent,
                        f"已处理 {progress}/{len(symbols)} 只股票，获取 {stats['news_count']} 条新闻"
                    )

                #API limit flow
                if i + self.batch_size < len(symbols):
                    await asyncio.sleep(self.rate_limit_delay)

            #3. Completion of statistics
            stats["end_time"] = datetime.utcnow()
            stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

            logger.info(f"Synchronization of news data:"
                       f"Total{stats['total_processed']}It's just stocks."
                       f"Success{stats['success_count']}Only,"
                       f"Access{stats['news_count']}The news,"
                       f"Error{stats['error_count']}Only,"
                       f"Time-consuming{stats['duration']:.2f}sec")

            return stats

        except Exception as e:
            logger.error(f"News data sync failed:{e}")
            stats["errors"].append({"error": str(e), "context": "sync_news_data"})
            return stats

    async def _process_news_batch(
        self,
        batch: List[str],
        hours_back: int,
        max_news_per_stock: int
    ) -> Dict[str, Any]:
        """Processing of news batches"""
        batch_stats = {
            "success_count": 0,
            "error_count": 0,
            "news_count": 0,
            "errors": []
        }

        for symbol in batch:
            try:
                #Get news data from Tushare
                news_data = await self.provider.get_stock_news(
                    symbol=symbol,
                    limit=max_news_per_stock,
                    hours_back=hours_back
                )

                if news_data:
                    #Preservation of news data
                    saved_count = await self.news_service.save_news_data(
                        news_data=news_data,
                        data_source="tushare",
                        market="CN"
                    )

                    batch_stats["success_count"] += 1
                    batch_stats["news_count"] += saved_count

                    logger.debug(f"✅ {symbol}News Synchronization Success:{saved_count}Article")
                else:
                    logger.debug(f"⚠️ {symbol}No news data obtained")
                    batch_stats["success_count"] += 1  #It's a success without news.

                #🔥API limit flow: successful hibernation
                await asyncio.sleep(0.2)

            except Exception as e:
                batch_stats["error_count"] += 1
                error_msg = f"{symbol}: {str(e)}"
                batch_stats["errors"].append(error_msg)
                logger.error(f"❌ {symbol}News Synchronisation Failed:{e}")

                #And when you fail, you're going to sleep.
                #Longer hibernation in failure, giving the API server a chance to recover
                await asyncio.sleep(1.0)

        return batch_stats

    #== sync, corrected by elderman == @elder man

    async def _should_stop(self, job_id: str) -> bool:
        """Check if the mission should stop.

Args:
Job id: Task ID

Returns:
Should it stop?
"""
        try:
            #Query execution records, check cancer requested tags
            execution = await self.db.scheduler_executions.find_one(
                {"job_id": job_id, "status": "running"},
                sort=[("timestamp", -1)]
            )

            if execution and execution.get("cancel_requested"):
                return True

            return False

        except Exception as e:
            logger.error(f"Check mission stop tag failed:{e}")
            return False

    async def _update_progress(self, job_id: str, progress: int, message: str):
        """Update Task Progress

Args:
Job id: Task ID
Progress: percentage (0-100)
message: progress message
"""
        try:
            from app.services.scheduler_service import TaskCancelledException
            from pymongo import MongoClient
            from app.core.config import settings

            logger.info(f"[Progress update]{job_id}Progress:{progress}% - {message}")

            #Use sync PyMongo client (avoiding event cycle conflicts)
            sync_client = MongoClient(settings.MONGO_URI)
            sync_db = sync_client[settings.MONGODB_DATABASE]

            #Find the latest running record
            execution = sync_db.scheduler_executions.find_one(
                {"job_id": job_id, "status": "running"},
                sort=[("timestamp", -1)]
            )

            if not execution:
                logger.warning(f"No job found.{job_id}Implementation records")
                sync_client.close()
                return

            logger.info(f"[Progress update]{execution['_id']}, current progress ={execution.get('progress', 0)}%")

            #Check for cancellation requests.
            if execution.get("cancel_requested"):
                sync_client.close()
                raise TaskCancelledException(f"任务 {job_id} 已被用户取消")

            #Update progress (using UTC+8 time)
            result = sync_db.scheduler_executions.update_one(
                {"_id": execution["_id"]},
                {
                    "$set": {
                        "progress": progress,
                        "progress_message": message,
                        "updated_at": get_utc8_now()
                    }
                }
            )

            logger.info(f"📊{result.matched_count}, modified={result.modified_count}")

            sync_client.close()
            logger.info(f"Mission{job_id}Progress update successful:{progress}% - {message}")

        except Exception as e:
            if "TaskCancelledException" in str(type(e).__name__):
                raise
            logger.error(f"The mission has failed:{e}", exc_info=True)


#Examples of global sync services
_tushare_sync_service = None

async def get_tushare_sync_service() -> TushareSyncService:
    """Get instance of Tushare sync service"""
    global _tushare_sync_service
    if _tushare_sync_service is None:
        _tushare_sync_service = TushareSyncService()
        await _tushare_sync_service.initialize()
    return _tushare_sync_service


#Task Functions compatible with APSscheduler
async def run_tushare_basic_info_sync(force_update: bool = False):
    """APScheduler mission: Synchronizing basic stock information"""
    try:
        service = await get_tushare_sync_service()
        result = await service.sync_stock_basic_info(force_update, job_id="tushare_basic_info_sync")
        logger.info(f"✅Tushare base information synchronised:{result}")
        return result
    except Exception as e:
        logger.error(f"Tushare failed to synchronise basic information:{e}")
        raise


async def run_tushare_quotes_sync(force: bool = False):
    """APSscheduler mission: Sync real-time patterns

Args:
force: enforcement ( Skip transaction time check), default False
"""
    try:
        service = await get_tushare_sync_service()
        result = await service.sync_realtime_quotes(force=force)
        logger.info(f"The Tushare line is completed:{result}")
        return result
    except Exception as e:
        logger.error(f"Tushare lines failed:{e}")
        raise


async def run_tushare_historical_sync(incremental: bool = True):
    """APScheduler: Synchronizing historical data"""
    logger.info(f"[APScheduler]{incremental})")
    try:
        service = await get_tushare_sync_service()
        logger.info(f"[APScheduler] Tushare Synchronization Service Initialized")
        result = await service.sync_historical_data(incremental=incremental, job_id="tushare_historical_sync")
        logger.info(f"[APScheduler] Tushare history data synchronised:{result}")
        return result
    except Exception as e:
        logger.error(f"[APScheduler] Tushare's historical data sync failed:{e}")
        import traceback
        logger.error(f"Detailed error:{traceback.format_exc()}")
        raise


async def run_tushare_financial_sync():
    """APSscheduler mission: Synchronization of financial data (access to the latest 20 issues, approximately 5 years)"""
    try:
        service = await get_tushare_sync_service()
        result = await service.sync_financial_data(limit=20, job_id="tushare_financial_sync")  #Last 20 issues (approximately 5 years)
        logger.info(f"✅Tushare ' s financial data are synchronized:{result}")
        return result
    except Exception as e:
        logger.error(f"Tushare's financial data synchronised failed:{e}")
        raise


async def run_tushare_status_check():
    """APScheduler mission: check for synchronization"""
    try:
        service = await get_tushare_sync_service()
        result = await service.get_sync_status()
        logger.info(f"Tushare status check complete:{result}")
        return result
    except Exception as e:
        logger.error(f"Tushare state check failed:{e}")
        return {"error": str(e)}


async def run_tushare_news_sync(hours_back: int = 24, max_news_per_stock: int = 20):
    """APSscheduler mission: Synchronizing news data"""
    try:
        service = await get_tushare_sync_service()
        result = await service.sync_news_data(
            hours_back=hours_back,
            max_news_per_stock=max_news_per_stock,
            job_id="tushare_news_sync"
        )
        logger.info(f"Tushare news synchronised:{result}")
        return result
    except Exception as e:
        logger.error(f"Tushare NewsSync failed:{e}")
        raise
