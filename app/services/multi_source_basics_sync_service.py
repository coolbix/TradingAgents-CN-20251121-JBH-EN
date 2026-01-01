"""
Multi-source stock basics synchronization service
- Supports multiple data sources with fallback mechanism
- Priority: Tushare > AKShare > BaoStock 
- Fetches A-share stock basic info with extended financial metrics
- Upserts into MongoDB collection `stock_basic_info`
- Provides unified interface for different data sources
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne

from app.core.database import get_mongo_db
from app.services.basics_sync import add_financial_metrics as _add_financial_metrics_util


logger = logging.getLogger(__name__)

# Collection names
COLLECTION_NAME = "stock_basic_info"
STATUS_COLLECTION = "sync_status"
JOB_KEY = "stock_basics_multi_source"


class DataSourcePriority(Enum):
    """Data source priority count"""
    TUSHARE = 1
    AKSHARE = 2
    BAOSTOCK = 3


@dataclass
class SyncStats:
    """Sync Statistical Information"""
    job: str = JOB_KEY
    data_type: str = "stock_basics"  #Add a Data type field to meet database index requirements
    status: str = "idle"
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    total: int = 0
    inserted: int = 0
    updated: int = 0
    errors: int = 0
    last_trade_date: Optional[str] = None
    data_sources_used: List[str] = field(default_factory=list)
    source_stats: Dict[str, Dict[str, int]] = field(default_factory=dict)
    message: Optional[str] = None


class MultiSourceBasicsSyncService:
    """Multi Source (Tushare, AKShare, BaoStock) China Stock Basic Information Synchronization Service"""
    #Stock Basic Information: code, name, area, industry, market, list_date, sse, full_symbol, category, source, updated_at

    def __init__(self):
        self._lock = asyncio.Lock()
        self._running = False
        self._last_status: Optional[Dict[str, Any]] = None

    async def get_status(self) -> Dict[str, Any]:
        """Get Sync Status"""
        if self._last_status:
            return self._last_status

        db = get_mongo_db()
        doc = await db[STATUS_COLLECTION].find_one({"job": JOB_KEY})
        if doc:
            #Remove the  id field of MongoDB to avoid serialization problems
            doc.pop("_id", None)
            return doc
        return {"job": JOB_KEY, "status": "never_run"}

    async def _persist_status(self, db: AsyncIOMotorDatabase, stats: Dict[str, Any]) -> None:
        """Sustained Sync Status"""
        stats["job"] = JOB_KEY

        #Use upset to avoid errors
        #Update or insert based on data  type and job
        filter_query = {
            "data_type": stats.get("data_type", "stock_basics"),
            "job": JOB_KEY
        }

        await db[STATUS_COLLECTION].update_one(
            filter_query,
            {"$set": stats},
            upsert=True
        )

        self._last_status = {k: v for k, v in stats.items() if k != "_id"}

    async def _execute_bulk_write_with_retry(
        self,
        db: AsyncIOMotorDatabase,
        operations: List,
        max_retries: int = 3
    ) -> Tuple[int, int]:
        """Implementation batch writing with retry mechanism

Args:
db: Example of MongoDB database
Organisations: Batch Operations List
max retries: maximum number of retries

Returns:
(Add, Update)
"""
        inserted = 0
        updated = 0
        retry_count = 0

        while retry_count < max_retries:
            try:
                result = await db[COLLECTION_NAME].bulk_write(operations, ordered=False)
                inserted = result.upserted_count
                updated = result.modified_count
                logger.debug(f"Bulk writing success: Add{inserted}Update{updated}")
                return inserted, updated

            except asyncio.TimeoutError as e:
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 2 ** retry_count  #Index retreat: 2 seconds, 4 seconds, 8 seconds
                    logger.warning(f"⚠️ Bulk writing timeout (no.{retry_count}Try again, wait{wait_time}Try again in seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Failed to write batch ❌. Try again{max_retries}Times:{e}")
                    return 0, 0

            except Exception as e:
                logger.error(f"Batch writing failed:{e}")
                return 0, 0

        return inserted, updated

    async def run_full_sync(self, force: bool = False, preferred_sources: List[str] = None) -> Dict[str, Any]:
        """Run Full Sync
        Args:
        force: whether to enforce (even if already in operation)
        Prefered sources: Priority list of data sources
        """

        async with self._lock:
            """
            The lock is there to prevent two overlapping sync runs from trampling each other.
            In MultiSourceBasicsSyncService.__init__, self._lock = asyncio.Lock() is used in run_full_sync to serialize access to the _running flag and ensure only one sync starts at a time.
            
            How it’s used:
            At the start of run_full_sync, it does async with self._lock: and checks _running.
            If a sync is already running and force is false, it returns the current status.
            If not running, it sets _running = True under the lock so no other coroutine can slip in and start another run.
            In the finally: block, it again uses async with self._lock: to set _running = False, ensuring the running flag is reset safely when the job finishes or fails.
            Net effect: concurrent callers of run_full_sync won’t trigger duplicate work or race on _running; they’ll either wait or get a “already running” response.
            """
            if self._running and not force:
                logger.info("Multi-source stock basics sync already running; skip start")
                return await self.get_status()
            self._running = True

        db = get_mongo_db()
        stats = SyncStats()
        stats.started_at = datetime.now().isoformat()
        stats.status = "running"
        await self._persist_status(db, stats.__dict__.copy())

        try:
            #Step 1: Access data source manager
            from app.services.data_sources.manager import DataSourceManager
            manager = DataSourceManager()
            available_adapters = manager.get_available_adapters()

            if not available_adapters:
                raise RuntimeError("No available data sources found")

            logger.info(f"Available data sources: {[adapter.name for adapter in available_adapters]}")

            #Log if priority data source is specified
            if preferred_sources:
                logger.info(f"Using preferred data sources: {preferred_sources}")

            #Step 2: Try to retrieve the list of shares from data sources
            stock_df, source_used = await asyncio.to_thread(
                manager.get_stock_list_with_fallback, preferred_sources
            )
            if stock_df is None or getattr(stock_df, "empty", True):
                raise RuntimeError("All data sources failed to provide stock list")

            stats.data_sources_used.append(f"stock_list:{source_used}")
            logger.info(f"Successfully fetched {len(stock_df)} stocks from {source_used}")

            #Step 3: Obtain updated transaction dates and financial data
            latest_trade_date = await asyncio.to_thread(
                manager.find_latest_trade_date_with_fallback, preferred_sources
            )
            stats.last_trade_date = latest_trade_date

            daily_data_map = {}
            daily_source = ""
            if latest_trade_date:
                daily_df, daily_source = await asyncio.to_thread(
                    manager.get_daily_basic_with_fallback, latest_trade_date, preferred_sources
                )
                if daily_df is not None and not daily_df.empty:
                    for _, row in daily_df.iterrows():
                        ts_code = row.get("ts_code")
                        if ts_code:
                            daily_data_map[ts_code] = row.to_dict()
                    stats.data_sources_used.append(f"daily_data:{daily_source}")

            #Step 5: Processing and updating of data (various processing)
            ops = []
            inserted = updated = errors = 0
            batch_size = 500  #500 shares per batch to avoid time overruns
            total_stocks = len(stock_df)

            logger.info(f"Let's go.{total_stocks}Stock only, data source:{source_used}")

            for idx, (_, row) in enumerate(stock_df.iterrows(), 1):
                try:
                    #Extract Basic Information
                    name = row.get("name") or ""
                    area = row.get("area") or ""
                    industry = row.get("industry") or ""
                    market = row.get("market") or ""
                    list_date = row.get("list_date") or ""
                    ts_code = row.get("ts_code") or ""

                    #Extract 6-bit stock code.
                    if isinstance(ts_code, str) and "." in ts_code:
                        code = ts_code.split(".")[0]
                    else:
                        symbol = row.get("symbol") or ""
                        code = str(symbol).zfill(6) if symbol else ""

                    #Based on ts code
                    if isinstance(ts_code, str):
                        if ts_code.endswith(".SH"):
                            sse = "上海证券交易所"
                        elif ts_code.endswith(".SZ"):
                            sse = "深圳证券交易所"
                        elif ts_code.endswith(".BJ"):
                            sse = "北京证券交易所"
                        else:
                            sse = "未知"
                    else:
                        sse = "未知"

                    category = "stock_cn"

                    #Access to financial data
                    daily_metrics = {}
                    if isinstance(ts_code, str) and ts_code in daily_data_map:
                        daily_metrics = daily_data_map[ts_code]

                    #Generate full symbol
                    full_symbol = ts_code if ts_code else self._generate_full_symbol(code)

                    #Identification of data sources
                    #Set the source field according to the actual data source used
                    #Note: no longer using "multi source" as default, there must be clear data Source
                    if not source_used:
                        logger.warning(f"Equities{code}No clear data source, skip")
                        errors += 1
                        continue
                    data_source = source_used

                    #Build Document
                    doc = {
                        "code": code,
                        "symbol": code,  #Add symbol field (standardized field)
                        "name": name,
                        "area": area,
                        "industry": industry,
                        "market": market,
                        "list_date": list_date,
                        "sse": sse,
                        "full_symbol": full_symbol,  #Add full symbol field
                        "category": category,
                        "source": data_source,  #Using actual data sources 🔥
                        "updated_at": datetime.now(),
                    }

                    #Add Financial Indicators
                    self._add_financial_metrics(doc, daily_metrics)

                    #Use (code, source) of joint query conditions
                    ops.append(UpdateOne({"code": code, "source": data_source}, {"$set": doc}, upsert=True))

                except Exception as e:
                    logger.error(f"Error processing stock {row.get('ts_code', 'unknown')}: {e}")
                    errors += 1

                #🔥 Phased database operation
                if len(ops) >= batch_size or idx == total_stocks:
                    if ops:
                        progress_pct = (idx / total_stocks) * 100
                        logger.info(f"The execution batch 📝 reads:{len(ops)}Article record(){idx}/{total_stocks}, {progress_pct:.1f}%)")

                        batch_inserted, batch_updated = await self._execute_bulk_write_with_retry(db, ops)

                        if batch_inserted > 0 or batch_updated > 0:
                            inserted += batch_inserted
                            updated += batch_updated
                            logger.info(f"Volume completed:{batch_inserted}Update{batch_updated}Cumulative: Add{inserted}Update{updated}, Error{errors}")
                        else:
                            errors += len(ops)
                            logger.warning(f"Batch writing failed, tag{len(ops)}Record as error")

                        ops = []  #Empty Operation List

            #Step 7: Update statistical information
            stats.total = total_stocks  #Use of total stocks
            stats.inserted = inserted
            stats.updated = updated
            stats.errors = errors
            stats.status = "success" if errors == 0 else "success_with_errors"
            stats.finished_at = datetime.now().isoformat()

            await self._persist_status(db, stats.__dict__.copy())
            logger.info(
                f"✅ Multi-source sync finished: total={stats.total} inserted={inserted} "
                f"updated={updated} errors={errors} sources={stats.data_sources_used}"
            )
            return stats.__dict__

        except Exception as e:
            stats.status = "failed"
            stats.message = str(e)
            stats.finished_at = datetime.now().isoformat()
            await self._persist_status(db, stats.__dict__.copy())
            logger.exception(f"Multi-source sync failed: {e}")
            return stats.__dict__
        finally:
            async with self._lock:
                self._running = False



    def _add_financial_metrics(self, doc: Dict, daily_metrics: Dict) -> None:
        """Entrusted to Basics sync.procing.add financial medias"""
        return _add_financial_metrics_util(doc, daily_metrics)

    def _generate_full_symbol(self, code: str) -> str:
        """Generate full standard code by stock code

Args:
code: 6-bit stock code

Returns:
Full standardized code, return original code if unidentifiable (ensure not to be empty)
"""
        #Make sure the code isn't empty.
        if not code:
            return ""

        #Standardise as string and remove spaces
        code = str(code).strip()

        #If length is not 6, return original code
        if len(code) != 6:
            return code

        #By prefixing the exchange
        if code.startswith(('60', '68', '90')):  #Shanghai Stock Exchange
            return f"{code}.SS"
        elif code.startswith(('00', '30', '20')):  #Shenzhen Stock Exchange
            return f"{code}.SZ"
        elif code.startswith(('8', '4')):  #Beijing Stock Exchange
            return f"{code}.BJ"
        else:
            #Unidentifiable code, return original code (ensure not to be empty)
            return code if code else ""


#Examples of global services
_multi_source_sync_service = None

def get_multi_source_sync_service() -> MultiSourceBasicsSyncService:
    """Examples of accessing multiple data sources sync service"""
    global _multi_source_sync_service
    if _multi_source_sync_service is None:
        _multi_source_sync_service = MultiSourceBasicsSyncService()
    return _multi_source_sync_service
