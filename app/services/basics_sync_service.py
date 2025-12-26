"""
Stock basics synchronization service
- Fetches A-share stock basic info from Tushare
- Enriches with latest market cap (total_mv)
- Upserts into MongoDB collection `stock_basic_info`
- Persists status in collection `sync_status` with key `stock_basics`
- Provides a singleton accessor for reuse across routers/scheduler

This module is async-friendly and offloads blocking IO (Tushare/pandas) to a thread.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne

from app.core.database import get_mongo_db
from app.core.config import settings

from app.services.basics_sync import (
    fetch_stock_basic_df as _fetch_stock_basic_df_util,
    find_latest_trade_date as _find_latest_trade_date_util,
    fetch_daily_basic_mv_map as _fetch_daily_basic_mv_map_util,
    fetch_latest_roe_map as _fetch_latest_roe_map_util,
)

logger = logging.getLogger(__name__)

STATUS_COLLECTION = "sync_status"
DATA_COLLECTION = "stock_basic_info"
JOB_KEY = "stock_basics"


@dataclass
class SyncStats:
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    status: str = "idle"  # idle|running|success|failed
    total: int = 0
    inserted: int = 0
    updated: int = 0
    errors: int = 0
    message: str = ""
    last_trade_date: Optional[str] = None  # YYYYMMDD


class BasicsSyncService:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._running = False
        self._last_status: Optional[Dict[str, Any]] = None
        self._indexes_ensured = False

    async def _ensure_indexes(self, db: AsyncIOMotorDatabase) -> None:
        """Ensure the necessary index exists"""
        if self._indexes_ensured:
            return

        try:
            collection = db[DATA_COLLECTION]
            logger.info("Check and create a stock base index...")

            #1. Composite unique index: stock code + data source (for upset)
            await collection.create_index([
                ("code", 1),
                ("source", 1)
            ], unique=True, name="code_source_unique", background=True)

            #Stock code index (search of all data sources)
            await collection.create_index([("code", 1)], name="code_index", background=True)

            #3. Data source index (screened by data source)
            await collection.create_index([("source", 1)], name="source_index", background=True)

            #4. Index of stock names (search by name)
            await collection.create_index([("name", 1)], name="name_index", background=True)

            #5. Industry index (screened by industry)
            await collection.create_index([("industry", 1)], name="industry_index", background=True)

            #Market index (market-based screening)
            await collection.create_index([("market", 1)], name="market_index", background=True)

            #7. Index of total market value by market value
            await collection.create_index([("total_mv", -1)], name="total_mv_desc", background=True)

            #8. Index of market value in circulation (in order of market value in circulation)
            await collection.create_index([("circ_mv", -1)], name="circ_mv_desc", background=True)

            #9. Update time index (data maintenance)
            await collection.create_index([("updated_at", -1)], name="updated_at_desc", background=True)

            #10. PE Index (valued)
            await collection.create_index([("pe", 1)], name="pe_index", background=True)

            #11. PB Index (valued)
            await collection.create_index([("pb", 1)], name="pb_index", background=True)

            #12. Exchange rate index (screened by activity)
            await collection.create_index([("turnover_rate", -1)], name="turnover_rate_desc", background=True)

            self._indexes_ensured = True
            logger.info("The stock base index check is complete.")
        except Exception as e:
            #Index creation failure should not prevent service startup
            logger.warning(f"Warning (possibly exists) when creating index:{e}")

    async def get_status(self, db: Optional[AsyncIOMotorDatabase] = None) -> Dict[str, Any]:
        """Return last persisted status; falls back to in-memory snapshot."""
        try:
            db = db or get_mongo_db()
            doc = await db[STATUS_COLLECTION].find_one({"job": JOB_KEY})
            if doc:
                doc.pop("_id", None)
                return doc
        except Exception as e:
            logger.warning(f"Failed to load sync status from DB: {e}")
        return self._last_status or {"job": JOB_KEY, "status": "idle"}

    async def _persist_status(self, db: AsyncIOMotorDatabase, stats: Dict[str, Any]) -> None:
        stats["job"] = JOB_KEY
        await db[STATUS_COLLECTION].update_one({"job": JOB_KEY}, {"$set": stats}, upsert=True)
        self._last_status = {k: v for k, v in stats.items() if k != "_id"}

    async def _execute_bulk_write_with_retry(
        self,
        db: AsyncIOMotorDatabase,
        operations: List,
        max_retries: int = 3
    ) -> tuple:
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
                result = await db[DATA_COLLECTION].bulk_write(operations, ordered=False)
                inserted = len(result.upserted_ids) if result.upserted_ids else 0
                updated = result.modified_count or 0
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

    async def run_full_sync(self, force: bool = False) -> Dict[str, Any]:
        """Run a full sync. If already running, return current status unless force."""
        async with self._lock:
            if self._running and not force:
                logger.info("Stock basics sync already running; skip start")
                return await self.get_status()
            self._running = True

        db = get_mongo_db()

        #🔥 to ensure that index exists (upgrade query and upsert performance)
        await self._ensure_indexes(db)

        stats = SyncStats()
        stats.started_at = datetime.utcnow().isoformat()
        stats.status = "running"
        await self._persist_status(db, stats.__dict__.copy())

        try:
            # Step 0: Check if Tushare is enabled
            if not settings.TUSHARE_ENABLED:
                error_msg = (
                    "❌ Tushare 数据源已禁用 (TUSHARE_ENABLED=false)\n"
                    "💡 此服务仅支持 Tushare 数据源\n"
                    "📋 解决方案：\n"
                    "   1. 在 .env 文件中设置 TUSHARE_ENABLED=true 并配置 TUSHARE_TOKEN\n"
                    "   2. 系统已自动切换到多数据源同步服务（支持 AKShare/BaoStock）"
                )
                logger.warning(error_msg)
                raise RuntimeError(error_msg)

            # Step 1: Fetch stock basic list from Tushare (blocking -> thread)
            stock_df = await asyncio.to_thread(self._fetch_stock_basic_df)
            if stock_df is None or getattr(stock_df, "empty", True):
                raise RuntimeError("Tushare returned empty stock_basic list")

            # Step 2: Determine latest trade_date and fetch daily_basic for financial metrics (blocking -> thread)
            latest_trade_date = await asyncio.to_thread(self._find_latest_trade_date)
            stats.last_trade_date = latest_trade_date
            daily_data_map = await asyncio.to_thread(self._fetch_daily_basic_mv_map, latest_trade_date)

            # Step 2b: Fetch latest ROE snapshot from fina_indicator (blocking -> thread)
            roe_map = await asyncio.to_thread(self._fetch_latest_roe_map)

            # Step 3: Upsert into MongoDB (batched bulk writes)
            ops: List[UpdateOne] = []
            now_iso = datetime.utcnow().isoformat()
            for _, row in stock_df.iterrows():  # type: ignore
                name = row.get("name") or ""
                area = row.get("area") or ""
                industry = row.get("industry") or ""
                market = row.get("market") or ""
                list_date = row.get("list_date") or ""
                ts_code = row.get("ts_code") or ""

                # Extract 6-digit stock code from ts_code (e.g., "000001.SZ" -> "000001")
                if isinstance(ts_code, str) and "." in ts_code:
                    code = ts_code.split(".")[0]  # Keep the 6-digit format
                else:
                    # Fallback to symbol with zero-padding if ts_code is invalid
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

                # Extract daily financial metrics - use ts_code directly for matching
                daily_metrics = {}
                if isinstance(ts_code, str) and ts_code in daily_data_map:
                    daily_metrics = daily_data_map[ts_code]

                #Process market cap
                total_mv_yi = None
                circ_mv_yi = None
                if "total_mv" in daily_metrics:
                    try:
                        total_mv_yi = float(daily_metrics["total_mv"]) / 10000.0
                    except Exception:
                        pass
                if "circ_mv" in daily_metrics:
                    try:
                        circ_mv_yi = float(daily_metrics["circ_mv"]) / 10000.0
                    except Exception:
                        pass

                #Generate full symbol
                full_symbol = self._generate_full_symbol(code)

                doc = {
                    "code": code,
                    "symbol": code,  #Add symbol field (standardized field)
                    "name": name,
                    "area": area,
                    "industry": industry,
                    "market": market,
                    "list_date": list_date,
                    "sse": sse,
                    "sec": category,
                    "source": "tushare",  #🔥 Data source identifier
                    "updated_at": now_iso,
                    "full_symbol": full_symbol,  #Add Full Standard Code
                }

                # Add market cap fields
                if total_mv_yi is not None:
                    doc["total_mv"] = total_mv_yi
                if circ_mv_yi is not None:
                    doc["circ_mv"] = circ_mv_yi

                #Add financial radios (🔥) Add ps and ps tm
                for field in ["pe", "pb", "ps", "pe_ttm", "pb_mrq", "ps_ttm"]:
                    if field in daily_metrics:
                        doc[field] = daily_metrics[field]
                # ROE from fina_indicator snapshot
                if isinstance(ts_code, str) and ts_code in roe_map:
                    roe_val = roe_map[ts_code].get("roe")
                    if roe_val is not None:
                        doc["roe"] = roe_val

                # Add trading metrics
                for field in ["turnover_rate", "volume_ratio"]:
                    if field in daily_metrics:
                        doc[field] = daily_metrics[field]

                # 🔥 Add share capital fields (total_share, float_share)
                for field in ["total_share", "float_share"]:
                    if field in daily_metrics:
                        doc[field] = daily_metrics[field]

                #Use (code, source) of joint query conditions
                ops.append(
                    UpdateOne({"code": code, "source": "tushare"}, {"$set": doc}, upsert=True)
                )

            inserted = 0
            updated = 0
            errors = 0
            # Execute in chunks to avoid oversized batches
            BATCH = 1000
            for i in range(0, len(ops), BATCH):
                batch = ops[i : i + BATCH]
                batch_inserted, batch_updated = await self._execute_bulk_write_with_retry(db, batch)

                if batch_inserted > 0 or batch_updated > 0:
                    inserted += batch_inserted
                    updated += batch_updated
                else:
                    errors += 1
                    logger.error(f"Bulk write error on batch {i//BATCH}")

            stats.total = len(ops)
            stats.inserted = inserted
            stats.updated = updated
            stats.errors = errors
            stats.status = "success" if errors == 0 else "success_with_errors"
            stats.finished_at = datetime.utcnow().isoformat()
            await self._persist_status(db, stats.__dict__.copy())
            logger.info(
                f"Stock basics sync finished: total={stats.total} inserted={inserted} updated={updated} errors={errors} trade_date={latest_trade_date}"
            )
            return stats.__dict__

        except Exception as e:
            stats.status = "failed"
            stats.message = str(e)
            stats.finished_at = datetime.utcnow().isoformat()
            await self._persist_status(db, stats.__dict__.copy())
            logger.exception(f"Stock basics sync failed: {e}")
            return stats.__dict__
        finally:
            async with self._lock:
                self._running = False

    # ---- Blocking helpers (run in thread) ----
    def _fetch_stock_basic_df(self):
        """Entrusted blockage to Basics sync.utils"""
        return _fetch_stock_basic_df_util()

    def _find_latest_trade_date(self) -> str:
        """Delegate to basics_sync.utils (blocking)"""
        return _find_latest_trade_date_util()

    def _fetch_daily_basic_mv_map(self, trade_date: str) -> Dict[str, Dict[str, float]]:
        """Delegate to basics_sync.utils (blocking)"""
        return _fetch_daily_basic_mv_map_util(trade_date)

    def _fetch_latest_roe_map(self) -> Dict[str, Dict[str, float]]:
        """Delegate to basics_sync.utils (blocking)"""
        return _fetch_latest_roe_map_util()

    def _generate_full_symbol(self, code: str) -> str:
        """Generate full standard code by stock code

Args:
code: 6-bit stock code

Returns:
Full standardized code (e.g. 00001.SZ) returns original code if the code is invalid (ensure not to be empty)
"""
        #Make sure the code isn't empty.
        if not code:
            return ""

        #Standardise as string and remove spaces
        code = str(code).strip()

        #Return original code if length is not 6 (avoid not return None)
        if len(code) != 6:
            return code

        #By code, the exchange.
        if code.startswith(('60', '68', '90')):
            return f"{code}.SS"  #Shanghai Stock Exchange
        elif code.startswith(('00', '30', '20')):
            return f"{code}.SZ"  #Shenzhen Stock Exchange
        elif code.startswith(('8', '4')):
            return f"{code}.BJ"  #Beijing Stock Exchange
        else:
            #Unidentifiable code, return original code (ensure not to be empty)
            return code if code else ""


# Singleton accessor
_basics_sync_service: Optional[BasicsSyncService] = None


def get_basics_sync_service() -> BasicsSyncService:
    global _basics_sync_service
    if _basics_sync_service is None:
        _basics_sync_service = BasicsSyncService()
    return _basics_sync_service

