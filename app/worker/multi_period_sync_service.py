#!/usr/bin/env python3
"""Multi-cycle historical data sync service
Support for unified synchronization of dayline, weekline, moonline data
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from app.services.historical_data_service import get_historical_data_service
from app.worker.tushare_sync_service import TushareSyncService
from app.worker.akshare_sync_service import AKShareSyncService
from app.worker.baostock_sync_service import BaoStockSyncService

logger = logging.getLogger(__name__)


@dataclass
class MultiPeriodSyncStats:
    """Multi-cycle Sync Statistics"""
    total_symbols: int = 0
    daily_records: int = 0
    weekly_records: int = 0
    monthly_records: int = 0
    success_count: int = 0
    error_count: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class MultiPeriodSyncService:
    """Multi-cycle historical data sync service"""
    
    def __init__(self):
        self.historical_service = None
        self.tushare_service = None
        self.akshare_service = None
        self.baostock_service = None
        
    async def initialize(self):
        """Initialization services"""
        try:
            self.historical_service = await get_historical_data_service()
            
            #Initialization of data source services
            self.tushare_service = TushareSyncService()
            await self.tushare_service.initialize()
            
            self.akshare_service = AKShareSyncService()
            await self.akshare_service.initialize()
            
            self.baostock_service = BaoStockSyncService()
            await self.baostock_service.initialize()
            
            logger.info("✅ multi-cycle sync service initialization completed")
            
        except Exception as e:
            logger.error(f"The initialization of multi-cycle sync services failed:{e}")
            raise
    
    async def sync_multi_period_data(
        self,
        symbols: List[str] = None,
        periods: List[str] = None,
        data_sources: List[str] = None,
        start_date: str = None,
        end_date: str = None,
        all_history: bool = False
    ) -> MultiPeriodSyncStats:
        """Synchronize multi-cycle historical data

Args:
Symbols: list of stock codes, None means all stocks
periods: periodic list (daily/weekly/montly)
Data sources: list of data sources (tushare/akshare/baostock)
Start date: Start date
End date: End date
all history: whether to sync all historical data (overlooking time frames)
"""
        if self.historical_service is None:
            await self.initialize()
        
        #Default parameters
        if periods is None:
            periods = ["daily", "weekly", "monthly"]
        if data_sources is None:
            data_sources = ["tushare", "akshare", "baostock"]
        if symbols is None:
            symbols = await self._get_all_symbols()

        #Process all history parameters
        if all_history:
            start_date, end_date = await self._get_full_history_date_range()
            logger.info(f"Synchronising folder{start_date}Present.{end_date}")

        stats = MultiPeriodSyncStats()
        stats.total_symbols = len(symbols)

        logger.info(f"Multi-cycle synchronisation of data:{len(symbols)}It's just stocks."
                   f"Cycle{periods}, data sources{data_sources}, "
                   f"Time frame:{start_date or 'Default'}Present.{end_date or 'Today'}")
        
        try:
            #Synchronize by data source and cycle combination
            for data_source in data_sources:
                for period in periods:
                    period_stats = await self._sync_period_data(
                        data_source, period, symbols, start_date, end_date
                    )
                    
                    #Cumulative statistics
                    if period == "daily":
                        stats.daily_records += period_stats.get("records", 0)
                    elif period == "weekly":
                        stats.weekly_records += period_stats.get("records", 0)
                    elif period == "monthly":
                        stats.monthly_records += period_stats.get("records", 0)
                    
                    stats.success_count += period_stats.get("success", 0)
                    stats.error_count += period_stats.get("errors", 0)
                    
                    #Progress Log
                    logger.info(f"📊 {data_source}-{period}Other Organiser"
                               f"{period_stats.get('records', 0)}Notes")
            
            logger.info(f"Multi-cycle data synchronised:"
                       f"Dayline{stats.daily_records},weekline{stats.weekly_records}, "
                       f"Moonline{stats.monthly_records}Notes")
            
            return stats
            
        except Exception as e:
            logger.error(f"Multi-cycle data synchronisation failed:{e}")
            stats.errors.append(str(e))
            return stats
    
    async def _sync_period_data(
        self,
        data_source: str,
        period: str,
        symbols: List[str],
        start_date: str = None,
        end_date: str = None
    ) -> Dict[str, Any]:
        """Synchronize data for a given cycle"""
        stats = {"records": 0, "success": 0, "errors": 0}
        
        try:
            logger.info(f"Synchronize{data_source}-{period}Data:{len(symbols)}Only stocks")
            
            #Select the corresponding service
            if data_source == "tushare":
                service = self.tushare_service
            elif data_source == "akshare":
                service = self.akshare_service
            elif data_source == "baostock":
                service = self.baostock_service
            else:
                logger.error(f"Data sources not supported:{data_source}")
                return stats
            
            #Batch processing
            batch_size = 50
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                batch_stats = await self._sync_batch_period_data(
                    service, data_source, period, batch, start_date, end_date
                )
                
                stats["records"] += batch_stats["records"]
                stats["success"] += batch_stats["success"]
                stats["errors"] += batch_stats["errors"]
                
                #Progress Log
                progress = min(i + batch_size, len(symbols))
                logger.info(f"📊 {data_source}-{period}Progress:{progress}/{len(symbols)}")
                
                #API limit flow
                await asyncio.sleep(0.5)
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ {data_source}-{period}Synchronising folder{e}")
            stats["errors"] += 1
            return stats
    
    async def _sync_batch_period_data(
        self,
        service,
        data_source: str,
        period: str,
        symbols: List[str],
        start_date: str = None,
        end_date: str = None
    ) -> Dict[str, Any]:
        """Sync batch cycle data"""
        stats = {"records": 0, "success": 0, "errors": 0}
        
        for symbol in symbols:
            try:
                #Access to historical data
                if data_source == "tushare":
                    hist_data = await service.provider.get_historical_data(
                        symbol, start_date, end_date, period
                    )
                elif data_source == "akshare":
                    hist_data = await service.provider.get_historical_data(
                        symbol, start_date, end_date, period
                    )
                elif data_source == "baostock":
                    hist_data = await service.provider.get_historical_data(
                        symbol, start_date, end_date, period
                    )
                else:
                    continue
                
                if hist_data is not None and not hist_data.empty:
                    #Save to Database
                    saved_count = await self.historical_service.save_historical_data(
                        symbol=symbol,
                        data=hist_data,
                        data_source=data_source,
                        market="CN",
                        period=period
                    )
                    
                    stats["records"] += saved_count
                    stats["success"] += 1
                else:
                    stats["errors"] += 1
                    
            except Exception as e:
                logger.error(f"❌ {symbol}-{period}Synchronising folder{e}")
                stats["errors"] += 1
        
        return stats
    
    async def _get_all_symbols(self) -> List[str]:
        """Get All Stock Codes"""
        try:
            #Retrieving stock lists from databases
            from app.core.database import get_mongo_db
            db = get_mongo_db()
            collection = db.stock_basic_info

            cursor = collection.find({}, {"symbol": 1})
            symbols = [doc["symbol"] async for doc in cursor]

            logger.info(f"📊 for the list of shares:{len(symbols)}Only stocks")
            return symbols

        except Exception as e:
            logger.error(f"Can not get folder: %s: %s{e}")
            return []

    async def _get_full_history_date_range(self) -> tuple[str, str]:
        """Date range for obtaining historical data"""
        try:
            from datetime import datetime, timedelta

            #Other Organiser
            end_date = datetime.now().strftime('%Y-%m-%d')

            #Start date: Based on data sources
            #Tushare: Started in 1990
            #AKShare: Started in 1990
            #BaoStock: Started in 1990
            #For safety reasons, since 1990
            start_date = "1990-01-01"

            logger.info(f"Full historical range:{start_date}Present.{end_date}")
            return start_date, end_date

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            #Default returns data for the last 5 years
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=365*5)).strftime('%Y-%m-%d')
            return start_date, end_date
    
    async def get_sync_statistics(self) -> Dict[str, Any]:
        """Get Sync Statistical Information"""
        try:
            if self.historical_service is None:
                await self.initialize()
            
            #By cycle
            from app.core.database import get_mongo_db
            db = get_mongo_db()
            collection = db.stock_daily_quotes
            
            pipeline = [
                {"$group": {
                    "_id": {
                        "period": "$period",
                        "data_source": "$data_source"
                    },
                    "count": {"$sum": 1},
                    "latest_date": {"$max": "$trade_date"}
                }}
            ]
            
            results = await collection.aggregate(pipeline).to_list(length=None)
            
            #Format statistical results
            stats = {}
            for result in results:
                period = result["_id"]["period"]
                source = result["_id"]["data_source"]
                
                if period not in stats:
                    stats[period] = {}
                
                stats[period][source] = {
                    "count": result["count"],
                    "latest_date": result["latest_date"]
                }
            
            return {
                "period_statistics": stats,
                "last_updated": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return {}


#Examples of global services
_multi_period_sync_service = None


async def get_multi_period_sync_service() -> MultiPeriodSyncService:
    """Examples of accessing multi-cycle sync services"""
    global _multi_period_sync_service
    if _multi_period_sync_service is None:
        _multi_period_sync_service = MultiPeriodSyncService()
        await _multi_period_sync_service.initialize()
    return _multi_period_sync_service


#APSscheduler Job Functions
async def run_multi_period_sync(periods: List[str] = None):
    """APSscheduler task: Synchronization of data over time"""
    try:
        service = await get_multi_period_sync_service()
        result = await service.sync_multi_period_data(periods=periods)
        logger.info(f"Multi-cycle data synchronised:{result}")
        return result
    except Exception as e:
        logger.error(f"Multi-cycle data synchronisation failed:{e}")
        raise


async def run_daily_sync():
    """APScheduler Job: Synchronization of Dayline Data"""
    return await run_multi_period_sync(["daily"])


async def run_weekly_sync():
    """APSscheduler task: Synchronization of weekly line data"""
    return await run_multi_period_sync(["weekly"])


async def run_monthly_sync():
    """APScheduler Task: Synchronizing Moonline Data"""
    return await run_multi_period_sync(["monthly"])
