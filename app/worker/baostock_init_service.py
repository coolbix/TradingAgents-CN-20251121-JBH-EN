#!/usr/bin/env python3
"""BaoStock Data Initialisation Service
Provide complete initialization of BaoStock data
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from app.core.config import get_settings
from app.core.database import get_database
from app.worker.baostock_sync_service import BaoStockSyncService, BaoStockSyncStats

logger = logging.getLogger(__name__)


@dataclass
class BaoStockInitializationStats:
    """BaoStock Initialization Statistics"""
    completed_steps: int = 0
    total_steps: int = 6
    current_step: str = ""
    basic_info_count: int = 0
    quotes_count: int = 0
    historical_records: int = 0
    weekly_records: int = 0
    monthly_records: int = 0
    financial_records: int = 0
    errors: List[str] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    @property
    def duration(self) -> float:
        """Calculate time (seconds)"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    @property
    def progress(self) -> str:
        """Progress String"""
        return f"{self.completed_steps}/{self.total_steps}"


class BaoStockInitService:
    """BaoStock Data Initialisation Service"""

    def __init__(self):
        """Initialization services

Note: Database connection initialized in initialize() method
"""
        try:
            self.settings = get_settings()
            self.db = None  #Delayed initialization
            self.sync_service = BaoStockSyncService()
            logger.info("BaoStock Initialization Service Successfully")
        except Exception as e:
            logger.error(f"The initialization of the BaoStock service failed:{e}")
            raise

    async def initialize(self):
        """Spacing Initialization Services"""
        try:
            #Initialization of database connections
            from app.core.database import get_mongo_db
            self.db = get_mongo_db()

            #Initialization sync service
            await self.sync_service.initialize()

            logger.info("BaoStock Initialization Service Initiation Complete")
        except Exception as e:
            logger.error(f"The initialization of BaoStock service has failed:{e}")
            raise
    
    async def check_database_status(self) -> Dict[str, Any]:
        """Check database status"""
        try:
            #Check Basic Information
            basic_info_count = await self.db.stock_basic_info.count_documents({"data_source": "baostock"})
            basic_info_latest = None
            if basic_info_count > 0:
                latest_doc = await self.db.stock_basic_info.find_one(
                    {"data_source": "baostock"},
                    sort=[("last_sync", -1)]
                )
                if latest_doc:
                    basic_info_latest = latest_doc.get("last_sync")
            
            #Check Line Data
            quotes_count = await self.db.market_quotes.count_documents({"data_source": "baostock"})
            quotes_latest = None
            if quotes_count > 0:
                latest_doc = await self.db.market_quotes.find_one(
                    {"data_source": "baostock"},
                    sort=[("last_sync", -1)]
                )
                if latest_doc:
                    quotes_latest = latest_doc.get("last_sync")
            
            return {
                "basic_info_count": basic_info_count,
                "basic_info_latest": basic_info_latest,
                "quotes_count": quotes_count,
                "quotes_latest": quotes_latest,
                "status": "ready" if basic_info_count > 0 else "empty"
            }
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return {"status": "error", "error": str(e)}
    
    async def full_initialization(self, historical_days: int = 365,
                                force: bool = False,
                                enable_multi_period: bool = False) -> BaoStockInitializationStats:
        """Full Data Initialization

Args:
History days: History data days
Forced re-initiation
enabled multi-cycle data sync (daily, weekly, moon)

Returns:
Initialize statistical information
"""
        stats = BaoStockInitializationStats()
        stats.total_steps = 8 if enable_multi_period else 6
        stats.start_time = datetime.now()
        
        try:
            logger.info("Start the initialization of BaoStock complete data...")
            
            #Step 1: Check database status
            stats.current_step = "检查数据库状态"
            logger.info(f"1️⃣ {stats.current_step}...")
            
            db_status = await self.check_database_status()
            if db_status["status"] != "empty" and not force:
                logger.info("ℹ️ Database is available, skipping initialization (using --force re-initiation)")
                stats.completed_steps = 6
                stats.end_time = datetime.now()
                return stats
            
            stats.completed_steps += 1
            
            #Step 2: Initialization of stock base information
            stats.current_step = "初始化股票基础信息"
            logger.info(f"2️⃣ {stats.current_step}...")
            
            basic_stats = await self.sync_service.sync_stock_basic_info()
            stats.basic_info_count = basic_stats.basic_info_count
            stats.errors.extend(basic_stats.errors)
            stats.completed_steps += 1
            
            if stats.basic_info_count == 0:
                raise Exception("基础信息同步失败，无法继续")
            
            #Step 3: Synchronize historical data (daily)
            stats.current_step = "同步历史数据（日线）"
            logger.info(f"3️⃣ {stats.current_step}(Recently){historical_days}God...")

            historical_stats = await self.sync_service.sync_historical_data(days=historical_days, period="daily")
            stats.historical_records = historical_stats.historical_records
            stats.errors.extend(historical_stats.errors)
            stats.completed_steps += 1

            #Step 4: Synchronize multi-cycle data (if enabled)
            if enable_multi_period:
                #Sync weekly data
                stats.current_step = "同步周线数据"
                logger.info(f"4️⃣a {stats.current_step}(Recently){historical_days}God...")
                try:
                    weekly_stats = await self.sync_service.sync_historical_data(days=historical_days, period="weekly")
                    stats.weekly_records = weekly_stats.historical_records
                    stats.errors.extend(weekly_stats.errors)
                    logger.info(f"✅ weekline data synchronised:{stats.weekly_records}Notes")
                except Exception as e:
                    logger.warning(f"Weekline data synchronisation failed:{e}(Continuing next steps)")
                stats.completed_steps += 1

                #Sync Moonline Data
                stats.current_step = "同步月线数据"
                logger.info(f"4️⃣b {stats.current_step}(Recently){historical_days}God...")
                try:
                    monthly_stats = await self.sync_service.sync_historical_data(days=historical_days, period="monthly")
                    stats.monthly_records = monthly_stats.historical_records
                    stats.errors.extend(monthly_stats.errors)
                    logger.info(f"Synchronization of moonline data:{stats.monthly_records}Notes")
                except Exception as e:
                    logger.warning(f"@⚠️ > Moonline data sync failed:{e}(Continuing next steps)")
                stats.completed_steps += 1
            
            #Step 4: Synchronization of financial data
            stats.current_step = "同步财务数据"
            logger.info(f"4️⃣ {stats.current_step}...")
            
            financial_stats = await self._sync_financial_data()
            stats.financial_records = financial_stats
            stats.completed_steps += 1
            
            #Step 5: Synchronize the latest developments
            stats.current_step = "同步最新行情"
            logger.info(f"5️⃣ {stats.current_step}...")
            
            quotes_stats = await self.sync_service.sync_realtime_quotes()
            stats.quotes_count = quotes_stats.quotes_count
            stats.errors.extend(quotes_stats.errors)
            stats.completed_steps += 1
            
            #Step 6: Validate data integrity
            stats.current_step = "验证数据完整性"
            logger.info(f"6️⃣ {stats.current_step}...")
            
            await self._verify_data_integrity(stats)
            stats.completed_steps += 1
            
            stats.end_time = datetime.now()
            logger.info(f"BaoStock complete and completed! Time-consuming:{stats.duration:.1f}sec")
            
            return stats
            
        except Exception as e:
            stats.end_time = datetime.now()
            error_msg = f"BaoStock初始化失败: {e}"
            logger.error(f"❌ {error_msg}")
            stats.errors.append(error_msg)
            return stats
    
    async def _sync_financial_data(self) -> int:
        """Sync Financial Data"""
        try:
            #Get Stock List
            collection = self.db.stock_basic_info
            cursor = collection.find({"data_source": "baostock"}, {"code": 1})
            stock_codes = [doc["code"] async for doc in cursor]
            
            if not stock_codes:
                return 0
            
            #Limit number to avoid timeout
            limited_codes = stock_codes[:50]  #Only the top 50 stocks.
            financial_count = 0
            
            for code in limited_codes:
                try:
                    financial_data = await self.sync_service.provider.get_financial_data(code)
                    if financial_data:
                        #Update to Database
                        await collection.update_one(
                            {"code": code},
                            {"$set": {
                                "financial_data": financial_data,
                                "financial_data_updated": datetime.now()
                            }}
                        )
                        financial_count += 1
                    
                    #Avoid API Limit
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.debug(f"Access{code}Financial data failed:{e}")
                    continue
            
            logger.info(f"Synchronization of financial data:{financial_count}Notes")
            return financial_count
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return 0
    
    async def _verify_data_integrity(self, stats: BaoStockInitializationStats):
        """Validate data integrity"""
        try:
            #Check Basic Information
            basic_count = await self.db.stock_basic_info.count_documents({"data_source": "baostock"})
            if basic_count != stats.basic_info_count:
                logger.warning(f"⚠️ base information quantity mismatch: expected{stats.basic_info_count}actual{basic_count}")
            
            #Check Line Data
            quotes_count = await self.db.market_quotes.count_documents({"data_source": "baostock"})
            if quotes_count != stats.quotes_count:
                logger.warning(f"⚠️ The number of cases does not match:{stats.quotes_count}actual{quotes_count}")
            
            logger.info("Data integrity check completed")
            
        except Exception as e:
            logger.error(f"Data integrity verification failed:{e}")
            stats.errors.append(f"数据完整性验证失败: {e}")
    
    async def basic_initialization(self) -> BaoStockInitializationStats:
        """Initialization of basic data (basic information and practice only)"""
        stats = BaoStockInitializationStats()
        stats.start_time = datetime.now()
        stats.total_steps = 3
        
        try:
            logger.info("Start initializing the BaoStock base data...")
            
            #Step 1: Initializing basic stock information
            stats.current_step = "初始化股票基础信息"
            logger.info(f"1️⃣ {stats.current_step}...")
            
            basic_stats = await self.sync_service.sync_stock_basic_info()
            stats.basic_info_count = basic_stats.basic_info_count
            stats.errors.extend(basic_stats.errors)
            stats.completed_steps += 1
            
            #Step 2: Synchronize the latest developments
            stats.current_step = "同步最新行情"
            logger.info(f"2️⃣ {stats.current_step}...")
            
            quotes_stats = await self.sync_service.sync_realtime_quotes()
            stats.quotes_count = quotes_stats.quotes_count
            stats.errors.extend(quotes_stats.errors)
            stats.completed_steps += 1
            
            #Step 3: Validate data
            stats.current_step = "验证数据完整性"
            logger.info(f"3️⃣ {stats.current_step}...")
            
            await self._verify_data_integrity(stats)
            stats.completed_steps += 1
            
            stats.end_time = datetime.now()
            logger.info(f"BaoStock Foundation is complete! Time-consuming:{stats.duration:.1f}sec")
            
            return stats
            
        except Exception as e:
            stats.end_time = datetime.now()
            error_msg = f"BaoStock基础初始化失败: {e}"
            logger.error(f"❌ {error_msg}")
            stats.errors.append(error_msg)
            return stats


#Initialisation function compatible with APSscheduler
async def run_baostock_full_initialization():
    """Run BaoStock Complete Initialization"""
    try:
        service = BaoStockInitService()
        await service.initialize()  #It has to be initialized.
        stats = await service.full_initialization()
        logger.info(f"BaoStock completes the initialization:{stats.progress}, time consuming:{stats.duration:.1f}sec")
    except Exception as e:
        logger.error(f"BaoStock failed:{e}")


async def run_baostock_basic_initialization():
    """Run the BaoStock Foundation Initialization"""
    try:
        service = BaoStockInitService()
        await service.initialize()  #It has to be initialized.
        stats = await service.basic_initialization()
        logger.info(f"The initialization of BaoStock Foundation was completed:{stats.progress}, time consuming:{stats.duration:.1f}sec")
    except Exception as e:
        logger.error(f"The initialization of BaoStock Foundation failed:{e}")
