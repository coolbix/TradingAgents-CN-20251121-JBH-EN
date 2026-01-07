"""AKShare Data Initialization Service
Initialization of complete data for initial deployment, including basic, historical, financial, etc.
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from app.core.database import get_mongo_db_async
from app.worker.akshare_sync_service import get_akshare_sync_service

logger = logging.getLogger(__name__)


@dataclass
class AKShareInitializationStats:
    """Initialization of statistical information by AKShare"""
    started_at: datetime
    finished_at: Optional[datetime] = None
    total_steps: int = 0
    completed_steps: int = 0
    current_step: str = ""
    basic_info_count: int = 0
    historical_records: int = 0
    weekly_records: int = 0
    monthly_records: int = 0
    financial_records: int = 0
    quotes_count: int = 0
    news_count: int = 0
    errors: List[Dict[str, Any]] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class AKShareInitService:
    """AKShare Data Initialization Service

    Responsible for initialization of complete data at first deployment:
    1. Check database status
    2. Initialization of stock base information
    Synchronization of historical data (configurable time frames)
    4. Synchronization of financial data
    5. Synchronization of up-to-date behaviour data
    Validation of data integrity
    """
    
    def __init__(self):
        self.db = None
        self.sync_service = None
        self.stats = None
    
    async def initialize(self):
        """Initialization services"""
        self.db = get_mongo_db_async()
        self.sync_service = await get_akshare_sync_service()
        logger.info("The initialization service for AKShare is ready.")
    
    async def run_full_initialization(
        self,
        historical_days: int = 365,
        skip_if_exists: bool = True,
        batch_size: int = 100,
        enable_multi_period: bool = False,
        sync_items: List[str] = None
    ) -> Dict[str, Any]:
        """Run full data initialization

        Args:
            History days: days of historical data (default 1 year)
            sskip if exists: Skipped if data already exists
            Watch size: Batch size
            enabled multi-cycle data sync (daily, weekly, moon)
            sync items: list of data types to synchronize, optional values:
            - 'basic info': basic stock information
            - 'historic': historical patterns (daily)
            - 'Weekly': weekline data
            - 'Monthly': Moonline data.
            - 'financial': financial data
            - 'Quotes':
            - 'news': news data
            - None: Sync all data (default)

        Returns:
            Initialization Results Statistics
        """
        #Sync all data if sync items are not specified
        if sync_items is None:
            sync_items = ['basic_info', 'historical', 'financial', 'quotes']
            if enable_multi_period:
                sync_items.extend(['weekly', 'monthly'])

        logger.info("Starting the initialization of AKshare data...")
        logger.info(f"Synchronising items:{', '.join(sync_items)}")

        #Calculate the total number of steps (check status + number of synchronized items + validation)
        total_steps = 1 + len(sync_items) + 1

        self.stats = AKShareInitializationStats(
            started_at=datetime.utcnow(),
            total_steps=total_steps
        )

        try:
            #Step 1: Check database status
            #Check if skipping only when synchronizing Basic info
            if 'basic_info' in sync_items:
                await self._step_check_database_status(skip_if_exists)
            else:
                logger.info("Check database status...")
                basic_count = await self.db.stock_basic_info.count_documents({})
                logger.info(f"Current stock base information:{basic_count}Article")
                if basic_count == 0:
                    logger.warning("Basic information on stocks is not available in the database.")

            #Step 2: Initialization of stock base information
            if 'basic_info' in sync_items:
                await self._step_initialize_basic_info()
            else:
                logger.info("Skipping stock base information sync")

            #Step 3: Synchronize historical data (daily)
            if 'historical' in sync_items:
                await self._step_initialize_historical_data(historical_days)
            else:
                logger.info("Skipped History Data Synchronization")

            #Step 4: Synchronize weekly data
            if 'weekly' in sync_items:
                await self._step_initialize_weekly_data(historical_days)
            else:
                logger.info("Skipping weekline data sync")

            #Step 5: Synchronize Moonline data
            if 'monthly' in sync_items:
                await self._step_initialize_monthly_data(historical_days)
            else:
                logger.info("Skip the Moonline Data Sync")

            #Step 6: Synchronization of financial data
            if 'financial' in sync_items:
                await self._step_initialize_financial_data()
            else:
                logger.info("SkipSync of Financial Data")

            #Step 7: Synchronize the latest developments
            if 'quotes' in sync_items:
                await self._step_initialize_quotes()
            else:
                logger.info("Skip the latest line sync")

            #Step 8: Synchronize news data
            if 'news' in sync_items:
                await self._step_initialize_news_data()
            else:
                logger.info("Skip NewsSync")

            #Final: Validation of data integrity
            await self._step_verify_data_integrity()
            
            self.stats.finished_at = datetime.utcnow()
            duration = (self.stats.finished_at - self.stats.started_at).total_seconds()
            
            logger.info(f"The initialization of the AKShare data is complete!{duration:.2f}sec")
            
            return self._get_initialization_summary()
            
        except Exception as e:
            logger.error(f"The initialization of AKShare data failed:{e}")
            self.stats.errors.append({
                "step": self.stats.current_step,
                "error": str(e),
                "timestamp": datetime.utcnow()
            })
            return self._get_initialization_summary()
    
    async def _step_check_database_status(self, skip_if_exists: bool):
        """Step 1: Check database status"""
        self.stats.current_step = "检查数据库状态"
        logger.info(f"📊 {self.stats.current_step}...")
        
        #Check the amount of data collected
        basic_count = await self.db.stock_basic_info.count_documents({})
        quotes_count = await self.db.market_quotes.count_documents({})
        
        logger.info(f"Current data status:")
        logger.info(f"Basic equity information:{basic_count}Article")
        logger.info(f"Line data:{quotes_count}Article")
        
        if skip_if_exists and basic_count > 0:
            logger.info("⚠️ detected data available, skipping initialization (which can be mandatory by sskip if exists=False)")
            raise Exception("数据已存在，跳过初始化")
        
        self.stats.completed_steps += 1
        logger.info(f"✅ {self.stats.current_step}Completed")
    
    async def _step_initialize_basic_info(self):
        """Step 2: Initialization of stock base information"""
        self.stats.current_step = "初始化股票基础信息"
        logger.info(f"📋 {self.stats.current_step}...")
        
        #Force update of all basic information
        result = await self.sync_service.sync_stock_basic_info(force_update=True)
        
        if result:
            self.stats.basic_info_count = result.get("success_count", 0)
            logger.info(f"Initialization of basic information completed:{self.stats.basic_info_count}Only stocks")
        else:
            raise Exception("基础信息初始化失败")
        
        self.stats.completed_steps += 1
    
    async def _step_initialize_historical_data(self, historical_days: int):
        """Step 3: Synchronize historical data"""
        self.stats.current_step = f"同步历史数据({historical_days}天)"
        logger.info(f"📊 {self.stats.current_step}...")

        #Calculate Date Range
        end_date = datetime.now().strftime('%Y-%m-%d')

        #Synchronize history if history days are greater than or equal to 10 years (3650 days)
        if historical_days >= 3650:
            start_date = "1990-01-01"  #All History Sync
            logger.info(f"Historical data range: history (from 1990-01-01 to{end_date}）")
        else:
            start_date = (datetime.now() - timedelta(days=historical_days)).strftime('%Y-%m-%d')
            logger.info(f"Historical data range:{start_date}Present.{end_date}")

        #Sync Historical Data
        result = await self.sync_service.sync_historical_data(
            start_date=start_date,
            end_date=end_date,
            incremental=False  #Full Sync
        )
        
        if result:
            self.stats.historical_records = result.get("total_records", 0)
            logger.info(f"Initialization of historical data is complete:{self.stats.historical_records}Notes")
        else:
            logger.warning("⚠️ Part of the initialization of historical data failed, continuing next steps")
        
        self.stats.completed_steps += 1

    async def _step_initialize_weekly_data(self, historical_days: int):
        """Step 4a: Synchronize weekline data"""
        self.stats.current_step = f"同步周线数据({historical_days}天)"
        logger.info(f"📊 {self.stats.current_step}...")

        #Calculate Date Range
        end_date = datetime.now().strftime('%Y-%m-%d')

        #Synchronize history if history days are greater than or equal to 10 years (3650 days)
        if historical_days >= 3650:
            start_date = "1990-01-01"  #All History Sync
            logger.info(f"Weekline data range: full history (from 1990-01-01 to{end_date}）")
        else:
            start_date = (datetime.now() - timedelta(days=historical_days)).strftime('%Y-%m-%d')
            logger.info(f"Weekline data range:{start_date}Present.{end_date}")

        try:
            #Sync weekly data
            result = await self.sync_service.sync_historical_data(
                start_date=start_date,
                end_date=end_date,
                incremental=False,
                period="weekly"  #Specify a weekline
            )

            if result:
                weekly_records = result.get("total_records", 0)
                self.stats.weekly_records = weekly_records
                logger.info(f"Initialization of weekly data is complete:{weekly_records}Notes")
            else:
                logger.warning("⚠️ Weekline data initialization partially failed, continuing next steps")
        except Exception as e:
            logger.warning(f"The weekline data initialization failed:{e}(Continuing next steps)")

        self.stats.completed_steps += 1

    async def _step_initialize_monthly_data(self, historical_days: int):
        """Step 4b: Sync Moonline data"""
        self.stats.current_step = f"同步月线数据({historical_days}天)"
        logger.info(f"📊 {self.stats.current_step}...")

        #Calculate Date Range
        end_date = datetime.now().strftime('%Y-%m-%d')

        #Synchronize history if history days are greater than or equal to 10 years (3650 days)
        if historical_days >= 3650:
            start_date = "1990-01-01"  #All History Sync
            logger.info(f"Monthly data range: full history (from 1990-01-01 to{end_date}）")
        else:
            start_date = (datetime.now() - timedelta(days=historical_days)).strftime('%Y-%m-%d')
            logger.info(f"Month data range:{start_date}Present.{end_date}")

        try:
            #Sync Moonline Data
            result = await self.sync_service.sync_historical_data(
                start_date=start_date,
                end_date=end_date,
                incremental=False,
                period="monthly"  #Specify moonline
            )

            if result:
                monthly_records = result.get("total_records", 0)
                self.stats.monthly_records = monthly_records
                logger.info(f"Initialization of moonline data is complete:{monthly_records}Notes")
            else:
                logger.warning("⚠️ The initialization of the moon line data has failed and next steps continue")
        except Exception as e:
            logger.warning(f"The initialization of the monthly data failed:{e}(Continuing next steps)")

        self.stats.completed_steps += 1

    async def _step_initialize_financial_data(self):
        """Step 4: Synchronization of financial data"""
        self.stats.current_step = "同步财务数据"
        logger.info(f"💰 {self.stats.current_step}...")
        
        try:
            result = await self.sync_service.sync_financial_data()
            
            if result:
                self.stats.financial_records = result.get("success_count", 0)
                logger.info(f"Initialization of financial data completed:{self.stats.financial_records}Notes")
            else:
                logger.warning("Initialization of financial data failed")
        except Exception as e:
            logger.warning(f"The initialization of financial data failed:{e}(Continuing next steps)")
        
        self.stats.completed_steps += 1
    
    async def _step_initialize_quotes(self):
        """Step 5: Synchronize the latest developments"""
        self.stats.current_step = "同步最新行情"
        logger.info(f"📈 {self.stats.current_step}...")

        try:
            result = await self.sync_service.sync_realtime_quotes()

            if result:
                self.stats.quotes_count = result.get("success_count", 0)
                logger.info(f"✅ Initialization completed:{self.stats.quotes_count}Only stocks")
            else:
                logger.warning("Initialization of the latest movement failed")
        except Exception as e:
            logger.warning(f"The initialization of the latest event failed:{e}(Continuing next steps)")

        self.stats.completed_steps += 1

    async def _step_initialize_news_data(self):
        """Step 6: Synchronize news data"""
        self.stats.current_step = "同步新闻数据"
        logger.info(f"📰 {self.stats.current_step}...")

        try:
            result = await self.sync_service.sync_news_data(
                max_news_per_stock=20
            )

            if result:
                self.stats.news_count = result.get("news_count", 0)
                logger.info(f"Initialization of news data completed:{self.stats.news_count}News")
            else:
                logger.warning("Initialization of news data failed")
        except Exception as e:
            logger.warning(f"The initialization of news data failed:{e}(Continuing next steps)")

        self.stats.completed_steps += 1

    async def _step_verify_data_integrity(self):
        """Step 6: Validate data integrity"""
        self.stats.current_step = "验证数据完整性"
        logger.info(f"🔍 {self.stats.current_step}...")
        
        #Check final data status
        basic_count = await self.db.stock_basic_info.count_documents({})
        quotes_count = await self.db.market_quotes.count_documents({})
        
        #Check data quality
        extended_count = await self.db.stock_basic_info.count_documents({
            "full_symbol": {"$exists": True},
            "market_info": {"$exists": True}
        })
        
        logger.info(f"Data integrity validation:")
        logger.info(f"Basic equity information:{basic_count}Article")
        logger.info(f"Expand field overwrite:{extended_count}Article (){extended_count/basic_count*100:.1f}%)")
        logger.info(f"Line data:{quotes_count}Article")
        
        if basic_count == 0:
            raise Exception("数据初始化失败：无基础数据")
        
        if extended_count / basic_count < 0.9:  #More than 90% should have extended fields
            logger.warning("Low coverage of extension fields and possible data quality problems")
        
        self.stats.completed_steps += 1
        logger.info(f"✅ {self.stats.current_step}Completed")
    
    def _get_initialization_summary(self) -> Dict[str, Any]:
        """Get Initialization Summary"""
        duration = 0
        if self.stats.finished_at:
            duration = (self.stats.finished_at - self.stats.started_at).total_seconds()
        
        return {
            "success": self.stats.completed_steps == self.stats.total_steps,
            "started_at": self.stats.started_at,
            "finished_at": self.stats.finished_at,
            "duration": duration,
            "completed_steps": self.stats.completed_steps,
            "total_steps": self.stats.total_steps,
            "progress": f"{self.stats.completed_steps}/{self.stats.total_steps}",
            "data_summary": {
                "basic_info_count": self.stats.basic_info_count,
                "daily_records": self.stats.historical_records,
                "weekly_records": self.stats.weekly_records,
                "monthly_records": self.stats.monthly_records,
                "financial_records": self.stats.financial_records,
                "quotes_count": self.stats.quotes_count,
                "news_count": self.stats.news_count
            },
            "errors": self.stats.errors,
            "current_step": self.stats.current_step
        }


#Examples of global initialization services
_akshare_init_service = None

async def get_akshare_init_service() -> AKShareInitService:
    """Examples of accessing AKShare initialization services"""
    global _akshare_init_service
    if _akshare_init_service is None:
        _akshare_init_service = AKShareInitService()
        await _akshare_init_service.initialize()
    return _akshare_init_service


#Initialise task function for APSscheduler compatibility
async def run_akshare_full_initialization(
    historical_days: int = 365,
    skip_if_exists: bool = True
):
    """APScheduler mission: Initialization of fully run AKShare data"""
    try:
        service = await get_akshare_init_service()
        result = await service.run_full_initialization(
            historical_days=historical_days,
            skip_if_exists=skip_if_exists
        )
        logger.info(f"The complete initialization of AKShare is complete:{result}")
        return result
    except Exception as e:
        logger.error(f"The complete initialization of AKShare failed:{e}")
        raise
