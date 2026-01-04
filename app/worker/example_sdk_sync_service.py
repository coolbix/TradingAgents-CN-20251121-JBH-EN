"""Example: SDK Data Sync Service (app layer)
Show how to create a data synchronisation service to write external SDK data to a standardized MongoDB collection

Structure description:
- Tradingagents Layer: Pure data acquisition and standardization, not involving database operations
- App layer: Data Synchronization Service, responsible for database operations and business logic
- Data stream: external SDK → Tradingagents adapter → app sync service → MongoDB
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import os
from app.services.stock_data_service import get_stock_data_service
from app.core.database import get_mongo_db
from tradingagents.dataflows.providers.examples.example_sdk import ExampleSDKProvider

logger = logging.getLogger(__name__)


class ExampleSDKSyncService:
    """Example: SDK Data Sync Service (app layer)

    Duties:
    - Calling SDK adapters on the TradingAGents floor for standardized data
    - Implementation of business logic processing and data validation
    - Write data to the MongoDB database
    - Manage synchronization and error processing
    - Provide performance monitoring and statistics

    Structure layer:
    -Traditions/dataworks/: Pure data acquisition adapter
    -app/worker/: Data Synchronization Service (this category)
    -app/services/: Data access services
    """

    def __init__(self):
        #Appendant using trapping layers (pure data acquisition)
        self.provider = ExampleSDKProvider()
        #Use the data service (database operation) on the app layer
        self.stock_service = get_stock_data_service()
        
        #Sync Configuration
        self.batch_size = int(os.getenv("EXAMPLE_SDK_BATCH_SIZE", "100"))
        self.retry_times = int(os.getenv("EXAMPLE_SDK_RETRY_TIMES", "3"))
        self.retry_delay = int(os.getenv("EXAMPLE_SDK_RETRY_DELAY", "5"))
        
        #Statistical information
        self.sync_stats = {
            "basic_info": {"total": 0, "success": 0, "failed": 0},
            "quotes": {"total": 0, "success": 0, "failed": 0},
            "financial": {"total": 0, "success": 0, "failed": 0}
        }
    
    async def sync_all_data(self):
        """Sync all data"""
        logger.info("Start ExampleSDK FullSync...")
        
        start_time = datetime.now()
        
        try:
            #Connect data sources
            if not await self.provider.connect():
                logger.error("ExampleSDK connection failed. Sync aborted")
                return False
            
            #Sync Basic Information
            await self.sync_basic_info()
            
            #Sync Real Time Line
            await self.sync_realtime_quotes()
            
            #Sync Financial Data
            await self.sync_financial_data()
            
            #Record Sync Status
            await self._record_sync_status("success", start_time)
            
            logger.info("ExampleSDK full synchronised")
            self._log_sync_stats()
            
            return True
            
        except Exception as e:
            logger.error(f"ExampleSDK data sync failed:{e}")
            await self._record_sync_status("failed", start_time, str(e))
            return False
            
        finally:
            await self.provider.disconnect()
    
    async def sync_basic_info(self):
        """Sync Equation Basic Information"""
        logger.info("Start syncing stock base information...")
        
        try:
            #Get Stock List
            stock_list = await self.provider.get_stock_list()
            
            if not stock_list:
                logger.warning("⚠️ Unretrieved list of shares")
                return
            
            self.sync_stats["basic_info"]["total"] = len(stock_list)
            
            #Batch processing
            for i in range(0, len(stock_list), self.batch_size):
                batch = stock_list[i:i + self.batch_size]
                await self._process_basic_info_batch(batch)
                
                #Progress Log
                processed = min(i + self.batch_size, len(stock_list))
                logger.info(f"📈Sync progress of basic information:{processed}/{len(stock_list)}")
                
                #Avoid API Limit
                await asyncio.sleep(0.1)
            
            logger.info(f"✅Equal basic information synchronized:{self.sync_stats['basic_info']['success']}/{self.sync_stats['basic_info']['total']}")
            
        except Exception as e:
            logger.error(f"❌SystemSync failed:{e}")
    
    async def sync_realtime_quotes(self):
        """Sync Real Time Line"""
        logger.info("Commencing synchronous real-time behavior...")
        
        try:
            #Fetch list of stock codes that need to be synchronized
            db = get_mongo_db()
            cursor = db.stock_basic_info.find({}, {"code": 1})
            stock_codes = [doc["code"] async for doc in cursor]
            
            if not stock_codes:
                logger.warning("No shares requiring walk-by.")
                return
            
            self.sync_stats["quotes"]["total"] = len(stock_codes)
            
            #Batch processing
            for i in range(0, len(stock_codes), self.batch_size):
                batch = stock_codes[i:i + self.batch_size]
                await self._process_quotes_batch(batch)
                
                #Progress Log
                processed = min(i + self.batch_size, len(stock_codes))
                logger.info(f"Real-time synchronisation progress:{processed}/{len(stock_codes)}")
                
                #Avoid API Limit
                await asyncio.sleep(0.1)
            
            logger.info(f"✅ Timeline sync completed:{self.sync_stats['quotes']['success']}/{self.sync_stats['quotes']['total']}")
            
        except Exception as e:
            logger.error(f"Real-time line sync failed:{e}")
    
    async def sync_financial_data(self):
        """Sync Financial Data"""
        logger.info("Start synchronizing financial data...")
        
        try:
            #Access to equities requiring updated financial data
            #This can be filtered against business needs, for example, only synchronized major stocks or updated regularly.
            db = get_mongo_db()
            cursor = db.stock_basic_info.find(
                {"total_mv": {"$gte": 100}},  #Only synchronized shares with market value greater than $10 billion
                {"code": 1}
            ).limit(50)  #Limit numbers to avoid over-allocation of API
            
            stock_codes = [doc["code"] async for doc in cursor]
            
            if not stock_codes:
                logger.warning("⚠️ Equities requiring simultaneous financial data were not found")
                return
            
            self.sync_stats["financial"]["total"] = len(stock_codes)
            
            #Individually (financial data are usually more restrictive)
            for code in stock_codes:
                await self._process_financial_data(code)
                await asyncio.sleep(1)  #Longer delay
            
            logger.info(f"Synchronization of financial data:{self.sync_stats['financial']['success']}/{self.sync_stats['financial']['total']}")
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
    
    async def _process_basic_info_batch(self, batch: List[Dict[str, Any]]):
        """Process basic information batch"""
        for stock_info in batch:
            try:
                code = stock_info.get("code")
                if not code:
                    continue
                
                #Update to Database
                success = await self.stock_service.update_stock_basic_info(code, stock_info)
                
                if success:
                    self.sync_stats["basic_info"]["success"] += 1
                else:
                    self.sync_stats["basic_info"]["failed"] += 1
                    logger.warning(f"Update{code}Basic information failed")
                    
            except Exception as e:
                self.sync_stats["basic_info"]["failed"] += 1
                logger.error(f"Treatment{stock_info.get('code', 'N/A')}Could not close temporary folder: %s{e}")
    
    async def _process_quotes_batch(self, batch: List[str]):
        """Processing line batches"""
        for code in batch:
            try:
                #Get Real Time Lines
                quotes = await self.provider.get_stock_quotes(code)
                
                if quotes:
                    #Update to Database
                    success = await self.stock_service.update_market_quotes(code, quotes)
                    
                    if success:
                        self.sync_stats["quotes"]["success"] += 1
                    else:
                        self.sync_stats["quotes"]["failed"] += 1
                        logger.warning(f"Update{code}It's a failure.")
                else:
                    self.sync_stats["quotes"]["failed"] += 1
                    
            except Exception as e:
                self.sync_stats["quotes"]["failed"] += 1
                logger.error(f"Treatment{code}Project failure:{e}")
    
    async def _process_financial_data(self, code: str):
        """Processing of financial data"""
        try:
            #Access to financial data
            financial_data = await self.provider.get_financial_data(code)
            
            if financial_data:
                #There is a need to realize the logic of financial data storage.
                #Could need to create a new collection
                db = get_mongo_db()
                
                #Build Update Data
                update_data = {
                    "code": code,
                    "financial_data": financial_data,
                    "updated_at": datetime.utcnow()
                }
                
                #Update or insert financial data
                await db.stock_financial_data.update_one(
                    {"code": code},
                    {"$set": update_data},
                    upsert=True
                )
                
                self.sync_stats["financial"]["success"] += 1
                logger.debug(f"Update{code}Financial data successfully")
            else:
                self.sync_stats["financial"]["failed"] += 1
                
        except Exception as e:
            self.sync_stats["financial"]["failed"] += 1
            logger.error(f"Treatment{code}Financial data failed:{e}")
    
    async def _record_sync_status(self, status: str, start_time: datetime, error_msg: str = None):
        """Record Sync Status"""
        try:
            db = get_mongo_db()
            
            sync_record = {
                "job": "example_sdk_sync",
                "status": status,
                "started_at": start_time,
                "finished_at": datetime.now(),
                "duration": (datetime.now() - start_time).total_seconds(),
                "stats": self.sync_stats.copy(),
                "error_message": error_msg,
                "created_at": datetime.now()
            }
            
            await db.sync_status.update_one(
                {"job": "example_sdk_sync"},
                {"$set": sync_record},
                upsert=True
            )
            
        except Exception as e:
            logger.error(f"❌ to record a synchronous state failure:{e}")
    
    def _log_sync_stats(self):
        """Record Sync Statistical Information"""
        logger.info("ExampleSDK Sync Statistics:")
        for data_type, stats in self.sync_stats.items():
            total = stats["total"]
            success = stats["success"]
            failed = stats["failed"]
            success_rate = (success / total * 100) if total > 0 else 0
            
            logger.info(f"   {data_type}: {success}/{total} ({success_rate:.1f}% successful,{failed}Failed")
    
    async def sync_incremental(self):
        """Incremental Sync - only in real time"""
        logger.info("Starting ExampleSDK Incremental Synchronization...")
        
        try:
            if not await self.provider.connect():
                logger.error("❌ExampleSDK connection failed, incremental sync aborted")
                return False
            
            #Sync only real-time lines
            await self.sync_realtime_quotes()
            
            logger.info("ExampleSDK incrementally synchronised")
            return True
            
        except Exception as e:
            logger.error(f"ExampleSDK's incremental sync failed:{e}")
            return False
            
        finally:
            await self.provider.disconnect()


#== sync, corrected by elderman == @elder man

async def run_full_sync():
    """Run FullSync - For Time Task Call"""
    sync_service = ExampleSDKSyncService()
    return await sync_service.sync_all_data()


async def run_incremental_sync():
    """Run Incremental Sync - For Time Task Call"""
    sync_service = ExampleSDKSyncService()
    return await sync_service.sync_incremental()


#== sync, corrected by elderman == @elder man

async def main():
    """Main function - for testing"""
    logging.basicConfig(level=logging.INFO)
    
    sync_service = ExampleSDKSyncService()
    
    #Test FullSync
    await sync_service.sync_all_data()


if __name__ == "__main__":
    asyncio.run(main())
