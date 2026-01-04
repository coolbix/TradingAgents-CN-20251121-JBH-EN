#!/usr/bin/env python3
"""BaoStock Data Sync Service
Provide batch synchronisation of BaoStock data, integrated into the APScheduler dispatch system
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from app.core.config import get_settings
from app.core.database import get_database
from app.services.historical_data_service import get_historical_data_service
from tradingagents.dataflows.providers.china.baostock import BaoStockProvider

logger = logging.getLogger(__name__)


@dataclass
class BaoStockSyncStats:
    """BaoStock Sync Statistics"""
    basic_info_count: int = 0
    quotes_count: int = 0
    historical_records: int = 0
    financial_records: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class BaoStockSyncService:
    """BaoStock Data Sync Service"""

    def __init__(self):
        """Initializing Sync Service

        Note: Database connection initialized in initialize() method
        """
        try:
            self.settings = get_settings()
            self.provider = BaoStockProvider()
            self.historical_service = None  #Delay Initialization
            self.db = None  #🔥 Delayed initialization, set in initialize()

            logger.info("The BaoStock Sync Service was successfully initiated")
        except Exception as e:
            logger.error(f"The initialization of the BaoStock sync service failed:{e}")
            raise

    async def initialize(self):
        """Spacing Initialization Services"""
        try:
            #🔥 Initialised database connection (must be in an off-site context)
            from app.core.database import get_mongo_db
            self.db = get_mongo_db()

            #Initialization of historical data services
            if self.historical_service is None:
                from app.services.historical_data_service import get_historical_data_service
                self.historical_service = await get_historical_data_service()

            logger.info("BaoStock Sync Service Initialization Complete")
        except Exception as e:
            logger.error(f"The initial phase of BaoStock sync service failed:{e}")
            raise
    
    async def sync_stock_basic_info(self, batch_size: int = 100) -> BaoStockSyncStats:
        """Sync Equation Basic Information

        Args:
            Watch size: Batch size

        Returns:
            Sync Statistical Information
        """
        stats = BaoStockSyncStats()
        
        try:
            logger.info("Start synchronizing BaoSstock Basic Information...")
            
            #Get Stock List
            stock_list = await self.provider.get_stock_list()
            if not stock_list:
                logger.warning("BaoStock list is empty")
                return stats
            
            logger.info(f"Other Organiser{len(stock_list)}Stock only, start batch sync...")
            
            #Batch processing
            for i in range(0, len(stock_list), batch_size):
                batch = stock_list[i:i + batch_size]
                batch_stats = await self._sync_basic_info_batch(batch)
                
                stats.basic_info_count += batch_stats.basic_info_count
                stats.errors.extend(batch_stats.errors)
                
                logger.info(f"Progress of batch:{i + len(batch)}/{len(stock_list)}, "
                          f"Success:{batch_stats.basic_info_count}, "
                          f"Error:{len(batch_stats.errors)}")
                
                #Avoid API Limit
                await asyncio.sleep(0.1)
            
            logger.info(f"BaoStock basic information is synchronised:{stats.basic_info_count}Notes")
            return stats
            
        except Exception as e:
            logger.error(f"BaoStock base information synchronised:{e}")
            stats.errors.append(str(e))
            return stats
    
    async def _sync_basic_info_batch(self, stock_batch: List[Dict[str, Any]]) -> BaoStockSyncStats:
        """Synchronization of basic information batches (including valuation data and total market value)"""
        stats = BaoStockSyncStats()

        for stock in stock_batch:
            try:
                code = stock['code']

                #1. Access to basic information
                basic_info = await self.provider.get_stock_basic_info(code)

                if not basic_info:
                    stats.errors.append(f"获取{code}基础信息失败")
                    continue

                #2. Acquisition of valuation data (PE, PB, PS, PCF, etc.)
                try:
                    valuation_data = await self.provider.get_valuation_data(code)
                    if valuation_data:
                        #Consolidated valuation data to basic information
                        basic_info['pe'] = valuation_data.get('pe_ttm')  #Market gain (TTM)
                        basic_info['pb'] = valuation_data.get('pb_mrq')  #Net market rate (MRQ)
                        basic_info['pe_ttm'] = valuation_data.get('pe_ttm')
                        basic_info['pb_mrq'] = valuation_data.get('pb_mrq')
                        basic_info['ps'] = valuation_data.get('ps_ttm')  #Marketing rate
                        basic_info['pcf'] = valuation_data.get('pcf_ttm')  #Current rate
                        basic_info['close'] = valuation_data.get('close')  #Recent prices

                        #3. Calculation of total market value (total equity required)
                        close_price = valuation_data.get('close')
                        if close_price and close_price > 0:
                            #Attempt to obtain total equity from financial data
                            total_shares_wan = await self._get_total_shares(code)
                            if total_shares_wan and total_shares_wan > 0:
                                #Total market value (billions of dollars) = gross equity (millions) x 10000
                                total_mv_yi = (close_price * total_shares_wan) / 10000
                                basic_info['total_mv'] = total_mv_yi
                                logger.debug(f"✅ {code}Total market value calculated:{close_price}Dollar x{total_shares_wan}Ten thousand shares / 10000 ={total_mv_yi:.2f}Billions.")
                            else:
                                logger.debug(f"⚠️ {code}Unable to obtain total equity, skip market value calculations")

                        logger.debug(f"✅ {code}Valuation data: PE={basic_info.get('pe')}, PB={basic_info.get('pb')}market value ={basic_info.get('total_mv')}")
                except Exception as e:
                    logger.warning(f"Access{code}Valuation data failed:{e}")
                    #Failure to obtain valuation data does not affect the synchronization of basic information

                #Updating the database
                await self._update_stock_basic_info(basic_info)
                stats.basic_info_count += 1

            except Exception as e:
                stats.errors.append(f"处理{stock.get('code', 'unknown')}失败: {e}")

        return stats
    
    async def _get_total_shares(self, code: str) -> Optional[float]:
        """Acquisition of gross equity (millions of shares)

        Args:
            code: stock code

        Returns:
            Total equity (one million shares) if no one returns
        """
        try:
            #Attempt to obtain total equity from financial data
            financial_data = await self.provider.get_financial_data(code)

            if financial_data:
                #Total equity fields in BaoStock financial data
                #TotalShare (total equity, in 10,000 shares)
                profit_data = financial_data.get('profit_data', {})
                if profit_data:
                    total_shares = profit_data.get('totalShare')
                    if total_shares:
                        return self._safe_float(total_shares)

                #There may also be total equity in growth data.
                growth_data = financial_data.get('growth_data', {})
                if growth_data:
                    total_shares = growth_data.get('totalShare')
                    if total_shares:
                        return self._safe_float(total_shares)

            #If financial data are not available, try to obtain from data available in the database
            collection = self.db.stock_financial_data
            doc = await collection.find_one(
                {"code": code},
                {"total_shares": 1, "totalShare": 1},
                sort=[("report_period", -1)]
            )

            if doc:
                total_shares = doc.get('total_shares') or doc.get('totalShare')
                if total_shares:
                    return self._safe_float(total_shares)

            return None

        except Exception as e:
            logger.debug(f"Access{code}Total equity failed:{e}")
            return None

    def _safe_float(self, value) -> Optional[float]:
        """Convert safe to floating point"""
        try:
            if value is None or value == '' or value == 'None':
                return None
            return float(value)
        except (ValueError, TypeError):
            return None

    async def _update_stock_basic_info(self, basic_info: Dict[str, Any]):
        """Update stock base information to database"""
        try:
            collection = self.db.stock_basic_info

            #Ensure that symbol fields exist (standardized fields)
            if "symbol" not in basic_info and "code" in basic_info:
                basic_info["symbol"] = basic_info["code"]

            #Make sure field exists
            if "source" not in basic_info:
                basic_info["source"] = "baostock"

            #Use (code, source) of joint query conditions
            await collection.update_one(
                {"code": basic_info["code"], "source": "baostock"},
                {"$set": basic_info},
                upsert=True
            )

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            raise
    
    async def sync_daily_quotes(self, batch_size: int = 50) -> BaoStockSyncStats:
        """Sync day K-line data (latest transaction date)

        Note: BaoStock does not support real-time lines, which captures Japanese-K-line data on the latest transaction date

        Args:
            Watch size: Batch size

        Returns:
            Sync Statistical Information
        """
        stats = BaoStockSyncStats()

        try:
            logger.info("Start the BaoStock-K-line sync.")
            logger.info("Note: BaoStock does not support real-time travel, and this task syncs day K-line data on the latest transaction date")

            #Retrieving stock lists from databases
            collection = self.db.stock_basic_info
            cursor = collection.find({"data_source": "baostock"}, {"code": 1})
            stock_codes = [doc["code"] async for doc in cursor]

            if not stock_codes:
                logger.warning("⚠️ database does not contain BaoStock stock data")
                return stats

            logger.info(f"Synchronize{len(stock_codes)}Stock-only Japanese-K-line data...")

            #Batch processing
            for i in range(0, len(stock_codes), batch_size):
                batch = stock_codes[i:i + batch_size]
                batch_stats = await self._sync_quotes_batch(batch)

                stats.quotes_count += batch_stats.quotes_count
                stats.errors.extend(batch_stats.errors)

                logger.info(f"Progress of batch:{i + len(batch)}/{len(stock_codes)}, "
                          f"Success:{batch_stats.quotes_count}, "
                          f"Error:{len(batch_stats.errors)}")

                #Avoid API Limit
                await asyncio.sleep(0.2)

            logger.info(f"BaoStockKline synchronised:{stats.quotes_count}Notes")
            return stats

        except Exception as e:
            logger.error(f"BaoStockKline failed:{e}")
            stats.errors.append(str(e))
            return stats
    
    async def _sync_quotes_batch(self, code_batch: List[str]) -> BaoStockSyncStats:
        """Sync day K-line batch"""
        stats = BaoStockSyncStats()

        for code in code_batch:
            try:
                #Note: Get stock quotes actually returns the latest day K-line data, not real time patterns
                quotes = await self.provider.get_stock_quotes(code)

                if quotes:
                    #Update database
                    await self._update_stock_quotes(quotes)
                    stats.quotes_count += 1
                else:
                    stats.errors.append(f"获取{code}日K线失败")

            except Exception as e:
                stats.errors.append(f"处理{code}日K线失败: {e}")

        return stats

    async def _update_stock_quotes(self, quotes: Dict[str, Any]):
        """Update stock day Kline to database"""
        try:
            collection = self.db.market_quotes

            #Ensure that the symbol field exists
            code = quotes.get("code", "")
            if code and "symbol" not in quotes:
                quotes["symbol"] = code

            #Update or insert withupsert
            await collection.update_one(
                {"code": code},
                {"$set": quotes},
                upsert=True
            )

        except Exception as e:
            logger.error(f"Update day Kline to database failed:{e}")
            raise
    
    async def sync_historical_data(self, days: int = 30, batch_size: int = 20, period: str = "daily", incremental: bool = True) -> BaoStockSyncStats:
        """Sync Historical Data

        Args:
            Days: Synchronization Days (if > = 3650 sync all history, if <0 use incremental mode)
            Watch size: Batch size
            period: data cycle (daily/weekly/montly)
            Incremental: Does the incremental synchronize (each stock starts on its own final date)

        Returns:
            Sync Statistical Information
        """
        stats = BaoStockSyncStats()

        try:
            period_name = {"daily": "日线", "weekly": "周线", "monthly": "月线"}.get(period, "日线")

            #Calculate Date Range
            end_date = datetime.now().strftime('%Y-%m-%d')

            #Determine Sync Mode
            use_incremental = incremental or days < 0

            #Retrieving stock lists from databases
            collection = self.db.stock_basic_info
            cursor = collection.find({"data_source": "baostock"}, {"code": 1})
            stock_codes = [doc["code"] async for doc in cursor]

            if not stock_codes:
                logger.warning("⚠️ database does not contain BaoStock stock data")
                return stats

            if use_incremental:
                logger.info(f"Here we go.{period_name}Historical Data Sync (Incremental Mode: Equities from Last Date to{end_date})...")
            elif days >= 3650:
                logger.info(f"Here we go.{period_name}Synchronization of historical data{end_date})...")
            else:
                logger.info(f"Here we go.{period_name}Synchronization of historical data (recent){days}God damn it!{end_date})...")

            logger.info(f"Synchronize{len(stock_codes)}Only stock history...")

            #Batch processing
            for i in range(0, len(stock_codes), batch_size):
                batch = stock_codes[i:i + batch_size]
                batch_stats = await self._sync_historical_batch(batch, days, end_date, period, use_incremental)
                
                stats.historical_records += batch_stats.historical_records
                stats.errors.extend(batch_stats.errors)
                
                logger.info(f"Progress of batch:{i + len(batch)}/{len(stock_codes)}, "
                          f"Records:{batch_stats.historical_records}, "
                          f"Error:{len(batch_stats.errors)}")
                
                #Avoid API Limit
                await asyncio.sleep(0.5)
            
            logger.info(f"BaoStock has synchronised:{stats.historical_records}Notes")
            return stats
            
        except Exception as e:
            logger.error(f"BaoStock history data sync failed:{e}")
            stats.errors.append(str(e))
            return stats
    
    async def _sync_historical_batch(
        self,
        code_batch: List[str],
        days: int,
        end_date: str,
        period: str = "daily",
        incremental: bool = False
    ) -> BaoStockSyncStats:
        """Synchronize historical data batches"""
        stats = BaoStockSyncStats()

        for code in code_batch:
            try:
                #Determine the start date of the stock
                if incremental:
                    #Incremental sync: due date for acquisition of the stock
                    start_date = await self._get_last_sync_date(code)
                    logger.debug(f"📅 {code}From:{start_date}Start Synchronization")
                elif days >= 3650:
                    #All History Sync
                    start_date = "1990-01-01"
                else:
                    #Fixed Day Sync
                    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

                hist_data = await self.provider.get_historical_data(code, start_date, end_date, period)

                if hist_data is not None and not hist_data.empty:
                    #Update database
                    records_count = await self._update_historical_data(code, hist_data, period)
                    stats.historical_records += records_count
                else:
                    stats.errors.append(f"获取{code}历史数据失败")

            except Exception as e:
                stats.errors.append(f"处理{code}历史数据失败: {e}")

        return stats

    async def _update_historical_data(self, code: str, hist_data, period: str = "daily") -> int:
        """Update historical data to database"""
        try:
            if hist_data is None or hist_data.empty:
                logger.warning(f"⚠️ {code}History data empty, skip saving")
                return 0

            #Initialization of historical data services
            if self.historical_service is None:
                self.historical_service = await get_historical_data_service()

            #Save to Unified Historical Data Collection
            saved_count = await self.historical_service.save_historical_data(
                symbol=code,
                data=hist_data,
                data_source="baostock",
                market="CN",
                period=period
            )

            #Also update meta-information on the market quotes collection (maintain compatibility)
            if self.db is not None:
                collection = self.db.market_quotes
                latest_record = hist_data.iloc[-1] if not hist_data.empty else None

                await collection.update_one(
                    {"code": code},
                    {"$set": {
                        "historical_data_updated": datetime.now(),
                        "latest_historical_date": latest_record.get('date') if latest_record is not None else None,
                        "historical_records_count": saved_count
                    }},
                    upsert=True
                )

            return saved_count

        except Exception as e:
            logger.error(f"Update of historical data to database failed:{e}")
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
                latest_date = await self.historical_service.get_latest_date(symbol, "baostock")
                if latest_date:
                    #Return to the next day of the final date (duplicate)
                    try:
                        last_date_obj = datetime.strptime(latest_date, '%Y-%m-%d')
                        next_date = last_date_obj + timedelta(days=1)
                        return next_date.strftime('%Y-%m-%d')
                    except ValueError:
                        #If the date is not formatted correctly, return directly
                        return latest_date

            #Default returns 30 days ago (ensure that data are not missing)
            return (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{symbol}: {e}")
            #Returns 30 days before error to ensure that data is not missing
            return (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    async def check_service_status(self) -> Dict[str, Any]:
        """Check service status"""
        try:
            #Test BaoStock Connection
            connection_ok = await self.provider.test_connection()
            
            #Check database connections
            db_ok = True
            try:
                await self.db.stock_basic_info.count_documents({})
            except Exception:
                db_ok = False
            
            #Statistics
            basic_info_count = await self.db.stock_basic_info.count_documents({"data_source": "baostock"})
            quotes_count = await self.db.market_quotes.count_documents({"data_source": "baostock"})
            
            return {
                "service": "BaoStock同步服务",
                "baostock_connection": connection_ok,
                "database_connection": db_ok,
                "basic_info_count": basic_info_count,
                "quotes_count": quotes_count,
                "status": "healthy" if connection_ok and db_ok else "unhealthy",
                "last_check": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"BaoStock service check failed:{e}")
            return {
                "service": "BaoStock同步服务",
                "status": "error",
                "error": str(e),
                "last_check": datetime.now().isoformat()
            }


#Task Functions compatible with APSscheduler
async def run_baostock_basic_info_sync():
    """Synchronising Task for BaoStock Basic Information"""
    try:
        service = BaoStockSyncService()
        await service.initialize()  #It has to be initialized.
        stats = await service.sync_stock_basic_info()
        logger.info(f"BaoStock basic information is synchronised:{stats.basic_info_count}The record,{len(stats.errors)}A mistake.")
    except Exception as e:
        logger.error(f"BaoStock Basic Information Synchronization failed:{e}")


async def run_baostock_daily_quotes_sync():
    """Run BaoStockKline Sync Task (late transaction date)"""
    try:
        service = BaoStockSyncService()
        await service.initialize()  #It has to be initialized.
        stats = await service.sync_daily_quotes()
        logger.info(f"BaoStockKline synchronised:{stats.quotes_count}The record,{len(stats.errors)}A mistake.")
    except Exception as e:
        logger.error(f"BaoStockKline sync failed:{e}")


async def run_baostock_historical_sync():
    """Synchronising Task for BaoStock Historical Data"""
    try:
        service = BaoStockSyncService()
        await service.initialize()  #It has to be initialized.
        stats = await service.sync_historical_data()
        logger.info(f"BaoStock has synchronised:{stats.historical_records}The record,{len(stats.errors)}A mistake.")
    except Exception as e:
        logger.error(f"BaoStock history data sync failed:{e}")


async def run_baostock_status_check():
    """Run a BaoStock status check job"""
    try:
        service = BaoStockSyncService()
        await service.initialize()  #It has to be initialized.
        status = await service.check_service_status()
        logger.info(f"BaoStock service status:{status['status']}")
    except Exception as e:
        logger.error(f"BaoStock failed:{e}")
