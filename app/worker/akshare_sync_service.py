"""AKShare Data Sync Service
Harmonized Data Synchronization Program based on AKShare Providers
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from app.core.database import get_mongo_db
from app.services.historical_data_service import get_historical_data_service
from app.services.news_data_service import get_news_data_service
from tradingagents.dataflows.providers.china.akshare import get_akshare_provider

logger = logging.getLogger(__name__)


class AKShareSyncService:
    """AKShare Data Sync Service

    Provide complete data synchronisation:
    - Synchronization of stock base information
    - Real-time line sync
    - Synchronization of historical data
    - Synchronizing financial data
    """
    
    def __init__(self):
        self.provider = None
        self.historical_service = None  #Delay Initialization
        self.news_service = None  #Delay Initialization
        self.db = None
        self.batch_size = 100
        self.rate_limit_delay = 0.2  #Delay recommended by Akshare
    
    async def initialize(self):
        """Initializing Sync Service"""
        try:
            #Initialize database connection
            self.db = get_mongo_db()

            #Initialization of historical data services
            self.historical_service = await get_historical_data_service()

            #Initialization of news data services
            self.news_service = await get_news_data_service()

            #Initialization of AKShare Provider (use of a global, single example to ensure that Monkey Patch is effective)
            self.provider = get_akshare_provider()

            #Test Connection
            if not await self.provider.test_connection():
                raise RuntimeError("❌ AKShare连接失败，无法启动同步服务")

            logger.info("Initialization of the AKShare Sync Service completed")
            
        except Exception as e:
            logger.error(f"The initialization of the AKShare sync service failed:{e}")
            raise
    
    async def sync_stock_basic_info(self, force_update: bool = False) -> Dict[str, Any]:
        """Sync Equation Basic Information

        Args:
            Force update

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
            "end_time": None,
            "duration": 0,
            "errors": []
        }
        
        try:
            #1. Taking stock lists
            stock_list = await self.provider.get_stock_list()
            if not stock_list:
                logger.warning("⚠️ Unretrieved list of shares")
                return stats
            
            stats["total_processed"] = len(stock_list)
            logger.info(f"Other Organiser{len(stock_list)}Stock information only")
            
            #2. Batch processing
            for i in range(0, len(stock_list), self.batch_size):
                batch = stock_list[i:i + self.batch_size]
                batch_stats = await self._process_basic_info_batch(batch, force_update)
                
                #Update statistics
                stats["success_count"] += batch_stats["success_count"]
                stats["error_count"] += batch_stats["error_count"]
                stats["skipped_count"] += batch_stats["skipped_count"]
                stats["errors"].extend(batch_stats["errors"])
                
                #Progress Log
                progress = min(i + self.batch_size, len(stock_list))
                logger.info(f"📈Sync progress of basic information:{progress}/{len(stock_list)} "
                           f"(success:{stats['success_count']}, Error:{stats['error_count']})")
                
                #API limit flow
                if i + self.batch_size < len(stock_list):
                    await asyncio.sleep(self.rate_limit_delay)
            
            #3. Completion of statistics
            stats["end_time"] = datetime.utcnow()
            stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()
            
            logger.info(f"The stock base is synchronized!")
            logger.info(f"Total:{stats['total_processed']}Only,"
                       f"Success:{stats['success_count']}, "
                       f"Error:{stats['error_count']}, "
                       f"Skip:{stats['skipped_count']}, "
                       f"Time-consuming:{stats['duration']:.2f}sec")
            
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
                code = stock_info["code"]
                
                #Check for updates
                if not force_update:
                    existing = await self.db.stock_basic_info.find_one({"code": code})
                    if existing and self._is_data_fresh(existing.get("updated_at"), hours=24):
                        batch_stats["skipped_count"] += 1
                        continue
                
                #Access to detailed basic information
                basic_info = await self.provider.get_stock_basic_info(code)
                
                if basic_info:
                    #Convert to Dictionary Format
                    if hasattr(basic_info, 'model_dump'):
                        basic_data = basic_info.model_dump()
                    elif hasattr(basic_info, 'dict'):
                        basic_data = basic_info.dict()
                    else:
                        basic_data = basic_info
                    
                    #Make sure field exists
                    if "source" not in basic_data:
                        basic_data["source"] = "akshare"

                    #Make sure the symbol field exists
                    if "symbol" not in basic_data:
                        basic_data["symbol"] = code

                    #Update to database (with code + source query)
                    try:
                        await self.db.stock_basic_info.update_one(
                            {"code": code, "source": "akshare"},
                            {"$set": basic_data},
                            upsert=True
                        )
                        batch_stats["success_count"] += 1
                    except Exception as e:
                        batch_stats["error_count"] += 1
                        batch_stats["errors"].append({
                            "code": code,
                            "error": f"数据库更新失败: {str(e)}",
                            "context": "update_stock_basic_info"
                        })
                else:
                    batch_stats["error_count"] += 1
                    batch_stats["errors"].append({
                        "code": code,
                        "error": "获取基础信息失败",
                        "context": "get_stock_basic_info"
                    })
                
            except Exception as e:
                batch_stats["error_count"] += 1
                batch_stats["errors"].append({
                    "code": stock_info.get("code", "unknown"),
                    "error": str(e),
                    "context": "_process_basic_info_batch"
                })
        
        return batch_stats
    
    def _is_data_fresh(self, updated_at: Any, hours: int = 24) -> bool:
        """Check if the data is fresh."""
        if not updated_at:
            return False
        
        try:
            if isinstance(updated_at, str):
                updated_at = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
            elif isinstance(updated_at, datetime):
                pass
            else:
                return False
            
            #Convert to UTC time for comparison
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=None)
            else:
                updated_at = updated_at.replace(tzinfo=None)
            
            now = datetime.utcnow()
            time_diff = now - updated_at
            
            return time_diff.total_seconds() < (hours * 3600)
            
        except Exception as e:
            logger.debug(f"Could not close temporary folder: %s{e}")
            return False
    
    async def sync_realtime_quotes(self, symbols: List[str] = None, force: bool = False) -> Dict[str, Any]:
        """Sync Real Time Line Data

        Args:
            symbols: specify a list of stock codes and synchronize all stocks as empty
            force: enforcement ( Skip transaction time check), default False

        Returns:
            Sync Results Statistics
        """
        #If a list of shares is specified, logs
        if symbols:
            logger.info(f"🔄 Start synchronizing the real-time relationship of specified shares{len(symbols)}Only:{symbols}")
        else:
            logger.info("We'll start synchronizing the whole market for real time...")

        stats = {
            "total_processed": 0,
            "success_count": 0,
            "error_count": 0,
            "start_time": datetime.utcnow(),
            "end_time": None,
            "duration": 0,
            "errors": []
        }

        try:
            #1. Identification of shares to synchronize
            if symbols is None:
                #Obtain all listed stock codes from the database (exclusion of refunded stocks)
                basic_info_cursor = self.db.stock_basic_info.find(
                    {"list_status": "L"},  #Get only listed shares
                    {"code": 1}
                )
                symbols = [doc["code"] async for doc in basic_info_cursor]

            if not symbols:
                logger.warning("No shares to synchronize.")
                return stats

            stats["total_processed"] = len(symbols)
            logger.info(f"Ready to sync.{len(symbols)}Only stock.")

            #Optimization: if only one stock is synchronized, call directly to the single stock interface without taking a batch interface
            if len(symbols) == 1:
                logger.info(f"📈Sync for single stocks, directly using the get stock quotes interface")
                symbol = symbols[0]
                success = await self._get_and_save_quotes(symbol)
                if success:
                    stats["success_count"] = 1
                else:
                    stats["error_count"] = 1
                    stats["errors"].append({
                        "code": symbol,
                        "error": "获取行情失败",
                        "context": "sync_realtime_quotes_single"
                    })

                logger.info(f"📈Sync progress: 1/1 (successful:{stats['success_count']}, Error:{stats['error_count']})")
            else:
                #2. Batch synchronization: one-time acquisition of market-wide snapshots (avoid multiple calls of interfaces restricted)
                logger.info("Get a market-wide real-time picture...")
                quotes_map = await self.provider.get_batch_stock_quotes(symbols)

                if not quotes_map:
                    logger.warning("Getting a full-market snapshot failed, back to the one-by-one mode.")
                    #Back to Retrieving Mode
                    for i in range(0, len(symbols), self.batch_size):
                        batch = symbols[i:i + self.batch_size]
                        batch_stats = await self._process_quotes_batch_fallback(batch)

                        #Update statistics
                        stats["success_count"] += batch_stats["success_count"]
                        stats["error_count"] += batch_stats["error_count"]
                        stats["errors"].extend(batch_stats["errors"])

                        #Progress Log
                        progress = min(i + self.batch_size, len(symbols))
                        logger.info(f"Synchronization progress:{progress}/{len(symbols)} "
                                   f"(success:{stats['success_count']}, Error:{stats['error_count']})")

                        #API limit flow
                        if i + self.batch_size < len(symbols):
                            await asyncio.sleep(self.rate_limit_delay)
                else:
                    #Use of acquired market-wide data to save data in batches Library
                    logger.info(f"Other Organiser{len(quotes_map)}Only stock data, start saving...")

                    for i in range(0, len(symbols), self.batch_size):
                        batch = symbols[i:i + self.batch_size]

                        #Extract and save current batch data from market-wide data
                        for symbol in batch:
                            try:
                                quotes = quotes_map.get(symbol)
                                if quotes:
                                    #Convert to Dictionary Format
                                    if hasattr(quotes, 'model_dump'):
                                        quotes_data = quotes.model_dump()
                                    elif hasattr(quotes, 'dict'):
                                        quotes_data = quotes.dict()
                                    else:
                                        quotes_data = quotes

                                    #Ensure that symbol and code fields exist
                                    if "symbol" not in quotes_data:
                                        quotes_data["symbol"] = symbol
                                    if "code" not in quotes_data:
                                        quotes_data["code"] = symbol

                                    #Update to Database
                                    await self.db.market_quotes.update_one(
                                        {"code": symbol},
                                        {"$set": quotes_data},
                                        upsert=True
                                    )
                                    stats["success_count"] += 1
                                else:
                                    stats["error_count"] += 1
                                    stats["errors"].append({
                                        "code": symbol,
                                        "error": "未找到行情数据",
                                        "context": "sync_realtime_quotes"
                                    })
                            except Exception as e:
                                stats["error_count"] += 1
                                stats["errors"].append({
                                    "code": symbol,
                                    "error": str(e),
                                    "context": "sync_realtime_quotes"
                                })

                        #Progress Log
                        progress = min(i + self.batch_size, len(symbols))
                        logger.info(f"Save progress:{progress}/{len(symbols)} "
                                   f"(success:{stats['success_count']}, Error:{stats['error_count']})")

            #4. Completion of statistics
            stats["end_time"] = datetime.utcnow()
            stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

            logger.info(f"Real-time synchronised!")
            logger.info(f"Total:{stats['total_processed']}Only,"
                       f"Success:{stats['success_count']}, "
                       f"Error:{stats['error_count']}, "
                       f"Time-consuming:{stats['duration']:.2f}sec")

            return stats

        except Exception as e:
            logger.error(f"Real-time line sync failed:{e}")
            stats["errors"].append({"error": str(e), "context": "sync_realtime_quotes"})
            return stats
    
    async def _process_quotes_batch(self, batch: List[str]) -> Dict[str, Any]:
        """Processing batches - Optimized version: a market-wide snapshot at a time"""
        batch_stats = {
            "success_count": 0,
            "error_count": 0,
            "errors": []
        }

        try:
            #One-time acquisition of market-wide snapshots (avoid frequent calls to interfaces)
            logger.debug(f"Get a market-wide snapshot for processing.{len(batch)}Only stocks...")
            quotes_map = await self.provider.get_batch_stock_quotes(batch)

            if not quotes_map:
                logger.warning("Getting a full-market snapshot failed. Back to one by one.")
                #Back to the original pick-up mode.
                return await self._process_quotes_batch_fallback(batch)

            #Batch to Database
            for symbol in batch:
                try:
                    quotes = quotes_map.get(symbol)
                    if quotes:
                        #Convert to Dictionary Format
                        if hasattr(quotes, 'model_dump'):
                            quotes_data = quotes.model_dump()
                        elif hasattr(quotes, 'dict'):
                            quotes_data = quotes.dict()
                        else:
                            quotes_data = quotes

                        #Ensure that symbol and code fields exist
                        if "symbol" not in quotes_data:
                            quotes_data["symbol"] = symbol
                        if "code" not in quotes_data:
                            quotes_data["code"] = symbol

                        #Update to Database
                        await self.db.market_quotes.update_one(
                            {"code": symbol},
                            {"$set": quotes_data},
                            upsert=True
                        )
                        batch_stats["success_count"] += 1
                    else:
                        batch_stats["error_count"] += 1
                        batch_stats["errors"].append({
                            "code": symbol,
                            "error": "未找到行情数据",
                            "context": "_process_quotes_batch"
                        })
                except Exception as e:
                    batch_stats["error_count"] += 1
                    batch_stats["errors"].append({
                        "code": symbol,
                        "error": str(e),
                        "context": "_process_quotes_batch"
                    })

            return batch_stats

        except Exception as e:
            logger.error(f"Batch handling failed:{e}")
            #Back to the original pick-up mode.
            return await self._process_quotes_batch_fallback(batch)

    async def _process_quotes_batch_fallback(self, batch: List[str]) -> Dict[str, Any]:
        """Processing cases - Backup: individual access"""
        batch_stats = {
            "success_count": 0,
            "error_count": 0,
            "errors": []
        }

        #Retrieving line data on a case-by-case basis (adding delay avoidance frequency limit)
        for symbol in batch:
            try:
                success = await self._get_and_save_quotes(symbol)
                if success:
                    batch_stats["success_count"] += 1
                else:
                    batch_stats["error_count"] += 1
                    batch_stats["errors"].append({
                        "code": symbol,
                        "error": "获取行情数据失败",
                        "context": "_process_quotes_batch_fallback"
                    })

                #Add Delay Avoid Frequency Limit
                await asyncio.sleep(0.1)

            except Exception as e:
                batch_stats["error_count"] += 1
                batch_stats["errors"].append({
                    "code": symbol,
                    "error": str(e),
                    "context": "_process_quotes_batch_fallback"
                })

        return batch_stats
    
    async def _get_and_save_quotes(self, symbol: str) -> bool:
        """Get and save individual stock lines"""
        try:
            quotes = await self.provider.get_stock_quotes(symbol)
            if quotes:
                #Convert to Dictionary Format
                if hasattr(quotes, 'model_dump'):
                    quotes_data = quotes.model_dump()
                elif hasattr(quotes, 'dict'):
                    quotes_data = quotes.dict()
                else:
                    quotes_data = quotes

                #Ensure that the symbol field exists
                if "symbol" not in quotes_data:
                    quotes_data["symbol"] = symbol

                #Print data that is about to be saved to the database
                logger.info(f"Ready to save{symbol}Line to database:")
                logger.info(f"- The latest price (price):{quotes_data.get('price')}")
                logger.info(f"- Maximum price (high):{quotes_data.get('high')}")
                logger.info(f"- Minimum price (low):{quotes_data.get('low')}")
                logger.info(f"- Opening price (open):{quotes_data.get('open')}")
                logger.info(f"- yesterday's price (pre clos):{quotes_data.get('pre_close')}")
                logger.info(f"- Volume:{quotes_data.get('volume')}")
                logger.info(f"- Deal (amount):{quotes_data.get('amount')}")
                logger.info(f"- Change percent:{quotes_data.get('change_percent')}%")

                #Update to Database
                result = await self.db.market_quotes.update_one(
                    {"code": symbol},
                    {"$set": quotes_data},
                    upsert=True
                )

                logger.info(f"✅ {symbol}Lines saved to database{result.matched_count}, modified={result.modified_count}, upserted_id={result.upserted_id})")
                return True
            return False
        except Exception as e:
            logger.error(f"Access{symbol}Project failure:{e}", exc_info=True)
            return False

    async def sync_historical_data(
        self,
        start_date: str = None,
        end_date: str = None,
        symbols: List[str] = None,
        incremental: bool = True,
        period: str = "daily"
    ) -> Dict[str, Any]:
        """Sync Historical Data

        Args:
            Start date: Start date
            End date: End date
            symbols: Specify list of stock codes
            increment: Incremental sync
            period: data cycle (daily/weekly/montly)

        Returns:
            Sync Results Statistics
        """
        period_name = {"daily": "日线", "weekly": "周线", "monthly": "月线"}.get(period, "日线")
        logger.info(f"Synchronize{period_name}Historical Data...")

        stats = {
            "total_processed": 0,
            "success_count": 0,
            "error_count": 0,
            "total_records": 0,
            "start_time": datetime.utcnow(),
            "end_time": None,
            "duration": 0,
            "errors": []
        }

        try:
            #1. Determination of global end date
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')

            #2. Identification of shares to synchronize
            if symbols is None:
                basic_info_cursor = self.db.stock_basic_info.find({}, {"code": 1})
                symbols = [doc["code"] async for doc in basic_info_cursor]

            if not symbols:
                logger.warning("No shares to synchronize.")
                return stats

            stats["total_processed"] = len(symbols)

            #3. Determination of global start date (for log display only)
            global_start_date = start_date
            if not global_start_date:
                if incremental:
                    global_start_date = "各股票最后日期"
                else:
                    global_start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

            logger.info(f"Historical data sync: End date={end_date}, stock ={len(symbols)}mode ={'Incremental' if incremental else 'Full'}")

            #4. Batch processing
            for i in range(0, len(symbols), self.batch_size):
                batch = symbols[i:i + self.batch_size]
                batch_stats = await self._process_historical_batch(
                    batch, start_date, end_date, period, incremental
                )

                #Update statistics
                stats["success_count"] += batch_stats["success_count"]
                stats["error_count"] += batch_stats["error_count"]
                stats["total_records"] += batch_stats["total_records"]
                stats["errors"].extend(batch_stats["errors"])

                #Progress Log
                progress = min(i + self.batch_size, len(symbols))
                logger.info(f"Synchronization of historical data:{progress}/{len(symbols)} "
                           f"(success:{stats['success_count']}, Records:{stats['total_records']})")

                #API limit flow
                if i + self.batch_size < len(symbols):
                    await asyncio.sleep(self.rate_limit_delay)

            #4. Completion of statistics
            stats["end_time"] = datetime.utcnow()
            stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

            logger.info(f"The historical data is synchronised!")
            logger.info(f"Total:{stats['total_processed']}It's just stocks."
                       f"Success:{stats['success_count']}, "
                       f"Records:{stats['total_records']}Article,"
                       f"Time-consuming:{stats['duration']:.2f}sec")

            return stats

        except Exception as e:
            logger.error(f"History data sync failed:{e}")
            stats["errors"].append({"error": str(e), "context": "sync_historical_data"})
            return stats

    async def _process_historical_batch(
        self,
        batch: List[str],
        start_date: str,
        end_date: str,
        period: str = "daily",
        incremental: bool = False
    ) -> Dict[str, Any]:
        """Process historical data batches"""
        batch_stats = {
            "success_count": 0,
            "error_count": 0,
            "total_records": 0,
            "errors": []
        }

        for symbol in batch:
            try:
                #Determine the start date of the stock
                symbol_start_date = start_date
                if not symbol_start_date:
                    if incremental:
                        #Incremental sync: due date for acquisition of the stock
                        symbol_start_date = await self._get_last_sync_date(symbol)
                        logger.debug(f"📅 {symbol}From:{symbol_start_date}Start Synchronization")
                    else:
                        #Full Synchronization: the last year
                        symbol_start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

                #Access to historical data
                hist_data = await self.provider.get_historical_data(symbol, symbol_start_date, end_date, period)

                if hist_data is not None and not hist_data.empty:
                    #Save to Unified Historical Data Collection
                    if self.historical_service is None:
                        self.historical_service = await get_historical_data_service()

                    saved_count = await self.historical_service.save_historical_data(
                        symbol=symbol,
                        data=hist_data,
                        data_source="akshare",
                        market="CN",
                        period=period
                    )

                    batch_stats["success_count"] += 1
                    batch_stats["total_records"] += saved_count
                    logger.debug(f"✅ {symbol}Historical data synchronization success:{saved_count}Notes")
                else:
                    batch_stats["error_count"] += 1
                    batch_stats["errors"].append({
                        "code": symbol,
                        "error": "历史数据为空",
                        "context": "_process_historical_batch"
                    })

            except Exception as e:
                batch_stats["error_count"] += 1
                batch_stats["errors"].append({
                    "code": symbol,
                    "error": str(e),
                    "context": "_process_historical_batch"
                })

        return batch_stats

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
                latest_date = await self.historical_service.get_latest_date(symbol, "akshare")
                if latest_date:
                    #Return to the next day of the final date (duplicate)
                    try:
                        last_date_obj = datetime.strptime(latest_date, '%Y-%m-%d')
                        next_date = last_date_obj + timedelta(days=1)
                        return next_date.strftime('%Y-%m-%d')
                    except ValueError:
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

    async def sync_financial_data(self, symbols: List[str] = None) -> Dict[str, Any]:
        """Sync Financial Data

        Args:
            symbols: Specify list of stock codes

        Returns:
            Sync Results Statistics
        """
        logger.info("Start synchronizing financial data...")

        stats = {
            "total_processed": 0,
            "success_count": 0,
            "error_count": 0,
            "start_time": datetime.utcnow(),
            "end_time": None,
            "duration": 0,
            "errors": []
        }

        try:
            #1. Identification of shares to synchronize
            if symbols is None:
                basic_info_cursor = self.db.stock_basic_info.find(
                    {
                        "$or": [
                            {"market_info.market": "CN"},  #New data structure
                            {"category": "stock_cn"},      #Old data structure
                            {"market": {"$in": ["主板", "创业板", "科创板", "北交所"]}}  #By market type
                        ]
                    },
                    {"code": 1}
                )
                symbols = [doc["code"] async for doc in basic_info_cursor]
                logger.info(f"From stock basic info{len(symbols)}Only stocks")

            if not symbols:
                logger.warning("No shares to synchronize.")
                return stats

            stats["total_processed"] = len(symbols)
            logger.info(f"Ready to sync.{len(symbols)}Financial data for equities only")

            #2. Batch processing
            for i in range(0, len(symbols), self.batch_size):
                batch = symbols[i:i + self.batch_size]
                batch_stats = await self._process_financial_batch(batch)

                #Update statistics
                stats["success_count"] += batch_stats["success_count"]
                stats["error_count"] += batch_stats["error_count"]
                stats["errors"].extend(batch_stats["errors"])

                #Progress Log
                progress = min(i + self.batch_size, len(symbols))
                logger.info(f"Synchronization of financial data:{progress}/{len(symbols)} "
                           f"(success:{stats['success_count']}, Error:{stats['error_count']})")

                #API limit flow
                if i + self.batch_size < len(symbols):
                    await asyncio.sleep(self.rate_limit_delay)

            #3. Completion of statistics
            stats["end_time"] = datetime.utcnow()
            stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

            logger.info(f"Synchronize the financial data!")
            logger.info(f"Total:{stats['total_processed']}It's just stocks."
                       f"Success:{stats['success_count']}, "
                       f"Error:{stats['error_count']}, "
                       f"Time-consuming:{stats['duration']:.2f}sec")

            return stats

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            stats["errors"].append({"error": str(e), "context": "sync_financial_data"})
            return stats

    async def _process_financial_batch(self, batch: List[str]) -> Dict[str, Any]:
        """Processing of financial data batches"""
        batch_stats = {
            "success_count": 0,
            "error_count": 0,
            "errors": []
        }

        for symbol in batch:
            try:
                #Access to financial data
                financial_data = await self.provider.get_financial_data(symbol)

                if financial_data:
                    #Use of harmonized financial data services for data preservation
                    success = await self._save_financial_data(symbol, financial_data)
                    if success:
                        batch_stats["success_count"] += 1
                        logger.debug(f"✅ {symbol}Financial data retention success")
                    else:
                        batch_stats["error_count"] += 1
                        batch_stats["errors"].append({
                            "code": symbol,
                            "error": "财务数据保存失败",
                            "context": "_process_financial_batch"
                        })
                else:
                    batch_stats["error_count"] += 1
                    batch_stats["errors"].append({
                        "code": symbol,
                        "error": "财务数据为空",
                        "context": "_process_financial_batch"
                    })

            except Exception as e:
                batch_stats["error_count"] += 1
                batch_stats["errors"].append({
                    "code": symbol,
                    "error": str(e),
                    "context": "_process_financial_batch"
                })

        return batch_stats

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
                data_source="akshare",
                market="CN",
                report_type="quarterly"
            )

            return saved_count > 0

        except Exception as e:
            logger.error(f"Save{symbol}Financial data failed:{e}")
            return False

    async def run_status_check(self) -> Dict[str, Any]:
        """Run Status Check"""
        try:
            logger.info("Let's start the AKshare status check...")

            #Check provider connections
            provider_connected = await self.provider.test_connection()

            #Check database collection status
            collections_status = {}

            #Check basic information sets
            basic_count = await self.db.stock_basic_info.count_documents({})
            latest_basic = await self.db.stock_basic_info.find_one(
                {}, sort=[("updated_at", -1)]
            )
            collections_status["stock_basic_info"] = {
                "count": basic_count,
                "latest_update": latest_basic.get("updated_at") if latest_basic else None
            }

            #Check line data set
            quotes_count = await self.db.market_quotes.count_documents({})
            latest_quotes = await self.db.market_quotes.find_one(
                {}, sort=[("updated_at", -1)]
            )
            collections_status["market_quotes"] = {
                "count": quotes_count,
                "latest_update": latest_quotes.get("updated_at") if latest_quotes else None
            }

            status_result = {
                "provider_connected": provider_connected,
                "collections": collections_status,
                "status_time": datetime.utcnow()
            }

            logger.info(f"The AKShare status check is complete:{status_result}")
            return status_result

        except Exception as e:
            logger.error(f"The AKShare status check failed:{e}")
            return {
                "provider_connected": False,
                "error": str(e),
                "status_time": datetime.utcnow()
            }

    #== sync, corrected by elderman == @elder man

    async def _get_favorite_stocks(self) -> List[str]:
        """Retrieving list of selected shares for all users
        Note: Only the most up-to-date documents are obtained and historical data are avoided

        Returns:
            List of selected shares
        """
        try:
            favorite_codes = set()

            #Method 1: Retrieved from a group of users
            users_cursor = self.db.users.find(
                {"favorite_stocks": {"$exists": True, "$ne": []}},
                {"favorite_stocks.stock_code": 1, "_id": 0}
            )

            async for user in users_cursor:
                for fav in user.get("favorite_stocks", []):
                    code = fav.get("stock_code")
                    if code:
                        favorite_codes.add(code)

            #Method 2: Access from user favorites collection (compatible with old data structures)
            #🔥 Retrieving only the latest document (in descending order)
            latest_doc = await self.db.user_favorites.find_one(
                {"favorites": {"$exists": True, "$ne": []}},
                {"favorites.stock_code": 1, "_id": 0},
                sort=[("updated_at", -1)]  #Get the most up-to-date drop order
            )

            if latest_doc:
                logger.info(f"Select units to retrieve up-to-date documents from user favorites")
                for fav in latest_doc.get("favorites", []):
                    code = fav.get("stock_code")
                    if code:
                        favorite_codes.add(code)

            result = sorted(list(favorite_codes))
            logger.info(f"Other Organiser{len(result)}Only selected units")
            return result

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return []

    async def sync_news_data(
        self,
        symbols: List[str] = None,
        max_news_per_stock: int = 20,
        force_update: bool = False,
        favorites_only: bool = True
    ) -> Dict[str, Any]:
        """Sync News Data

        Args:
            symbols: list of stock codes to determine the sync range for Noone based on favorites only
            Max news per stock: Maximum number of news per stock
            Force update
            Favorites only: whether to sync only to the selected unit (defaultTrue)

        Returns:
            Sync Results Statistics
        """
        logger.info("Starting syncing AKshare news data...")

        stats = {
            "total_processed": 0,
            "success_count": 0,
            "error_count": 0,
            "news_count": 0,
            "start_time": datetime.utcnow(),
            "favorites_only": favorites_only,
            "errors": []
        }

        try:
            #1. Taking stock lists
            if symbols is None:
                if favorites_only:
                    #Synchronization of selected units only
                    symbols = await self._get_favorite_stocks()
                    logger.info(f"All right.{len(symbols)}Only")
                else:
                    #Access to all stocks (no restrictions on data sources)
                    stock_list = await self.db.stock_basic_info.find(
                        {},
                        {"code": 1, "_id": 0}
                    ).to_list(None)
                    symbols = [stock["code"] for stock in stock_list if stock.get("code")]
                    logger.info(f"Synchronization of all stocks,{len(symbols)}Only")

            if not symbols:
                logger.warning("No shares have been found that need to synchronize news.")
                return stats

            stats["total_processed"] = len(symbols)
            logger.info(f"We need to sync.{len(symbols)}Only stock news.")

            #2. Batch processing
            for i in range(0, len(symbols), self.batch_size):
                batch = symbols[i:i + self.batch_size]
                batch_stats = await self._process_news_batch(
                    batch, max_news_per_stock
                )

                #Update statistics
                stats["success_count"] += batch_stats["success_count"]
                stats["error_count"] += batch_stats["error_count"]
                stats["news_count"] += batch_stats["news_count"]
                stats["errors"].extend(batch_stats["errors"])

                #Progress Log
                progress = min(i + self.batch_size, len(symbols))
                logger.info(f"NewsSync:{progress}/{len(symbols)} "
                           f"(success:{stats['success_count']}News:{stats['news_count']})")

                #API limit flow
                if i + self.batch_size < len(symbols):
                    await asyncio.sleep(self.rate_limit_delay)

            #3. Completion of statistics
            stats["end_time"] = datetime.utcnow()
            stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

            logger.info(f"AKShare news data synchronised:"
                       f"Total{stats['total_processed']}It's just stocks."
                       f"Success{stats['success_count']}Only,"
                       f"Access{stats['news_count']}The news,"
                       f"Error{stats['error_count']}Only,"
                       f"Time-consuming{stats['duration']:.2f}sec")

            return stats

        except Exception as e:
            logger.error(f"AKShare NewsSync failed:{e}")
            stats["errors"].append({"error": str(e), "context": "sync_news_data"})
            return stats

    async def _process_news_batch(
        self,
        batch: List[str],
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
                #Get news data from Akshare.
                news_data = await self.provider.get_stock_news(
                    symbol=symbol,
                    limit=max_news_per_stock
                )

                if news_data:
                    #Preservation of news data
                    saved_count = await self.news_service.save_news_data(
                        news_data=news_data,
                        data_source="akshare",
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


#Examples of global sync services
_akshare_sync_service = None

async def get_akshare_sync_service() -> AKShareSyncService:
    """Get instance of AKShare sync service"""
    global _akshare_sync_service
    if _akshare_sync_service is None:
        _akshare_sync_service = AKShareSyncService()
        await _akshare_sync_service.initialize()
    return _akshare_sync_service


#Task Functions compatible with APSscheduler
async def run_akshare_basic_info_sync(force_update: bool = False):
    """APScheduler mission: Synchronizing basic stock information"""
    try:
        service = await get_akshare_sync_service()
        result = await service.sync_stock_basic_info(force_update=force_update)
        logger.info(f"AKshare's basic information is synchronised:{result}")
        return result
    except Exception as e:
        logger.error(f"AKShare's base message failed:{e}")
        raise


async def run_akshare_quotes_sync(force: bool = False):
    """APSscheduler mission: Sync real-time patterns

    Args:
        force: enforcement ( Skip transaction time check), default False
    """
    try:
        service = await get_akshare_sync_service()
        #Note: AKShare has no transaction time check logic, force parameters only for interface consistency
        result = await service.sync_realtime_quotes(force=force)
        logger.info(f"The AKShare line has been synchronized:{result}")
        return result
    except Exception as e:
        logger.error(f"The AKShare line failed:{e}")
        raise


async def run_akshare_historical_sync(incremental: bool = True):
    """APScheduler: Synchronizing historical data"""
    try:
        service = await get_akshare_sync_service()
        result = await service.sync_historical_data(incremental=incremental)
        logger.info(f"AKShare's historical data are synchronised:{result}")
        return result
    except Exception as e:
        logger.error(f"AKShare's historical data sync failed:{e}")
        raise


async def run_akshare_financial_sync():
    """APSscheduler mission: Synchronization of financial data"""
    try:
        service = await get_akshare_sync_service()
        result = await service.sync_financial_data()
        logger.info(f"Synchronization of AKShare financial data:{result}")
        return result
    except Exception as e:
        logger.error(f"AKShare's financial data synchronised failed:{e}")
        raise


async def run_akshare_status_check():
    """APScheduler mission: status check"""
    try:
        service = await get_akshare_sync_service()
        result = await service.run_status_check()
        logger.info(f"The AKShare status check is complete:{result}")
        return result
    except Exception as e:
        logger.error(f"The AKShare status check failed:{e}")
        raise


async def run_akshare_news_sync(max_news_per_stock: int = 20):
    """APSscheduler mission: Synchronizing news data"""
    try:
        service = await get_akshare_sync_service()
        result = await service.sync_news_data(
            max_news_per_stock=max_news_per_stock
        )
        logger.info(f"AKShare news data synchronised:{result}")
        return result
    except Exception as e:
        logger.error(f"AKShare NewsSync failed:{e}")
        raise
