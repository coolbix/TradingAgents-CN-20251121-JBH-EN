#!/usr/bin/env python3
"""Integrated historical data management services
Provide a unified historical data storage and query interface for three data sources
"""
import asyncio
import logging
from datetime import datetime, date
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.database import get_database_async

logger = logging.getLogger(__name__)


class HistoricalDataService:
    """Integrated historical data management services"""
    
    def __init__(self):
        """Initialization services"""
        self.db = None
        self.collection = None
        
    async def initialize(self):
        """Initialize database connection"""
        try:
            self.db = get_database_async()
            self.collection = self.db.stock_daily_quotes

            #🔥 to ensure that index exists (upgrade query and upsert performance)
            await self._ensure_indexes()

            logger.info("✅historical data services successfully initiated")
        except Exception as e:
            logger.error(f"The initialization of the historical data service failed:{e}")
            raise

    async def _ensure_indexes(self):
        """Ensure the necessary index exists"""
        try:
            logger.info("Check and create an index of historical data...")

            #1. Composite unique index: stock code + date of transaction + data source + cycle (for upset)
            await self.collection.create_index([
                ("symbol", 1),
                ("trade_date", 1),
                ("data_source", 1),
                ("period", 1)
            ], unique=True, name="symbol_date_source_period_unique", background=True)

            #Stock code index (search for historical data on single stocks)
            await self.collection.create_index([("symbol", 1)], name="symbol_index", background=True)

            #3. Transaction date index (survey by date range)
            await self.collection.create_index([("trade_date", -1)], name="trade_date_index", background=True)

            #4. Composite index: stock code + date of transaction (common query)
            await self.collection.create_index([
                ("symbol", 1),
                ("trade_date", -1)
            ], name="symbol_date_index", background=True)

            logger.info("Historical data index check completed")
        except Exception as e:
            #Index creation failure should not prevent service startup
            logger.warning(f"Warning (possibly exists) when creating index:{e}")
    
    async def save_historical_data(
        self,
        symbol: str,
        data: pd.DataFrame,
        data_source: str,
        market: str = "CN",
        period: str = "daily"
    ) -> int:
        """Save historical data to database

        Args:
            symbol: stock code
            Data: Historical DataFrame
            Data source: Data source (tushare/akshare/baostock)
            Market type (CN/HK/US)
            period: data cycle (daily/weekly/montly)

        Returns:
            Number of records kept
        """
        if self.collection is None:
            await self.initialize()
        
        try:
            if data is None or data.empty:
                logger.warning(f"⚠️ {symbol}History data empty, skip saving")
                return 0

            from datetime import datetime
            total_start = datetime.now()

            logger.info(f"Start saving{symbol}Historical data:{len(data)}Article record (data source:{data_source})")

            #Performance monitoring: unit conversion
            convert_start = datetime.now()
            #Unit conversion at the DataFrame level (to quantitative operations, much faster than line by line)
            if data_source == "tushare":
                #Deal: thousands - > dollars
                if 'amount' in data.columns:
                    data['amount'] = data['amount'] * 1000
                elif 'turnover' in data.columns:
                    data['turnover'] = data['turnover'] * 1000

                #Exchange: Hands - > Stock
                if 'volume' in data.columns:
                    data['volume'] = data['volume'] * 100
                elif 'vol' in data.columns:
                    data['vol'] = data['vol'] * 100

            #🔥 Port/US data: add pre close field (retributed from close the previous day)
            if market in ["HK", "US"] and 'pre_close' not in data.columns and 'close' in data.columns:
                #Use Shift(1) to move the close column down and get the previous day's closing price
                data['pre_close'] = data['close'].shift(1)
                logger.debug(f"✅ {symbol}Add pre close field (retrieved from the previous day 's close)")

            convert_duration = (datetime.now() - convert_start).total_seconds()

            #⏱️ Performance Monitor: Build Operations List
            prepare_start = datetime.now()
            #Prepare batch operations
            operations = []
            saved_count = 0
            batch_size = 200  #Further volume reduction to avoid time overrun (from 500 to 200)

            for date_index, row in data.iterrows():
                try:
                    #Standardized data (index to transmission date)
                    doc = self._standardize_record(symbol, row, data_source, market, period, date_index)

                    #Create upset operation
                    filter_doc = {
                        "symbol": doc["symbol"],
                        "trade_date": doc["trade_date"],
                        "data_source": doc["data_source"],
                        "period": doc["period"]
                    }

                    from pymongo import ReplaceOne
                    operations.append(ReplaceOne(
                        filter=filter_doc,
                        replacement=doc,
                        upsert=True
                    ))

                    #Batch execution (per 200)
                    if len(operations) >= batch_size:
                        batch_write_start = datetime.now()
                        batch_saved = await self._execute_bulk_write_with_retry(symbol, operations)
                        batch_write_duration = (datetime.now() - batch_write_start).total_seconds()
                        logger.debug(f"Batch writing{len(operations)}Article, time-consuming{batch_write_duration:.2f}sec")
                        saved_count += batch_saved
                        operations = []

                except Exception as e:
                    #Fetch date information for error log
                    date_str = str(date_index) if hasattr(date_index, '__str__') else 'unknown'
                    logger.error(f"Processing log failed{symbol} {date_str}: {e}")
                    continue

            prepare_duration = (datetime.now() - prepare_start).total_seconds()

            #Performance monitoring: last batch written
            final_write_start = datetime.now()
            #Perform residual operations
            if operations:
                saved_count += await self._execute_bulk_write_with_retry(
                    symbol, operations
                )
            final_write_duration = (datetime.now() - final_write_start).total_seconds()

            total_duration = (datetime.now() - total_start).total_seconds()
            logger.info(
                f"✅ {symbol}Historical data preservation complete:{saved_count}The record,"
                f"Total time-consuming{total_duration:.2f}sec"
                f"(Transformation:{convert_duration:.3f}Seconds, ready:{prepare_duration:.2f}seconds, final writing:{final_write_duration:.2f}sec)"
            )
            return saved_count
            
        except Exception as e:
            logger.error(f"Failed to save historical data{symbol}: {e}")
            return 0

    async def _execute_bulk_write_with_retry(
        self,
        symbol: str,
        operations: List,
        max_retries: int = 5  #Increase in the number of retests: from 3 to 5
    ) -> int:
        """Implementation batch writing with retry mechanism

        Args:
            symbol: stock code
            Organisations: Batch Operations List
            max retries: maximum number of retries

        Returns:
            Number of records successfully saved
        """
        saved_count = 0
        retry_count = 0

        while retry_count < max_retries:
            try:
                result = await self.collection.bulk_write(operations, ordered=False)
                saved_count = result.upserted_count + result.modified_count
                logger.debug(f"✅ {symbol}Batch Save{len(operations)}Record successful (add:{result.upserted_count}, Update:{result.modified_count})")
                return saved_count

            except asyncio.TimeoutError as e:
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 3 ** retry_count  #Longer index retreat: 3 seconds, 9 seconds, 27 seconds, 81 seconds
                    logger.warning(f"⚠️ {symbol}Batch writing timeout (No.{retry_count}/{max_retries}Try again, wait{wait_time}Try again in seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"❌ {symbol}Batch writing failed, try again{max_retries}Times:{e}")
                    return 0

            except Exception as e:
                #Check if it's a time-out-related error
                error_msg = str(e).lower()
                if 'timeout' in error_msg or 'timed out' in error_msg:
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = 3 ** retry_count
                        logger.warning(f"⚠️ {symbol}Batch writing timeout (No.{retry_count}/{max_retries}Try again, wait{wait_time}Try again after seconds... error:{e}")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"❌ {symbol}Batch writing failed, try again{max_retries}Times:{e}")
                        return 0
                else:
                    logger.error(f"❌ {symbol}Batch writing failed:{e}")
                    return 0

        return saved_count

    def _standardize_record(
        self,
        symbol: str,
        row: pd.Series,
        data_source: str,
        market: str,
        period: str = "daily",
        date_index = None
    ) -> Dict[str, Any]:
        """Standardized single record"""
        now = datetime.utcnow()

        #Acquiring Date - Prefer to fetch from a column if the index is a date type
        trade_date = None

        #Try fetching dates from columns first
        date_from_column = row.get('date') or row.get('trade_date')

        #If there are dates in columns, give preference to the dates in columns
        if date_from_column is not None:
            trade_date = self._format_date(date_from_column)
        #If there is no date in the column and the index is a date type, the index is used
        elif date_index is not None and isinstance(date_index, (date, datetime, pd.Timestamp)):
            trade_date = self._format_date(date_index)
        #Otherwise use the current date
        else:
            trade_date = self._format_date(None)

        #Base Field Map
        doc = {
            "symbol": symbol,
            "code": symbol,  #Add a code field, consistent with symbol (backward compatible)
            "full_symbol": self._get_full_symbol(symbol, market),
            "market": market,
            "trade_date": trade_date,
            "period": period,
            "data_source": data_source,
            "created_at": now,
            "updated_at": now,
            "version": 1
        }
        
        #OHLCV data (unit conversion completed at DataFrame level)
        amount_value = self._safe_float(row.get('amount') or row.get('turnover'))
        volume_value = self._safe_float(row.get('volume') or row.get('vol'))

        doc.update({
            "open": self._safe_float(row.get('open')),
            "high": self._safe_float(row.get('high')),
            "low": self._safe_float(row.get('low')),
            "close": self._safe_float(row.get('close')),
            "pre_close": self._safe_float(row.get('pre_close') or row.get('preclose')),
            "volume": volume_value,
            "amount": amount_value
        })
        
        #Calculating Rising and Falling Data
        if doc["close"] and doc["pre_close"]:
            doc["change"] = round(doc["close"] - doc["pre_close"], 4)
            doc["pct_chg"] = round((doc["change"] / doc["pre_close"]) * 100, 4)
        else:
            doc["change"] = self._safe_float(row.get('change'))
            doc["pct_chg"] = self._safe_float(row.get('pct_chg') or row.get('change_percent'))
        
        #Optional Fields
        optional_fields = {
            "turnover_rate": row.get('turnover_rate') or row.get('turn'),
            "volume_ratio": row.get('volume_ratio'),
            "pe": row.get('pe'),
            "pb": row.get('pb'),
            "ps": row.get('ps'),
            "adjustflag": row.get('adjustflag') or row.get('adj_factor'),
            "tradestatus": row.get('tradestatus'),
            "isST": row.get('isST')
        }
        
        for key, value in optional_fields.items():
            if value is not None:
                doc[key] = self._safe_float(value)
        
        return doc
    
    def _get_full_symbol(self, symbol: str, market: str) -> str:
        """Generate full stock code"""
        if market == "CN":
            if symbol.startswith('6'):
                return f"{symbol}.SH"
            elif symbol.startswith(('0', '3')):
                return f"{symbol}.SZ"
            else:
                return f"{symbol}.SZ"  #Default Shenzhen
        elif market == "HK":
            return f"{symbol}.HK"
        elif market == "US":
            return symbol
        else:
            return symbol
    
    def _format_date(self, date_value) -> str:
        """Formatting Date"""
        if date_value is None:
            return datetime.now().strftime('%Y-%m-%d')
        
        if isinstance(date_value, str):
            #Deal with different date formats
            if len(date_value) == 8:  # YYYYMMDD
                return f"{date_value[:4]}-{date_value[4:6]}-{date_value[6:8]}"
            elif len(date_value) == 10:  # YYYY-MM-DD
                return date_value
            else:
                return date_value
        elif isinstance(date_value, (date, datetime)):
            return date_value.strftime('%Y-%m-%d')
        else:
            return str(date_value)
    
    def _safe_float(self, value) -> Optional[float]:
        """Convert safe to floating point"""
        if value is None or value == '' or pd.isna(value):
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    async def get_historical_data(
        self,
        symbol: str,
        start_date: str = None,
        end_date: str = None,
        data_source: str = None,
        period: str = None,
        limit: int = None
    ) -> List[Dict[str, Any]]:
        """Query Historical Data

        Args:
            symbol: stock code
            Start date: Start date (YYYYY-MM-DD)
            End date: End Date (YYYYY-MM-DD)
            Data source: Data source filter
            period: data cycle screening (daily/weekly/montly)
            Limited number of returns

        Returns:
            Historical Data List
        """
        if self.collection is None:
            await self.initialize()
        
        try:
            #Build query conditions
            query = {"symbol": symbol}
            
            if start_date or end_date:
                date_filter = {}
                if start_date:
                    date_filter["$gte"] = start_date
                if end_date:
                    date_filter["$lte"] = end_date
                query["trade_date"] = date_filter
            
            if data_source:
                query["data_source"] = data_source

            if period:
                query["period"] = period
            
            #Execute queries
            cursor = self.collection.find(query).sort("trade_date", -1)
            
            if limit:
                cursor = cursor.limit(limit)
            
            results = await cursor.to_list(length=None)
            
            logger.info(f"For historical data:{symbol}Back{len(results)}Notes")
            return results
            
        except Exception as e:
            logger.error(f"Failed to query historical data{symbol}: {e}")
            return []
    
    async def get_latest_date(self, symbol: str, data_source: str) -> Optional[str]:
        """Date of acquisition of latest data"""
        if self.collection is None:
            await self.initialize()
        
        try:
            result = await self.collection.find_one(
                {"symbol": symbol, "data_source": data_source},
                sort=[("trade_date", -1)]
            )
            
            if result:
                return result["trade_date"]
            return None
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{symbol}: {e}")
            return None
    
    async def get_data_statistics(self) -> Dict[str, Any]:
        """Access to statistical information"""
        if self.collection is None:
            await self.initialize()
        
        try:
            #Total records
            total_count = await self.collection.count_documents({})
            
            #By data source
            source_stats = await self.collection.aggregate([
                {"$group": {
                    "_id": "$data_source",
                    "count": {"$sum": 1},
                    "latest_date": {"$max": "$trade_date"}
                }}
            ]).to_list(length=None)
            
            #By market
            market_stats = await self.collection.aggregate([
                {"$group": {
                    "_id": "$market",
                    "count": {"$sum": 1}
                }}
            ]).to_list(length=None)
            
            #Stock count
            symbol_count = len(await self.collection.distinct("symbol"))
            
            return {
                "total_records": total_count,
                "total_symbols": symbol_count,
                "by_source": {item["_id"]: {
                    "count": item["count"],
                    "latest_date": item.get("latest_date")
                } for item in source_stats},
                "by_market": {item["_id"]: item["count"] for item in market_stats},
                "last_updated": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Can not get folder: %s: %s{e}")
            return {}


#Examples of global services
_historical_data_service = None


async def get_historical_data_service() -> HistoricalDataService:
    """Examples of access to historical data services"""
    global _historical_data_service
    if _historical_data_service is None:
        _historical_data_service = HistoricalDataService()
        await _historical_data_service.initialize()
    return _historical_data_service
