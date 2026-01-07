#!/usr/bin/env python3
"""Financial data services
Harmonized management of financial data storage and searching for three data sources
"""
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import pandas as pd
from pymongo import ReplaceOne

from app.core.database import get_mongo_db_async

logger = logging.getLogger(__name__)


class FinancialDataService:
    """Integrated financial data management services"""
    
    def __init__(self):
        self.collection_name = "stock_financial_data"
        self.db = None
        
    async def initialize(self):
        """Initialization services"""
        try:
            self.db = get_mongo_db_async()
            if self.db is None:
                raise Exception("MongoDB数据库未初始化")

            #🔥 to ensure that index exists (upgrade query and upsert performance)
            await self._ensure_indexes()

            logger.info("✅ Financial data services were successfully initiated")

        except Exception as e:
            logger.error(f"The initialization of the financial data service failed:{e}")
            raise

    async def _ensure_indexes(self):
        """Ensure the necessary index exists"""
        try:
            collection = self.db[self.collection_name]
            logger.info("Check and create a financial data index...")

            #1. Composite unique index: stock code + reporting period + data source (for upset)
            await collection.create_index([
                ("symbol", 1),
                ("report_period", 1),
                ("data_source", 1)
            ], unique=True, name="symbol_period_source_unique", background=True)

            #2. Stock code index (search for single equity financial data)
            await collection.create_index([("symbol", 1)], name="symbol_index", background=True)

            #3. Indexes for reporting periods (by time frame)
            await collection.create_index([("report_period", -1)], name="report_period_index", background=True)

            #4. Composite index: stock code + reporting period (common query)
            await collection.create_index([
                ("symbol", 1),
                ("report_period", -1)
            ], name="symbol_period_index", background=True)

            #5. Index of reporting types (screened by quarterly/annual reports)
            await collection.create_index([("report_type", 1)], name="report_type_index", background=True)

            #6. Update time index (data maintenance)
            await collection.create_index([("updated_at", -1)], name="updated_at_index", background=True)

            logger.info("Financial data index check completed")
        except Exception as e:
            #Index creation failure should not prevent service startup
            logger.warning(f"Warning (possibly exists) when creating index:{e}")
    
    async def save_financial_data(
        self,
        symbol: str,
        financial_data: Dict[str, Any],
        data_source: str,
        market: str = "CN",
        report_period: str = None,
        report_type: str = "quarterly"
    ) -> int:
        """Save financial data to database

        Args:
            symbol: stock code
            Financial data: Financial data dictionary
            Data source: Data source (tushare/akshare/baostock)
            Market type (CN/HK/US)
            Report period: Reporting period (YYYYMMDD)
            Report type: Report type (quarterly/annual)

        Returns:
            Number of records kept
        """
        if self.db is None:
            await self.initialize()
        
        try:
            logger.info(f"Start saving{symbol}Financial data (data source:{data_source})")
            
            collection = self.db[self.collection_name]
            
            #Standardized financial data
            standardized_data = self._standardize_financial_data(
                symbol, financial_data, data_source, market, report_period, report_type
            )
            
            if not standardized_data:
                logger.warning(f"⚠️ {symbol}Standardized financial data empty")
                return 0
            
            #Batch Operations
            operations = []
            saved_count = 0
            
            #If multiple periods, separate periods
            if isinstance(standardized_data, list):
                for data_item in standardized_data:
                    filter_doc = {
                        "symbol": data_item["symbol"],
                        "report_period": data_item["report_period"],
                        "data_source": data_item["data_source"]
                    }
                    
                    operations.append(ReplaceOne(
                        filter=filter_doc,
                        replacement=data_item,
                        upsert=True
                    ))
                    saved_count += 1
            else:
                #Single-period data
                filter_doc = {
                    "symbol": standardized_data["symbol"],
                    "report_period": standardized_data["report_period"],
                    "data_source": standardized_data["data_source"]
                }
                
                operations.append(ReplaceOne(
                    filter=filter_doc,
                    replacement=standardized_data,
                    upsert=True
                ))
                saved_count = 1
            
            #Execute Batch Operations
            if operations:
                result = await collection.bulk_write(operations)
                actual_saved = result.upserted_count + result.modified_count
                
                logger.info(f"✅ {symbol}Financial data retention complete:{actual_saved}Notes")
                return actual_saved
            
            return 0
            
        except Exception as e:
            logger.error(f"Failed to save financial data{symbol}: {e}")
            return 0
    
    async def get_financial_data(
        self,
        symbol: str,
        report_period: str = None,
        data_source: str = None,
        report_type: str = None,
        limit: int = None
    ) -> List[Dict[str, Any]]:
        """Query financial data

        Args:
            symbol: stock code
            Report period: Screening for reporting period Select
            Data source: Data source filter
            Report type: report type screening Select
            Limited number of returns

        Returns:
            List of financial data
        """
        if self.db is None:
            await self.initialize()
        
        try:
            collection = self.db[self.collection_name]
            
            #Build query conditions
            query = {"symbol": symbol}
            
            if report_period:
                query["report_period"] = report_period
            
            if data_source:
                query["data_source"] = data_source
            
            if report_type:
                query["report_type"] = report_type
            
            #Execute queries
            cursor = collection.find(query, {"_id": 0}).sort("report_period", -1)
            
            if limit:
                cursor = cursor.limit(limit)
            
            results = await cursor.to_list(length=None)
            
            logger.info(f"For financial data:{symbol}Back{len(results)}Notes")
            return results
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{symbol}: {e}")
            return []
    
    async def get_latest_financial_data(
        self,
        symbol: str,
        data_source: str = None
    ) -> Optional[Dict[str, Any]]:
        """Access to up-to-date financial data"""
        results = await self.get_financial_data(
            symbol=symbol,
            data_source=data_source,
            limit=1
        )
        
        return results[0] if results else None
    
    async def get_financial_statistics(self) -> Dict[str, Any]:
        """Access to financial data statistics"""
        if self.db is None:
            await self.initialize()
        
        try:
            collection = self.db[self.collection_name]
            
            #By data source
            pipeline = [
                {"$group": {
                    "_id": {
                        "data_source": "$data_source",
                        "report_type": "$report_type"
                    },
                    "count": {"$sum": 1},
                    "latest_period": {"$max": "$report_period"},
                    "symbols": {"$addToSet": "$symbol"}
                }}
            ]
            
            results = await collection.aggregate(pipeline).to_list(length=None)
            
            #Format statistical results
            stats = {}
            total_records = 0
            total_symbols = set()
            
            for result in results:
                source = result["_id"]["data_source"]
                report_type = result["_id"]["report_type"]
                count = result["count"]
                symbols = result["symbols"]
                
                if source not in stats:
                    stats[source] = {}
                
                stats[source][report_type] = {
                    "count": count,
                    "latest_period": result["latest_period"],
                    "symbol_count": len(symbols)
                }
                
                total_records += count
                total_symbols.update(symbols)
            
            return {
                "total_records": total_records,
                "total_symbols": len(total_symbols),
                "by_source": stats,
                "last_updated": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Access to financial data statistics failed:{e}")
            return {}
    
    def _standardize_financial_data(
        self,
        symbol: str,
        financial_data: Dict[str, Any],
        data_source: str,
        market: str,
        report_period: str = None,
        report_type: str = "quarterly"
    ) -> Optional[Dict[str, Any]]:
        """Standardized financial data"""
        try:
            now = datetime.now(timezone.utc)
            
            #Different standardized processing according to data sources
            if data_source == "tushare":
                return self._standardize_tushare_data(
                    symbol, financial_data, market, report_period, report_type, now
                )
            elif data_source == "akshare":
                return self._standardize_akshare_data(
                    symbol, financial_data, market, report_period, report_type, now
                )
            elif data_source == "baostock":
                return self._standardize_baostock_data(
                    symbol, financial_data, market, report_period, report_type, now
                )
            else:
                logger.warning(f"Data sources not supported:{data_source}")
                return None
                
        except Exception as e:
            logger.error(f"Standardized financial data failed{symbol}: {e}")
            return None
    
    def _standardize_tushare_data(
        self,
        symbol: str,
        financial_data: Dict[str, Any],
        market: str,
        report_period: str,
        report_type: str,
        now: datetime
    ) -> Dict[str, Any]:
        """Standardized Tushare financial data"""
        #Tushare data have been standardized in provider and used directly
        base_data = {
            "code": symbol,  #Add a code field to fit the only index
            "symbol": symbol,
            "full_symbol": self._get_full_symbol(symbol, market),
            "market": market,
            "report_period": report_period or financial_data.get("report_period"),
            "report_type": report_type or financial_data.get("report_type", "quarterly"),
            "data_source": "tushare",
            "created_at": now,
            "updated_at": now,
            "version": 1
        }

        #Consolidation of financial data standardized with Tushare
        #Excludes fields that do not need to be repeated
        exclude_fields = {'symbol', 'data_source', 'updated_at'}
        for key, value in financial_data.items():
            if key not in exclude_fields:
                base_data[key] = value

        #Ensure that key fields exist
        if 'ann_date' in financial_data:
            base_data['ann_date'] = financial_data['ann_date']

        return base_data
    
    def _standardize_akshare_data(
        self,
        symbol: str,
        financial_data: Dict[str, Any],
        market: str,
        report_period: str,
        report_type: str,
        now: datetime
    ) -> Dict[str, Any]:
        """Standardize AKShare financial data"""
        #AKShare data need to extract key indicators from multiple data concentrations
        base_data = {
            "code": symbol,  #Add a code field to fit the only index
            "symbol": symbol,
            "full_symbol": self._get_full_symbol(symbol, market),
            "market": market,
            "report_period": report_period or self._extract_latest_period(financial_data),
            "report_type": report_type,
            "data_source": "akshare",
            "created_at": now,
            "updated_at": now,
            "version": 1
        }

        #Extracting key financial indicators
        base_data.update(self._extract_akshare_indicators(financial_data))
        return base_data
    
    def _standardize_baostock_data(
        self,
        symbol: str,
        financial_data: Dict[str, Any],
        market: str,
        report_period: str,
        report_type: str,
        now: datetime
    ) -> Dict[str, Any]:
        """Standardized BaoStock financial data"""
        base_data = {
            "code": symbol,  #Add a code field to fit the only index
            "symbol": symbol,
            "full_symbol": self._get_full_symbol(symbol, market),
            "market": market,
            "report_period": report_period or self._generate_current_period(),
            "report_type": report_type,
            "data_source": "baostock",
            "created_at": now,
            "updated_at": now,
            "version": 1
        }

        #Consolidation of BaoStock financial data
        base_data.update(financial_data)
        return base_data
    
    def _get_full_symbol(self, symbol: str, market: str) -> str:
        """Get the full stock code"""
        if market == "CN":
            if symbol.startswith("6"):
                return f"{symbol}.SH"
            else:
                return f"{symbol}.SZ"
        return symbol
    
    def _extract_latest_period(self, financial_data: Dict[str, Any]) -> str:
        """Recent reporting period extracted from AKshare data"""
        #Attempt to extract reporting periods from various data collections
        for key in ['main_indicators', 'balance_sheet', 'income_statement']:
            if key in financial_data and financial_data[key]:
                records = financial_data[key]
                if isinstance(records, list) and records:
                    #Suppose the first record is the latest.
                    first_record = records[0]
                    for date_field in ['报告期', '报告日期', 'date', '日期']:
                        if date_field in first_record:
                            return str(first_record[date_field]).replace('-', '')
        
        #If not available, use current quarter
        return self._generate_current_period()
    
    def _extract_akshare_indicators(self, financial_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract key financial indicators from AKShare data"""
        indicators = {}

        #Extract from key financial indicators
        if 'main_indicators' in financial_data and financial_data['main_indicators']:
            main_data = financial_data['main_indicators'][0] if financial_data['main_indicators'] else {}
            indicators.update({
                "revenue": self._safe_float(main_data.get('营业收入')),
                "net_income": self._safe_float(main_data.get('净利润')),
                "total_assets": self._safe_float(main_data.get('总资产')),
                "total_equity": self._safe_float(main_data.get('股东权益合计')),
            })

            #New: withdrawal of ROE (net asset rate of return)
            roe = main_data.get('净资产收益率(ROE)') or main_data.get('净资产收益率')
            if roe is not None:
                indicators["roe"] = self._safe_float(roe)

            #🔥New: drawing liability ratio (asset liability ratio)
            debt_ratio = main_data.get('资产负债率') or main_data.get('负债率')
            if debt_ratio is not None:
                indicators["debt_to_assets"] = self._safe_float(debt_ratio)

        #Extract from balance sheet
        if 'balance_sheet' in financial_data and financial_data['balance_sheet']:
            balance_data = financial_data['balance_sheet'][0] if financial_data['balance_sheet'] else {}
            indicators.update({
                "total_liab": self._safe_float(balance_data.get('负债合计')),
                "cash_and_equivalents": self._safe_float(balance_data.get('货币资金')),
            })

            #🔥 If there is no liability ratio for key indicators, calculated from the balance sheet
            if "debt_to_assets" not in indicators:
                total_liab = indicators.get("total_liab")
                total_assets = indicators.get("total_assets")
                if total_liab is not None and total_assets is not None and total_assets > 0:
                    indicators["debt_to_assets"] = (total_liab / total_assets) * 100

        return indicators
    
    def _generate_current_period(self) -> str:
        """Generation of current reporting period"""
        now = datetime.now()
        year = now.year
        month = now.month
        
        #Quarterly based on month
        if month <= 3:
            quarter = 1
        elif month <= 6:
            quarter = 2
        elif month <= 9:
            quarter = 3
        else:
            quarter = 4
        
        #Format for generating reporting period YYYYMMDD
        quarter_end_months = {1: "03", 2: "06", 3: "09", 4: "12"}
        quarter_end_days = {1: "31", 2: "30", 3: "30", 4: "31"}
        
        return f"{year}{quarter_end_months[quarter]}{quarter_end_days[quarter]}"
    
    def _safe_float(self, value) -> Optional[float]:
        """Convert safe to floating point"""
        if value is None:
            return None
        try:
            if isinstance(value, str):
                #Remove possible units and formatting words Arguments
                value = value.replace(',', '').replace('万', '').replace('亿', '')
            return float(value)
        except (ValueError, TypeError):
            return None


#Examples of global services
_financial_data_service = None


async def get_financial_data_service() -> FinancialDataService:
    """Examples of access to financial data services"""
    global _financial_data_service
    if _financial_data_service is None:
        _financial_data_service = FinancialDataService()
        await _financial_data_service.initialize()
    return _financial_data_service
