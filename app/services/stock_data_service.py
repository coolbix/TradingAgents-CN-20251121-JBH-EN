"""Stock data service level - Unified data access interface
Standardized data access services based on the existing MongoDB collection
"""
import logging
from datetime import datetime, date
from typing import Optional, Dict, Any, List
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.database import get_mongo_db_async
from app.models.stock_models import (
    StockBasicInfoExtended, 
    MarketQuotesExtended,
    MarketInfo,
    MarketType,
    ExchangeType,
    CurrencyType
)

logger = logging.getLogger(__name__)


class StockDataService:
    """Equities Data Services - Unified Data Access Level
    Based on existing pools, maintain backward compatibility
    """
    
    def __init__(self):
        self.basic_info_collection = "stock_basic_info"
        self.market_quotes_collection = "market_quotes"
    
    async def get_stock_basic_info(
        self,
        symbol: str,
        source: Optional[str] = None
    ) -> Optional[StockBasicInfoExtended]:
        """Access to basic stock information
        Args:
            symbol: 6-bit stock code
            Source: Data source (tushare/akshare/baostock/multi source), default priority: tushare > multi source > akshare > baostock
        Returns:
            StockBasicInfoExtended: extended stock base information
        """
        try:
            db = get_mongo_db_async()
            symbol6 = str(symbol).zfill(6)

            #Build Query Conditions
            query = {"$or": [{"symbol": symbol6}, {"code": symbol6}]}

            if source:
                #Specify data source
                query["source"] = source
                doc = await db[self.basic_info_collection].find_one(query, {"_id": 0})
            else:
                #🔥 No data sources specified, query by priority
                source_priority = ["tushare", "multi_source", "akshare", "baostock"]
                doc = None

                for src in source_priority:
                    query_with_source = query.copy()
                    query_with_source["source"] = src
                    doc = await db[self.basic_info_collection].find_one(query_with_source, {"_id": 0})
                    if doc:
                        logger.debug(f"Using data sources:{src}")
                        break

                #Try without source condition query (compatible with old data) if all data sources are missing
                if not doc:
                    doc = await db[self.basic_info_collection].find_one(
                        {"$or": [{"symbol": symbol6}, {"code": symbol6}]},
                        {"_id": 0}
                    )
                    if doc:
                        logger.warning(f"Use old data (no source field):{symbol6}")

            if not doc:
                return None

            #Standardized data processing
            standardized_doc = self._standardize_basic_info(doc)

            return StockBasicInfoExtended(**standardized_doc)

        except Exception as e:
            logger.error(f"Failed to get basic stock information symbol={symbol}, source={source}: {e}")
            return None
    
    async def get_market_quotes(self, symbol: str) -> Optional[MarketQuotesExtended]:
        """Get Real Time Line Data
        Args:
            symbol: 6-bit stock code
        Returns:
            MarketQuotesExtended: extended real-time behavioral data
        """
        try:
            db = get_mongo_db_async()
            symbol6 = str(symbol).zfill(6)

            #Query from existing collections (prefer symbol fields to code fields)
            doc = await db[self.market_quotes_collection].find_one(
                {"$or": [{"symbol": symbol6}, {"code": symbol6}]},
                {"_id": 0}
            )

            if not doc:
                return None

            #Standardized data processing
            standardized_doc = self._standardize_market_quotes(doc)

            return MarketQuotesExtended(**standardized_doc)

        except Exception as e:
            logger.error(f"Failed to get real time line{symbol}: {e}")
            return None
    
    async def get_stock_list(
        self,
        market: Optional[str] = None,
        industry: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
        source: Optional[str] = None
    ) -> List[StockBasicInfoExtended]:
        """Get Stock List
        Args:
            Market screening Select
            industry:
            Page: Page Number
            Page size: per page size
            source: data source (optional), default use of highest priority data source
        Returns:
            List [Stock BasicInfoExtended]:
        """
        try:
            db = get_mongo_db_async()

            #Access source priority configuration
            if not source:
                from app.core.unified_config import UnifiedConfigManager
                config = UnifiedConfigManager()
                data_source_configs = await config.get_data_source_configs_async()

                #Extract enabled data sources in order of priority
                enabled_sources = [
                    ds.type.lower() for ds in data_source_configs
                    if ds.enabled and ds.type.lower() in ['tushare', 'akshare', 'baostock']
                ]

                if not enabled_sources:
                    enabled_sources = ['tushare', 'akshare', 'baostock']

                source = enabled_sources[0] if enabled_sources else 'tushare'

            #Build query conditions
            query = {"source": source}  #Add Data Source Filter
            if market:
                query["market"] = market
            if industry:
                query["industry"] = industry

            #Page Break Query
            skip = (page - 1) * page_size
            cursor = db[self.basic_info_collection].find(
                query,
                {"_id": 0}
            ).skip(skip).limit(page_size)

            docs = await cursor.to_list(length=page_size)

            #Standardized data processing
            result = []
            for doc in docs:
                standardized_doc = self._standardize_basic_info(doc)
                result.append(StockBasicInfoExtended(**standardized_doc))

            return result
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return []
    
    async def update_stock_basic_info(
        self,
        symbol: str,
        update_data: Dict[str, Any],
        source: str = "tushare"
    ) -> bool:
        """Update stock base information
        Args:
            symbol: 6-bit stock code
            update data: Update data
            source: data source (tushare/akshare/baostock), default Tushare
        Returns:
            Bool: Successful update
        """
        try:
            db = get_mongo_db_async()
            symbol6 = str(symbol).zfill(6)

            #Add Update Time
            update_data["updated_at"] = datetime.utcnow()

            #Make sure symbol field exists
            if "symbol" not in update_data:
                update_data["symbol"] = symbol6

            #Make sure the code field exists
            if "code" not in update_data:
                update_data["code"] = symbol6

            #Make sure field exists
            if "source" not in update_data:
                update_data["source"] = source

            #🔥 Execute updates (using code + source query)
            result = await db[self.basic_info_collection].update_one(
                {"code": symbol6, "source": source},
                {"$set": update_data},
                upsert=True
            )

            return result.modified_count > 0 or result.upserted_id is not None

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{symbol}, source={source}: {e}")
            return False
    
    async def update_market_quotes(
        self,
        symbol: str,
        quote_data: Dict[str, Any]
    ) -> bool:
        """Update Real Time Line Data
        Args:
            symbol: 6-bit stock code
            Quote data: fine data
        Returns:
            Bool: Successful update
        """
        try:
            db = get_mongo_db_async()
            symbol6 = str(symbol).zfill(6)

            #Add Update Time
            quote_data["updated_at"] = datetime.utcnow()

            #🔥 Ensure that symbol and code fields exist (compatible with old index)
            if "symbol" not in quote_data:
                quote_data["symbol"] = symbol6
            if "code" not in quote_data:
                quote_data["code"] = symbol6  #Code and symbol use the same value

            #Execute updates (using symbol fields as a search condition)
            result = await db[self.market_quotes_collection].update_one(
                {"symbol": symbol6},
                {"$set": quote_data},
                upsert=True
            )

            return result.modified_count > 0 or result.upserted_id is not None

        except Exception as e:
            logger.error(f"Update of real-time line failed{symbol}: {e}")
            return False
    
    def _standardize_basic_info(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Standardized stock basic information data
        Map existing fields to standardized fields
        """
        #Keep existing fields unchanged
        result = doc.copy()

        #Get stock code (prior to symbol, code compatible)
        symbol = doc.get("symbol") or doc.get("code", "")
        result["symbol"] = symbol

        #Compatible old fields
        if "code" in doc and "symbol" not in doc:
            result["code"] = doc["code"]
        
        #Generate a complete code (prefer existing full symbol)
        if "full_symbol" not in result or not result["full_symbol"]:
            if symbol and len(symbol) == 6:
                #By code, the exchange.
                if symbol.startswith(('60', '68', '90')):
                    result["full_symbol"] = f"{symbol}.SS"
                    exchange = "SSE"
                    exchange_name = "上海证券交易所"
                elif symbol.startswith(('00', '30', '20')):
                    result["full_symbol"] = f"{symbol}.SZ"
                    exchange = "SZSE"
                    exchange_name = "深圳证券交易所"
                else:
                    result["full_symbol"] = f"{symbol}.SZ"  #Default Deep Intersection
                    exchange = "SZSE"
                    exchange_name = "深圳证券交易所"
            else:
                exchange = "SZSE"
                exchange_name = "深圳证券交易所"
        else:
            #From full symbol
            full_symbol = result["full_symbol"]
            if ".SS" in full_symbol or ".SH" in full_symbol:
                exchange = "SSE"
                exchange_name = "上海证券交易所"
            else:
                exchange = "SZSE"
                exchange_name = "深圳证券交易所"
            
            #Add Market Information
            result["market_info"] = {
                "market": "CN",
                "exchange": exchange,
                "exchange_name": exchange_name,
                "currency": "CNY",
                "timezone": "Asia/Shanghai",
                "trading_hours": {
                    "open": "09:30",
                    "close": "15:00",
                    "lunch_break": ["11:30", "13:00"]
                }
            }
        
        #Field mapping and standardization
        result["board"] = doc.get("sse")  #Standardize plates
        result["sector"] = doc.get("sec")  #Standardize owned plates
        result["status"] = "L"  #Default listing status
        result["data_version"] = 1

        #Process date field conversion
        list_date = doc.get("list_date")
        if list_date and isinstance(list_date, int):
            #Convert integer date to string format (YYYYMMDD->YYYY-MM-DD)
            date_str = str(list_date)
            if len(date_str) == 8:
                result["list_date"] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            else:
                result["list_date"] = str(list_date)
        elif list_date:
            result["list_date"] = str(list_date)

        return result
    
    def _standardize_market_quotes(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Standardized real-time behaviour data
        Map existing fields to standardized fields
        """
        #Keep existing fields unchanged
        result = doc.copy()
        
        #Get stock code (prior to symbol, code compatible)
        symbol = doc.get("symbol") or doc.get("code", "")
        result["symbol"] = symbol

        #Compatible old fields
        if "code" in doc and "symbol" not in doc:
            result["code"] = doc["code"]

        #Generate complete code and market identification (prior to full symbol)
        if "full_symbol" not in result or not result["full_symbol"]:
            if symbol and len(symbol) == 6:
                if symbol.startswith(('60', '68', '90')):
                    result["full_symbol"] = f"{symbol}.SS"
                else:
                    result["full_symbol"] = f"{symbol}.SZ"

        if "market" not in result:
            result["market"] = "CN"
        
        #Field Map
        result["current_price"] = doc.get("close")  #Current price
        if doc.get("close") and doc.get("pre_close"):
            try:
                result["change"] = float(doc["close"]) - float(doc["pre_close"])
            except (ValueError, TypeError):
                result["change"] = None
        
        result["data_source"] = "market_quotes"
        result["data_version"] = 1
        
        return result


#Examples of global services
_stock_data_service = None

def get_stock_data_service() -> StockDataService:
    """Examples of accessing stock data services"""
    global _stock_data_service
    if _stock_data_service is None:
        _stock_data_service = StockDataService()
    return _stock_data_service
