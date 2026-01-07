"""Stock screening service based on MongoDB
Efficient screening using basic stock information in local databases Select
"""

import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from app.core.database import get_mongo_db_async
#From app.models. avoiding import cycle

logger = logging.getLogger(__name__)


class DatabaseScreeningService:
    """Database-based stock screening services"""
    
    def __init__(self):
        #Use view instead of base information sheet, which already contains real-time line data
        self.collection_name = "stock_screening_view"
        
        #Supported base information field map
        self.basic_fields = {
            #Basic information
            "code": "code",
            "name": "name", 
            "industry": "industry",
            "area": "area",
            "market": "market",
            "list_date": "list_date",
            
            #Market value information (billions of dollars)
            "total_mv": "total_mv",      #Total market value
            "circ_mv": "circ_mv",        #Market value in circulation
            "market_cap": "total_mv",    #Market value aliases

            #Financial indicators
            "pe": "pe",                  #Earnings
            "pb": "pb",                  #Net market rate
            "pe_ttm": "pe_ttm",         #Rolling surplus
            "pb_mrq": "pb_mrq",         #Newest net market rate
            "roe": "roe",                #Net asset rate of return (latest period)

            #Transaction indicators
            "turnover_rate": "turnover_rate",  #Exchange rate %
            "volume_ratio": "volume_ratio",    #Scale

            #Real-time line-line fields (require contact query from markt quotes)
            "pct_chg": "pct_chg",              #% Increase or Decline
            "amount": "amount",                #(millions of dollars)
            "close": "close",                  #Discount price
            "volume": "volume",                #Exchange
        }
        
        #Supported Operators
        self.operators = {
            ">": "$gt",
            "<": "$lt", 
            ">=": "$gte",
            "<=": "$lte",
            "==": "$eq",
            "!=": "$ne",
            "between": "$between",  #Custom Process
            "in": "$in",
            "not_in": "$nin",
            "contains": "$regex",   #String contains
        }
    
    async def can_handle_conditions(self, conditions: List[Dict[str, Any]]) -> bool:
        """Check if these conditions can be fully processed through database screening

        Args:
            Conditions: Filter Condition List

        Returns:
            Bool: Can it be handled
        """
        for condition in conditions:
            field = condition.get("field") if isinstance(condition, dict) else condition.field
            operator = condition.get("operator") if isinstance(condition, dict) else condition.operator
            
            #Checks if fields support
            if field not in self.basic_fields:
                logger.debug(f"Fields{field}Database filter is not supported")
                return False
            
            #Check if operator supports
            if operator not in self.operators:
                logger.debug(f"Operator{operator}Database filter is not supported")
                return False
        
        return True
    
    async def screen_stocks(
        self,
        conditions: List[Dict[str, Any]],
        limit: int = 50,
        offset: int = 0,
        order_by: Optional[List[Dict[str, str]]] = None,
        source: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Share screening based on database

        Args:
            Conditions: Filter Condition List
            Limited number of returns
            offset: offset
            Order by: Sort Conditions [  FMT 0   ]
            source: data source (optional), default use of highest priority data source

        Returns:
            Tuple [List [Dict], int]: (screening results, total number)
        """
        try:
            db = get_mongo_db_async()
            collection = db[self.collection_name]

            #Access source priority configuration
            if not source:
                from app.core.unified_config import UnifiedConfigManager
                config = UnifiedConfigManager()
                data_source_configs = await config.get_data_source_configs_async()

                logger.info(f"[database screening]{len(data_source_configs)}Data source configuration")
                for ds in data_source_configs:
                    logger.info(f"   - {ds.name}: type={ds.type}, priority={ds.priority}, enabled={ds.enabled}")

                #Extract enabled data sources in order of priority
                enabled_sources = [
                    ds.type.lower() for ds in data_source_configs
                    if ds.enabled and ds.type.lower() in ['tushare', 'akshare', 'baostock']
                ]

                logger.info(f"🔍 [database screening] enabled data sources (by priority):{enabled_sources}")

                if not enabled_sources:
                    enabled_sources = ['tushare', 'akshare', 'baostock']
                    logger.warning(f"[database screening]{enabled_sources}")

                source = enabled_sources[0] if enabled_sources else 'tushare'
                logger.info(f"[database screening]{source}")

            #Build query conditions (now the view contains real-time line data and can directly query all fields)
            query = await self._build_query(conditions)

            #Add Data Source Filter
            query["source"] = source

            logger.info(f"Database search conditions:{query}")

            #Build Sort Conditions
            sort_conditions = self._build_sort_conditions(order_by)

            #Total acquisitions
            total_count = await collection.count_documents(query)

            #Execute queries
            cursor = collection.find(query)

            #Apply Sorting
            if sort_conditions:
                cursor = cursor.sort(sort_conditions)

            #Apply page breaks
            cursor = cursor.skip(offset).limit(limit)

            #Get results
            results = []
            codes = []
            async for doc in cursor:
                #Convert Results Formatting
                result = self._format_result(doc)
                results.append(result)
                codes.append(doc.get("code"))

            #Batch query for financial data (ROE etc.) - if not included in view
            if codes:
                await self._enrich_with_financial_data(results, codes)

            logger.info(f"Database screening completed: Total ={total_count}returns ={len(results)}, data source={source}")

            return results, total_count
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            raise Exception(f"数据库筛选失败: {str(e)}")
    
    async def _build_query(self, conditions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build MongoDB query condition"""
        query = {}

        for condition in conditions:
            field = condition.get("field") if isinstance(condition, dict) else condition.field
            operator = condition.get("operator") if isinstance(condition, dict) else condition.operator
            value = condition.get("value") if isinstance(condition, dict) else condition.value

            logger.info(f"[ building query]{field}, operator={operator}, value={value}")

            #Map field name
            db_field = self.basic_fields.get(field)
            if not db_field:
                logger.warning(f"Fields{field}Not in Basic fields Map, Skip")
                continue

            logger.info(f"✅[build query] field map:{field} -> {db_field}")
            
            #Deal with different operators
            if operator == "between":
                #Between takes two values
                if isinstance(value, list) and len(value) == 2:
                    query[db_field] = {
                        "$gte": value[0],
                        "$lte": value[1]
                    }
            elif operator == "contains":
                #String contains (no case sensitive)
                query[db_field] = {
                    "$regex": str(value),
                    "$options": "i"
                }
            elif operator in self.operators:
                #Standard Operator
                mongo_op = self.operators[operator]
                query[db_field] = {mongo_op: value}
            
        return query
    
    def _build_sort_conditions(self, order_by: Optional[List[Dict[str, str]]]) -> List[Tuple[str, int]]:
        """Build Sort Conditions"""
        if not order_by:
            #Default Sort By Total Market Value Decline
            return [("total_mv", -1)]
        
        sort_conditions = []
        for order in order_by:
            field = order.get("field")
            direction = order.get("direction", "desc")
            
            #Map field name
            db_field = self.basic_fields.get(field)
            if not db_field:
                continue
            
            #Map Sort Direction
            sort_direction = -1 if direction.lower() == "desc" else 1
            sort_conditions.append((db_field, sort_direction))
        
        return sort_conditions
    
    async def _enrich_with_financial_data(self, results: List[Dict[str, Any]], codes: List[str]) -> None:
        """Batch search for financial data and fill in results

        Args:
            Results: Filter List
            codes: list of stock codes
        """
        try:
            db = get_mongo_db_async()
            financial_collection = db['stock_financial_data']

            #Access source priority configuration
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

            #Prioritize the highest priority data sources
            preferred_source = enabled_sources[0] if enabled_sources else 'tushare'

            #Batch searching for up-to-date financial data
            #Group by code to take the latest data for each code (only to query the highest priority data sources)
            pipeline = [
                {"$match": {"code": {"$in": codes}, "data_source": preferred_source}},
                {"$sort": {"code": 1, "report_period": -1}},
                {"$group": {
                    "_id": "$code",
                    "roe": {"$first": "$roe"},
                    "roa": {"$first": "$roa"},
                    "netprofit_margin": {"$first": "$netprofit_margin"},
                    "gross_margin": {"$first": "$gross_margin"},
                }}
            ]

            financial_data_map = {}
            async for doc in financial_collection.aggregate(pipeline):
                code = doc.get("_id")
                financial_data_map[code] = {
                    "roe": doc.get("roe"),
                    "roa": doc.get("roa"),
                    "netprofit_margin": doc.get("netprofit_margin"),
                    "gross_margin": doc.get("gross_margin"),
                }

            #Filling financial data to result
            for result in results:
                code = result.get("code")
                if code in financial_data_map:
                    financial_data = financial_data_map[code]
                    #Update ROE only (if not available at stock basic info)
                    if result.get("roe") is None:
                        result["roe"] = financial_data.get("roe")
                    #More financial indicators could be added
                    # result["roa"] = financial_data.get("roa")
                    # result["netprofit_margin"] = financial_data.get("netprofit_margin")

            logger.debug(f"Filled{len(financial_data_map)}Financial data")

        except Exception as e:
            logger.warning(f"Could not close temporary folder: %s{e}")
            #Do not throw anomalies and allow continued return to base data

    def _format_result(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Format query results, use back-end fields First Name"""
        #Infer market type by stock code
        code = doc.get("code", "")
        market_type = "A股"  #Default A Unit
        if code:
            if code.startswith("6"):
                market_type = "A股"  #Shanghai
            elif code.startswith(("0", "3")):
                market_type = "A股"  #Shenzhen.
            elif code.startswith("8") or code.startswith("4"):
                market_type = "A股"  #North China

        result = {
            #Basic information
            "code": doc.get("code"),
            "name": doc.get("name"),
            "industry": doc.get("industry"),
            "area": doc.get("area"),
            "market": market_type,  #Market type (A, US, HK)
            "board": doc.get("market"),  #Board (main board, entrepreneurship board, science board, etc.)
            "exchange": doc.get("sse"),  #Exchange (Shanghai Stock Exchange, Shenzhen Stock Exchange, etc.)
            "list_date": doc.get("list_date"),

            #Market value information (billions of dollars)
            "total_mv": doc.get("total_mv"),
            "circ_mv": doc.get("circ_mv"),

            #Financial indicators
            "pe": doc.get("pe"),
            "pb": doc.get("pb"),
            "pe_ttm": doc.get("pe_ttm"),
            "pb_mrq": doc.get("pb_mrq"),
            "roe": doc.get("roe"),

            #Transaction indicators
            "turnover_rate": doc.get("turnover_rate"),
            "volume_ratio": doc.get("volume_ratio"),

            #Transaction data (required from view, view already containing real-time line data)
            "close": doc.get("close"),              #Discount price
            "pct_chg": doc.get("pct_chg"),          #Increase/decrease (%)
            "amount": doc.get("amount"),            #Done
            "volume": doc.get("volume"),            #Exchange
            "open": doc.get("open"),                #Opening price
            "high": doc.get("high"),                #Maximum price
            "low": doc.get("low"),                  #Minimum price

            #Technical indicators (None for basic information screening)
            "ma20": None,
            "rsi14": None,
            "kdj_k": None,
            "kdj_d": None,
            "kdj_j": None,
            "dif": None,
            "dea": None,
            "macd_hist": None,

            #Metadata
            "source": doc.get("source", "database"),
            "updated_at": doc.get("updated_at"),
        }
        
        #Remove Noone
        return {k: v for k, v in result.items() if v is not None}
    
    async def get_field_statistics(self, field: str) -> Dict[str, Any]:
        """Fetch field statistics

        Args:
            Field: First Name

        Returns:
            Dict: Statistical information FMT 0 
        """
        try:
            db_field = self.basic_fields.get(field)
            if not db_field:
                return {}
            
            db = get_mongo_db_async()
            collection = db[self.collection_name]
            
            #Access to statistical information using a polymer conduit
            pipeline = [
                {"$match": {db_field: {"$exists": True, "$ne": None}}},
                {"$group": {
                    "_id": None,
                    "min": {"$min": f"${db_field}"},
                    "max": {"$max": f"${db_field}"},
                    "avg": {"$avg": f"${db_field}"},
                    "count": {"$sum": 1}
                }}
            ]
            
            result = await collection.aggregate(pipeline).to_list(length=1)
            
            if result:
                stats = result[0]
                avg_value = stats.get("avg")
                return {
                    "field": field,
                    "min": stats.get("min"),
                    "max": stats.get("max"),
                    "avg": round(avg_value, 2) if avg_value is not None else None,
                    "count": stats.get("count", 0)
                }
            
            return {"field": field, "count": 0}
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return {"field": field, "error": str(e)}
    
    def _separate_conditions(self, conditions: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Separation of basic information conditions and real-time conditions

        Args:
            Options: All filter conditions

        Returns:
            Tuple [list of basic information conditions, list of real-time line conditions]
        """
        #Real-time line field (required from market quotes)
        quote_fields = {"pct_chg", "amount", "close", "volume"}

        basic_conditions = []
        quote_conditions = []

        for condition in conditions:
            field = condition.get("field") if isinstance(condition, dict) else condition.field
            if field in quote_fields:
                quote_conditions.append(condition)
            else:
                basic_conditions.append(condition)

        return basic_conditions, quote_conditions

    async def _filter_by_quotes(
        self,
        results: List[Dict[str, Any]],
        codes: List[str],
        quote_conditions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Second screening based on real-time patterns

        Args:
            Results: Preliminary screening
            codes: list of stock codes
            quote conditions: real-time line filter conditions

        Returns:
            List [Dict]: Post-screen results
        """
        try:
            db = get_mongo_db_async()
            quotes_collection = db['market_quotes']

            #Batch query real-time line data
            quotes_cursor = quotes_collection.find({"code": {"$in": codes}})
            quotes_map = {}
            async for quote in quotes_cursor:
                code = quote.get("code")
                quotes_map[code] = {
                    "close": quote.get("close"),
                    "pct_chg": quote.get("pct_chg"),
                    "amount": quote.get("amount"),
                    "volume": quote.get("volume"),
                }

            logger.info(f"Other Organiser{len(quotes_map)}Real-time behaviour data for stocks only")

            #Filter Results
            filtered_results = []
            for result in results:
                code = result.get("code")
                quote_data = quotes_map.get(code)

                if not quote_data:
                    #No real-time behavioral data. Skip
                    continue

                #Check if all real-time conditions are met
                match = True
                for condition in quote_conditions:
                    field = condition.get("field") if isinstance(condition, dict) else condition.field
                    operator = condition.get("operator") if isinstance(condition, dict) else condition.operator
                    value = condition.get("value") if isinstance(condition, dict) else condition.value

                    field_value = quote_data.get(field)
                    if field_value is None:
                        match = False
                        break

                    #Check Conditions
                    if operator == "between" and isinstance(value, list) and len(value) == 2:
                        if not (value[0] <= field_value <= value[1]):
                            match = False
                            break
                    elif operator == ">":
                        if not (field_value > value):
                            match = False
                            break
                    elif operator == "<":
                        if not (field_value < value):
                            match = False
                            break
                    elif operator == ">=":
                        if not (field_value >= value):
                            match = False
                            break
                    elif operator == "<=":
                        if not (field_value <= value):
                            match = False
                            break

                if match:
                    #Merge real-time line data into results
                    result.update(quote_data)
                    filtered_results.append(result)

            logger.info(f"✅ Timeline screening complete: ={len(results)}, after filter ={len(filtered_results)}")
            return filtered_results

        except Exception as e:
            logger.error(f"The real-time line filter failed:{e}")
            #If failed, return original result
            return results

    async def get_available_values(self, field: str, limit: int = 100) -> List[str]:
        """List of optional values for fetching fields (for list type fields)

        Args:
            Field: First Name
            Limited number of returns

        Returns:
            List [str]: Optional list
        """
        try:
            db_field = self.basic_fields.get(field)
            if not db_field:
                return []
            
            db = get_mongo_db_async()
            collection = db[self.collection_name]
            
            #Get field non-duplicate values
            values = await collection.distinct(db_field)
            
            #Filter and sort Noone values
            values = [v for v in values if v is not None]
            values.sort()
            
            return values[:limit]
            
        except Exception as e:
            logger.error(f"Fetching fields failed:{e}")
            return []


#Examples of global services
_database_screening_service: Optional[DatabaseScreeningService] = None


def get_database_screening_service() -> DatabaseScreeningService:
    """Examples of accessing database filter services"""
    global _database_screening_service
    if _database_screening_service is None:
        _database_screening_service = DatabaseScreeningService()
    return _database_screening_service
