#!/usr/bin/env python3
"""MongoDB cache adapter
According to TA USE APP CACHE configuration, priority is given to simultaneous data from MongoDB
"""

import pandas as pd
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timedelta, timezone

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')

#Import Configuration
from tradingagents.config.runtime_settings import is_use_app_cache_enabled

class MongoDBCacheAdapter:
    """MongoDB cache adapter (read synchronized data from MongoDB in app)"""
    
    def __init__(self):
        self.use_app_cache = is_use_app_cache_enabled(False)
        self.mongodb_client = None
        self.db = None
        
        if self.use_app_cache:
            self._init_mongodb_connection()
            logger.info("MongoDB cache adapter enabled - Priority is given to MongoDB data")
        else:
            logger.info("MongoDB Cache Adapter uses traditional cache mode")
    
    def _init_mongodb_connection(self):
        """Initialize MongoDB connection"""
        try:
            from tradingagents.config.database_manager import get_mongodb_client
            self.mongodb_client = get_mongodb_client()
            if self.mongodb_client:
                self.db = self.mongodb_client.get_database('tradingagents')
                logger.debug("The MongoDB connection was successfully initialized")
            else:
                logger.warning("MongoDB client not available, back to traditional mode")
                self.use_app_cache = False
        except Exception as e:
            logger.warning(f"The initialization of the MongoDB connection failed:{e}")
            self.use_app_cache = False
    
    def get_stock_basic_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Access to stock base information (data source priority query)"""
        if not self.use_app_cache or self.db is None:
            return None

        try:
            code6 = str(symbol).zfill(6)
            collection = self.db.stock_basic_info

            #Access to data source priorities
            source_priority = self._get_data_source_priority(symbol)

            #Query by priority
            doc = None
            for src in source_priority:
                doc = collection.find_one({"code": code6, "source": src}, {"_id": 0})
                if doc:
                    logger.debug(f"Get basic information from MongoDB:{symbol}, data source:{src}")
                    return doc

            #Try without source condition query (compatible with old data) if all data sources are missing
            if not doc:
                doc = collection.find_one({"code": code6}, {"_id": 0})
                if doc:
                    logger.debug(f"Basic information from MongoDB (old data):{symbol}")
                    return doc
                else:
                    logger.debug(f"Basic information not found in MongoDB:{symbol}")
                    return None

        except Exception as e:
            logger.warning(f"Access to basic information failed:{e}")
            return None
    
    def _get_data_source_priority(self, symbol: str) -> list:
        """Get data source priorities

        Args:
            symbol: stock code

        Returns:
            List of data sources in order of priority, e.g.: ["tushare", "akshare", "baostock"]
        """
        try:
            #1. Identification of market classifications
            from tradingagents.utils.stock_utils import StockUtils, StockMarket
            market = StockUtils.identify_stock_market(symbol)

            market_mapping = {
                StockMarket.CHINA_A: 'a_shares',
                StockMarket.US: 'us_stocks',
                StockMarket.HONG_KONG: 'hk_stocks',
            }
            market_category = market_mapping.get(market)
            logger.info(f"[Data source priority] Stock code:{symbol}, market classification:{market_category}")

            #2. Read configuration from the database
            if self.db is not None:
                config_collection = self.db.system_configs
                config_data = config_collection.find_one(
                    {"is_active": True},
                    sort=[("version", -1)]
                )

                if config_data and config_data.get('data_source_configs'):
                    configs = config_data['data_source_configs']
                    logger.info(f"[Data Source Priority]{len(configs)}Data source configuration")

                    #3. Filter enabled data sources
                    enabled = []
                    for ds in configs:
                        ds_type = ds.get('type', '')
                        ds_enabled = ds.get('enabled', True)
                        ds_priority = ds.get('priority', 0)
                        ds_categories = ds.get('market_categories', [])

                        logger.info(f"[Data source configuration] Type:{ds_type}, enabled:{ds_enabled}, priority:{ds_priority}, Market:{ds_categories}")

                        if not ds_enabled:
                            logger.info(f"[Data source priority]{ds_type}Not enabled, Skip")
                            continue

                        #Check market classifications
                        if ds_categories and market_category:
                            if market_category not in ds_categories:
                                logger.info(f"[Data source priority]{ds_type}Not supporting markets{market_category}Skip")
                                continue

                        enabled.append(ds)

                    logger.info(f"[Data source priority]{len(enabled)}individual")

                    #4. Prioritization (the larger the number, the higher the priority)
                    enabled.sort(key=lambda x: x.get('priority', 0), reverse=True)

                    #Return list of data source types
                    result = [ds.get('type', '').lower() for ds in enabled if ds.get('type')]
                    if result:
                        logger.info(f"[Data source priority]{symbol} ({market_category}): {result}")
                        return result
                    else:
                        logger.warning(f"⚠️ [Data Source Priority] No data source configuration available, use default order")
                else:
                    logger.warning(f"⚠️ [data source priority] No data source configuration found in the database")

        except Exception as e:
            logger.error(f"Access to data source priority failed:{e}", exc_info=True)

        #Default order: Tushare > AKshare > BaoStock
        logger.info(f"['tushare', 'akshare', 'baostock']")
        return ['tushare', 'akshare', 'baostock']

    def get_historical_data(self, symbol: str, start_date: str = None, end_date: str = None,
                          period: str = "daily") -> Optional[pd.DataFrame]:
        """Access to historical data to support multi-cycle queries by data source priority

        Args:
            symbol: stock code
            Start date: Start date
            End date: End date
            period: data cycle (daily/weekly/monthly), default is Daily

        Returns:
            DataFrame: Historical data
        """
        if not self.use_app_cache or self.db is None:
            return None

        try:
            code6 = str(symbol).zfill(6)
            collection = self.db.stock_daily_quotes

            #Acquiring Data Source Priority
            priority_order = self._get_data_source_priority(symbol)

            #Query by Priority
            for data_source in priority_order:
                #Build query conditions
                query = {
                    "symbol": code6,
                    "period": period,
                    "data_source": data_source  #Specify data source
                }

                if start_date:
                    query["trade_date"] = {"$gte": start_date}
                if end_date:
                    if "trade_date" in query:
                        query["trade_date"]["$lte"] = end_date
                    else:
                        query["trade_date"] = {"$lte": end_date}

                #Query Data
                logger.debug(f"[MongoDB query]{data_source}, symbol={code6}, period={period}")
                cursor = collection.find(query, {"_id": 0}).sort("trade_date", 1)
                data = list(cursor)

                if data:
                    df = pd.DataFrame(data)
                    logger.info(f"[Data source: MongoDB-{data_source}] {symbol}, {len(df)}Record (period=){period})")
                    return df
                else:
                    logger.debug(f"⚠️ [MongoDB-{data_source}Not found{period}Data:{symbol}")

            #All data sources have no data.
            logger.warning(f"⚠️ [data source: MongoDB] All data sources{', '.join(priority_order)}None.{period}Data:{symbol}down to other data sources")
            return None

        except Exception as e:
            logger.warning(f"This post is part of our special coverage Egypt Protests 2011.{e}")
            return None
    
    def get_financial_data(self, symbol: str, report_period: str = None) -> Optional[Dict[str, Any]]:
        """Obtain financial data, query by data source priority"""
        if not self.use_app_cache or self.db is None:
            return None

        try:
            code6 = str(symbol).zfill(6)
            collection = self.db.stock_financial_data

            #Acquiring Data Source Priority
            priority_order = self._get_data_source_priority(symbol)

            #Query by Priority
            for data_source in priority_order:
                #Build query conditions
                query = {
                    "code": code6,
                    "data_source": data_source  #Specify data source
                }
                if report_period:
                    query["report_period"] = report_period

                #Access to up-to-date financial data
                doc = collection.find_one(query, {"_id": 0}, sort=[("report_period", -1)])

                if doc:
                    logger.info(f"[Data source: MongoDB-{data_source}] {symbol}Financial data")
                    logger.debug(f"📊 [Financial data] Successful extraction{symbol}, containing fields:{list(doc.keys())}")
                    return doc

            #All data sources have no data.
            logger.debug(f"All data sources have no financial data:{symbol}")
            return None

        except Exception as e:
            logger.warning(f"⚠️ [Data source: MongoDB-financial data]{e}")
            return None
    
    def get_news_data(self, symbol: str = None, hours_back: int = 24, limit: int = 20) -> Optional[List[Dict[str, Any]]]:
        """Access to news data"""
        if not self.use_app_cache or self.db is None:
            return None

        try:
            collection = self.db.stock_news  #Fix Collective Name
            
            #Build query conditions
            query = {}
            if symbol:
                code6 = str(symbol).zfill(6)
                query["symbol"] = code6
            
            #Time frame
            if hours_back:
                start_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)
                query["publish_time"] = {"$gte": start_time}
            
            #Query Data
            cursor = collection.find(query, {"_id": 0}).sort("publish_time", -1).limit(limit)
            data = list(cursor)
            
            if data:
                logger.debug(f"✅ [Data source: MongoDB-news data]{len(data)}Article")
                return data
            else:
                logger.debug(f"No news data found in MongoDB")
                return None

        except Exception as e:
            logger.warning(f"⚠️ [Data source: MongoDB-news data]{e}")
            return None
    
    def get_social_media_data(self, symbol: str = None, hours_back: int = 24, limit: int = 20) -> Optional[List[Dict[str, Any]]]:
        """Access to media data"""
        if not self.use_app_cache or self.db is None:
            return None
            
        try:
            collection = self.db.social_media_messages
            
            #Build query conditions
            query = {}
            if symbol:
                code6 = str(symbol).zfill(6)
                query["symbol"] = code6
            
            #Time frame
            if hours_back:
                start_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)
                query["publish_time"] = {"$gte": start_time}
            
            #Query Data
            cursor = collection.find(query, {"_id": 0}).sort("publish_time", -1).limit(limit)
            data = list(cursor)
            
            if data:
                logger.debug(f"Get media data from MongoDB:{len(data)}Article")
                return data
            else:
                logger.debug(f"Social data not found in MongoDB")
                return None
                
        except Exception as e:
            logger.warning(f"Access to media data failed:{e}")
            return None
    
    def get_market_quotes(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get Real Time Line Data"""
        if not self.use_app_cache or self.db is None:
            return None
            
        try:
            code6 = str(symbol).zfill(6)
            collection = self.db.market_quotes
            
            #Get Updates
            doc = collection.find_one({"code": code6}, {"_id": 0}, sort=[("timestamp", -1)])
            
            if doc:
                logger.debug(f"Get the data from MongoDB:{symbol}")
                return doc
            else:
                logger.debug(f"No line data found in MongoDB:{symbol}")
                return None
                
        except Exception as e:
            logger.warning(f"Can not get folder: %s: %s{e}")
            return None


#Global Examples
_mongodb_cache_adapter = None

def get_mongodb_cache_adapter() -> MongoDBCacheAdapter:
    """Get instance of a MongoDB cache adapter"""
    global _mongodb_cache_adapter
    if _mongodb_cache_adapter is None:
        _mongodb_cache_adapter = MongoDBCacheAdapter()
    return _mongodb_cache_adapter

#Backward compatible aliases
def get_enhanced_data_adapter() -> MongoDBCacheAdapter:
    """Example of acquisition of enhanced data adapter (postcompatibility, recommended for use"""
    return get_mongodb_cache_adapter()


def get_stock_data_with_fallback(symbol: str, start_date: str = None, end_date: str = None, 
                                fallback_func=None) -> Union[pd.DataFrame, str, None]:
    """Degraded stock acquisition

    Args:
        symbol: stock code
        Start date: Start date
        End date: End date
        fallback func: downgrade function

    Returns:
        Prioritize returns of MongoDB data, and call downgrade if failure
    """
    adapter = get_enhanced_data_adapter()
    
    #Try to get from MongoDB
    if adapter.use_app_cache:
        df = adapter.get_historical_data(symbol, start_date, end_date)
        if df is not None and not df.empty:
            logger.info(f"Using MongoDB historical data:{symbol}")
            return df
    
    #Down to the traditional way.
    if fallback_func:
        logger.info(f"Degraded to traditional data sources:{symbol}")
        return fallback_func(symbol, start_date, end_date)
    
    return None


def get_financial_data_with_fallback(symbol: str, fallback_func=None) -> Union[Dict[str, Any], str, None]:
    """Access to downgraded financial data

    Args:
        symbol: stock code
        fallback func: downgrade function

    Returns:
        Prioritize returns of MongoDB data, and call downgrade if failure
    """
    adapter = get_enhanced_data_adapter()
    
    #Try to get from MongoDB
    if adapter.use_app_cache:
        data = adapter.get_financial_data(symbol)
        if data:
            logger.info(f"Using MongoDB financial data:{symbol}")
            return data
    
    #Down to the traditional way.
    if fallback_func:
        logger.info(f"Degraded to traditional data sources:{symbol}")
        return fallback_func(symbol)
    
    return None
