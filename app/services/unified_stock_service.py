#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Harmonization of equity data services (cross-market, multi-data source support)

Function:
1. Cross-market data access (Unit A/Hong Kong/US)
2. Multi-data source priority queries
3. Unified query interface

Design specifications:
- Reference A multi-data source design
- The same stock has multiple data sources.
- Joint query through (code, source)
- Data source priority read from database configuration
"""

import logging
from typing import Dict, List, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase

logger = logging.getLogger("webapi")


class UnifiedStockService:
    """Harmonization of equity data services (cross-market, multi-data source support)"""

    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        
        #Pool Map
        self.collection_map = {
            "CN": {
                "basic_info": "stock_basic_info",
                "quotes": "market_quotes",
                "daily": "stock_daily_quotes",
                "financial": "stock_financial_data",
                "news": "stock_news"
            },
            "HK": {
                "basic_info": "stock_basic_info_hk",
                "quotes": "market_quotes_hk",
                "daily": "stock_daily_quotes_hk",
                "financial": "stock_financial_data_hk",
                "news": "stock_news_hk"
            },
            "US": {
                "basic_info": "stock_basic_info_us",
                "quotes": "market_quotes_us",
                "daily": "stock_daily_quotes_us",
                "financial": "stock_financial_data_us",
                "news": "stock_news_us"
            }
        }

    async def get_stock_info(
        self, 
        market: str, 
        code: str, 
        source: Optional[str] = None
    ) -> Optional[Dict]:
        """Access to stock base information (support to multiple data sources)

Args:
Market type (CN/HK/US)
code: stock code
source: specify data source (optional)

Returns:
Basic stock dictionary
"""
        collection_name = self.collection_map[market]["basic_info"]
        collection = self.db[collection_name]
        
        if source:
            #Specify data source
            query = {"code": code, "source": source}
            doc = await collection.find_one(query, {"_id": 0})
            if doc:
                logger.debug(f"Use specified data sources:{source}")
        else:
            #🔥 for priority queries (described in Unit A)
            source_priority = await self._get_source_priority(market)
            doc = None
            
            for src in source_priority:
                query = {"code": code, "source": src}
                doc = await collection.find_one(query, {"_id": 0})
                if doc:
                    logger.debug(f"Using data sources:{src}(Priority query)")
                    break
            
            #If not found, try not specifying source query (compatible with old data)
            if not doc:
                doc = await collection.find_one({"code": code}, {"_id": 0})
                if doc:
                    logger.debug(f"Use default data sources (compatibility mode)")
        
        return doc

    async def _get_source_priority(self, market: str) -> List[str]:
        """Data source priority from database

Args:
Market type (CN/HK/US)

Returns:
Data Source Priority List
"""
        market_category_map = {
            "CN": "a_shares",
            "HK": "hk_stocks",
            "US": "us_stocks"
        }
        
        market_category_id = market_category_map.get(market)
        
        try:
            #Query from data groupings
            groupings = await self.db.datasource_groupings.find({
                "market_category_id": market_category_id,
                "enabled": True
            }).sort("priority", -1).to_list(length=None)
            
            if groupings:
                priority_list = [g["data_source_name"] for g in groupings]
                logger.debug(f"📊 {market}Data source priority (from database):{priority_list}")
                return priority_list
        except Exception as e:
            logger.warning(f"Access to data source priorities from databases failed:{e}")
        
        #Default Priority
        default_priority = {
            "CN": ["tushare", "akshare", "baostock"],
            "HK": ["yfinance_hk", "akshare_hk"],
            "US": ["yfinance_us"]
        }
        priority_list = default_priority.get(market, [])
        logger.debug(f"📊 {market}Data source priority (default):{priority_list}")
        return priority_list

    async def get_stock_quote(self, market: str, code: str) -> Optional[Dict]:
        """Get Real Time Lines

Args:
Market type (CN/HK/US)
code: stock code

Returns:
Real-time Dictionary
"""
        collection_name = self.collection_map[market]["quotes"]
        collection = self.db[collection_name]
        return await collection.find_one({"code": code}, {"_id": 0})

    async def search_stocks(
        self, 
        market: str, 
        query: str, 
        limit: int = 20
    ) -> List[Dict]:
        """Search for stocks (weighted, return only to the best data source for each stock)

Args:
Market type (CN/HK/US)
query: Search key Word
Limited number of returns

Returns:
List of stocks
"""
        collection_name = self.collection_map[market]["basic_info"]
        collection = self.db[collection_name]

        #Support code and name search
        filter_query = {
            "$or": [
                {"code": {"$regex": query, "$options": "i"}},
                {"name": {"$regex": query, "$options": "i"}},
                {"name_en": {"$regex": query, "$options": "i"}}
            ]
        }

        #Query all matching records
        cursor = collection.find(filter_query)
        all_results = await cursor.to_list(length=None)
        
        if not all_results:
            return []
        
        #Group by code, each code only maintains the highest priority data source
        source_priority = await self._get_source_priority(market)
        unique_results = {}
        
        for doc in all_results:
            code = doc.get("code")
            source = doc.get("source")
            
            if code not in unique_results:
                unique_results[code] = doc
            else:
                #More Priority
                current_source = unique_results[code].get("source")
                try:
                    if source in source_priority and current_source in source_priority:
                        if source_priority.index(source) < source_priority.index(current_source):
                            unique_results[code] = doc
                except ValueError:
                    #Keep the current record if source is not in the priority list
                    pass
        
        #Return before limit
        result_list = list(unique_results.values())[:limit]
        logger.info(f"Search{market}Market: '{query}' -> {len(result_list)}Result (heavy)")
        return result_list

    async def get_daily_quotes(
        self,
        market: str,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """Access to historical K-line data

Args:
Market type (CN/HK/US)
code: stock code
Start date: Start date (YYYYY-MM-DD)
End date: End Date (YYYYY-MM-DD)
Limited number of returns

Returns:
K-line Data List
"""
        collection_name = self.collection_map[market]["daily"]
        collection = self.db[collection_name]
        
        query = {"code": code}
        if start_date or end_date:
            query["trade_date"] = {}
            if start_date:
                query["trade_date"]["$gte"] = start_date
            if end_date:
                query["trade_date"]["$lte"] = end_date
        
        cursor = collection.find(query, {"_id": 0}).sort("trade_date", -1).limit(limit)
        return await cursor.to_list(length=limit)

    async def get_supported_markets(self) -> List[Dict]:
        """List of markets to obtain support

Returns:
Market List
"""
        return [
            {
                "code": "CN",
                "name": "A股",
                "name_en": "China A-Share",
                "currency": "CNY",
                "timezone": "Asia/Shanghai"
            },
            {
                "code": "HK",
                "name": "港股",
                "name_en": "Hong Kong Stock",
                "currency": "HKD",
                "timezone": "Asia/Hong_Kong"
            },
            {
                "code": "US",
                "name": "美股",
                "name_en": "US Stock",
                "currency": "USD",
                "timezone": "America/New_York"
            }
        ]

