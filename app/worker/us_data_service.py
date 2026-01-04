#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""United States share data services (Access + Cache mode)

Function:
1. Access to United States share information from data sources, as required (yfinance/finnhub)
Automatically cache to MongoDB to avoid duplication of requests
3. Supporting multiple data sources: multiple data sources can be recorded for the same stock
4. Use (code, source) joint query for upsert operations

Design specifications:
- Use a needs-based + cache model to avoid batch-synchronised trigger rate limits
- Reference to Unit A data source management (Tushare/AKshare/BaoStock)
- Cache duration configured (default 24 hours)
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Any

#Import U.S. Stock Data Provider
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from tradingagents.dataflows.providers.us.optimized import OptimizedUSDataProvider
from app.core.database import get_mongo_db
from app.core.config import settings

logger = logging.getLogger(__name__)


class USDataService:
    """United States share data services (Access + Cache mode)"""

    def __init__(self):
        self.db = get_mongo_db()
        self.settings = settings

        #Data Provider Map
        self.providers = {
            "yfinance": OptimizedUSDataProvider(),
            #Add more data sources, e. g. Finnhub
        }
        
        #Cache Configuration
        self.cache_hours = getattr(settings, 'US_DATA_CACHE_HOURS', 24)
        self.default_source = getattr(settings, 'US_DEFAULT_DATA_SOURCE', 'yfinance')

    async def initialize(self):
        """Initializing data services"""
        logger.info("Initialization of U.S. stock data service completed")
    
    async def get_stock_info(
        self, 
        stock_code: str, 
        source: Optional[str] = None,
        force_refresh: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Access to US stock basic information (Access + Cache)

        Args:
            Stock code: Stock code (e. g. "AAPL")
            source: data source (yfinance/finnhub), None uses default data source
            Force refresh: whether to forcibly refresh (ignore cache)

        Returns:
            Stock Dictionary, failed to return None
        """
        try:
            #Use default data sources
            if source is None:
                source = self.default_source
            
            #Standardised stock code (US stock code usually capitalised)
            normalized_code = stock_code.upper()
            
            #Check Cache
            if not force_refresh:
                cached_info = await self._get_cached_info(normalized_code, source)
                if cached_info:
                    logger.debug(f"Use cache data:{normalized_code} ({source})")
                    return cached_info
            
            #Obtaining from data sources
            provider = self.providers.get(source)
            if not provider:
                logger.error(f"Data sources not supported:{source}")
                return None
            
            logger.info(f"From{source}Access to U.S. stock information:{stock_code}")
            stock_info = provider.get_stock_info(stock_code)
            
            if not stock_info or not stock_info.get('name'):
                logger.warning(f"Could not close temporary folder: %s{stock_code} ({source})")
                return None
            
            #Standardize and save to cache
            normalized_info = self._normalize_stock_info(stock_info, source)
            normalized_info["code"] = normalized_code
            normalized_info["source"] = source
            normalized_info["updated_at"] = datetime.now()
            
            await self._save_to_cache(normalized_info)
            
            logger.info(f"Success:{normalized_code} - {stock_info.get('name')} ({source})")
            return normalized_info
            
        except Exception as e:
            logger.error(f"This post is part of our special coverage Egypt Protests 2011.{stock_code} ({source}): {e}")
            return None
    
    async def _get_cached_info(self, code: str, source: str) -> Optional[Dict[str, Any]]:
        """Fetching stock information from cache"""
        try:
            cache_expire_time = datetime.now() - timedelta(hours=self.cache_hours)
            
            cached = await self.db.stock_basic_info_us.find_one({
                "code": code,
                "source": source,
                "updated_at": {"$gte": cache_expire_time}
            })
            
            return cached
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{code} ({source}): {e}")
            return None
    
    async def _save_to_cache(self, stock_info: Dict[str, Any]) -> bool:
        """Can not open message"""
        try:
            await self.db.stock_basic_info_us.update_one(
                {"code": stock_info["code"], "source": stock_info["source"]},
                {"$set": stock_info},
                upsert=True
            )
            return True
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{stock_info.get('code')} ({stock_info.get('source')}): {e}")
            return False
    
    def _normalize_stock_info(self, stock_info: Dict, source: str) -> Dict:
        """Standardized stock information format

        Args:
            stock info: raw stock information
            source:

        Returns:
            Standardized equity information
        """
        normalized = {
            "name": stock_info.get("name", ""),
            "currency": stock_info.get("currency", "USD"),
            "exchange": stock_info.get("exchange", "NASDAQ"),
            "market": stock_info.get("market", "美国市场"),
            "area": stock_info.get("area", "美国"),
        }
        
        #Optional Fields
        optional_fields = [
            "industry", "sector", "list_date", "total_mv", "circ_mv",
            "pe", "pb", "ps", "pcf", "market_cap", "shares_outstanding",
            "float_shares", "employees", "website", "description"
        ]
        
        for field in optional_fields:
            if field in stock_info and stock_info[field]:
                normalized[field] = stock_info[field]
        
        return normalized


#== sync, corrected by elderman == @elder man

_us_data_service = None


async def get_us_data_service() -> USDataService:
    """Examples of access to United States stock data services (single model)"""
    global _us_data_service
    if _us_data_service is None:
        _us_data_service = USDataService()
        await _us_data_service.initialize()
    return _us_data_service

