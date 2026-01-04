#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""U.S. Data Synchronization Service (support for multiple data sources)

Function:
1. Synchronize basic information and behaviour from yfinance
2. Supporting multi-data source storage: The same stock has multiple data sources.
3. Use (code, source) joint query for upsert operations

Design specifications:
- Reference A Multidata Source Sync Service Design (Tushare/AKshare/BaoStock)
- Mainly use yfinance as a data source
- Batch update operation to improve performance
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any
from pymongo import UpdateOne

#Import U.S. Stock Data Provider
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from tradingagents.dataflows.providers.us.yfinance import YFinanceUtils
from app.core.database import get_mongo_db
from app.core.config import settings

logger = logging.getLogger(__name__)


class USSyncService:
    """U.S. Data Synchronization Service (support for multiple data sources)"""

    def __init__(self):
        self.db = get_mongo_db()
        self.settings = settings

        #Data provider
        self.yfinance_provider = YFinanceUtils()

        #American share list cache (from Finnhub dynamic)
        self.us_stock_list = []
        self._stock_list_cache_time = None
        self._stock_list_cache_ttl = 3600 * 24  #Cache 24 hours

        #Finnhub client (delayed initialization)
        self._finnhub_client = None

    async def initialize(self):
        """Initializing Sync Service"""
        logger.info("Initialization of U.S.S.S. Synchronization Service completed")

    def _get_finnhub_client(self):
        """Get Finnhub client (delayed initialization)"""
        if self._finnhub_client is None:
            try:
                import finnhub
                import os

                api_key = os.getenv('FINNHUB_API_KEY')
                if not api_key:
                    logger.warning("Unconfigured FINNHUB API KEY, cannot use Finnhub data source")
                    return None

                self._finnhub_client = finnhub.Client(api_key=api_key)
                logger.info("Finnhub client initialised successfully")
            except Exception as e:
                logger.error(f"The initialization of Finnhub client failed:{e}")
                return None

        return self._finnhub_client

    def _get_us_stock_list_from_finnhub(self) -> List[str]:
        """Can not open message

        Returns:
            List [str]: List of US stock codes
        """
        try:
            from datetime import datetime, timedelta

            #Check if the cache is valid
            if (self.us_stock_list and self._stock_list_cache_time and
                datetime.now() - self._stock_list_cache_time < timedelta(seconds=self._stock_list_cache_ttl)):
                logger.debug(f"Use the cached list of American shares:{len(self.us_stock_list)}Only")
                return self.us_stock_list

            logger.info("Get America's List from Finnhub...")

            #Get Finnhub Client
            client = self._get_finnhub_client()
            if not client:
                logger.warning("⚠️ Finnhub client is not available, use a backup list")
                return self._get_fallback_stock_list()

            #Get US Stock List (US Exchange)
            symbols = client.stock_symbols('US')

            if not symbols:
                logger.warning("Finnhub returns empty data using the backup list")
                return self._get_fallback_stock_list()

            #Extract list of stock codes (sole stock only, filter ETF, fund etc.)
            stock_codes = []
            for symbol_info in symbols:
                symbol = symbol_info.get('symbol', '')
                symbol_type = symbol_info.get('type', '')

                #Only Common Stock
                if symbol and symbol_type == 'Common Stock':
                    stock_codes.append(symbol)

            logger.info(f"Successfully accessed{len(stock_codes)}US only (general)")

            #Update Cache
            self.us_stock_list = stock_codes
            self._stock_list_cache_time = datetime.now()

            return stock_codes

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            logger.info("Use the reserve US list")
            return self._get_fallback_stock_list()

    def _get_fallback_stock_list(self) -> List[str]:
        """Get a list of back-up United States shares (main US shares)

        Returns:
            List [str]: List of US stock codes
        """
        return [
            #The technology giant.
            "AAPL",   #Apple.
            "MSFT",   #Microsoft
            "GOOGL",  #Google.
            "AMZN",   #Amazon
            "META",   # Meta
            "TSLA",   #Tesla.
            "NVDA",   #Young Wai Da.
            "AMD",    # AMD
            "INTC",   #Intel.
            "NFLX",   #Nafie!
            #Finance
            "JPM",    #Morgan Chase.
            "BAC",    #Bank of America
            "WFC",    #Bank of Rich Countries
            "GS",     #Goldman Sachs.
            "MS",     #Morgan Stanley.
            #Consumption
            "KO",     #Coke.
            "PEP",    #Pepsi.
            "WMT",    #Wal-Mart.
            "HD",     #Family treasure.
            "MCD",    #McDonald's.
            #Medical
            "JNJ",    #Johnson.
            "PFE",    #Pfizer.
            "UNH",    #United Health
            "ABBV",   #Albwey.
            #Energy
            "XOM",    #Exxon.
            "CVX",    #Chevron.
        ]

    async def sync_basic_info_from_source(
        self,
        source: str = "yfinance",
        force_update: bool = False
    ) -> Dict[str, int]:
        """Synchronize U.S. fundamentals from specified data sources

        Args:
            source: data source name (default yfinance)
            force update: mandatory update (forced refreshing of list of shares)

        Returns:
            Dict: Sync Statistical Information
        """
        if source != "yfinance":
            logger.error(f"Data sources not supported:{source}")
            return {"updated": 0, "inserted": 0, "failed": 0}

        #Clear Cache if mandatory update
        if force_update:
            self._stock_list_cache_time = None
            logger.info("Force refreshing list of equity shares")

        #Retrieving list of shares (from Finnhub or cache)
        stock_list = self._get_us_stock_list_from_finnhub()

        if not stock_list:
            logger.error("Can not get folder: %s: %s")
            return {"updated": 0, "inserted": 0, "failed": 0}

        logger.info(f"Synchronization of US stock base information (data source:{source})")
        logger.info(f"Number of shares to be synchronized:{len(stock_list)}")

        operations = []
        failed_count = 0

        for stock_code in stock_list:
            try:
                #Fetch data from yfinance
                stock_info = self.yfinance_provider.get_stock_info(stock_code)
                
                if not stock_info or not stock_info.get('shortName'):
                    logger.warning(f"Skip invalid data:{stock_code}")
                    failed_count += 1
                    continue
                
                #Standardized data format
                normalized_info = self._normalize_stock_info(stock_info, source)
                normalized_info["code"] = stock_code.upper()
                normalized_info["source"] = source
                normalized_info["updated_at"] = datetime.now()
                
                #Batch Update Operation
                operations.append(
                    UpdateOne(
                        {"code": normalized_info["code"], "source": source},  #Joint query condition
                        {"$set": normalized_info},
                        upsert=True
                    )
                )
                
                logger.debug(f"Ready to sync:{stock_code} ({stock_info.get('shortName')}) from {source}")
                
            except Exception as e:
                logger.error(f"Synchronising {stock_code} from {source}: {e}")
                failed_count += 1
        
        #Execute Batch Operations
        result = {"updated": 0, "inserted": 0, "failed": failed_count}
        
        if operations:
            try:
                bulk_result = await self.db.stock_basic_info_us.bulk_write(operations)
                result["updated"] = bulk_result.modified_count
                result["inserted"] = bulk_result.upserted_count
                
                logger.info(
                    f"Synchronization of U.S. stock base information{source}): "
                    f"Update{result['updated']}Article,"
                    f"Insert{result['inserted']}Article,"
                    f"Failed{result['failed']}Article"
                )
            except Exception as e:
                logger.error(f"Batch writing failed:{e}")
                result["failed"] += len(operations)
        
        return result
    
    def _normalize_stock_info(self, stock_info: Dict, source: str) -> Dict:
        """Standardized stock information format

        Args:
            stock info: raw stock information
            source:

        Returns:
            Dict: Standardized stock information
        """
        #Extract General Fields
        normalized = {
            "name": stock_info.get("shortName", ""),
            "name_en": stock_info.get("longName", stock_info.get("shortName", "")),
            "currency": stock_info.get("currency", "USD"),
            "exchange": stock_info.get("exchange", "NASDAQ"),
            "market": stock_info.get("exchange", "NASDAQ"),
            "area": stock_info.get("country", "US"),
        }
        
        #Optional Fields
        if "marketCap" in stock_info and stock_info["marketCap"]:
            #Convert to US$ billion
            normalized["total_mv"] = stock_info["marketCap"] / 100000000
        
        if "sector" in stock_info:
            normalized["sector"] = stock_info["sector"]
        
        if "industry" in stock_info:
            normalized["industry"] = stock_info["industry"]
        
        return normalized
    
    async def sync_quotes_from_source(
        self,
        source: str = "yfinance"
    ) -> Dict[str, int]:
        """Sync U.S.S. Real-time from specified data source

        Args:
            source: data source name (default yfinance)

        Returns:
            Dict: Sync Statistical Information
        """
        if source != "yfinance":
            logger.error(f"Data sources not supported:{source}")
            return {"updated": 0, "inserted": 0, "failed": 0}
        
        logger.info(f"🇺🇸 >Sync U.S. Real-time performance (data source:{source})")
        
        operations = []
        failed_count = 0
        
        for stock_code in self.us_stock_list:
            try:
                #Obtain data from the latest day as a real-time case
                import yfinance as yf
                ticker = yf.Ticker(stock_code)
                data = ticker.history(period="1d")
                
                if data.empty:
                    logger.warning(f"Skipping invalids:{stock_code}")
                    failed_count += 1
                    continue
                
                latest = data.iloc[-1]
                
                #Standardized practice data
                normalized_quote = {
                    "code": stock_code.upper(),
                    "close": float(latest['Close']),
                    "open": float(latest['Open']),
                    "high": float(latest['High']),
                    "low": float(latest['Low']),
                    "volume": int(latest['Volume']),
                    "currency": "USD",
                    "updated_at": datetime.now()
                }
                
                #Calculating Increases and Declines
                if normalized_quote["open"] > 0:
                    pct_chg = ((normalized_quote["close"] - normalized_quote["open"]) / normalized_quote["open"]) * 100
                    normalized_quote["pct_chg"] = round(pct_chg, 2)
                
                operations.append(
                    UpdateOne(
                        {"code": normalized_quote["code"]},
                        {"$set": normalized_quote},
                        upsert=True
                    )
                )
                
                logger.debug(f"Ready to walk:{stock_code}(Pricing:{normalized_quote['close']} USD)")
                
            except Exception as e:
                logger.error(f"@❌ > & Walking failed:{stock_code}: {e}")
                failed_count += 1
        
        #Execute Batch Operations
        result = {"updated": 0, "inserted": 0, "failed": failed_count}
        
        if operations:
            try:
                bulk_result = await self.db.market_quotes_us.bulk_write(operations)
                result["updated"] = bulk_result.modified_count
                result["inserted"] = bulk_result.upserted_count
                
                logger.info(
                    f"U.S. stockline completion:"
                    f"Update{result['updated']}Article,"
                    f"Insert{result['inserted']}Article,"
                    f"Failed{result['failed']}Article"
                )
            except Exception as e:
                logger.error(f"Batch writing failed:{e}")
                result["failed"] += len(operations)
        
        return result


#== sync, corrected by elderman == @elder man

_us_sync_service = None

async def get_us_sync_service() -> USSyncService:
    """Example of getting U.S.S.S. Sync Service"""
    global _us_sync_service
    if _us_sync_service is None:
        _us_sync_service = USSyncService()
        await _us_sync_service.initialize()
    return _us_sync_service


#== sync, corrected by elderman == @elder man

async def run_us_yfinance_basic_info_sync(force_update: bool = False):
    """APScheduler mission: U.S. Basic Information Synchronization (yfinance)"""
    try:
        service = await get_us_sync_service()
        result = await service.sync_basic_info_from_source("yfinance", force_update)
        logger.info(f"U.S.U. Basic Information Synchronization (yfinance):{result}")
        return result
    except Exception as e:
        logger.error(f"U.S.U.S. Basic Information Failed (yfinance):{e}")
        raise


async def run_us_yfinance_quotes_sync():
    """APScheduler mission: real-time synchronization (yfinance)"""
    try:
        service = await get_us_sync_service()
        result = await service.sync_quotes_from_source("yfinance")
        logger.info(f"Real-time synchronisation of U.S. shares:{result}")
        return result
    except Exception as e:
        logger.error(f"U.S.U.S. Real-time sync failed:{e}")
        raise


async def run_us_status_check():
    """APScheduler mission: U.S. data source check"""
    try:
        service = await get_us_sync_service()
        #Refresh list of shares (if cache expired)
        stock_list = service._get_us_stock_list_from_finnhub()

        #Simple status check: returns stock list number
        result = {
            "status": "ok",
            "stock_count": len(stock_list),
            "data_source": "yfinance + finnhub",
            "timestamp": datetime.now().isoformat()
        }
        logger.info(f"The U.S. stock check is complete:{result}")
        return result
    except Exception as e:
        logger.error(f"The U.S. stock check failed:{e}")
        return {"status": "error", "error": str(e)}

