#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Port Unit data services (Access + Cache mode)

Function:
1. Access to port unit information (yfinance/akshare) on demand from data sources
Automatically cache to MongoDB to avoid duplication of requests
3. Supporting multiple data sources: multiple data sources can be recorded for the same stock
4. Use (code, source) joint query for upsert operations

Design specifications:
- Use a needs-based + cache model to avoid batch-synchronised trigger rate limits
- Reference to Unit A data source management (Tushare/AKshare/BaoStock)
- Cache duration configured (default 24 hours)
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from pymongo import UpdateOne

#Import Port Unit Data Provider
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from tradingagents.dataflows.providers.hk.hk_stock import HKStockProvider
from tradingagents.dataflows.providers.hk.improved_hk import ImprovedHKStockProvider
from app.core.database import get_mongo_db
from app.core.config import settings

logger = logging.getLogger(__name__)


class HKDataService:
    """Port Unit data services (Access + Cache mode)"""

    def __init__(self):
        self.db = get_mongo_db()
        self.settings = settings

        #Data Provider Map
        self.providers = {
            "yfinance": HKStockProvider(),
            "akshare": ImprovedHKStockProvider(),
        }

        #Cache Configuration
        self.cache_hours = getattr(settings, 'HK_DATA_CACHE_HOURS', 24)
        self.default_source = getattr(settings, 'HK_DEFAULT_DATA_SOURCE', 'yfinance')

        #Port List Cache (from AKShare Dynamic)
        self.hk_stock_list = []
        self._stock_list_cache_time = None
        self._stock_list_cache_ttl = 3600 * 24  #Cache 24 hours

    async def initialize(self):
        """Initializing Sync Service"""
        logger.info("Initialization of the Hong Kong Unit Synchronization Service completed")

    def _get_hk_stock_list_from_akshare(self) -> List[str]:
        """Can not open message

        Returns:
            List [str]: List of port unit codes
        """
        try:
            import akshare as ak
            from datetime import datetime, timedelta

            #Check if the cache is valid
            if (self.hk_stock_list and self._stock_list_cache_time and
                datetime.now() - self._stock_list_cache_time < timedelta(seconds=self._stock_list_cache_ttl)):
                logger.debug(f"📦 Use the cache list of port shares:{len(self.hk_stock_list)}Only")
                return self.hk_stock_list

            logger.info("Get the list of Hong Kong shares from AKShare...")

            #Access to all port units in real time (including codes and names)
            #Use of the New Wave financial interface (more stable)
            df = ak.stock_hk_spot()

            if df is None or df.empty:
                logger.warning("AKShare returns empty data using the backup list")
                return self._get_fallback_stock_list()

            #Extract stock code list
            stock_codes = df['代码'].tolist()

            #Standardized code format (ensure 5-digit)
            stock_codes = [code.zfill(5) for code in stock_codes if code]

            logger.info(f"Successfully accessed{len(stock_codes)}Port-only Unit")

            #Update Cache
            self.hk_stock_list = stock_codes
            self._stock_list_cache_time = datetime.now()

            return stock_codes

        except Exception as e:
            logger.error(f"The list of Hong Kong shares from AKShare failed:{e}")
            logger.info("📋Use the reserve list")
            return self._get_fallback_stock_list()

    def _get_fallback_stock_list(self) -> List[str]:
        """Getting a list of stand-by port units (based on major port markers)

        Returns:
            List [str]: List of port unit codes
        """
        return [
            "00700",  #Information Control
            "09988",  #Ali Baba.
            "03690",  #Corps
            "01810",  #Mi Group
            "00941",  #China Moves
            "00762",  #China Connect
            "00728",  #China Telecommunications
            "00939",  #Building banks
            "01398",  #Business Bank
            "03988",  #Bank of China
            "00005",  #HSBC Holdings
            "01299",  #Friends Insurance
            "02318",  #China is safe.
            "02628",  #Life in China
            "00857",  #Chinese Oil
            "00386",  #Chinese petrochemicals
            "01211",  #Biadi.
            "02015",  #The ideal car.
            "09868",  #Peng.
            "09866",  #A car.
        ]
    
    async def sync_basic_info_from_source(
        self,
        source: str,
        force_update: bool = False
    ) -> Dict[str, int]:
        """Synchronize basic information from specified data sources

        Args:
            Source name (yfinance/akshare)
            force update: mandatory update (forced refreshing of list of shares)

        Returns:
            Dict: Sync Statistical Information
        """
        #AKShare data source using batch sync
        if source == "akshare":
            return await self._sync_basic_info_from_akshare_batch(force_update)

        #yfinance Data Source Use Synchronization
        provider = self.providers.get(source)
        if not provider:
            logger.error(f"Data sources not supported:{source}")
            return {"updated": 0, "inserted": 0, "failed": 0}

        #Clear Cache if mandatory update
        if force_update:
            self._stock_list_cache_time = None
            logger.info("🔄 Forced updating of the list of shares")

        #Fetch list of port shares (from AKshare or cache)
        stock_list = self._get_hk_stock_list_from_akshare()

        if not stock_list:
            logger.error("Could not close temporary folder: %s")
            return {"updated": 0, "inserted": 0, "failed": 0}

        logger.info(f"Synchronization of basic information on the Port Unit (data source:{source})")
        logger.info(f"Number of shares to be synchronized:{len(stock_list)}")

        operations = []
        failed_count = 0

        for stock_code in stock_list:
            try:
                #Obtaining data from data sources
                stock_info = provider.get_stock_info(stock_code)

                if not stock_info or not stock_info.get('name'):
                    logger.warning(f"Skip invalid data:{stock_code}")
                    failed_count += 1
                    continue

                #Standardized data format
                normalized_info = self._normalize_stock_info(stock_info, source)
                normalized_info["code"] = stock_code.lstrip('0').zfill(5)  #Standardize to 5-bit code.
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

                logger.debug(f"Ready to sync:{stock_code} ({stock_info.get('name')}) from {source}")

            except Exception as e:
                logger.error(f"Synchronising {stock_code} from {source}: {e}")
                failed_count += 1

        #Execute Batch Operations
        result = {"updated": 0, "inserted": 0, "failed": failed_count}

        if operations:
            try:
                bulk_result = await self.db.stock_basic_info_hk.bulk_write(operations)
                result["updated"] = bulk_result.modified_count
                result["inserted"] = bulk_result.upserted_count

                logger.info(
                    f"✅Hong Kong Unit Basic Information Synchronized{source}): "
                    f"Update{result['updated']}Article,"
                    f"Insert{result['inserted']}Article,"
                    f"Failed{result['failed']}Article"
                )
            except Exception as e:
                logger.error(f"Batch writing failed:{e}")
                result["failed"] += len(operations)

        return result

    async def _sync_basic_info_from_akshare_batch(self, force_update: bool = False) -> Dict[str, int]:
        """Basic information (one API call for all data) from the AKShare Batch Synchronization Unit

        Args:
            force update: mandatory update (forced update of data)

        Returns:
            Dict: Sync Statistical Information
        """
        try:
            import akshare as ak
            from datetime import datetime

            logger.info("🇭🇰 Start batch synchronization of port unit basic information (data source: kshare)")

            #Access to all port units in real time (including basic information such as codes, names, etc.)
            #Use of the New Wave financial interface (more stable)
            df = ak.stock_hk_spot()

            if df is None or df.empty:
                logger.error("AKShare returns empty data")
                return {"updated": 0, "inserted": 0, "failed": 0}

            logger.info(f"Other Organiser{len(df)}Port-only data")

            operations = []
            failed_count = 0

            for _, row in df.iterrows():
                try:
                    #Extract stock code and name
                    stock_code = str(row.get('代码', '')).strip()
                    #The list for the New Wave interface is the Chinese name.
                    stock_name = str(row.get('中文名称', '')).strip()

                    if not stock_code or not stock_name:
                        failed_count += 1
                        continue

                    #Standardized code format (ensure 5-digit)
                    normalized_code = stock_code.lstrip('0').zfill(5)

                    #Build Basic Information
                    stock_info = {
                        "code": normalized_code,
                        "name": stock_name,
                        "currency": "HKD",
                        "exchange": "HKG",
                        "market": "香港交易所",
                        "area": "香港",
                        "source": "akshare",
                        "updated_at": datetime.now()
                    }

                    #optional field: extract other information from line data
                    if '最新价' in row and row['最新价']:
                        stock_info["latest_price"] = float(row['最新价'])

                    if '涨跌幅' in row and row['涨跌幅']:
                        stock_info["change_percent"] = float(row['涨跌幅'])

                    if '总市值' in row and row['总市值']:
                        #Convert to HK$ billion
                        stock_info["total_mv"] = float(row['总市值']) / 100000000

                    if '市盈率' in row and row['市盈率']:
                        stock_info["pe"] = float(row['市盈率'])

                    #Batch Update Operation
                    operations.append(
                        UpdateOne(
                            {"code": normalized_code, "source": "akshare"},
                            {"$set": stock_info},
                            upsert=True
                        )
                    )

                except Exception as e:
                    logger.debug(f"⚠️ failed to process stock data:{stock_code}: {e}")
                    failed_count += 1

            #Execute Batch Operations
            result = {"updated": 0, "inserted": 0, "failed": failed_count}

            if operations:
                try:
                    bulk_result = await self.db.stock_basic_info_hk.bulk_write(operations)
                    result["updated"] = bulk_result.modified_count
                    result["inserted"] = bulk_result.upserted_count

                    logger.info(
                        f"✅."
                        f"Update{result['updated']}Article,"
                        f"Insert{result['inserted']}Article,"
                        f"Failed{result['failed']}Article"
                    )
                except Exception as e:
                    logger.error(f"Batch writing failed:{e}")
                    result["failed"] += len(operations)

            return result

        except Exception as e:
            logger.error(f"An AKShare batch sync failed:{e}")
            return {"updated": 0, "inserted": 0, "failed": 0}

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
            "name": stock_info.get("name", ""),
            "name_en": stock_info.get("name_en", ""),
            "currency": stock_info.get("currency", "HKD"),
            "exchange": stock_info.get("exchange", "HKG"),
            "market": "香港交易所",
            "area": "香港",
        }
        
        #Optional Fields
        if "market_cap" in stock_info and stock_info["market_cap"]:
            #Convert to HK$ billion
            normalized["total_mv"] = stock_info["market_cap"] / 100000000
        
        if "sector" in stock_info:
            normalized["sector"] = stock_info["sector"]
        
        if "industry" in stock_info:
            normalized["industry"] = stock_info["industry"]
        
        return normalized
    
    async def sync_quotes_from_source(
        self,
        source: str = "yfinance"
    ) -> Dict[str, int]:
        """Synchronization of port units from specified data sources

        Args:
            source: data source name (default yfinance)

        Returns:
            Dict: Sync Statistical Information
        """
        provider = self.providers.get(source)
        if not provider:
            logger.error(f"Data sources not supported:{source}")
            return {"updated": 0, "inserted": 0, "failed": 0}
        
        logger.info(f"🇭🇰 to synchronize the real-time performance of the Port Unit (data source:{source})")
        
        operations = []
        failed_count = 0
        
        for stock_code in self.hk_stock_list:
            try:
                #Get real-time prices
                quote = provider.get_real_time_price(stock_code)
                
                if not quote or not quote.get('price'):
                    logger.warning(f"Skipping invalids:{stock_code}")
                    failed_count += 1
                    continue
                
                #Standardized practice data
                normalized_quote = {
                    "code": stock_code.lstrip('0').zfill(5),
                    "close": float(quote.get('price', 0)),
                    "open": float(quote.get('open', 0)),
                    "high": float(quote.get('high', 0)),
                    "low": float(quote.get('low', 0)),
                    "volume": int(quote.get('volume', 0)),
                    "currency": "HKD",
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
                
                logger.debug(f"Ready to walk:{stock_code}(Pricing:{normalized_quote['close']} HKD)")
                
            except Exception as e:
                logger.error(f"@❌ > & Walking failed:{stock_code}: {e}")
                failed_count += 1
        
        #Execute Batch Operations
        result = {"updated": 0, "inserted": 0, "failed": failed_count}
        
        if operations:
            try:
                bulk_result = await self.db.market_quotes_hk.bulk_write(operations)
                result["updated"] = bulk_result.modified_count
                result["inserted"] = bulk_result.upserted_count
                
                logger.info(
                    f"The Hong Kong stockline has been synchronized:"
                    f"Update{result['updated']}Article,"
                    f"Insert{result['inserted']}Article,"
                    f"Failed{result['failed']}Article"
                )
            except Exception as e:
                logger.error(f"Batch writing failed:{e}")
                result["failed"] += len(operations)
        
        return result


#== sync, corrected by elderman == @elder man

_hk_sync_service = None

async def get_hk_sync_service() -> HKSyncService:
    """Examples of access to port unit synchronization services"""
    global _hk_sync_service
    if _hk_sync_service is None:
        _hk_sync_service = HKSyncService()
        await _hk_sync_service.initialize()
    return _hk_sync_service


#== sync, corrected by elderman == @elder man

async def run_hk_yfinance_basic_info_sync(force_update: bool = False):
    """APScheduler mission: Basic information synchronization (yfinance)"""
    try:
        service = await get_hk_sync_service()
        result = await service.sync_basic_info_from_source("yfinance", force_update)
        logger.info(f"✅Synthesizing Basic Information (yfinance):{result}")
        return result
    except Exception as e:
        logger.error(f"❌Synthesis failed (yfinance):{e}")
        raise


async def run_hk_akshare_basic_info_sync(force_update: bool = False):
    """APScheduler mission: Basic information synchronization (kshare)"""
    try:
        service = await get_hk_sync_service()
        result = await service.sync_basic_info_from_source("akshare", force_update)
        logger.info(f"✅Hong Kong Unit Basic Information Synchronized (AKShare):{result}")
        return result
    except Exception as e:
        logger.error(f"(AKShare):{e}")
        raise


async def run_hk_yfinance_quotes_sync():
    """APScheduler mission: Real-time synchronization (yfinance)"""
    try:
        service = await get_hk_sync_service()
        result = await service.sync_quotes_from_source("yfinance")
        logger.info(f"✅ Time-time synchronization of Hong Kong Unit completed:{result}")
        return result
    except Exception as e:
        logger.error(f"The real-time synchronisation of the Hong Kong Unit failed:{e}")
        raise


async def run_hk_status_check():
    """APScheduler mission: Port Unit data source check"""
    try:
        service = await get_hk_sync_service()
        #Refresh list of shares (if cache expired)
        stock_list = service._get_hk_stock_list_from_akshare()

        #Simple status check: returns stock list number
        result = {
            "status": "ok",
            "stock_count": len(stock_list),
            "data_sources": list(service.providers.keys()),
            "timestamp": datetime.now().isoformat()
        }
        logger.info(f"Port Unit status check completed:{result}")
        return result
    except Exception as e:
        logger.error(f"Port Unit status check failed:{e}")
        return {"status": "error", "error": str(e)}

