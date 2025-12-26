#!/usr/bin/env python3
"""Financial Data Synchronization Service
Synchronization of financial data for the integrated management of the three data sources
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from app.core.database import get_mongo_db
from app.services.financial_data_service import get_financial_data_service
from tradingagents.dataflows.providers.china.tushare import get_tushare_provider
from tradingagents.dataflows.providers.china.akshare import get_akshare_provider
from tradingagents.dataflows.providers.china.baostock import get_baostock_provider

logger = logging.getLogger(__name__)


@dataclass
class FinancialSyncStats:
    """Synchronization of financial data"""
    total_symbols: int = 0
    success_count: int = 0
    error_count: int = 0
    skipped_count: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration: float = 0.0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to Dictionary"""
        return {
            "total_symbols": self.total_symbols,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "skipped_count": self.skipped_count,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration": self.duration,
            "success_rate": round(self.success_count / max(self.total_symbols, 1) * 100, 2),
            "errors": self.errors[:10]  #Only 10 previous errors returned
        }


class FinancialDataSyncService:
    """Financial Data Synchronization Service"""
    
    def __init__(self):
        self.db = None
        self.financial_service = None
        self.providers = {}
        
    async def initialize(self):
        """Initialization services"""
        try:
            self.db = get_mongo_db()
            self.financial_service = await get_financial_data_service()
            
            #Provider of initialized data sources
            self.providers = {
                "tushare": get_tushare_provider(),
                "akshare": get_akshare_provider(),
                "baostock": get_baostock_provider()
            }
            
            logger.info("✅Initiation of financial data synchronization service successfully")
            
        except Exception as e:
            logger.error(f"The initialization of the Financial Data Synchronization Service failed:{e}")
            raise
    
    async def sync_financial_data(
        self,
        symbols: List[str] = None,
        data_sources: List[str] = None,
        report_types: List[str] = None,
        batch_size: int = 50,
        delay_seconds: float = 1.0
    ) -> Dict[str, FinancialSyncStats]:
        """Sync Financial Data

Args:
Symbols: list of stock codes. None means sync all stocks
Data sources: list of data sources
Report types: list of report types
Watch size: Batch size
Delay seconds: API call delay

Returns:
Synchronization of statistical results by data source
"""
        if self.db is None:
            await self.initialize()
        
        #Default parameters
        if data_sources is None:
            data_sources = ["tushare", "akshare", "baostock"]
        if report_types is None:
            report_types = ["quarterly", "annual"]  #We'll synchronize the quarterly and annual newspapers.
        
        logger.info(f"🔄 start financial data synchronization: data source={data_sources}, report type ={report_types}")
        
        #Get Stock List
        if symbols is None:
            symbols = await self._get_stock_symbols()
        
        if not symbols:
            logger.warning("No shares to synchronize.")
            return {}
        
        logger.info(f"Ready to sync.{len(symbols)}Financial data for equities only")
        
        #Synchronize for each data source
        results = {}
        
        for data_source in data_sources:
            if data_source not in self.providers:
                logger.warning(f"Data sources not supported:{data_source}")
                continue
            
            logger.info(f"Here we go.{data_source}Sync Financial Data...")
            
            stats = await self._sync_source_financial_data(
                data_source=data_source,
                symbols=symbols,
                report_types=report_types,
                batch_size=batch_size,
                delay_seconds=delay_seconds
            )
            
            results[data_source] = stats
            
            logger.info(f"✅ {data_source}Financial data synchronized:"
                       f"Success{stats.success_count}/{stats.total_symbols} "
                       f"({stats.success_count/max(stats.total_symbols,1)*100:.1f}%)")
        
        return results
    
    async def _sync_source_financial_data(
        self,
        data_source: str,
        symbols: List[str],
        report_types: List[str],
        batch_size: int,
        delay_seconds: float
    ) -> FinancialSyncStats:
        """Synchronize financial data for individual data sources"""
        stats = FinancialSyncStats()
        stats.total_symbols = len(symbols)
        stats.start_time = datetime.now(timezone.utc)
        
        provider = self.providers[data_source]
        
        #Check data source availability
        if not provider.is_available():
            logger.warning(f"⚠️ {data_source}Data source not available")
            stats.skipped_count = len(symbols)
            stats.end_time = datetime.now(timezone.utc)
            return stats
        
        #Batch treatment of stocks
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i + batch_size]
            
            logger.info(f"📈 {data_source}Process batch{i//batch_size + 1}: "
                       f"{len(batch_symbols)}Only stocks")
            
            #Shares in batch processed
            tasks = []
            for symbol in batch_symbols:
                task = self._sync_symbol_financial_data(
                    symbol=symbol,
                    data_source=data_source,
                    provider=provider,
                    report_types=report_types
                )
                tasks.append(task)
            
            #Carry out concurrent tasks
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            #Statistical batch results
            for j, result in enumerate(batch_results):
                symbol = batch_symbols[j]
                
                if isinstance(result, Exception):
                    stats.error_count += 1
                    stats.errors.append({
                        "symbol": symbol,
                        "data_source": data_source,
                        "error": str(result),
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })
                    logger.error(f"❌ {symbol}Synchronization of financial data failed (%1){data_source}): {result}")
                elif result:
                    stats.success_count += 1
                    logger.debug(f"✅ {symbol}Financial data synchronized successfully (%1){data_source})")
                else:
                    stats.skipped_count += 1
                    logger.debug(f"⏭️ {symbol}Financial data skip (%1){data_source})")
            
            #API-restricted flow delay
            if i + batch_size < len(symbols):
                await asyncio.sleep(delay_seconds)
        
        stats.end_time = datetime.now(timezone.utc)
        stats.duration = (stats.end_time - stats.start_time).total_seconds()
        
        return stats
    
    async def _sync_symbol_financial_data(
        self,
        symbol: str,
        data_source: str,
        provider: Any,
        report_types: List[str]
    ) -> bool:
        """Synchronize single equity financial data"""
        try:
            #Access to financial data
            financial_data = await provider.get_financial_data(symbol)
            
            if not financial_data:
                logger.debug(f"⚠️ {symbol}No financial data available{data_source})")
                return False
            
            #Save data for each reporting type
            saved_count = 0
            for report_type in report_types:
                count = await self.financial_service.save_financial_data(
                    symbol=symbol,
                    financial_data=financial_data,
                    data_source=data_source,
                    report_type=report_type
                )
                saved_count += count
            
            return saved_count > 0
            
        except Exception as e:
            logger.error(f"❌ {symbol}Financial Data Synchronization Abnormal ({data_source}): {e}")
            raise
    
    async def _get_stock_symbols(self) -> List[str]:
        """Get Stock Code List"""
        try:
            cursor = self.db.stock_basic_info.find(
                {
                    "$or": [
                        {"market_info.market": "CN"},  #New data structure
                        {"category": "stock_cn"},      #Old data structure
                        {"market": {"$in": ["主板", "创业板", "科创板", "北交所"]}}  #By market type
                    ]
                },
                {"code": 1}
            )

            symbols = [doc["code"] async for doc in cursor]
            logger.info(f"From stock basic info{len(symbols)}Stock code only")

            return symbols

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return []
    
    async def get_sync_statistics(self) -> Dict[str, Any]:
        """Get Sync Statistical Information"""
        try:
            if self.financial_service is None:
                await self.initialize()
            
            return await self.financial_service.get_financial_statistics()
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return {}
    
    async def sync_single_stock(
        self,
        symbol: str,
        data_sources: List[str] = None
    ) -> Dict[str, bool]:
        """Synchronize single equity financial data"""
        if self.db is None:
            await self.initialize()
        
        if data_sources is None:
            data_sources = ["tushare", "akshare", "baostock"]
        
        results = {}
        
        for data_source in data_sources:
            if data_source not in self.providers:
                results[data_source] = False
                continue
            
            try:
                provider = self.providers[data_source]
                
                if not provider.is_available():
                    results[data_source] = False
                    continue
                
                result = await self._sync_symbol_financial_data(
                    symbol=symbol,
                    data_source=data_source,
                    provider=provider,
                    report_types=["quarterly"]
                )
                
                results[data_source] = result
                
            except Exception as e:
                logger.error(f"❌ {symbol}Synchronization of single stock financial data failed (%1){data_source}): {e}")
                results[data_source] = False
        
        return results


#Examples of global services
_financial_sync_service = None


async def get_financial_sync_service() -> FinancialDataSyncService:
    """Examples of obtaining financial data synchronization services"""
    global _financial_sync_service
    if _financial_sync_service is None:
        _financial_sync_service = FinancialDataSyncService()
        await _financial_sync_service.initialize()
    return _financial_sync_service
