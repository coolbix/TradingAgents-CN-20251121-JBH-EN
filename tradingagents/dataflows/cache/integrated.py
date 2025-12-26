#!/usr/bin/env python3
"""Integrated Cache Manager
Support in conjunction with the existing cache system and the new adaptation database
Provide backward compatible interfaces
"""

import os
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Union
import pandas as pd

#Import Unified Log System
from tradingagents.utils.logging_init import setup_dataflow_logging

#Import old cache system
from .file_cache import StockDataCache

#Import self-adapted cache system
try:
    from .adaptive import AdaptiveCacheSystem
    from tradingagents.config.database_manager import get_database_manager
    ADAPTIVE_CACHE_AVAILABLE = True
except ImportError as e:
    ADAPTIVE_CACHE_AVAILABLE = False
    import logging
    logging.getLogger(__name__).debug(f"自适应缓存不可用: {e}")

class IntegratedCacheManager:
    """Integrated Cache Manager - Smart Select Cache Policy"""
    
    def __init__(self, cache_dir: str = None):
        self.logger = setup_dataflow_logging()
        
        #Initialization of old cache system (as backup)
        self.legacy_cache = StockDataCache(cache_dir)
        
        #Try initializing self-adapted cache systems
        self.adaptive_cache = None
        self.use_adaptive = False
        
        if ADAPTIVE_CACHE_AVAILABLE:
            try:
                self.adaptive_cache = AdaptiveCacheSystem(cache_dir)
                self.db_manager = get_database_manager()
                self.use_adaptive = True
                self.logger.info("✅According cache system enabled")
            except Exception as e:
                self.logger.warning(f"Initialization of self-adapted cache systems failed, using traditional caches:{e}")
                self.use_adaptive = False
        else:
            self.logger.info("Self-adapted cache system not available, use traditional file cache")
        
        #Show Current Configuration
        self._log_cache_status()
    
    def _log_cache_status(self):
        """Record Cache Status"""
        if self.use_adaptive:
            backend = self.adaptive_cache.primary_backend
            mongodb_available = self.db_manager.is_mongodb_available()
            redis_available = self.db_manager.is_redis_available()
            
            self.logger.info(f"Cache configuration:")
            self.logger.info(f"Main backend:{backend}")
            self.logger.info(f"  MongoDB: {'Available' if mongodb_available else 'Not available'}")
            self.logger.info(f"  Redis: {'Available' if redis_available else 'Not available'}")
            self.logger.info(f"Deduction support:{'Enabled' if self.adaptive_cache.fallback_enabled else 'Disable'}")
        else:
            self.logger.info("Use the traditional file cache system")
    
    def save_stock_data(self, symbol: str, data: Any, start_date: str = None, 
                       end_date: str = None, data_source: str = "default") -> str:
        """Save stock data to cache

Args:
symbol: stock code
Data: Stock data
Start date: Start date
End date: End date
data source: data source

Returns:
Cache keys
"""
        if self.use_adaptive:
            #Use self-adapted cache system
            return self.adaptive_cache.save_data(
                symbol=symbol,
                data=data,
                start_date=start_date or "",
                end_date=end_date or "",
                data_source=data_source,
                data_type="stock_data"
            )
        else:
            #Use of traditional cache systems
            return self.legacy_cache.save_stock_data(
                symbol=symbol,
                data=data,
                start_date=start_date,
                end_date=end_date,
                data_source=data_source
            )
    
    def load_stock_data(self, cache_key: str) -> Optional[Any]:
        """Loading stock data from cache

Args:
Cache key: Cache keys

Returns:
Equities or None
"""
        if self.use_adaptive:
            #Use self-adapted cache system
            return self.adaptive_cache.load_data(cache_key)
        else:
            #Use of traditional cache systems
            return self.legacy_cache.load_stock_data(cache_key)
    
    def find_cached_stock_data(self, symbol: str, start_date: str = None, 
                              end_date: str = None, data_source: str = "default") -> Optional[str]:
        """Find cached stock data

Args:
symbol: stock code
Start date: Start date
End date: End date
data source: data source

Returns:
Cache keys or None
"""
        if self.use_adaptive:
            #Use self-adapted cache system
            return self.adaptive_cache.find_cached_data(
                symbol=symbol,
                start_date=start_date or "",
                end_date=end_date or "",
                data_source=data_source,
                data_type="stock_data"
            )
        else:
            #Use of traditional cache systems
            return self.legacy_cache.find_cached_stock_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                data_source=data_source
            )
    
    def save_news_data(self, symbol: str, data: Any, data_source: str = "default") -> str:
        """Preservation of news data"""
        if self.use_adaptive:
            return self.adaptive_cache.save_data(
                symbol=symbol,
                data=data,
                data_source=data_source,
                data_type="news_data"
            )
        else:
            return self.legacy_cache.save_news_data(symbol, data, data_source)
    
    def load_news_data(self, cache_key: str) -> Optional[Any]:
        """Loading news data"""
        if self.use_adaptive:
            return self.adaptive_cache.load_data(cache_key)
        else:
            return self.legacy_cache.load_news_data(cache_key)
    
    def save_fundamentals_data(self, symbol: str, data: Any, data_source: str = "default") -> str:
        """Save base face data"""
        if self.use_adaptive:
            return self.adaptive_cache.save_data(
                symbol=symbol,
                data=data,
                data_source=data_source,
                data_type="fundamentals_data"
            )
        else:
            return self.legacy_cache.save_fundamentals_data(symbol, data, data_source)
    
    def load_fundamentals_data(self, cache_key: str) -> Optional[Any]:
        """Load Basic Face Data"""
        if self.use_adaptive:
            return self.adaptive_cache.load_data(cache_key)
        else:
            return self.legacy_cache.load_fundamentals_data(cache_key)

    def find_cached_fundamentals_data(self, symbol: str, data_source: str = None,
                                     max_age_hours: int = None) -> Optional[str]:
        """Find matching base cache data

Args:
symbol: stock code
Data source: Data sources (e.g. "openai", "finnhub")
max age hours: maximum cache time (hours), use smart configuration for None

Returns:
Cache key: return the cache key if a valid cache is found, otherwise return the None
"""
        if self.use_adaptive:
            #Unsupported search function for custom cache, downgraded to file cache
            return self.legacy_cache.find_cached_fundamentals_data(symbol, data_source, max_age_hours)
        else:
            return self.legacy_cache.find_cached_fundamentals_data(symbol, data_source, max_age_hours)

    def is_fundamentals_cache_valid(self, symbol: str, data_source: str = None,
                                   max_age_hours: int = None) -> bool:
        """Check if the basic face cache is valid

Args:
symbol: stock code
data source: data source
max age hours: maximum cache time (hours)

Returns:
Bool: Cache validity
"""
        cache_key = self.find_cached_fundamentals_data(symbol, data_source, max_age_hours)
        return cache_key is not None

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistical information"""
        if self.use_adaptive:
            #Access to self-adapted cache statistics (already standard format)
            stats = self.adaptive_cache.get_cache_stats()

            #Add Cache System Information
            stats['cache_system'] = 'adaptive'

            #Ensure backend information exists
            if 'backend_info' not in stats:
                stats['backend_info'] = {}

            stats['backend_info']['database_available'] = self.db_manager.is_database_available()
            stats['backend_info']['mongodb_available'] = self.db_manager.is_mongodb_available()
            stats['backend_info']['redis_available'] = self.db_manager.is_redis_available()

            return stats
        else:
            #Return to traditional cache statistics (already standard format)
            stats = self.legacy_cache.get_cache_stats()

            #Add Cache System Information
            stats['cache_system'] = 'legacy'

            #Ensure backend information exists
            if 'backend_info' not in stats:
                stats['backend_info'] = {}

            stats['backend_info']['database_available'] = False
            stats['backend_info']['mongodb_available'] = False
            stats['backend_info']['redis_available'] = False

            return stats
    
    def clear_expired_cache(self):
        """Clear Expired Cache"""
        if self.use_adaptive:
            self.adaptive_cache.clear_expired_cache()

        #Always clear the traditional cache
        self.legacy_cache.clear_expired_cache()

    def clear_old_cache(self, max_age_days: int = 7):
        """Clear outdated caches (compatible with old interfaces)

Args:
max age days: Clean up how many days ago's cache, 0 means clear all caches

Returns:
Number of records cleared
"""
        cleared_count = 0

        #1. Clean-up of the Redis cache
        if self.use_adaptive and self.db_manager.is_redis_available():
            try:
                redis_client = self.db_manager.get_redis_client()
                if max_age_days == 0:
                    #Clear all caches
                    redis_client.flushdb()
                    self.logger.info(f"Redis cache cleared.")
                else:
                    #Redis will automatically expire. Only logs are recorded here.
                    self.logger.info(f"🧹Redis Cache will automatically expire (TTL mechanism)")
            except Exception as e:
                self.logger.error(f"Redis cache cleanup failed:{e}")

        #Clean-up MongoDB cache
        if self.use_adaptive and self.db_manager.is_mongodb_available():
            try:
                from datetime import datetime, timedelta
                from zoneinfo import ZoneInfo
                from tradingagents.config.runtime_settings import get_timezone_name

                mongodb_db = self.db_manager.get_mongodb_db()

                if max_age_days == 0:
                    #Clear all caches.
                    for collection_name in ["stock_data", "news_data", "fundamentals_data"]:
                        result = mongodb_db[collection_name].delete_many({})
                        cleared_count += result.deleted_count
                        self.logger.info(f"🧹 MongoDB {collection_name}It's empty.{result.deleted_count}Notes")
                else:
                    #Clear Expiry Data
                    cutoff_time = datetime.now(ZoneInfo(get_timezone_name())) - timedelta(days=max_age_days)
                    for collection_name in ["stock_data", "news_data", "fundamentals_data"]:
                        result = mongodb_db[collection_name].delete_many({"created_at": {"$lt": cutoff_time}})
                        cleared_count += result.deleted_count
                        self.logger.info(f"🧹 MongoDB {collection_name}It's clean.{result.deleted_count}Notes")
            except Exception as e:
                self.logger.error(f"MongoDB cache cleanup failed:{e}")

        #3. Clearing file caches
        try:
            file_cleared = self.legacy_cache.clear_old_cache(max_age_days)
            #File cache may return None, need to process
            if file_cleared is not None:
                cleared_count += file_cleared
                self.logger.info(f"File cache cleared.{file_cleared}File")
            else:
                self.logger.info(f"File cache cleanup completed (return value None)")
        except Exception as e:
            self.logger.error(f"File cache failed:{e}")

        self.logger.info(f"Total cleanup.{cleared_count}Cache Record")
        return cleared_count
    
    def get_cache_backend_info(self) -> Dict[str, Any]:
        """Fetch Cache Backend Information"""
        if self.use_adaptive:
            return {
                "system": "adaptive",
                "primary_backend": self.adaptive_cache.primary_backend,
                "fallback_enabled": self.adaptive_cache.fallback_enabled,
                "mongodb_available": self.db_manager.is_mongodb_available(),
                "redis_available": self.db_manager.is_redis_available()
            }
        else:
            return {
                "system": "legacy",
                "primary_backend": "file",
                "fallback_enabled": False,
                "mongodb_available": False,
                "redis_available": False
            }
    
    def is_database_available(self) -> bool:
        """Check database availability"""
        if self.use_adaptive:
            return self.db_manager.is_database_available()
        return False
    
    def get_performance_mode(self) -> str:
        """Acquisition Performance Mode"""
        if not self.use_adaptive:
            return "基础模式 (文件缓存)"
        
        mongodb_available = self.db_manager.is_mongodb_available()
        redis_available = self.db_manager.is_redis_available()
        
        if redis_available and mongodb_available:
            return "高性能模式 (Redis + MongoDB + 文件)"
        elif redis_available:
            return "快速模式 (Redis + 文件)"
        elif mongodb_available:
            return "持久化模式 (MongoDB + 文件)"
        else:
            return "标准模式 (智能文件缓存)"


#Examples of global integration cache manager
_integrated_cache = None

def get_cache() -> IntegratedCacheManager:
    """Fetch global integration cache manager instance"""
    global _integrated_cache
    if _integrated_cache is None:
        _integrated_cache = IntegratedCacheManager()
    return _integrated_cache

#Functions compatible backwards
def get_stock_cache():
    """Backward compatibility: capture stock caches"""
    return get_cache()

def create_cache_manager(cache_dir: str = None):
    """Backward compatibility: create cache manager"""
    return IntegratedCacheManager(cache_dir)
