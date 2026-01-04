"""Cache Management Module

Supporting a variety of cache strategies:
- File cache (default) - simple and stable, not dependent on external services
- Database cache (optional) - MongoDB + Redis, better performance
- Self-adapted Cache (Recommended) - Automatically select the best backend

Methods of use:
You know, from traffickingagents. dataworks.
Cache = get cache()# Automatically select the best cache policy

Configure cache policy:
Export TA CACHE STRATEGY=integrated# Enable Integrated Cache (MongoDB/Redis)
Report TA CACHE STRATEGY=file# Use file cache (default)
"""

import os
from typing import Union

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')

#Import File Cache
try:
    from .file_cache import StockDataCache
    FILE_CACHE_AVAILABLE = True
except ImportError:
    StockDataCache = None
    FILE_CACHE_AVAILABLE = False

#Import Database Cache
try:
    from .db_cache import DatabaseCacheManager
    DB_CACHE_AVAILABLE = True
except ImportError:
    DatabaseCacheManager = None
    DB_CACHE_AVAILABLE = False

#Import adapted to cache
try:
    from .adaptive import AdaptiveCacheSystem
    ADAPTIVE_CACHE_AVAILABLE = True
except ImportError:
    AdaptiveCacheSystem = None
    ADAPTIVE_CACHE_AVAILABLE = False

#Import Integration Cache
try:
    from .integrated import IntegratedCacheManager
    INTEGRATED_CACHE_AVAILABLE = True
except ImportError:
    IntegratedCacheManager = None
    INTEGRATED_CACHE_AVAILABLE = False

#Import application cache adapter (function, non-class)
try:
    from .app_adapter import get_basics_from_cache, get_market_quote_dataframe
    APP_CACHE_AVAILABLE = True
except ImportError:
    get_basics_from_cache = None
    get_market_quote_dataframe = None
    APP_CACHE_AVAILABLE = False

#Import MongoDB cache adapter
try:
    from .mongodb_cache_adapter import MongoDBCacheAdapter
    MONGODB_CACHE_ADAPTER_AVAILABLE = True
except ImportError:
    MongoDBCacheAdapter = None
    MONGODB_CACHE_ADAPTER_AVAILABLE = False

#Global Cache instance
_cache_instance = None

#Default Cache Policy (replaced as integrad, prioritize MongoDB/Redis Cache)
DEFAULT_CACHE_STRATEGY = os.getenv("TA_CACHE_STRATEGY", "integrated")

def get_cache() -> Union[StockDataCache, IntegratedCacheManager]:
    """Get Cache Examples (Unified Access)

    Select the cache policy based on the environment variable TA Cache STRATEGY:
    - "file" (default): use file cache
    - "integraded": use integrated cache (auto-selection MongoDB/Redis/File)
    - "adaptive": use self-adapted caches

    Environment variable settings:
    #Linux/Mac
    # Windows

    Return:
    StockDataCache or IntegradCacheManager
    """
    global _cache_instance

    if _cache_instance is None:
        if DEFAULT_CACHE_STRATEGY in ["integrated", "adaptive"]:
            if INTEGRATED_CACHE_AVAILABLE:
                try:
                    _cache_instance = IntegratedCacheManager()
                    logger.info("✅ Use an integrated cache system (supports MongoDB/Redis/File automatic selection)")
                except Exception as e:
                    logger.warning(f"Initialization of integrated cache failed, down to file cache:{e}")
                    _cache_instance = StockDataCache()
            else:
                logger.warning("Integrated cache is not available, using file cache")
                _cache_instance = StockDataCache()
        else:
            _cache_instance = StockDataCache()
            logger.info("Use file cache system")

    return _cache_instance

__all__ = [
    #Unified entrance (recommended)
    'get_cache',

    #Cache class (for direct use by advanced users)
    'StockDataCache',
    'IntegratedCacheManager',
    'DatabaseCacheManager',
    'AdaptiveCacheSystem',

    #Signs for usability
    'FILE_CACHE_AVAILABLE',
    'DB_CACHE_AVAILABLE',
    'ADAPTIVE_CACHE_AVAILABLE',
    'INTEGRATED_CACHE_AVAILABLE',

    #Apply cache adapter
    'get_basics_from_cache',
    'get_market_quote_dataframe',
    'APP_CACHE_AVAILABLE',

    #MongoDB cache adapter
    'MongoDBCacheAdapter',
    'MONGODB_CACHE_ADAPTER_AVAILABLE',
]

