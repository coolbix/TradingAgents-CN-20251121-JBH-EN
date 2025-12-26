#!/usr/bin/env python3
"""MongoDB + Redis Database Cache Manager
Provision of high performance stock data cache and sustainable storage
"""

import os
import json
import pickle
import hashlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from tradingagents.config.runtime_settings import get_timezone_name

from typing import Optional, Dict, Any, List, Union
import pandas as pd

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')

# MongoDB
try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    logger.warning(f"⚠️ pymongo is not installed, MongoDB is not available")

# Redis
try:
    import redis
    from redis.exceptions import ConnectionError as RedisConnectionError
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning(f"⚠️ edis is not installed and Redis is not available")


class DatabaseCacheManager:
    """MongoDB + Redis Database Cache Manager"""

    def __init__(self,
                 mongodb_url: Optional[str] = None,
                 redis_url: Optional[str] = None,
                 mongodb_db: str = "tradingagents",
                 redis_db: int = 0):
        """Initialise database cache manager

Args:
Mongodb url: MongoDB connects URLs by default using profileend mouth
REDIS url: Redis connects URLs by default using profileend mouth
Mongodb db: MongoDB database First Name
Redis db: Redis database number
"""
        #Get the correct port from the profile
        mongodb_port = os.getenv("MONGODB_PORT", "27018")
        redis_port = os.getenv("REDIS_PORT", "6380")
        mongodb_password = os.getenv("MONGODB_PASSWORD", "tradingagents123")
        redis_password = os.getenv("REDIS_PASSWORD", "tradingagents123")

        self.mongodb_url = mongodb_url or os.getenv("MONGODB_URL", f"mongodb://admin:{mongodb_password}@localhost:{mongodb_port}")
        self.redis_url = redis_url or os.getenv("REDIS_URL", f"redis://:{redis_password}@localhost:{redis_port}")
        self.mongodb_db_name = mongodb_db
        self.redis_db = redis_db

        #Initialize Connection
        self.mongodb_client = None
        self.mongodb_db = None
        self.redis_client = None

        self._init_mongodb()
        self._init_redis()

        logger.info(f"Initialization of database cache manager completed")
        logger.error(f"   MongoDB: {'It\'s connected.' if self.mongodb_client else 'Not connected'}")
        logger.error(f"   Redis: {'It\'s connected.' if self.redis_client else 'Not connected'}")

    def _init_mongodb(self):
        """Initialize MongoDB connection"""
        if not MONGODB_AVAILABLE:
            return

        try:
            #Read timeout configuration from environment variables, using reasonable defaults
            import os
            connect_timeout = int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "30000"))
            socket_timeout = int(os.getenv("MONGO_SOCKET_TIMEOUT_MS", "60000"))
            server_selection_timeout = int(os.getenv("MONGO_SERVER_SELECTION_TIMEOUT_MS", "5000"))

            self.mongodb_client = MongoClient(
                self.mongodb_url,
                serverSelectionTimeoutMS=server_selection_timeout,
                connectTimeoutMS=connect_timeout,
                socketTimeoutMS=socket_timeout
            )
            #Test Connection
            self.mongodb_client.admin.command('ping')
            self.mongodb_db = self.mongodb_client[self.mongodb_db_name]

            #Create Index
            self._create_mongodb_indexes()

            logger.info(f"The MongoDB connection was successful:{self.mongodb_url}")
            logger.info(f"Timeout configuration: confect Timeout={connect_timeout}ms, socketTimeout={socket_timeout}ms")

        except Exception as e:
            logger.error(f"There's no connection to MongoDB:{e}")
            self.mongodb_client = None
            self.mongodb_db = None

    def _init_redis(self):
        """Initialize Redis Connection"""
        if not REDIS_AVAILABLE:
            return

        try:
            self.redis_client = redis.from_url(
                self.redis_url,
                db=self.redis_db,
                socket_timeout=5,
                socket_connect_timeout=5,
                decode_responses=True
            )
            #Test Connection
            self.redis_client.ping()

            logger.info(f"Redis connection succeeded:{self.redis_url}")

        except Exception as e:
            logger.error(f"Redis connection failed:{e}")
            self.redis_client = None

    def _create_mongodb_indexes(self):
        """Create MongoDB Index"""
        if self.mongodb_db is None:
            return

        try:
            #Stock Data Pool Index
            stock_collection = self.mongodb_db.stock_data
            stock_collection.create_index([
                ("symbol", 1),
                ("data_source", 1),
                ("start_date", 1),
                ("end_date", 1)
            ])
            stock_collection.create_index([("created_at", 1)])

            #Index of news data collection
            news_collection = self.mongodb_db.news_data
            news_collection.create_index([
                ("symbol", 1),
                ("data_source", 1),
                ("date_range", 1)
            ])
            news_collection.create_index([("created_at", 1)])

            #Index to Basic Surface Data Pool
            fundamentals_collection = self.mongodb_db.fundamentals_data
            fundamentals_collection.create_index([
                ("symbol", 1),
                ("data_source", 1),
                ("analysis_date", 1)
            ])
            fundamentals_collection.create_index([("created_at", 1)])

            logger.info(f"The MongoDB index has been created.")

        except Exception as e:
            logger.error(f"The MongoDB index failed:{e}")

    def _generate_cache_key(self, data_type: str, symbol: str, **kwargs) -> str:
        """Generate Cache Keys"""
        params_str = f"{data_type}_{symbol}"
        for key, value in sorted(kwargs.items()):
            params_str += f"_{key}_{value}"

        cache_key = hashlib.md5(params_str.encode()).hexdigest()[:16]
        return f"{data_type}:{symbol}:{cache_key}"

    def save_stock_data(self, symbol: str, data: Union[pd.DataFrame, str],
                       start_date: str = None, end_date: str = None,
                       data_source: str = "unknown", market_type: str = None) -> str:
        """Save stock data to MongoDB and Redis

Args:
symbol: stock code
Data: Stock data
Start date: Start date
End date: End date
data source: data source
market type: Market type (us/china)

Returns:
Cache key: Cache keys
"""
        cache_key = self._generate_cache_key("stock", symbol,
                                           start_date=start_date,
                                           end_date=end_date,
                                           source=data_source)

        #Automatic extrapolation of market type
        if market_type is None:
            #Infer market type from stock code format
            import re

            if re.match(r'^\d{6}$', symbol):  #6-digit unit A
                market_type = "china"
            else:  #Other formats are U.S. shares
                market_type = "us"

        #Preparing document data
        doc = {
            "_id": cache_key,
            "symbol": symbol,
            "market_type": market_type,
            "data_type": "stock_data",
            "start_date": start_date,
            "end_date": end_date,
            "data_source": data_source,
            "created_at": datetime.now(ZoneInfo(get_timezone_name())),
            "updated_at": datetime.now(ZoneInfo(get_timezone_name()))
        }

        #Processing data formats
        if isinstance(data, pd.DataFrame):
            doc["data"] = data.to_json(orient='records', date_format='iso')
            doc["data_format"] = "dataframe_json"
        else:
            doc["data"] = str(data)
            doc["data_format"] = "text"

        #Save to MongoDB (persistent)
        if self.mongodb_db is not None:
            try:
                collection = self.mongodb_db.stock_data
                collection.replace_one({"_id": cache_key}, doc, upsert=True)
                logger.info(f"The stock data has been saved to MongoDB:{symbol} -> {cache_key}")
            except Exception as e:
                logger.error(f"MongoDB failed:{e}")

        #Save to Redis (quick cache, 6 hours expired)
        if self.redis_client:
            try:
                redis_data = {
                    "data": doc["data"],
                    "data_format": doc["data_format"],
                    "symbol": symbol,
                    "data_source": data_source,
                    "created_at": doc["created_at"].isoformat()
                }
                self.redis_client.setex(
                    cache_key,
                    6 * 3600,  #Six hours expired.
                    json.dumps(redis_data, ensure_ascii=False)
                )
                logger.info(f"The stock data is down to Redis:{symbol} -> {cache_key}")
            except Exception as e:
                logger.error(f"Redis cache failed:{e}")

        return cache_key

    def load_stock_data(self, cache_key: str) -> Optional[Union[pd.DataFrame, str]]:
        """Load stock data from Redis or MongoDB"""

        #First try loading from Redis (quicker)
        if self.redis_client:
            try:
                redis_data = self.redis_client.get(cache_key)
                if redis_data:
                    data_dict = json.loads(redis_data)
                    logger.info(f"Loading data from Redis:{cache_key}")

                    if data_dict["data_format"] == "dataframe_json":
                        return pd.read_json(data_dict["data"], orient='records')
                    else:
                        return data_dict["data"]
            except Exception as e:
                logger.error(f"Redis load failed:{e}")

        #If Redis doesn't, load from MongoDB
        if self.mongodb_db is not None:
            try:
                collection = self.mongodb_db.stock_data
                doc = collection.find_one({"_id": cache_key})

                if doc:
                    logger.info(f"Loading data from MongoDB:{cache_key}")

                    #Update to Redis Cache
                    if self.redis_client:
                        try:
                            redis_data = {
                                "data": doc["data"],
                                "data_format": doc["data_format"],
                                "symbol": doc["symbol"],
                                "data_source": doc["data_source"],
                                "created_at": doc["created_at"].isoformat()
                            }
                            self.redis_client.setex(
                                cache_key,
                                6 * 3600,
                                json.dumps(redis_data, ensure_ascii=False)
                            )
                            logger.info(f"Data synchronized to Redis cache")
                        except Exception as e:
                            logger.error(f"Redis sync failed:{e}")

                    if doc["data_format"] == "dataframe_json":
                        return pd.read_json(doc["data"], orient='records')
                    else:
                        return doc["data"]

            except Exception as e:
                logger.error(f"The MongoDB load failed:{e}")

        return None

    def find_cached_stock_data(self, symbol: str, start_date: str = None,
                              end_date: str = None, data_source: str = None,
                              max_age_hours: int = 6) -> Optional[str]:
        """Find matching cache data"""

        #Generate precise matching cache keys
        exact_key = self._generate_cache_key("stock", symbol,
                                           start_date=start_date,
                                           end_date=end_date,
                                           source=data_source)

        #Check for exact matches in Redis
        if self.redis_client and self.redis_client.exists(exact_key):
            logger.info(f"The exact match found in Redis:{symbol} -> {exact_key}")
            return exact_key

        #Check matches in MongoDB
        if self.mongodb_db is not None:
            try:
                collection = self.mongodb_db.stock_data
                cutoff_time = datetime.now(ZoneInfo(get_timezone_name())) - timedelta(hours=max_age_hours)

                query = {
                    "symbol": symbol,
                    "created_at": {"$gte": cutoff_time}
                }

                if data_source:
                    query["data_source"] = data_source
                if start_date:
                    query["start_date"] = start_date
                if end_date:
                    query["end_date"] = end_date

                doc = collection.find_one(query, sort=[("created_at", -1)])

                if doc:
                    cache_key = doc["_id"]
                    logger.info(f"We found a match in MongoDB:{symbol} -> {cache_key}")
                    return cache_key

            except Exception as e:
                logger.error(f"Could not close temporary folder: %s{e}")

        logger.error(f"No valid cache found:{symbol}")
        return None

    def save_news_data(self, symbol: str, news_data: str,
                      start_date: str = None, end_date: str = None,
                      data_source: str = "unknown") -> str:
        """Save news data to MongoDB and Redis"""
        cache_key = self._generate_cache_key("news", symbol,
                                           start_date=start_date,
                                           end_date=end_date,
                                           source=data_source)

        doc = {
            "_id": cache_key,
            "symbol": symbol,
            "data_type": "news_data",
            "date_range": f"{start_date}_{end_date}",
            "start_date": start_date,
            "end_date": end_date,
            "data_source": data_source,
            "data": news_data,
            "created_at": datetime.now(ZoneInfo(get_timezone_name())),
            "updated_at": datetime.now(ZoneInfo(get_timezone_name()))
        }

        #Save to MongoDB
        if self.mongodb_db is not None:
            try:
                collection = self.mongodb_db.news_data
                collection.replace_one({"_id": cache_key}, doc, upsert=True)
                logger.info(f"News data saved to MongoDB:{symbol} -> {cache_key}")
            except Exception as e:
                logger.error(f"MongoDB failed:{e}")

        #Save to Redis (over 24 hours)
        if self.redis_client:
            try:
                redis_data = {
                    "data": news_data,
                    "symbol": symbol,
                    "data_source": data_source,
                    "created_at": doc["created_at"].isoformat()
                }
                self.redis_client.setex(
                    cache_key,
                    24 * 3600,  #Expiry 24 hours
                    json.dumps(redis_data, ensure_ascii=False)
                )
                logger.info(f"News data is cached in Redis:{symbol} -> {cache_key}")
            except Exception as e:
                logger.error(f"Redis cache failed:{e}")

        return cache_key

    def save_fundamentals_data(self, symbol: str, fundamentals_data: str,
                              analysis_date: str = None,
                              data_source: str = "unknown") -> str:
        """Save base surface data to MongoDB and Redis"""
        if not analysis_date:
            analysis_date = datetime.now(ZoneInfo(get_timezone_name())).strftime("%Y-%m-%d")

        cache_key = self._generate_cache_key("fundamentals", symbol,
                                           date=analysis_date,
                                           source=data_source)

        doc = {
            "_id": cache_key,
            "symbol": symbol,
            "data_type": "fundamentals_data",
            "analysis_date": analysis_date,
            "data_source": data_source,
            "data": fundamentals_data,
            "created_at": datetime.now(ZoneInfo(get_timezone_name())),
            "updated_at": datetime.now(ZoneInfo(get_timezone_name()))
        }

        #Save to MongoDB
        if self.mongodb_db is not None:
            try:
                collection = self.mongodb_db.fundamentals_data
                collection.replace_one({"_id": cache_key}, doc, upsert=True)
                logger.info(f"Basic data saved to MongoDB:{symbol} -> {cache_key}")
            except Exception as e:
                logger.error(f"MongoDB failed:{e}")

        #Save to Redis (over 24 hours)
        if self.redis_client:
            try:
                redis_data = {
                    "data": fundamentals_data,
                    "symbol": symbol,
                    "data_source": data_source,
                    "analysis_date": analysis_date,
                    "created_at": doc["created_at"].isoformat()
                }
                self.redis_client.setex(
                    cache_key,
                    24 * 3600,  #Expiry 24 hours
                    json.dumps(redis_data, ensure_ascii=False)
                )
                logger.info(f"Basic data is cached in Redis:{symbol} -> {cache_key}")
            except Exception as e:
                logger.error(f"Redis cache failed:{e}")

        return cache_key

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistical information"""
        #Standard statistical format (consistent with file cache)
        stats = {
            'total_files': 0,
            'stock_data_count': 0,
            'news_count': 0,
            'fundamentals_count': 0,
            'total_size': 0,  #Bytes
            'total_size_mb': 0,  # MB
            'skipped_count': 0
        }

        #Detailed Backend Information
        backend_info = {
            "mongodb": {"available": self.mongodb_db is not None, "collections": {}},
            "redis": {"available": self.redis_client is not None, "keys": 0, "memory_usage": "N/A"}
        }

        #MongoDB Statistics
        total_size_bytes = 0
        if self.mongodb_db is not None:
            try:
                for collection_name in ["stock_data", "news_data", "fundamentals_data"]:
                    collection = self.mongodb_db[collection_name]
                    count = collection.count_documents({})
                    size = self.mongodb_db.command("collStats", collection_name).get("size", 0)
                    backend_info["mongodb"]["collections"][collection_name] = {
                        "count": count,
                        "size_mb": round(size / (1024 * 1024), 2)
                    }

                    #Add to Standard Statistics
                    total_size_bytes += size
                    stats['total_files'] += count

                    #By type
                    if collection_name == "stock_data":
                        stats['stock_data_count'] += count
                    elif collection_name == "news_data":
                        stats['news_count'] += count
                    elif collection_name == "fundamentals_data":
                        stats['fundamentals_count'] += count

            except Exception as e:
                logger.error(f"MongoDB statistical access failed:{e}")

        #Redis Statistics
        if self.redis_client:
            try:
                info = self.redis_client.info()
                backend_info["redis"]["keys"] = info.get("db0", {}).get("keys", 0)
                backend_info["redis"]["memory_usage"] = f"{info.get('used_memory_human', 'N/A')}"
            except Exception as e:
                logger.error(f"Redis statistical access failed:{e}")

        #Set Total Size
        stats['total_size'] = total_size_bytes
        stats['total_size_mb'] = round(total_size_bytes / (1024 * 1024), 2)

        #Add Backend Details
        stats['backend_info'] = backend_info

        return stats

    def clear_old_cache(self, max_age_days: int = 7):
        """Clear Expired Cache"""
        cutoff_time = datetime.now(ZoneInfo(get_timezone_name())) - timedelta(days=max_age_days)
        cleared_count = 0

        #Clear MongoDB
        if self.mongodb_db is not None:
            try:
                for collection_name in ["stock_data", "news_data", "fundamentals_data"]:
                    collection = self.mongodb_db[collection_name]
                    result = collection.delete_many({"created_at": {"$lt": cutoff_time}})
                    cleared_count += result.deleted_count
                    logger.info(f"🧹 MongoDB {collection_name}It's clean.{result.deleted_count}Notes")
            except Exception as e:
                logger.error(f"There's been a clean-up of MongoDB:{e}")

        #Redis will expire automatically, without manual cleaning.
        logger.info(f"Total cleanup.{cleared_count}Expiry record")
        return cleared_count

    def close(self):
        """Close database connection"""
        if self.mongodb_client:
            self.mongodb_client.close()
            logger.info(f"MongoDB connection closed")

        if self.redis_client:
            self.redis_client.close()
            logger.info(f"Redis connection closed.")


#Global database cache instance
_db_cache_instance = None

def get_db_cache() -> DatabaseCacheManager:
    """Fetch global database cache instance"""
    global _db_cache_instance
    if _db_cache_instance is None:
        _db_cache_instance = DatabaseCacheManager()
    return _db_cache_instance
