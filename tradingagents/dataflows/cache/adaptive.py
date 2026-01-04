#!/usr/bin/env python3
"""Self-adapted Cache System
Automatically select the best cache policy based on database availability
"""

import os
import json
import pickle
import hashlib
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Union
import pandas as pd

from tradingagents.config.database_manager import get_database_manager

class AdaptiveCacheSystem:
    """Self-adapted Cache System"""
    
    def __init__(self, cache_dir: str = None):
        self.logger = logging.getLogger(__name__)

        #Access database manager
        self.db_manager = get_database_manager()

        #Set Cache Directory
        if cache_dir is None:
            #Default use data/cache directory
            cache_dir = "data/cache"
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        #Fetch Configuration
        self.config = self.db_manager.get_database_config()
        self.cache_config = self.config["cache"]
        
        #Initialise Cache Backend
        self.primary_backend = self.cache_config["primary_backend"]
        self.fallback_enabled = self.cache_config["fallback_enabled"]
        
        self.logger.info(f"Initialization of the self-adapted cache system - Main backend:{self.primary_backend}")
    
    def _get_cache_key(self, symbol: str, start_date: str = "", end_date: str = "", 
                      data_source: str = "default", data_type: str = "stock_data") -> str:
        """Generate Cache Keys"""
        key_data = f"{symbol}_{start_date}_{end_date}_{data_source}_{data_type}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _get_ttl_seconds(self, symbol: str, data_type: str = "stock_data") -> int:
        """Get TTL seconds"""
        #Type of market judged
        if len(symbol) == 6 and symbol.isdigit():
            market = "china"
        else:
            market = "us"
        
        #Get TTL Configuration
        ttl_key = f"{market}_{data_type}"
        ttl_seconds = self.cache_config["ttl_settings"].get(ttl_key, 7200)
        return ttl_seconds
    
    def _is_cache_valid(self, cache_time: datetime, ttl_seconds: int) -> bool:
        """Check if the cache is valid"""
        if cache_time is None:
            return False
        
        expiry_time = cache_time + timedelta(seconds=ttl_seconds)
        return datetime.now() < expiry_time
    
    def _save_to_file(self, cache_key: str, data: Any, metadata: Dict) -> bool:
        """Save to File Cache"""
        try:
            cache_file = self.cache_dir / f"{cache_key}.pkl"
            cache_data = {
                'data': data,
                'metadata': metadata,
                'timestamp': datetime.now(),
                'backend': 'file'
            }
            
            with open(cache_file, 'wb') as f:
                pickle.dump(cache_data, f)
            
            self.logger.debug(f"File cache successfully saved:{cache_key}")
            return True
            
        except Exception as e:
            self.logger.error(f"File cache saved:{e}")
            return False
    
    def _load_from_file(self, cache_key: str) -> Optional[Dict]:
        """Load from file cache"""
        try:
            cache_file = self.cache_dir / f"{cache_key}.pkl"
            if not cache_file.exists():
                return None
            
            with open(cache_file, 'rb') as f:
                cache_data = pickle.load(f)
            
            self.logger.debug(f"File cache loaded successfully:{cache_key}")
            return cache_data
            
        except Exception as e:
            self.logger.error(f"File cache loading failed:{e}")
            return None
    
    def _save_to_redis(self, cache_key: str, data: Any, metadata: Dict, ttl_seconds: int) -> bool:
        """Save to Redis Cache"""
        redis_client = self.db_manager.get_redis_client()
        if not redis_client:
            return False
        
        try:
            cache_data = {
                'data': data,
                'metadata': metadata,
                'timestamp': datetime.now().isoformat(),
                'backend': 'redis'
            }
            
            serialized_data = pickle.dumps(cache_data)
            redis_client.setex(cache_key, ttl_seconds, serialized_data)
            
            self.logger.debug(f"Redis cache saved successfully:{cache_key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Redis cache failed:{e}")
            return False
    
    def _load_from_redis(self, cache_key: str) -> Optional[Dict]:
        """Load from Redis cache"""
        redis_client = self.db_manager.get_redis_client()
        if not redis_client:
            return None
        
        try:
            serialized_data = redis_client.get(cache_key)
            if not serialized_data:
                return None
            
            cache_data = pickle.loads(serialized_data)
            
            #Convert Timetamp
            if isinstance(cache_data['timestamp'], str):
                cache_data['timestamp'] = datetime.fromisoformat(cache_data['timestamp'])
            
            self.logger.debug(f"Redis cache loaded successfully:{cache_key}")
            return cache_data
            
        except Exception as e:
            self.logger.error(f"Redis cache loading failed:{e}")
            return None
    
    def _save_to_mongodb(self, cache_key: str, data: Any, metadata: Dict, ttl_seconds: int) -> bool:
        """Save to MongoDB Cache"""
        mongodb_client = self.db_manager.get_mongodb_client()
        if not mongodb_client:
            return False
        
        try:
            db = mongodb_client.tradingagents
            collection = db.cache
            
            #Sequenced Data
            if isinstance(data, pd.DataFrame):
                serialized_data = data.to_json()
                data_type = 'dataframe'
            else:
                serialized_data = pickle.dumps(data).hex()
                data_type = 'pickle'
            
            cache_doc = {
                '_id': cache_key,
                'data': serialized_data,
                'data_type': data_type,
                'metadata': metadata,
                'timestamp': datetime.now(),
                'expires_at': datetime.now() + timedelta(seconds=ttl_seconds),
                'backend': 'mongodb'
            }
            
            collection.replace_one({'_id': cache_key}, cache_doc, upsert=True)
            
            self.logger.debug(f"MongoDB cache saved successfully:{cache_key}")
            return True
            
        except Exception as e:
            self.logger.error(f"MongoDB cache failed:{e}")
            return False
    
    def _load_from_mongodb(self, cache_key: str) -> Optional[Dict]:
        """Load from MongoDB cache"""
        mongodb_client = self.db_manager.get_mongodb_client()
        if not mongodb_client:
            return None
        
        try:
            db = mongodb_client.tradingagents
            collection = db.cache
            
            doc = collection.find_one({'_id': cache_key})
            if not doc:
                return None
            
            #Check for expiry
            if doc.get('expires_at') and doc['expires_at'] < datetime.now():
                collection.delete_one({'_id': cache_key})
                return None
            
            #Inverse sequenced data
            if doc['data_type'] == 'dataframe':
                data = pd.read_json(doc['data'])
            else:
                data = pickle.loads(bytes.fromhex(doc['data']))
            
            cache_data = {
                'data': data,
                'metadata': doc['metadata'],
                'timestamp': doc['timestamp'],
                'backend': 'mongodb'
            }
            
            self.logger.debug(f"MongoDB cache loaded successfully:{cache_key}")
            return cache_data
            
        except Exception as e:
            self.logger.error(f"Could not close temporary folder: %s{e}")
            return None
    
    def save_data(self, symbol: str, data: Any, start_date: str = "", end_date: str = "", 
                  data_source: str = "default", data_type: str = "stock_data") -> str:
        """Save Data to Cache"""
        #Generate Cache Keys
        cache_key = self._get_cache_key(symbol, start_date, end_date, data_source, data_type)
        
        #Prepare metadata
        metadata = {
            'symbol': symbol,
            'start_date': start_date,
            'end_date': end_date,
            'data_source': data_source,
            'data_type': data_type
        }
        
        #Get TTL
        ttl_seconds = self._get_ttl_seconds(symbol, data_type)
        
        #Save from main backend
        success = False
        
        if self.primary_backend == "redis":
            success = self._save_to_redis(cache_key, data, metadata, ttl_seconds)
        elif self.primary_backend == "mongodb":
            success = self._save_to_mongodb(cache_key, data, metadata, ttl_seconds)
        elif self.primary_backend == "file":
            success = self._save_to_file(cache_key, data, metadata)
        
        #If main backend fails, use downgrade policy
        if not success and self.fallback_enabled:
            self.logger.warning(f"Main Backend{self.primary_backend}Save failed, file cache downgraded")
            success = self._save_to_file(cache_key, data, metadata)
        
        if success:
            self.logger.info(f"Data cache successfully:{symbol} -> {cache_key}(backend:{self.primary_backend})")
        else:
            self.logger.error(f"Data cache failed:{symbol}")
        
        return cache_key
    
    def load_data(self, cache_key: str) -> Optional[Any]:
        """Load data from cache"""
        cache_data = None
        
        #Load from main backend
        if self.primary_backend == "redis":
            cache_data = self._load_from_redis(cache_key)
        elif self.primary_backend == "mongodb":
            cache_data = self._load_from_mongodb(cache_key)
        elif self.primary_backend == "file":
            cache_data = self._load_from_file(cache_key)
        
        #If the main backend fails, try to downgrade.
        if not cache_data and self.fallback_enabled:
            self.logger.debug(f"Main Backend{self.primary_backend}Failed to load, try file cache")
            cache_data = self._load_from_file(cache_key)
        
        if not cache_data:
            return None
        
        #Check whether the cache is effective (only for file cache, database cache has its own TTL mechanism)
        if cache_data.get('backend') == 'file':
            symbol = cache_data['metadata'].get('symbol', '')
            data_type = cache_data['metadata'].get('data_type', 'stock_data')
            ttl_seconds = self._get_ttl_seconds(symbol, data_type)
            
            if not self._is_cache_valid(cache_data['timestamp'], ttl_seconds):
                self.logger.debug(f"File cache expired:{cache_key}")
                return None
        
        return cache_data['data']
    
    def find_cached_data(self, symbol: str, start_date: str = "", end_date: str = "", 
                        data_source: str = "default", data_type: str = "stock_data") -> Optional[str]:
        """Find Cache Data"""
        cache_key = self._get_cache_key(symbol, start_date, end_date, data_source, data_type)
        
        #Check whether the cache exists and is valid
        if self.load_data(cache_key) is not None:
            return cache_key
        
        return None
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistical information"""
        #Standard statistical format
        stats = {
            'total_files': 0,
            'stock_data_count': 0,
            'news_count': 0,
            'fundamentals_count': 0,
            'total_size': 0,  #Bytes
            'total_size_mb': 0,  # MB
            'skipped_count': 0
        }

        #Backend Information
        backend_info = {
            'primary_backend': self.primary_backend,
            'fallback_enabled': self.fallback_enabled,
            'database_available': self.db_manager.is_database_available(),
            'mongodb_available': self.db_manager.is_mongodb_available(),
            'redis_available': self.db_manager.is_redis_available(),
            'file_cache_directory': str(self.cache_dir),
            'file_cache_count': len(list(self.cache_dir.glob("*.pkl"))),
        }

        total_size_bytes = 0

        #MongoDB Statistics
        mongodb_client = self.db_manager.get_mongodb_client()
        if mongodb_client:
            try:
                db = mongodb_client.tradingagents

                #Statistical collections
                for collection_name in ["stock_data", "news_data", "fundamentals_data"]:
                    if collection_name in db.list_collection_names():
                        collection = db[collection_name]
                        count = collection.count_documents({})

                        #Fetch Collective Size
                        try:
                            coll_stats = db.command("collStats", collection_name)
                            size = coll_stats.get("size", 0)
                            total_size_bytes += size
                        except:
                            pass

                        stats['total_files'] += count

                        #By type
                        if collection_name == "stock_data":
                            stats['stock_data_count'] += count
                        elif collection_name == "news_data":
                            stats['news_count'] += count
                        elif collection_name == "fundamentals_data":
                            stats['fundamentals_count'] += count

                backend_info['mongodb_cache_count'] = stats['total_files']
            except:
                backend_info['mongodb_status'] = 'Error'

        #Redis Statistics
        redis_client = self.db_manager.get_redis_client()
        if redis_client:
            try:
                redis_info = redis_client.info()
                backend_info['redis_memory_used'] = redis_info.get('used_memory_human', 'N/A')
                backend_info['redis_keys'] = redis_client.dbsize()
            except:
                backend_info['redis_status'] = 'Error'

        #File cache statistics
        if self.primary_backend == 'file' or self.fallback_enabled:
            for pkl_file in self.cache_dir.glob("*.pkl"):
                try:
                    total_size_bytes += pkl_file.stat().st_size
                except:
                    pass

        #Set Total Size
        stats['total_size'] = total_size_bytes
        stats['total_size_mb'] = round(total_size_bytes / (1024 * 1024), 2)

        #Add Backend Details
        stats['backend_info'] = backend_info

        return stats
    
    def clear_expired_cache(self):
        """Clear Expired Cache"""
        self.logger.info("Start clearing expired caches...")
        
        #Clear File Cache
        cleared_files = 0
        for cache_file in self.cache_dir.glob("*.pkl"):
            try:
                with open(cache_file, 'rb') as f:
                    cache_data = pickle.load(f)
                
                symbol = cache_data['metadata'].get('symbol', '')
                data_type = cache_data['metadata'].get('data_type', 'stock_data')
                ttl_seconds = self._get_ttl_seconds(symbol, data_type)
                
                if not self._is_cache_valid(cache_data['timestamp'], ttl_seconds):
                    cache_file.unlink()
                    cleared_files += 1
                    
            except Exception as e:
                self.logger.error(f"Failed to clear cache file{cache_file}: {e}")
        
        self.logger.info(f"File cache cleanup complete, delete{cleared_files}Expiry file")
        
        #MongoDB automatically cleans out expired documents (through extires at fields)
        #Redis automatically cleans out expired keys.


#Example of a global cache system
_cache_system = None

def get_cache_system() -> AdaptiveCacheSystem:
    """Get instance of a global adaptation cache system"""
    global _cache_system
    if _cache_system is None:
        _cache_system = AdaptiveCacheSystem()
    return _cache_system
