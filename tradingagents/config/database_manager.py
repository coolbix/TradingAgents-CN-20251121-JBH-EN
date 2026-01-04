#!/usr/bin/env python3
"""Smart Database Manager
Automatically detect the availability of MongoDB and Redis to provide a downgrading programme
Use existing.env configuration of the project
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

class DatabaseManager:
    """Manage database: MongoDB, Redis, Cache backend"""
    #NOTE: "Synchronous" Database Manager
    #NOTE: there is another DatabaseManager in app/core/database.py (asynchronous version)
    #NOTE: consider unifying them in the future
    #NOTE: this class seems to being used in only the scripts folder which are used for testing and initial installation
    #      the main business logic of tradingagents itself seems to use DatabaseManager in app/core/database.py.

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        #Load MongoDB and Redis Configuration From .env File
        self._load_mongodb_redis_env_config()

        #Database Connection Status
        self.mongodb_available = False
        self.redis_available = False
        self.mongodb_client = None
        self.redis_client = None

        #Test MongoDB/Redis availability, and update Cache Backend configuration
        self._detect_databases()

        #Initialize Connection
        self._initialize_connections()

        self.logger.info(f"Database manager initialised - MongoDB:{self.mongodb_available}, Redis: {self.redis_available}")
    
    def _load_mongodb_redis_env_config(self):
        """Load MongoDB and Redis Configuration From .env File"""
        #Try loading python-dotenv
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            self.logger.info("python-dotenv is not installed, read environmental variables directly")

        #Use robust boolean resolution (compatible with Python 3.13+)
        from .env_utils import parse_bool_env
        self.mongodb_enabled = parse_bool_env("MONGODB_ENABLED", False)
        self.redis_enabled = parse_bool_env("REDIS_ENABLED", False)

        #Read MongoDB configurations from environmental variables
        self.mongodb_config = {
            "enabled": self.mongodb_enabled,
            "host": os.getenv("MONGODB_HOST", "localhost"),
            "port": int(os.getenv("MONGODB_PORT", "27017")),
            "username": os.getenv("MONGODB_USERNAME"),
            "password": os.getenv("MONGODB_PASSWORD"),
            "database": os.getenv("MONGODB_DATABASE", "tradingagents"),
            "auth_source": os.getenv("MONGODB_AUTH_SOURCE", "admin"),
            "timeout": 2000,
            #MongoDB timeout parameter (ms) - for processing large amounts of historical data
            "connect_timeout": int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "30000")),
            "socket_timeout": int(os.getenv("MONGO_SOCKET_TIMEOUT_MS", "60000")),
            "server_selection_timeout": int(os.getenv("MONGO_SERVER_SELECTION_TIMEOUT_MS", "5000"))
        }

        #Read Redis configurations from environment variables
        self.redis_config = {
            "enabled": self.redis_enabled,
            "host": os.getenv("REDIS_HOST", "localhost"),
            "port": int(os.getenv("REDIS_PORT", "6379")),
            "password": os.getenv("REDIS_PASSWORD"),
            "db": int(os.getenv("REDIS_DB", "0")),
            "timeout": 2
        }

        self.logger.info(f"MongoDB enabled:{self.mongodb_enabled}")
        self.logger.info(f"Redis enabled:{self.redis_enabled}")
        if self.mongodb_enabled:
            self.logger.info(f"MongoDB configuration:{self.mongodb_config['host']}:{self.mongodb_config['port']}")
        if self.redis_enabled:
            self.logger.info(f"Redis configuration:{self.redis_config['host']}:{self.redis_config['port']}")
    

    
    def _detect_mongodb(self) -> Tuple[bool, str]:
        """Check if MongoDB is available"""
        #First check if it's enabled
        if not self.mongodb_enabled:
            return False, "MongoDB未启用 (MONGODB_ENABLED=false)"

        try:
            import pymongo
            from pymongo import MongoClient

            #Build connection parameters
            connect_kwargs = {
                "host": self.mongodb_config["host"],
                "port": self.mongodb_config["port"],
                "serverSelectionTimeoutMS": self.mongodb_config["server_selection_timeout"],
                "connectTimeoutMS": self.mongodb_config["connect_timeout"],
                "socketTimeoutMS": self.mongodb_config["socket_timeout"]
            }

            #Add authentication if you have a username and password
            if self.mongodb_config["username"] and self.mongodb_config["password"]:
                connect_kwargs.update({
                    "username": self.mongodb_config["username"],
                    "password": self.mongodb_config["password"],
                    "authSource": self.mongodb_config["auth_source"]
                })

            client = MongoClient(**connect_kwargs)

            #Test Connection
            client.server_info()
            client.close()

            return True, "MongoDB连接成功"

        except ImportError:
            return False, "pymongo未安装"
        except Exception as e:
            return False, f"MongoDB连接失败: {str(e)}"
    
    def _detect_redis(self) -> Tuple[bool, str]:
        """Test for redis availability"""
        #First check if it's enabled
        if not self.redis_enabled:
            return False, "Redis未启用 (REDIS_ENABLED=false)"

        try:
            import redis

            #Build connection parameters
            connect_kwargs = {
                "host": self.redis_config["host"],
                "port": self.redis_config["port"],
                "db": self.redis_config["db"],
                "socket_timeout": self.redis_config["timeout"],
                "socket_connect_timeout": self.redis_config["timeout"]
            }

            #If you have a password, add it.
            if self.redis_config["password"]:
                connect_kwargs["password"] = self.redis_config["password"]

            client = redis.Redis(**connect_kwargs)

            #Test Connection
            client.ping()

            return True, "Redis连接成功"

        except ImportError:
            return False, "redis未安装"
        except Exception as e:
            return False, f"Redis连接失败: {str(e)}"
    
    def _detect_databases(self):
        """Test all databases"""
        self.logger.info("Start testing database availability...")
        
        #Test MongoDB
        mongodb_available, mongodb_msg = self._detect_mongodb()
        self.mongodb_available = mongodb_available
        
        if mongodb_available:
            self.logger.info(f"✅ MongoDB: {mongodb_msg}")
        else:
            self.logger.info(f"❌ MongoDB: {mongodb_msg}")
        
        #Testing Redis
        redis_available, redis_msg = self._detect_redis()
        self.redis_available = redis_available
        
        if redis_available:
            self.logger.info(f"✅ Redis: {redis_msg}")
        else:
            self.logger.info(f"❌ Redis: {redis_msg}")
        
        #Update Configuration
        self._update_primary_cache_backend()
    
    def _update_primary_cache_backend(self):
        """Update primary_cache_backend configuration based on test MongoDB and Redis results"""
        #Confirm Cache Backend
        if self.redis_available:
            self.primary_cache_backend = "redis"
        elif self.mongodb_available:
            self.primary_cache_backend = "mongodb"
        else:
            self.primary_cache_backend = "file"

        self.logger.info(f"Primary cache backend:{self.primary_cache_backend}")
    
    def _initialize_connections(self):
        """Initialize database connection"""
        #Initialize MongoDB connection
        if self.mongodb_available:
            try:
                import pymongo

                #Build connection parameters
                connect_kwargs = {
                    "host": self.mongodb_config["host"],
                    "port": self.mongodb_config["port"],
                    "serverSelectionTimeoutMS": self.mongodb_config["server_selection_timeout"],
                    "connectTimeoutMS": self.mongodb_config["connect_timeout"],
                    "socketTimeoutMS": self.mongodb_config["socket_timeout"]
                }

                #Add authentication if you have a username and password
                if self.mongodb_config["username"] and self.mongodb_config["password"]:
                    connect_kwargs.update({
                        "username": self.mongodb_config["username"],
                        "password": self.mongodb_config["password"],
                        "authSource": self.mongodb_config["auth_source"]
                    })

                self.mongodb_client = pymongo.MongoClient(**connect_kwargs)
                self.logger.info("MongoDB client initialization successfully")
            except Exception as e:
                self.logger.error(f"Could not close temporary folder: %s{e}")
                self.mongodb_available = False

        #Initialize Redis Connection
        if self.redis_available:
            try:
                import redis

                #Build connection parameters
                connect_kwargs = {
                    "host": self.redis_config["host"],
                    "port": self.redis_config["port"],
                    "db": self.redis_config["db"],
                    "socket_timeout": self.redis_config["timeout"]
                }

                #If you have a password, add it.
                if self.redis_config["password"]:
                    connect_kwargs["password"] = self.redis_config["password"]

                self.redis_client = redis.Redis(**connect_kwargs)
                self.logger.info("Redis client initialization successfully")
            except Exception as e:
                self.logger.error(f"Could not close temporary folder: %s{e}")
                self.redis_available = False
    
    def get_mongodb_client(self):
        """Get MongoDB Client"""
        if self.mongodb_available and self.mongodb_client:
            return self.mongodb_client
        return None

    def get_mongodb_db(self):
        """Example of accessing MongoDB database"""
        if self.mongodb_available and self.mongodb_client:
            db_name = self.mongodb_config.get("database", "tradingagents")
            return self.mongodb_client[db_name]
        return None

    def get_redis_client(self):
        """Get Redis client"""
        if self.redis_available and self.redis_client:
            return self.redis_client
        return None
    
    def is_mongodb_available(self) -> bool:
        """Check MongoDB for availability"""
        return self.mongodb_available
    
    def is_redis_available(self) -> bool:
        """Check for redis availability"""
        return self.redis_available
    
    def is_database_available(self) -> bool:
        """Check if any databases are available"""
        return self.mongodb_available or self.redis_available
    
    def get_cache_backend(self) -> str:
        """Get the current cache backend"""
        return self.primary_cache_backend

    def get_database_config(self) -> Dict[str, Any]:
        """Get Profile Information"""
        return {
            "mongodb": self.mongodb_config,
            "redis": self.redis_config,
            "primary_backend": self.primary_cache_backend,
            "mongodb_available": self.mongodb_available,
            "redis_available": self.redis_available,
            "cache": {
                "primary_backend": self.primary_cache_backend,
                "fallback_enabled": True,  #Always enable downgrade
                "ttl_settings": {
                    #US share data TTL (seconds)
                    "us_stock_data": 7200,  #Two hours.
                    "us_news": 21600,  #Six hours.
                    "us_fundamentals": 86400,  #24 hours
                    #Unit A data TTL (s)
                    "china_stock_data": 3600,  #1 hour
                    "china_news": 14400,  #Four hours.
                    "china_fundamentals": 43200,  #12 hours.
                }
            }
        }

    def get_status_report(self) -> Dict[str, Any]:
        """Get Status Report"""
        return {
            "database_available": self.is_database_available(),
            "mongodb": {
                "available": self.mongodb_available,
                "host": self.mongodb_config["host"],
                "port": self.mongodb_config["port"]
            },
            "redis": {
                "available": self.redis_available,
                "host": self.redis_config["host"],
                "port": self.redis_config["port"]
            },
            "cache_backend": self.get_cache_backend(),
            "fallback_enabled": True  #Always enable downgrade
        }

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistical information"""
        stats = {
            "mongodb_available": self.mongodb_available,
            "redis_available": self.redis_available,
            "redis_keys": 0,
            "redis_memory": "N/A"
        }

        #Redis Statistics
        if self.redis_available and self.redis_client:
            try:
                info = self.redis_client.info()
                stats["redis_keys"] = self.redis_client.dbsize()
                stats["redis_memory"] = info.get("used_memory_human", "N/A")
            except Exception as e:
                self.logger.error(f"Could not close temporary folder: %s{e}")

        return stats

    def cache_clear_pattern(self, pattern: str) -> int:
        """Clear the cache of matching mode"""
        cleared_count = 0

        if self.redis_available and self.redis_client:
            try:
                keys = self.redis_client.keys(pattern)
                if keys:
                    cleared_count += self.redis_client.delete(*keys)
            except Exception as e:
                self.logger.error(f"Redis cache cleanup failed:{e}")

        return cleared_count


#Examples of global database manager
_database_manager = None

def get_database_manager() -> DatabaseManager:
    """Get a global database manager instance"""
    global _database_manager
    if _database_manager is None:
        _database_manager = DatabaseManager()
    return _database_manager

def is_mongodb_available() -> bool:
    """Check MongoDB for availability"""
    return get_database_manager().is_mongodb_available()

def is_redis_available() -> bool:
    """Check for redis availability"""
    return get_database_manager().is_redis_available()

def get_cache_backend() -> str:
    """Get the current cache backend"""
    return get_database_manager().get_cache_backend()

def get_mongodb_client():
    """Get MongoDB Client"""
    return get_database_manager().get_mongodb_client()

def get_redis_client():
    """Get Redis client"""
    return get_database_manager().get_redis_client()
