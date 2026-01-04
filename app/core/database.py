"""Database Connection Management Module
Enhanced version to support connection pool, health check and error recovery
"""

import logging
import asyncio
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import MongoClient
from pymongo.database import Database
from redis.asyncio import Redis, ConnectionPool
from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure
from redis.exceptions import ConnectionError as RedisConnectionError
from .config import settings

logger = logging.getLogger(__name__)

#Examples of global connections
mongo_client: Optional[AsyncIOMotorClient] = None
mongo_db: Optional[AsyncIOMotorDatabase] = None
redis_client: Optional[Redis] = None
redis_pool: Optional[ConnectionPool] = None

#Sync MongoDB connection (for non-spacing context)
_synchronous_mongo_client: Optional[MongoClient] = None
_synchronous_mongo_db: Optional[Database] = None


class DatabaseManager:
    """Database Connection Manager"""
    #NOTE: "Asynchronous" Database Manager
    #NOTE: there is another DatabaseManager in tradingagents/config/database_manager.py (synchronous version)
    #NOTE: consider unifying them in the future

    def __init__(self):
        self.mongo_client: Optional[AsyncIOMotorClient] = None
        self.mongo_db: Optional[AsyncIOMotorDatabase] = None
        self.redis_client: Optional[Redis] = None
        self.redis_pool: Optional[ConnectionPool] = None
        self._mongo_healthy = False
        self._redis_healthy = False

    async def init_mongodb(self):
        """Initialize 'asynchronous' MongoDB connection"""
        try:
            logger.info("Initializing the MongoDB connection...")

            #Create 'asynchronous' MongoDB client and configure connect pool
            #NOTE: there is 'synchronous' version of MongoClient created in get_mongo_db_sync()
            self.mongo_client = AsyncIOMotorClient(
                settings.MONGO_URI,
                maxPoolSize=settings.MONGO_MAX_CONNECTIONS,
                minPoolSize=settings.MONGO_MIN_CONNECTIONS,
                maxIdleTimeMS=30000,  #30 seconds of free time.
                serverSelectionTimeoutMS=settings.MONGO_SERVER_SELECTION_TIMEOUT_MS,  #Server Select Timeout
                connectTimeoutMS=settings.MONGO_CONNECT_TIMEOUT_MS,  #Connection timed out
                socketTimeoutMS=settings.MONGO_SOCKET_TIMEOUT_MS,  #Socket Timeout
            )

            #Access to database examples
            self.mongo_db = self.mongo_client[settings.MONGO_DB]

            #Test Connection
            await self.mongo_client.admin.command('ping')
            self._mongo_healthy = True

            logger.info("MongoDB connection successfully established")
            logger.info(f"Database:{settings.MONGO_DB}")
            logger.info(f"Connecting pool:{settings.MONGO_MIN_CONNECTIONS}-{settings.MONGO_MAX_CONNECTIONS}")
            logger.info(f"Timeout configuration: confect Timeout={settings.MONGO_CONNECT_TIMEOUT_MS}ms, socketTimeout={settings.MONGO_SOCKET_TIMEOUT_MS}ms")

        except Exception as e:
            logger.error(f"There's no connection to MongoDB:{e}")
            self._mongo_healthy = False
            raise

    async def init_redis(self):
        """Initialize Redis Connection"""
        try:
            logger.info("Initializing Redis connection...")

            #Create 'asynchronous' Redis Connect Pool
            self.redis_pool = ConnectionPool.from_url(
                settings.REDIS_URL,
                max_connections=settings.REDIS_MAX_CONNECTIONS,
                retry_on_timeout=settings.REDIS_RETRY_ON_TIMEOUT,
                decode_responses=True,
                socket_connect_timeout=5,  #Five seconds connection timed out.
                socket_timeout=10,  #Ten seconds to fit.
            )

            #Create Redis client
            self.redis_client = Redis(connection_pool=self.redis_pool)

            #Test Connection
            await self.redis_client.ping()
            self._redis_healthy = True

            logger.info("Redis connection successfully established")
            logger.info(f"Connect pool size:{settings.REDIS_MAX_CONNECTIONS}")

        except Exception as e:
            logger.error(f"Redis connection failed:{e}")
            self._redis_healthy = False
            raise

    async def close_connections(self):
        """Close all database connections"""
        logger.info("Closing database connection...")

        #Close MongoDB connection
        if self.mongo_client:
            try:
                self.mongo_client.close()
                self._mongo_healthy = False
                logger.info("MongoDB connection closed")
            except Exception as e:
                logger.error(f"There was an error closing the MongoDB connection:{e}")

        #Close Redis Connection
        if self.redis_client:
            try:
                await self.redis_client.close()
                self._redis_healthy = False
                logger.info("Redis connection closed.")
            except Exception as e:
                logger.error(f"There was an error closing the Redis connection:{e}")

        #Close Redis Connection Pool
        if self.redis_pool:
            try:
                await self.redis_pool.disconnect()
                logger.info("The Redis connection pool is closed.")
            except Exception as e:
                logger.error(f"There was an error closing the Redis connection pool:{e}")

    async def health_check(self) -> dict:
        """Database health checks"""
        health_status = {
            "mongodb": {"status": "unknown", "details": None},
            "redis": {"status": "unknown", "details": None}
        }

        #Check MongoDB
        try:
            if self.mongo_client:
                result = await self.mongo_client.admin.command('ping')
                health_status["mongodb"] = {
                    "status": "healthy",
                    "details": {"ping": result, "database": settings.MONGO_DB}
                }
                self._mongo_healthy = True
            else:
                health_status["mongodb"]["status"] = "disconnected"
        except Exception as e:
            health_status["mongodb"] = {
                "status": "unhealthy",
                "details": {"error": str(e)}
            }
            self._mongo_healthy = False

        #Check for Redis.
        try:
            if self.redis_client:
                result = await self.redis_client.ping()
                health_status["redis"] = {
                    "status": "healthy",
                    "details": {"ping": result}
                }
                self._redis_healthy = True
            else:
                health_status["redis"]["status"] = "disconnected"
        except Exception as e:
            health_status["redis"] = {
                "status": "unhealthy",
                "details": {"error": str(e)}
            }
            self._redis_healthy = False

        return health_status

    @property
    def is_healthy(self) -> bool:
        """Check if all database connections are healthy"""
        return self._mongo_healthy and self._redis_healthy


#Examples of global database manager
db_manager = DatabaseManager()


async def init_database():
    """Initialize database connection"""
    global mongo_client, mongo_db, redis_client, redis_pool

    try:
        #Initialize MongoDB
        await db_manager.init_mongodb()
        mongo_client = db_manager.mongo_client
        mongo_db = db_manager.mongo_db

        #Initialise Redis
        await db_manager.init_redis()
        redis_client = db_manager.redis_client
        redis_pool = db_manager.redis_pool

        logger.info("Initialization of all database connections completed")

        #Initialization of database views and indexes
        await init_database_views_and_indexes()

    except Exception as e:
        logger.error(f"Initialization of the database failed:{e}")
        raise


async def init_database_views_and_indexes():
    """Initialize database views and indexes"""
    try:
        db = get_mongo_db()

        #1. Create stock filter view
        await create_stock_screening_view(db)

        #Creating the necessary index
        await create_database_indexes(db)

        logger.info("✅ Database view and index initialization completed")

    except Exception as e:
        logger.warning(f"Initialization of the database view and index failed:{e}")
        #Do not throw anomalies. Allow applications to continue.


async def create_stock_screening_view(db):
    """Create stock filter view"""
    try:
        #Check whether a view exists
        collections = await db.list_collection_names()
        if "stock_screening_view" in collections:
            logger.info("📋 View stock screenning view already exists, skip creation")
            return

        #Create view: associate stock basic info, markt quotes with stock financial data
        pipeline = [
            #Step 1: Associated real-time line data (market quotes)
            {
                "$lookup": {
                    "from": "market_quotes",
                    "localField": "code",
                    "foreignField": "code",
                    "as": "quote_data"
                }
            },
            #Step 2: Expand quate data arrays
            {
                "$unwind": {
                    "path": "$quote_data",
                    "preserveNullAndEmptyArrays": True
                }
            },
            #Step 3: Associated financial data (stock financial data)
            {
                "$lookup": {
                    "from": "stock_financial_data",
                    "let": {"stock_code": "$code", "stock_source": "$source"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$code", "$$stock_code"]},
                                        {"$eq": ["$data_source", "$$stock_source"]}
                                    ]
                                }
                            }
                        },
                        {"$sort": {"report_period": -1}},
                        {"$limit": 1}
                    ],
                    "as": "financial_data"
                }
            },
            #Step 4: Expand financial data array
            {
                "$unwind": {
                    "path": "$financial_data",
                    "preserveNullAndEmptyArrays": True
                }
            },
            #Step 5: Restructure the field
            {
                "$project": {
                    #Basic information field
                    "code": 1,
                    "name": 1,
                    "industry": 1,
                    "area": 1,
                    "market": 1,
                    "list_date": 1,
                    "source": 1,
                    #Market value information
                    "total_mv": 1,
                    "circ_mv": 1,
                    #Valuation indicators
                    "pe": 1,
                    "pb": 1,
                    "pe_ttm": 1,
                    "pb_mrq": 1,
                    #Financial indicators
                    "roe": "$financial_data.roe",
                    "roa": "$financial_data.roa",
                    "netprofit_margin": "$financial_data.netprofit_margin",
                    "gross_margin": "$financial_data.gross_margin",
                    "report_period": "$financial_data.report_period",
                    #Transaction indicators
                    "turnover_rate": 1,
                    "volume_ratio": 1,
                    #Real-time line data
                    "close": "$quote_data.close",
                    "open": "$quote_data.open",
                    "high": "$quote_data.high",
                    "low": "$quote_data.low",
                    "pre_close": "$quote_data.pre_close",
                    "pct_chg": "$quote_data.pct_chg",
                    "amount": "$quote_data.amount",
                    "volume": "$quote_data.volume",
                    "trade_date": "$quote_data.trade_date",
                    #Timetamp
                    "updated_at": 1,
                    "quote_updated_at": "$quote_data.updated_at",
                    "financial_updated_at": "$financial_data.updated_at"
                }
            }
        ]

        #Create View
        await db.command({
            "create": "stock_screening_view",
            "viewOn": "stock_basic_info",
            "pipeline": pipeline
        })

        logger.info("Could not close temporary folder: %s")

    except Exception as e:
        logger.warning(f"Could not close temporary folder: %s{e}")


async def create_database_indexes(db):
    """Create Database Index"""
    try:
        #Index to stock basic info
        basic_info = db["stock_basic_info"]
        await basic_info.create_index([("code", 1), ("source", 1)], unique=True)
        await basic_info.create_index([("industry", 1)])
        await basic_info.create_index([("total_mv", -1)])
        await basic_info.create_index([("pe", 1)])
        await basic_info.create_index([("pb", 1)])

        #Index of market quotes
        market_quotes = db["market_quotes"]
        await market_quotes.create_index([("code", 1)], unique=True)
        await market_quotes.create_index([("pct_chg", -1)])
        await market_quotes.create_index([("amount", -1)])
        await market_quotes.create_index([("updated_at", -1)])

        logger.info("✅ Database index created")

    except Exception as e:
        logger.warning(f"Could not close temporary folder: %s{e}")


async def close_database():
    """Close database connection"""
    global mongo_client, mongo_db, redis_client, redis_pool

    await db_manager.close_connections()

    #Empty Global Variables
    mongo_client = None
    mongo_db = None
    redis_client = None
    redis_pool = None


def get_mongo_client() -> AsyncIOMotorClient:
    """Get MongoDB Client"""
    if mongo_client is None:
        raise RuntimeError("MongoDB客户端未初始化")
    return mongo_client


def get_mongo_db() -> AsyncIOMotorDatabase:
    """Example of accessing MongoDB database"""
    if mongo_db is None:
        raise RuntimeError("MongoDB数据库未初始化")
    return mongo_db


def get_redis_client() -> Redis:
    """Get Redis client"""
    if redis_client is None:
        raise RuntimeError("Redis客户端未初始化")
    return redis_client


async def get_database_health() -> dict:
    """Access to database health status"""
    return await db_manager.health_check()


#JBH REMOVE  #Gender-compatible Names
#JBH REMOVE  init_db = init_database
#JBH REMOVE  close_db = close_database


def get_database():
    """Access to database examples"""
    if db_manager.mongo_client is None:
        raise RuntimeError("MongoDB客户端未初始化")
    #JBH  return db_manager.mongo_client.tradingagents
    return db_manager.mongo_client[settings.MONGO_DB]


#=================== Synchronous MongoDB Access ===================
def get_mongo_db_synchronous() -> Database:
    """
    Get instance of a 'synchronous' version of the MongoDB database
    NOTE: 'Synchronous'  version of MongoDB access
    NOTE: 'Asynchronous' version is created in DatabaseManager.init_mongodb()
    For non-spacing context (e.g., call by normal function)
    """
    global _synchronous_mongo_client, _synchronous_mongo_db

    if _synchronous_mongo_db is not None:
        return _synchronous_mongo_db

    #Create a 'synchronous' MongoDB client
    if _synchronous_mongo_client is None:
        _synchronous_mongo_client = MongoClient(
            settings.MONGO_URI,
            maxPoolSize=settings.MONGO_MAX_CONNECTIONS,
            minPoolSize=settings.MONGO_MIN_CONNECTIONS,
            maxIdleTimeMS=30000,
            serverSelectionTimeoutMS=5000
        )

    _synchronous_mongo_db = _synchronous_mongo_client[settings.MONGO_DB]
    return _synchronous_mongo_db