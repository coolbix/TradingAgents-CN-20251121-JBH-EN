"""Redis client configuration and connection management
"""

import redis.asyncio as redis
import logging
from typing import Optional
from .config import settings

logger = logging.getLogger(__name__)

#Global Redis Connection Pool
redis_pool: Optional[redis.ConnectionPool] = None
redis_client: Optional[redis.Redis] = None


async def init_redis():
    """Initialize Redis Connection"""
    global redis_pool, redis_client

    try:
        #Create Connection Pool
        redis_pool = redis.ConnectionPool.from_url(
            settings.REDIS_URL,
            max_connections=settings.REDIS_MAX_CONNECTIONS,  #Use the value in the configuration file
            retry_on_timeout=settings.REDIS_RETRY_ON_TIMEOUT,
            decode_responses=True,
            socket_keepalive=True,  #Enable TCP keepalive
            socket_keepalive_options={
                1: 60,  #TCP KEPEPIDLE: 60 seconds to launch the keepalive detection.
                2: 10,  #TCP KEPINTVL: 1 detection every 10 seconds
                3: 3,   #TCP KEPCNT: Send up to 3 detections
            },
            health_check_interval=30,  #Check your health every 30 seconds.
        )

        #Create Redis client
        redis_client = redis.Redis(connection_pool=redis_pool)

        #Test Connection
        await redis_client.ping()
        logger.info(f"Redis connection successfully created (max conventions=){settings.REDIS_MAX_CONNECTIONS})")

    except Exception as e:
        logger.error(f"Redis connection failed:{e}")
        raise


async def close_redis():
    """Close Redis Connection"""
    global redis_pool, redis_client
    
    try:
        if redis_client:
            await redis_client.close()
        if redis_pool:
            await redis_pool.disconnect()
        logger.info("Redis connection closed.")
    except Exception as e:
        logger.error(f"There was an error closing the Redis connection:{e}")


def get_redis() -> redis.Redis:
    """Fetch Redis client examples"""
    if redis_client is None:
        raise RuntimeError("Redis客户端未初始化")
    return redis_client


class RedisKeys:
    """Redis Key Name Constant"""
    
    #Queue Related
    USER_PENDING_QUEUE = "user:{user_id}:pending"
    USER_PROCESSING_SET = "user:{user_id}:processing"
    GLOBAL_PENDING_QUEUE = "global:pending"
    GLOBAL_PROCESSING_SET = "global:processing"
    
    #Mandate-related
    TASK_PROGRESS = "task:{task_id}:progress"
    TASK_RESULT = "task:{task_id}:result"
    TASK_LOCK = "task:{task_id}:lock"
    
    #Batch Relevant
    BATCH_PROGRESS = "batch:{batch_id}:progress"
    BATCH_TASKS = "batch:{batch_id}:tasks"
    BATCH_LOCK = "batch:{batch_id}:lock"
    
    #User-related
    USER_SESSION = "session:{session_id}"
    USER_RATE_LIMIT = "rate_limit:{user_id}:{endpoint}"
    USER_DAILY_QUOTA = "quota:{user_id}:{date}"
    
    #System-related
    QUEUE_STATS = "queue:stats"
    SYSTEM_CONFIG = "system:config"
    WORKER_HEARTBEAT = "worker:{worker_id}:heartbeat"
    
    #Cache Related
    SCREENING_CACHE = "screening:{cache_key}"
    ANALYSIS_CACHE = "analysis:{cache_key}"


class RedisService:
    """Redis service seal Category"""
    
    def __init__(self):
        self.redis = get_redis()
    
    async def set_with_ttl(self, key: str, value: str, ttl: int = 3600):
        """Set key value with TTL"""
        await self.redis.setex(key, ttl, value)
    
    async def get_json(self, key: str):
        """Get values in JSON format"""
        import json
        value = await self.redis.get(key)
        if value:
            return json.loads(value)
        return None
    
    async def set_json(self, key: str, value: dict, ttl: int = None):
        """Set JSON format values"""
        import json
        json_str = json.dumps(value, ensure_ascii=False)
        if ttl:
            await self.redis.setex(key, ttl, json_str)
        else:
            await self.redis.set(key, json_str)
    
    async def increment_with_ttl(self, key: str, ttl: int = 3600):
        """Incremental counter and set TTL"""
        pipe = self.redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, ttl)
        results = await pipe.execute()
        return results[0]
    
    async def add_to_queue(self, queue_key: str, item: dict):
        """Add Queue Items"""
        import json
        await self.redis.lpush(queue_key, json.dumps(item, ensure_ascii=False))
    
    async def pop_from_queue(self, queue_key: str, timeout: int = 1):
        """Popup Items From Queue"""
        import json
        result = await self.redis.brpop(queue_key, timeout=timeout)
        if result:
            return json.loads(result[1])
        return None
    
    async def get_queue_length(self, queue_key: str):
        """Fetch Queue Length"""
        return await self.redis.llen(queue_key)
    
    async def add_to_set(self, set_key: str, value: str):
        """Add to Pool"""
        await self.redis.sadd(set_key, value)
    
    async def remove_from_set(self, set_key: str, value: str):
        """Remove From Pool"""
        await self.redis.srem(set_key, value)
    
    async def is_in_set(self, set_key: str, value: str):
        """Check if it's in the assembly."""
        return await self.redis.sismember(set_key, value)
    
    async def get_set_size(self, set_key: str):
        """Fetch Collective Size"""
        return await self.redis.scard(set_key)
    
    async def acquire_lock(self, lock_key: str, timeout: int = 30):
        """Get distributed locks"""
        import uuid
        lock_value = str(uuid.uuid4())
        acquired = await self.redis.set(lock_key, lock_value, nx=True, ex=timeout)
        if acquired:
            return lock_value
        return None
    
    async def release_lock(self, lock_key: str, lock_value: str):
        """Release distributed lock"""
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        return await self.redis.eval(lua_script, 1, lock_key, lock_value)


#Global Redis Service Example
redis_service: Optional[RedisService] = None


def get_redis_service() -> RedisService:
    """Examples of accessing Redis services"""
    global redis_service
    if redis_service is None:
        redis_service = RedisService()
    return redis_service
