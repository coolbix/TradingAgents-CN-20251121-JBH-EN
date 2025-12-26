"""Queue the service 's auxiliary function (as it relates to the Redis operation) to facilitate thin commissioning in the main service.
"""
from __future__ import annotations
import time
from typing import Dict
from redis.asyncio import Redis

from .keys import (
    READY_LIST,
    TASK_PREFIX,
    SET_PROCESSING,
    USER_PROCESSING_PREFIX,
    VISIBILITY_TIMEOUT_PREFIX,
)


async def check_user_concurrent_limit(r: Redis, user_id: str, limit: int) -> bool:
    """Check user and limit"""
    user_processing_key = USER_PROCESSING_PREFIX + user_id
    current_count = await r.scard(user_processing_key)
    return current_count < limit


async def check_global_concurrent_limit(r: Redis, limit: int) -> bool:
    """Check global co-location limits (based on group size in processing)"""
    current_count = await r.scard(SET_PROCESSING)
    return current_count < limit


async def mark_task_processing(r: Redis, task_id: str, user_id: str) -> None:
    """Mark Task as Processing"""
    user_processing_key = USER_PROCESSING_PREFIX + user_id
    await r.sadd(user_processing_key, task_id)
    await r.sadd(SET_PROCESSING, task_id)


async def unmark_task_processing(r: Redis, task_id: str, user_id: str) -> None:
    """Unmark Task Processing"""
    user_processing_key = USER_PROCESSING_PREFIX + user_id
    await r.srem(user_processing_key, task_id)
    await r.srem(SET_PROCESSING, task_id)


async def set_visibility_timeout(r: Redis, task_id: str, worker_id: str, visibility_timeout: int) -> None:
    """Set Visibility Timeout"""
    timeout_key = VISIBILITY_TIMEOUT_PREFIX + task_id
    timeout_data: Dict[str, str] = {
        "task_id": task_id,
        "worker_id": worker_id,
        "timeout_at": str(int(time.time()) + visibility_timeout),
    }
    await r.hset(timeout_key, mapping=timeout_data)
    await r.expire(timeout_key, visibility_timeout)


async def clear_visibility_timeout(r: Redis, task_id: str) -> None:
    """Clear Visibility Timeout"""
    timeout_key = VISIBILITY_TIMEOUT_PREFIX + task_id
    await r.delete(timeout_key)

