"""Enhance Queue Service
Add complication control, priority queues, visibility overtime based on existing realizations
"""

import json
import time
import uuid
import asyncio
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from redis.asyncio import Redis

from app.core.database import get_redis_client

from app.services.queue import (
    READY_LIST,
    TASK_PREFIX,
    BATCH_PREFIX,
    SET_PROCESSING,
    SET_COMPLETED,
    SET_FAILED,
    BATCH_TASKS_PREFIX,
    USER_PROCESSING_PREFIX,
    GLOBAL_CONCURRENT_KEY,
    VISIBILITY_TIMEOUT_PREFIX,
    DEFAULT_USER_CONCURRENT_LIMIT,
    GLOBAL_CONCURRENT_LIMIT,
    VISIBILITY_TIMEOUT_SECONDS,
    check_user_concurrent_limit,
    check_global_concurrent_limit,
    mark_task_processing,
    unmark_task_processing,
    set_visibility_timeout,
    clear_visibility_timeout,
)

logger = logging.getLogger(__name__)

#Redis keynames and configuration constants are provided by app.services.queue.keys (defined here no more)


class QueueService:
    """Enhance the queue service class"""

    def __init__(self, redis: Redis):
        self.r = redis
        self.user_concurrent_limit = DEFAULT_USER_CONCURRENT_LIMIT
        self.global_concurrent_limit = GLOBAL_CONCURRENT_LIMIT
        self.visibility_timeout = VISIBILITY_TIMEOUT_SECONDS

    async def enqueue_task(
        self,
        user_id: str,
        symbol: str,
        params: Dict[str, Any],
        batch_id: Optional[str] = None
    ) -> str:
        """Tasks in formation, support and distribution control (open-source FIFO queue)"""

        #Check user and limit
        if not await self._check_user_concurrent_limit(user_id):
            raise ValueError(f"用户 {user_id} 达到并发限制 ({self.user_concurrent_limit})")

        #Check global and issue limits
        if not await self._check_global_concurrent_limit():
            raise ValueError(f"系统达到全局并发限制 ({self.global_concurrent_limit})")

        task_id = str(uuid.uuid4())
        key = TASK_PREFIX + task_id
        now = int(time.time())

        mapping = {
            "id": task_id,
            "user": user_id,
            "symbol": symbol,
            "status": "queued",
            "created_at": str(now),
            "params": json.dumps(params or {}),
            "enqueued_at": str(now)
        }

        if batch_id:
            mapping["batch_id"] = batch_id

        #Can not open message
        await self.r.hset(key, mapping=mapping)

        #Add to FIFO queue
        await self.r.lpush(READY_LIST, task_id)

        if batch_id:
            await self.r.sadd(BATCH_TASKS_PREFIX + batch_id, task_id)

        logger.info(f"Tasks in place:{task_id}")
        return task_id

    async def dequeue_task(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Remove Tasks from FIFO Queue"""
        try:
            #Can not open message
            task_id = await self.r.rpop(READY_LIST)
            if not task_id:
                return None

            #Can not open message
            task_data = await self.get_task(task_id)
            if not task_data:
                logger.warning(f"Task data does not exist:{task_id}")
                return None

            user_id = task_data.get("user")

            #Re-inspection and imposition of restrictions (preventing competitive conditions)
            if not await self._check_user_concurrent_limit(user_id):
                #Put the task back in line if the limit is exceeded
                await self.r.lpush(READY_LIST, task_id)
                logger.warning(f"User{user_id}The mission is back in line:{task_id}")
                return None

            #Mark Task as Processing
            await self._mark_task_processing(task_id, user_id, worker_id)

            #Set Visibility Timeout
            await self._set_visibility_timeout(task_id, worker_id)

            #Update Task Status
            await self.r.hset(TASK_PREFIX + task_id, mapping={
                "status": "processing",
                "worker_id": worker_id,
                "started_at": str(int(time.time()))
            })

            logger.info(f"Mission is out:{task_id} -> Worker: {worker_id}")
            return task_data

        except Exception as e:
            logger.error(f"Team failure:{e}")
            return None

    async def ack_task(self, task_id: str, success: bool = True) -> bool:
        """Confirm mission complete."""
        try:
            task_data = await self.get_task(task_id)
            if not task_data:
                return False

            user_id = task_data.get("user")
            worker_id = task_data.get("worker_id")

            #Remove from processing
            await self._unmark_task_processing(task_id, user_id)

            #Clear Visibility Timeout
            await self._clear_visibility_timeout(task_id)

            #Update Task Status
            status = "completed" if success else "failed"
            await self.r.hset(TASK_PREFIX + task_id, mapping={
                "status": status,
                "completed_at": str(int(time.time()))
            })

            #Add to the corresponding set
            if success:
                await self.r.sadd(SET_COMPLETED, task_id)
            else:
                await self.r.sadd(SET_FAILED, task_id)

            logger.info(f"Mandate confirmed:{task_id}(success:{success})")
            return True

        except Exception as e:
            logger.error(f"Confirm mission failure:{e}")
            return False

    async def create_batch(self, user_id: str, symbols: List[str], params: Dict[str, Any]) -> tuple[str, int]:
        batch_id = str(uuid.uuid4())
        now = int(time.time())
        batch_key = BATCH_PREFIX + batch_id
        await self.r.hset(batch_key, mapping={
            "id": batch_id,
            "user": user_id,
            "status": "queued",
            "submitted": str(len(symbols)),
            "created_at": str(now),
        })
        for s in symbols:
            await self.enqueue_task(user_id=user_id, symbol=s, params=params, batch_id=batch_id)
        return batch_id, len(symbols)

    async def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        key = TASK_PREFIX + task_id
        data = await self.r.hgetall(key)
        if not data:
            return None
        # parse fields
        if "params" in data:
            try:
                data["parameters"] = json.loads(data.pop("params"))
            except Exception:
                data["parameters"] = {}
        if "created_at" in data and data["created_at"].isdigit():
            data["created_at"] = int(data["created_at"])
        if "submitted" in data and str(data["submitted"]).isdigit():
            data["submitted"] = int(data["submitted"])
        return data

    async def get_batch(self, batch_id: str) -> Optional[Dict[str, Any]]:
        key = BATCH_PREFIX + batch_id
        data = await self.r.hgetall(key)
        if not data:
            return None
        # enrich with tasks count if set exists
        submitted = data.get("submitted")
        if submitted is not None and str(submitted).isdigit():
            data["submitted"] = int(submitted)
        if "created_at" in data and data["created_at"].isdigit():
            data["created_at"] = int(data["created_at"])
        data["tasks"] = list(await self.r.smembers(BATCH_TASKS_PREFIX + batch_id))
        return data

    async def stats(self) -> Dict[str, int]:
        queued = await self.r.llen(READY_LIST)
        processing = await self.r.scard(SET_PROCESSING)
        completed = await self.r.scard(SET_COMPLETED)
        failed = await self.r.scard(SET_FAILED)
        return {
            "queued": int(queued or 0),
            "processing": int(processing or 0),
            "completed": int(completed or 0),
            "failed": int(failed or 0),
        }

    #Add: Parallel control method
    async def _check_user_concurrent_limit(self, user_id: str) -> bool:
        """Check user and limit (commission helpers)"""
        return await check_user_concurrent_limit(self.r, user_id, self.user_concurrent_limit)

    async def _check_global_concurrent_limit(self) -> bool:
        """Check global and issue limits (commission helpers)"""
        return await check_global_concurrent_limit(self.r, self.global_concurrent_limit)

    async def _mark_task_processing(self, task_id: str, user_id: str, worker_id: str):
        """Mark task as processing (commission helpers)"""
        await mark_task_processing(self.r, task_id, user_id)

    async def _unmark_task_processing(self, task_id: str, user_id: str):
        """Unmark task processing (commissions helpers)"""
        await unmark_task_processing(self.r, task_id, user_id)

    async def _set_visibility_timeout(self, task_id: str, worker_id: str):
        """Set Visibility Timeout (commissions helpers)"""
        await set_visibility_timeout(self.r, task_id, worker_id, self.visibility_timeout)

    async def _clear_visibility_timeout(self, task_id: str):
        """Clear Visibility Timeout"""
        await clear_visibility_timeout(self.r, task_id)

    async def get_user_queue_status(self, user_id: str) -> Dict[str, int]:
        """Get User Queue Status"""
        user_processing_key = USER_PROCESSING_PREFIX + user_id
        processing_count = await self.r.scard(user_processing_key)

        return {
            "processing": int(processing_count or 0),
            "concurrent_limit": self.user_concurrent_limit,
            "available_slots": max(0, self.user_concurrent_limit - int(processing_count or 0))
        }

    async def cleanup_expired_tasks(self):
        """Clean-up of expired tasks (visibility timeout)"""
        try:
            #Get All Visibility Timeout Keys
            timeout_keys = await self.r.keys(VISIBILITY_TIMEOUT_PREFIX + "*")

            current_time = int(time.time())
            expired_tasks = []

            for timeout_key in timeout_keys:
                timeout_data = await self.r.hgetall(timeout_key)
                if timeout_data:
                    timeout_at = int(timeout_data.get("timeout_at", 0))
                    if current_time > timeout_at:
                        task_id = timeout_data.get("task_id")
                        if task_id:
                            expired_tasks.append(task_id)

            #Processing expired tasks
            for task_id in expired_tasks:
                await self._handle_expired_task(task_id)

            if expired_tasks:
                logger.warning(f"Got it.{len(expired_tasks)}Expire Tasks")

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")

    async def _handle_expired_task(self, task_id: str):
        """Processing expired tasks"""
        try:
            task_data = await self.get_task(task_id)
            if not task_data:
                return

            user_id = task_data.get("user")

            #Remove from processing
            await self._unmark_task_processing(task_id, user_id)

            #Clear Visibility Timeout
            await self._clear_visibility_timeout(task_id)

            #Rejoinder
            await self.r.lpush(READY_LIST, task_id)

            #Update Task Status
            await self.r.hset(TASK_PREFIX + task_id, mapping={
                "status": "queued",
                "worker_id": "",
                "requeued_at": str(int(time.time()))
            })

            logger.warning(f"Expired tasks re-enter:{task_id}")

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{task_id} - {e}")

    async def cancel_task(self, task_id: str) -> bool:
        """Cancel Task"""
        try:
            task_data = await self.get_task(task_id)
            if not task_data:
                return False

            status = task_data.get("status")
            user_id = task_data.get("user")

            if status == "processing":
                #Remove from processing pool if processed
                await self._unmark_task_processing(task_id, user_id)
                await self._clear_visibility_timeout(task_id)
            elif status == "queued":
                #If in queue, remove from queue
                await self.r.lrem(READY_LIST, 0, task_id)

            #Update Task Status
            await self.r.hset(TASK_PREFIX + task_id, mapping={
                "status": "cancelled",
                "cancelled_at": str(int(time.time()))
            })

            logger.info(f"Other Organiser{task_id}")
            return True

        except Exception as e:
            logger.error(f"Can not open message{e}")
            return False


def get_queue_service() -> QueueService:
    return QueueService(get_redis_client())