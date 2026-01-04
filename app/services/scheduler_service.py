#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Timed Task Management Service
Provides time task queries, pauses, recovery, manual triggers, etc.
"""

import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.job import Job
from apscheduler.events import (
    EVENT_JOB_EXECUTED,
    EVENT_JOB_ERROR,
    EVENT_JOB_MISSED,
    JobExecutionEvent
)

from app.core.database import get_mongo_db
from tradingagents.utils.logging_manager import get_logger
from app.utils.timezone import now_tz

logger = get_logger(__name__)

#UTC+8 Timezone
UTC_8 = timezone(timedelta(hours=8))


def get_utc8_now():
    """Fetch UTC+8 Current Time

    Note: returns a given datetime (without time zone information), MongoDB stores local time values as they are
    This allows the frontend to directly add +08:00 suffix display
    """
    return now_tz().replace(tzinfo=None)


class TaskCancelledException(Exception):
    """Mission canceled."""
    pass


class SchedulerService:
    """Timed Task Management Service"""

    def __init__(self, scheduler: AsyncIOScheduler):
        """Initialization services

        Args:
            Scheduler: Example of APScheduler Scheduler
        """
        self.scheduler = scheduler
        self.db = None

        #Add an event monitor and monitor mission execution
        self._setup_event_listeners()
    
    def _get_db(self):
        """Get database connections"""
        if self.db is None:
            self.db = get_mongo_db()
        return self.db
    
    async def list_jobs(self) -> List[Dict[str, Any]]:
        """Can not open message

        Returns:
            Chile
        """
        jobs = []
        for job in self.scheduler.get_jobs():
            job_dict = self._job_to_dict(job)
            #Fetch task metadata (trigger name and comment)
            metadata = await self._get_job_metadata(job.id)
            if metadata:
                job_dict["display_name"] = metadata.get("display_name")
                job_dict["description"] = metadata.get("description")
            jobs.append(job_dict)

        logger.info(f"Other Organiser{len(jobs)}Time job")
        return jobs
    
    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Can not open message

        Args:
            Job id: Task ID

        Returns:
            Task details, return None if not available
        """
        job = self.scheduler.get_job(job_id)
        if job:
            job_dict = self._job_to_dict(job, include_details=True)
            #Get Task Metadata
            metadata = await self._get_job_metadata(job_id)
            if metadata:
                job_dict["display_name"] = metadata.get("display_name")
                job_dict["description"] = metadata.get("description")
            return job_dict
        return None
    
    async def pause_job(self, job_id: str) -> bool:
        """Pause Task

        Args:
            Job id: Task ID

        Returns:
            Success
        """
        try:
            self.scheduler.pause_job(job_id)
            logger.info(f"Mission{job_id}Paused")
            
            #Record operation history
            await self._record_job_action(job_id, "pause", "success")
            return True
        except Exception as e:
            logger.error(f"The mission is suspended.{job_id}Failed:{e}")
            await self._record_job_action(job_id, "pause", "failed", str(e))
            return False
    
    async def resume_job(self, job_id: str) -> bool:
        """Resume Mission

        Args:
            Job id: Task ID

        Returns:
            Success
        """
        try:
            self.scheduler.resume_job(job_id)
            logger.info(f"Mission{job_id}Restored")
            
            #Record operation history
            await self._record_job_action(job_id, "resume", "success")
            return True
        except Exception as e:
            logger.error(f"Return mission.{job_id}Failed:{e}")
            await self._record_job_action(job_id, "resume", "failed", str(e))
            return False
    
    async def trigger_job(self, job_id: str, kwargs: Optional[Dict[str, Any]] = None) -> bool:
        """Manually trigger mission execution

        Note: if the task is suspended, the task will be temporarily resumed and will not be automatically suspended after one execution

        Args:
            Job id: Task ID
            kwargs: Keyword parameters passed to task function (optional)

        Returns:
            Success
        """
        try:
            job = self.scheduler.get_job(job_id)
            if not job:
                logger.error(f"Mission{job_id}does not exist")
                return False

            #Check if the task is suspended (next run time for Noone)
            was_paused = job.next_run_time is None
            if was_paused:
                logger.warning(f"Mission{job_id}Suspended, temporarily resumed for implementation once")
                self.scheduler.resume_job(job_id)
                #Retake job object (changed status after recovery)
                job = self.scheduler.get_job(job_id)
                logger.info(f"Mission{job_id}Provisionally recovered")

            #Merge in task kwargs if kwargs are provided
            if kwargs:
                #Can not open message
                original_kwargs = job.kwargs.copy() if job.kwargs else {}
                #Merge new kwargs
                merged_kwargs = {**original_kwargs, **kwargs}
                #Modify Task kwargs
                job.modify(kwargs=merged_kwargs)
                logger.info(f"Mission{job_id}Parameters updated:{kwargs}")

            #Manual Trigger Job - Use Current Time with Time Zone
            from datetime import timezone
            now = datetime.now(timezone.utc)
            job.modify(next_run_time=now)
            logger.info(f"A manual trigger.{job_id} (next_run_time={now}, was_paused={was_paused}, kwargs={kwargs})")

            #Record operation history
            action_note = f"手动触发执行 (暂停状态: {was_paused}"
            if kwargs:
                action_note += f", 参数: {kwargs}"
            action_note += ")"
            await self._record_job_action(job_id, "trigger", "success", action_note)

            #Create an "running" execution record immediately so that users can see the job being performed
            #Use local time
            await self._record_job_execution(
                job_id=job_id,
                status="running",
                scheduled_time=get_utc8_now(),  #Use local time (naive datetime)
                progress=0,
                is_manual=True  #Mark as manual trigger
            )

            return True
        except Exception as e:
            logger.error(f"Trigger mission{job_id}Failed:{e}")
            import traceback
            logger.error(f"Detailed error:{traceback.format_exc()}")
            await self._record_job_action(job_id, "trigger", "failed", str(e))
            return False
    
    async def get_job_history(
        self,
        job_id: str,
        limit: int = 20,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get Task Execution History

        Args:
            Job id: Task ID
            Limited number of returns
            offset: offset

        Returns:
            Execution history
        """
        try:
            db = self._get_db()
            cursor = db.scheduler_history.find(
                {"job_id": job_id}
            ).sort("timestamp", -1).skip(offset).limit(limit)
            
            history = []
            async for doc in cursor:
                doc.pop("_id", None)
                history.append(doc)
            
            return history
        except Exception as e:
            logger.error(f"Other Organiser{job_id}Implementation history failed:{e}")
            return []
    
    async def count_job_history(self, job_id: str) -> int:
        """Number of statistical missions performed

        Args:
            Job id: Task ID

        Returns:
            Number of historical records
        """
        try:
            db = self._get_db()
            count = await db.scheduler_history.count_documents({"job_id": job_id})
            return count
        except Exception as e:
            logger.error(f"Statistical missions{job_id}Implementation history failed:{e}")
            return 0
    
    async def get_all_history(
        self,
        limit: int = 50,
        offset: int = 0,
        job_id: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all tasks executed history

        Args:
            Limited number of returns
            offset: offset
            job id: Task ID filter
            status: status filter

        Returns:
            Execution history
        """
        try:
            db = self._get_db()
            
            #Build query conditions
            query = {}
            if job_id:
                query["job_id"] = job_id
            if status:
                query["status"] = status
            
            cursor = db.scheduler_history.find(query).sort("timestamp", -1).skip(offset).limit(limit)
            
            history = []
            async for doc in cursor:
                doc.pop("_id", None)
                history.append(doc)
            
            return history
        except Exception as e:
            logger.error(f"❌ Getting an implementation history failure:{e}")
            return []
    
    async def count_all_history(
        self,
        job_id: Optional[str] = None,
        status: Optional[str] = None
    ) -> int:
        """Statistics of all tasks performed

        Args:
            job id: Task ID filter
            status: status filter

        Returns:
            Number of historical records
        """
        try:
            db = self._get_db()

            #Build query conditions
            query = {}
            if job_id:
                query["job_id"] = job_id
            if status:
                query["status"] = status

            count = await db.scheduler_history.count_documents(query)
            return count
        except Exception as e:
            logger.error(f"❌ >Statistical implementation history failure:{e}")
            return 0

    async def get_job_executions(
        self,
        job_id: Optional[str] = None,
        status: Optional[str] = None,
        is_manual: Optional[bool] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get Task Execution History

        Args:
            job id: Task ID (optional, return all tasks if not specified)
            status: status filter (session/failed/missed/running)
            is manual: manual trigger (True = manual, False = automatic, Noe = all)
            Limited number of returns
            offset: offset

        Returns:
            Execute History List
        """
        try:
            db = self._get_db()

            #Build query conditions
            query = {}
            if job_id:
                query["job_id"] = job_id
            if status:
                query["status"] = status

            #Process is manual filter
            if is_manual is not None:
                if is_manual:
                    #Manual trigger: is manual must be true
                    query["is_manual"] = True
                else:
                    #Automatic trigger: is manual field does not exist or is false
                    #Use $ne (not equal) to exclude records from  manual=true
                    query["is_manual"] = {"$ne": True}

            cursor = db.scheduler_executions.find(query).sort("timestamp", -1).skip(offset).limit(limit)

            executions = []
            async for doc in cursor:
                #Convert  id as string
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])

                #Formatting time (MongoDB store is naive dateime, which is local time)
                #Direct sequence to ISO format string, the front end automatically adds +08:00 suffix
                for time_field in ["scheduled_time", "timestamp", "updated_at"]:
                    if doc.get(time_field):
                        dt = doc[time_field]
                        #Convert to ISO format string if datetime object
                        if hasattr(dt, 'isoformat'):
                            doc[time_field] = dt.isoformat()

                executions.append(doc)

            return executions
        except Exception as e:
            logger.error(f"❌ Gets mission history failure:{e}")
            return []

    async def count_job_executions(
        self,
        job_id: Optional[str] = None,
        status: Optional[str] = None,
        is_manual: Optional[bool] = None
    ) -> int:
        """Number of statistical missions performed

        Args:
            job id: Task ID (optional)
            status: status filter (optional)
            is manual: Manually triggered (optional)

        Returns:
            Number of implementation history
        """
        try:
            db = self._get_db()

            #Build query conditions
            query = {}
            if job_id:
                query["job_id"] = job_id
            if status:
                query["status"] = status

            #Process is manual filter
            if is_manual is not None:
                if is_manual:
                    #Manual trigger: is manual must be true
                    query["is_manual"] = True
                else:
                    #Automatic trigger: is manual field does not exist or is false
                    query["is_manual"] = {"$ne": True}

            count = await db.scheduler_executions.count_documents(query)
            return count
        except Exception as e:
            logger.error(f"The history of statistical mission failure:{e}")
            return 0

    async def cancel_job_execution(self, execution_id: str) -> bool:
        """Mandate execution cancelled/terminated

        (a) For ongoing tasks, the demarking is set;
        For quit but still running in the database, directly marked as failed

        Args:
            Exection id: Execute Record ID (MongoDB id)

        Returns:
            Success
        """
        try:
            from bson import ObjectId
            db = self._get_db()

            #Find Execution Record
            execution = await db.scheduler_executions.find_one({"_id": ObjectId(execution_id)})
            if not execution:
                logger.error(f"The execution record does not exist:{execution_id}")
                return False

            if execution.get("status") != "running":
                logger.warning(f"The execution record is not running:{execution_id} (status={execution.get('status')})")
                return False

            #Set Unmark
            await db.scheduler_executions.update_one(
                {"_id": ObjectId(execution_id)},
                {
                    "$set": {
                        "cancel_requested": True,
                        "updated_at": get_utc8_now()
                    }
                }
            )

            logger.info(f"Unmarked:{execution.get('job_name', execution.get('job_id'))} (execution_id={execution_id})")
            return True

        except Exception as e:
            logger.error(f"The mission failed:{e}")
            return False

    async def mark_execution_as_failed(self, execution_id: str, reason: str = "用户手动标记为失败") -> bool:
        """Mark execution record as a failed state

        Used to process outgoing but still running tasks in the database

        Args:
            Exection id: Execute Record ID (MongoDB id)
            Reason for failure

        Returns:
            Success
        """
        try:
            from bson import ObjectId
            db = self._get_db()

            #Find Execution Record
            execution = await db.scheduler_executions.find_one({"_id": ObjectId(execution_id)})
            if not execution:
                logger.error(f"The execution record does not exist:{execution_id}")
                return False

            #Update to failed state
            await db.scheduler_executions.update_one(
                {"_id": ObjectId(execution_id)},
                {
                    "$set": {
                        "status": "failed",
                        "error_message": reason,
                        "updated_at": get_utc8_now()
                    }
                }
            )

            logger.info(f"It was marked as a failure:{execution.get('job_name', execution.get('job_id'))} (execution_id={execution_id}, reason={reason})")
            return True

        except Exception as e:
            logger.error(f"❌ Tag execution record failed:{e}")
            return False

    async def delete_execution(self, execution_id: str) -> bool:
        """Delete Execution Record

        Args:
            Exection id: Execute Record ID (MongoDB id)

        Returns:
            Success
        """
        try:
            from bson import ObjectId
            db = self._get_db()

            #Find Execution Record
            execution = await db.scheduler_executions.find_one({"_id": ObjectId(execution_id)})
            if not execution:
                logger.error(f"The execution record does not exist:{execution_id}")
                return False

            #Can not allow the deletion of an active task
            if execution.get("status") == "running":
                logger.error(f"The task under way cannot be deleted:{execution_id}")
                return False

            #Delete Record
            result = await db.scheduler_executions.delete_one({"_id": ObjectId(execution_id)})

            if result.deleted_count > 0:
                logger.info(f"The execution record has been deleted:{execution.get('job_name', execution.get('job_id'))} (execution_id={execution_id})")
                return True
            else:
                logger.error(f"Deleting the execution record failed:{execution_id}")
                return False

        except Exception as e:
            logger.error(f"Deleting the execution record failed:{e}")
            return False

    async def get_job_execution_stats(self, job_id: str) -> Dict[str, Any]:
        """Access to statistical information on mandate implementation

        Args:
            Job id: Task ID

        Returns:
            Statistical information
        """
        try:
            db = self._get_db()

            #Statistics of the number of implementations by status
            pipeline = [
                {"$match": {"job_id": job_id}},
                {"$group": {
                    "_id": "$status",
                    "count": {"$sum": 1},
                    "avg_execution_time": {"$avg": "$execution_time"}
                }}
            ]

            stats = {
                "total": 0,
                "success": 0,
                "failed": 0,
                "missed": 0,
                "avg_execution_time": 0
            }

            async for doc in db.scheduler_executions.aggregate(pipeline):
                status = doc["_id"]
                count = doc["count"]
                stats["total"] += count
                stats[status] = count

                if status == "success" and doc.get("avg_execution_time"):
                    stats["avg_execution_time"] = round(doc["avg_execution_time"], 2)

            #Get the last one.
            last_execution = await db.scheduler_executions.find_one(
                {"job_id": job_id},
                sort=[("timestamp", -1)]
            )

            if last_execution:
                stats["last_execution"] = {
                    "status": last_execution.get("status"),
                    "timestamp": last_execution.get("timestamp").isoformat() if last_execution.get("timestamp") else None,
                    "execution_time": last_execution.get("execution_time")
                }

            return stats
        except Exception as e:
            logger.error(f"❌ for mission execution statistics failed:{e}")
            return {}
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get statistics from the scheduler

        Returns:
            Statistical information
        """
        jobs = self.scheduler.get_jobs()
        
        total = len(jobs)
        running = sum(1 for job in jobs if job.next_run_time is not None)
        paused = total - running
        
        return {
            "total_jobs": total,
            "running_jobs": running,
            "paused_jobs": paused,
            "scheduler_running": self.scheduler.running,
            "scheduler_state": self.scheduler.state
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """Dispatch health check

        Returns:
            Health status
        """
        return {
            "status": "healthy" if self.scheduler.running else "stopped",
            "running": self.scheduler.running,
            "state": self.scheduler.state,
            "timestamp": get_utc8_now().isoformat()
        }
    
    def _job_to_dict(self, job: Job, include_details: bool = False) -> Dict[str, Any]:
        """Convert Job Object to Dictionary

        Args:
            Job: Job Object
            include details: contains details

        Returns:
            Dictionary
        """
        result = {
            "id": job.id,
            "name": job.name or job.id,
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
            "paused": job.next_run_time is None,
            "trigger": str(job.trigger),
        }
        
        if include_details:
            result.update({
                "func": f"{job.func.__module__}.{job.func.__name__}",
                "args": job.args,
                "kwargs": job.kwargs,
                "misfire_grace_time": job.misfire_grace_time,
                "max_instances": job.max_instances,
            })
        
        return result
    
    def _setup_event_listeners(self):
        """Setup APScheduler event listening device"""
        #Monitor success of task execution
        self.scheduler.add_listener(
            self._on_job_executed,
            EVENT_JOB_EXECUTED
        )

        #Failed to listen to task execution
        self.scheduler.add_listener(
            self._on_job_error,
            EVENT_JOB_ERROR
        )

        #Missed performance on the bug
        self.scheduler.add_listener(
            self._on_job_missed,
            EVENT_JOB_MISSED
        )

        logger.info("The APSchedul event listening device is set")

        #Add a time job to detect a zombie mission (long running status)
        self.scheduler.add_job(
            self._check_zombie_tasks,
            'interval',
            minutes=5,
            id='check_zombie_tasks',
            name='检测僵尸任务',
            replace_existing=True
        )
        logger.info("✅Step-timed mission for zombie detection added")

    async def _check_zombie_tasks(self):
        """Zombie detection mission (long run-in)"""
        try:
            db = self._get_db()

            #Find jobs that have remained running for more than 30 minutes
            threshold_time = get_utc8_now() - timedelta(minutes=30)

            zombie_tasks = await db.scheduler_executions.find({
                "status": "running",
                "timestamp": {"$lt": threshold_time}
            }).to_list(length=100)

            for task in zombie_tasks:
                #Update to failed state
                await db.scheduler_executions.update_one(
                    {"_id": task["_id"]},
                    {
                        "$set": {
                            "status": "failed",
                            "error_message": "任务执行超时或进程异常终止",
                            "updated_at": get_utc8_now()
                        }
                    }
                )
                logger.warning(f"Zombie mission detected:{task.get('job_name', task.get('job_id'))}(Start time:{task.get('timestamp')})")

            if zombie_tasks:
                logger.info(f"It's marked.{len(zombie_tasks)}A zombie mission is a failure.")

        except Exception as e:
            logger.error(f"The mission failed:{e}")

    def _on_job_executed(self, event: JobExecutionEvent):
        """Mission execution returned successfully"""
        #Calculate implementation time (dealing with time zone issues)
        execution_time = None
        if event.scheduled_run_time:
            now = datetime.now(event.scheduled_run_time.tzinfo)
            execution_time = (now - event.scheduled_run_time).total_seconds()

        asyncio.create_task(self._record_job_execution(
            job_id=event.job_id,
            status="success",
            scheduled_time=event.scheduled_run_time,
            execution_time=execution_time,
            return_value=str(event.retval) if event.retval else None,
            progress=100  #Mission accomplished, 100 per cent progress
        ))

    def _on_job_error(self, event: JobExecutionEvent):
        """Job execution failed to return"""
        #Calculate implementation time (dealing with time zone issues)
        execution_time = None
        if event.scheduled_run_time:
            now = datetime.now(event.scheduled_run_time.tzinfo)
            execution_time = (now - event.scheduled_run_time).total_seconds()

        asyncio.create_task(self._record_job_execution(
            job_id=event.job_id,
            status="failed",
            scheduled_time=event.scheduled_run_time,
            execution_time=execution_time,
            error_message=str(event.exception) if event.exception else None,
            traceback=event.traceback if hasattr(event, 'traceback') else None,
            progress=None  #Do not set progress when failure
        ))

    def _on_job_missed(self, event: JobExecutionEvent):
        """Mission missed callback"""
        asyncio.create_task(self._record_job_execution(
            job_id=event.job_id,
            status="missed",
            scheduled_time=event.scheduled_run_time,
            progress=None  #Do not set progress when missing
        ))

    async def _record_job_execution(
        self,
        job_id: str,
        status: str,
        scheduled_time: datetime = None,
        execution_time: float = None,
        return_value: str = None,
        error_message: str = None,
        traceback: str = None,
        progress: int = None,
        is_manual: bool = False
    ):
        """Record mission execution history

        Args:
            Job id: Task ID
            Status: Status (running/success/failed/missed)
            Scheduled time: scheduled implementation time
            Exection time: actual execution time (sec)
            Return value
            error message
            trackback: Error stack
            Progress in implementation (0-100)
            is manual: manual trigger
        """
        try:
            db = self._get_db()

            #Fetch Task Name
            job = self.scheduler.get_job(job_id)
            job_name = job.name if job else job_id

            #If complete, check for running records
            if status in ["success", "failed"]:
                #Find Recent Running Records (5 minutes)
                five_minutes_ago = get_utc8_now() - timedelta(minutes=5)
                existing_record = await db.scheduler_executions.find_one(
                    {
                        "job_id": job_id,
                        "status": "running",
                        "timestamp": {"$gte": five_minutes_ago}
                    },
                    sort=[("timestamp", -1)]
                )

                if existing_record:
                    #Update existing records
                    update_data = {
                        "status": status,
                        "execution_time": execution_time,
                        "updated_at": get_utc8_now()
                    }

                    if return_value:
                        update_data["return_value"] = return_value
                    if error_message:
                        update_data["error_message"] = error_message
                    if traceback:
                        update_data["traceback"] = traceback
                    if progress is not None:
                        update_data["progress"] = progress

                    await db.scheduler_executions.update_one(
                        {"_id": existing_record["_id"]},
                        {"$set": update_data}
                    )

                    #Log
                    if status == "success":
                        logger.info(f"[Mission execution]{job_name}Implementation success, time-consuming:{execution_time:.2f}sec")
                    elif status == "failed":
                        logger.error(f"[Mission execution]{job_name}Implementation failed:{error_message}")

                    return

            #If Running Record is not found, or Running/missed state, insert new record
            #scheduled time probably aware datatime (from APScheduler) needs to be converted to give date
            scheduled_time_naive = None
            if scheduled_time:
                if scheduled_time.tzinfo is not None:
                    #Convert to local time zone, then remove time zone information
                    scheduled_time_naive = scheduled_time.astimezone(UTC_8).replace(tzinfo=None)
                else:
                    scheduled_time_naive = scheduled_time

            execution_record = {
                "job_id": job_id,
                "job_name": job_name,
                "status": status,
                "scheduled_time": scheduled_time_naive,
                "execution_time": execution_time,
                "timestamp": get_utc8_now(),
                "is_manual": is_manual
            }

            if return_value:
                execution_record["return_value"] = return_value
            if error_message:
                execution_record["error_message"] = error_message
            if traceback:
                execution_record["traceback"] = traceback
            if progress is not None:
                execution_record["progress"] = progress

            await db.scheduler_executions.insert_one(execution_record)

            #Log
            if status == "success":
                logger.info(f"[Mission execution]{job_name}Implementation success, time-consuming:{execution_time:.2f}sec")
            elif status == "failed":
                logger.error(f"[Mission execution]{job_name}Implementation failed:{error_message}")
            elif status == "missed":
                logger.warning(f"[Mission execution]{job_name}Missing execution time")
            elif status == "running":
                trigger_type = "手动触发" if is_manual else "自动触发"
                logger.info(f"[Mission execution]{job_name}Start implementation ({trigger_type}Progress:{progress}%")

        except Exception as e:
            logger.error(f"@❌ > Documenting mission history failures:{e}")

    async def _record_job_action(
        self,
        job_id: str,
        action: str,
        status: str,
        error_message: str = None
    ):
        """Log Task Operation History

        Args:
            Job id: Task ID
            action: operation type (pause/resume/trigger)
            status: status/failed
            error message
        """
        try:
            db = self._get_db()
            await db.scheduler_history.insert_one({
                "job_id": job_id,
                "action": action,
                "status": status,
                "error_message": error_message,
                "timestamp": get_utc8_now()
            })
        except Exception as e:
            logger.error(f"The mission operation history failed:{e}")

    async def _get_job_metadata(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Fetch task metadata (trigger name and comment)

        Args:
            Job id: Task ID

        Returns:
            Metadata dictionary, return None if not available
        """
        try:
            db = self._get_db()
            metadata = await db.scheduler_metadata.find_one({"job_id": job_id})
            if metadata:
                metadata.pop("_id", None)
                return metadata
            return None
        except Exception as e:
            logger.error(f"Other Organiser{job_id}Metadata failed:{e}")
            return None

    async def update_job_metadata(
        self,
        job_id: str,
        display_name: Optional[str] = None,
        description: Optional[str] = None
    ) -> bool:
        """Update Task Metadata

        Args:
            Job id: Task ID
            Display name: trigger name
            description:

        Returns:
            Success
        """
        try:
            #Check if the mission exists.
            job = self.scheduler.get_job(job_id)
            if not job:
                logger.error(f"Mission{job_id}does not exist")
                return False

            db = self._get_db()
            update_data = {
                "job_id": job_id,
                "updated_at": get_utc8_now()
            }

            if display_name is not None:
                update_data["display_name"] = display_name
            if description is not None:
                update_data["description"] = description

            #Update or insert with upset
            await db.scheduler_metadata.update_one(
                {"job_id": job_id},
                {"$set": update_data},
                upsert=True
            )

            logger.info(f"Mission{job_id}Metadata updated")
            return True
        except Exception as e:
            logger.error(f"Other Organiser{job_id}Metadata failed:{e}")
            return False


#Examples of global services
_scheduler_service: Optional[SchedulerService] = None
_scheduler_instance: Optional[AsyncIOScheduler] = None


def set_scheduler_instance(scheduler: AsyncIOScheduler):
    """Setup Scheduler instance

    Args:
        Scheduler: Example of APScheduler Scheduler
    """
    global _scheduler_instance
    _scheduler_instance = scheduler
    logger.info("The instance of the scheduler has been set")


def get_scheduler_service() -> SchedulerService:
    """Example of accessing scheduler service

    Returns:
        Scheduler service instance
    """
    global _scheduler_service, _scheduler_instance

    if _scheduler_instance is None:
        raise RuntimeError("调度器实例未设置，请先调用 set_scheduler_instance()")

    if _scheduler_service is None:
        _scheduler_service = SchedulerService(_scheduler_instance)
        logger.info("An instance of a scheduler service has been created")

    return _scheduler_service


async def update_job_progress(
    job_id: str,
    progress: int,
    message: str = None,
    current_item: str = None,
    total_items: int = None,
    processed_items: int = None
):
    """Update on progress in mandate implementation (for internal call in time)

    Args:
        Job id: Task ID
        Progress: percentage of progress (0-100)
        message: progress message
        current item: Current processing
        Total items: total
        Processed items: processed
    """
    try:
        from pymongo import MongoClient
        from app.core.config import settings

        #Use sync client to avoid recurring conflict of events
        sync_client = MongoClient(settings.MONGO_URI)
        sync_db = sync_client[settings.MONGO_DB]

        #Find Recent Implementation Records
        latest_execution = sync_db.scheduler_executions.find_one(
            {"job_id": job_id, "status": {"$in": ["running", "success", "failed"]}},
            sort=[("timestamp", -1)]
        )

        if latest_execution:
            #Check for cancellation requests
            if latest_execution.get("cancel_requested"):
                sync_client.close()
                logger.warning(f"Mission{job_id}Request for cancellation received.")
                raise TaskCancelledException(f"任务 {job_id} 已被用户取消")

            #Update existing records
            update_data = {
                "progress": progress,
                "status": "running",
                "updated_at": get_utc8_now()
            }

            if message:
                update_data["progress_message"] = message
            if current_item:
                update_data["current_item"] = current_item
            if total_items is not None:
                update_data["total_items"] = total_items
            if processed_items is not None:
                update_data["processed_items"] = processed_items

            sync_db.scheduler_executions.update_one(
                {"_id": latest_execution["_id"]},
                {"$set": update_data}
            )
        else:
            #Create new execution record (task just started)
            from apscheduler.schedulers.asyncio import AsyncIOScheduler

            #Fetch Task Name
            job_name = job_id
            if _scheduler_instance:
                job = _scheduler_instance.get_job(job_id)
                if job:
                    job_name = job.name

            execution_record = {
                "job_id": job_id,
                "job_name": job_name,
                "status": "running",
                "progress": progress,
                "scheduled_time": get_utc8_now(),
                "timestamp": get_utc8_now()
            }

            if message:
                execution_record["progress_message"] = message
            if current_item:
                execution_record["current_item"] = current_item
            if total_items is not None:
                execution_record["total_items"] = total_items
            if processed_items is not None:
                execution_record["processed_items"] = processed_items

            sync_db.scheduler_executions.insert_one(execution_record)

        sync_client.close()

    except Exception as e:
        logger.error(f"The mission has failed:{e}")

