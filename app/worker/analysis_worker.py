"""Analyse TaskWorker Process
Analysis tasks in the consumption queue, calling Trading Agencies for stock analysis
"""

import asyncio
import logging
import signal
import sys
import uuid
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

#Add root directory to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from app.services.queue_service import get_queue_service
from app.services.analysis_service import get_analysis_service
from app.core.database import init_database, close_database
from app.core.redis_client import init_redis, close_redis
from app.core.config import settings
from app.models.analysis import AnalysisTask, AnalysisParameters
from app.services.config_provider import provider as config_provider
from app.services.queue import DEFAULT_USER_CONCURRENT_LIMIT, GLOBAL_CONCURRENT_LIMIT, VISIBILITY_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)


class AnalysisWorker:
    """Analysis TaskWorker Category"""

    def __init__(self, worker_id: Optional[str] = None):
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.queue_service = None
        self.running = False
        self.current_task = None

        #Configure Parameters (can be covered by system settings)
        self.heartbeat_interval = int(getattr(settings, 'WORKER_HEARTBEAT_INTERVAL', 30))
        self.max_retries = int(getattr(settings, 'QUEUE_MAX_RETRIES', 3))
        self.poll_interval = float(getattr(settings, 'QUEUE_POLL_INTERVAL_SECONDS', 1))  #Queue Query interval (seconds)
        self.cleanup_interval = float(getattr(settings, 'QUEUE_CLEANUP_INTERVAL_SECONDS', 60))

        #Registered signal processor
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Signal processor, graceful shutdown."""
        logger.info(f"Roger that.{signum}Ready to shut down Walker...")
        self.running = False

    async def start(self):
        """Start Worker"""
        try:
            logger.info(f"🚀 Start analysis of Worker:{self.worker_id}")

            #Initialize database connection
            await init_database()
            await init_redis()

            #Read System Settings (ENV Priority DB)
            try:
                effective_settings = await config_provider.get_effective_system_settings()
            except Exception:
                effective_settings = {}

            #Get Queue Service
            self.queue_service = get_queue_service()

            self.running = True

            #Apply Queue Parallel / Overtime Configuration + Worker/Query Parameter
            try:
                self.queue_service.user_concurrent_limit = int(effective_settings.get("max_concurrent_tasks", DEFAULT_USER_CONCURRENT_LIMIT))
                self.queue_service.global_concurrent_limit = int(effective_settings.get("max_concurrent_tasks", GLOBAL_CONCURRENT_LIMIT))
                self.queue_service.visibility_timeout = int(effective_settings.get("default_analysis_timeout", VISIBILITY_TIMEOUT_SECONDS))
                # Worker intervals
                self.heartbeat_interval = int(effective_settings.get("worker_heartbeat_interval_seconds", self.heartbeat_interval))
                self.poll_interval = float(effective_settings.get("queue_poll_interval_seconds", self.poll_interval))
                self.cleanup_interval = float(effective_settings.get("queue_cleanup_interval_seconds", self.cleanup_interval))
            except Exception:
                pass
            #Start a heartbeat.
            heartbeat_task = asyncio.create_task(self._heartbeat_loop())

            #Start cleanup mission.
            cleanup_task = asyncio.create_task(self._cleanup_loop())

            #Main Work Cycle
            await self._work_loop()

            #Cancel Backstage Task
            heartbeat_task.cancel()
            cleanup_task.cancel()

            try:
                await heartbeat_task
                await cleanup_task
            except asyncio.CancelledError:
                pass

        except Exception as e:
            logger.error(f"Starter failed:{e}")
            raise
        finally:
            await self._cleanup()

    async def _work_loop(self):
        """Main Work Cycle"""
        logger.info(f"✅ Worker {self.worker_id}Get to work.")

        while self.running:
            try:
                #Can not open message
                task_data = await self.queue_service.dequeue_task(self.worker_id)

                if task_data:
                    await self._process_task(task_data)
                else:
                    #No mission. Short hibernation.
                    await asyncio.sleep(self.poll_interval)

            except Exception as e:
                logger.error(f"Work cycle anomaly:{e}")
                await asyncio.sleep(5)  #Wait five seconds after the anomaly.

        logger.info(f"🔄 Worker {self.worker_id}End of loop")

    async def _process_task(self, task_data: Dict[str, Any]):
        """Deal with individual tasks"""
        task_id = task_data.get("id")
        stock_code = task_data.get("symbol")
        user_id = task_data.get("user")

        logger.info(f"Let's do this.{task_id} - {stock_code}")

        self.current_task = task_id
        success = False

        try:
            #Build parsing task objects
            parameters_dict = task_data.get("parameters", {})
            if isinstance(parameters_dict, str):
                import json
                parameters_dict = json.loads(parameters_dict)

            parameters = AnalysisParameters(**parameters_dict)

            task = AnalysisTask(
                task_id=task_id,
                user_id=user_id,
                stock_code=stock_code,
                batch_id=task_data.get("batch_id"),
                parameters=parameters
            )

            #Implementation analysis
            result = await get_analysis_service().execute_analysis_task(
                task,
                progress_callback=self._progress_callback
            )

            success = True
            logger.info(f"Mission accomplished:{task_id}- Time-consuming:{result.execution_time:.2f}sec")

        except Exception as e:
            logger.error(f"Mission failure:{task_id} - {e}")
            logger.error(traceback.format_exc())

        finally:
            #Confirm mission complete.
            try:
                await self.queue_service.ack_task(task_id, success)
            except Exception as e:
                logger.error(f"Confirm mission failure:{task_id} - {e}")

            self.current_task = None

    def _progress_callback(self, progress: int, message: str):
        """Progress Return Function"""
        logger.debug(f"Task progress{self.current_task}: {progress}% - {message}")

    async def _heartbeat_loop(self):
        """Heart cycle"""
        while self.running:
            try:
                await self._send_heartbeat()
                await asyncio.sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heart rate:{e}")
                await asyncio.sleep(5)

    async def _send_heartbeat(self):
        """Send a heartbeat"""
        try:
            from app.core.redis_client import get_redis_service
            redis_service = get_redis_service()

            heartbeat_data = {
                "worker_id": self.worker_id,
                "timestamp": datetime.utcnow().isoformat(),
                "current_task": self.current_task,
                "status": "active" if self.running else "stopping"
            }

            heartbeat_key = f"worker:{self.worker_id}:heartbeat"
            await redis_service.set_json(heartbeat_key, heartbeat_data, ttl=self.heartbeat_interval * 2)

        except Exception as e:
            logger.error(f"Sending a heartbeat failed:{e}")

    async def _cleanup_loop(self):
        """Clean up the cycle and regularly clean up obsolete tasks"""
        while self.running:
            try:
                await asyncio.sleep(self.cleanup_interval)  #Cleanup interval (sec), matching
                if self.queue_service:
                    await self.queue_service.cleanup_expired_tasks()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleaning mission anomaly:{e}")

    async def _cleanup(self):
        """Cleaning up resources"""
        logger.info(f"Clean up the Worker resources:{self.worker_id}")

        try:
            #Clean up the heartbeat.
            from app.core.redis_client import get_redis_service
            redis_service = get_redis_service()
            heartbeat_key = f"worker:{self.worker_id}:heartbeat"
            await redis_service.redis.delete(heartbeat_key)
        except Exception as e:
            logger.error(f"Cleanup of heartbeat record failed:{e}")

        try:
            #Close database connection
            await close_database()
            await close_redis()
        except Exception as e:
            logger.error(f"Failed to close database connection:{e}")


async def main():
    """Main Functions"""
    #Set Log
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    #Create and start Worker
    worker = AnalysisWorker()

    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("We've got a break.")
    except Exception as e:
        logger.error(f"Worker exits abnormally:{e}")
        sys.exit(1)

    logger.info("Walker's safely out.")


if __name__ == "__main__":
    asyncio.run(main())
