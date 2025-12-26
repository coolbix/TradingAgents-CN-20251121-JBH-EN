"""Memory Status Manager
Akin to analysis-engine, providing rapid status reading Write
"""

import asyncio
import threading
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)

class TaskStatus(Enum):
    """Task status count"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class TaskState:
    """Task status data class"""
    task_id: str
    user_id: str
    stock_code: str
    status: TaskStatus
    stock_name: Optional[str] = None
    progress: int = 0
    message: str = ""
    current_step: str = ""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    result_data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    
    #Analysis Parameters
    parameters: Optional[Dict[str, Any]] = None

    #Performance indicators
    execution_time: Optional[float] = None
    tokens_used: Optional[int] = None
    estimated_duration: Optional[float] = None  #Total estimated duration (sec)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to Dictionary Format"""
        data = asdict(self)
        #Process Enumeration Type
        data['status'] = self.status.value
        #Process Time Format
        if self.start_time:
            data['start_time'] = self.start_time.isoformat()
        if self.end_time:
            data['end_time'] = self.end_time.isoformat()

        #Add time information for real time calculations
        if self.start_time:
            if self.end_time:
                #Task completed, use of final execution time
                data['elapsed_time'] = self.execution_time or (self.end_time - self.start_time).total_seconds()
                data['remaining_time'] = 0
                data['estimated_total_time'] = data['elapsed_time']
            else:
                #Task in progress, real time calculation time taken
                from datetime import datetime
                elapsed_time = (datetime.now() - self.start_time).total_seconds()
                data['elapsed_time'] = elapsed_time

                #Calculate the estimated remaining time and total duration
                progress = self.progress / 100 if self.progress > 0 else 0

                #The estimated total length of time at task creation, if not the default value (5 minutes)
                estimated_total = self.estimated_duration if self.estimated_duration else 300

                if progress >= 1.0:
                    #Task completed
                    data['remaining_time'] = 0
                    data['estimated_total_time'] = elapsed_time
                else:
                    #Use the estimated total duration (fixed)
                    data['estimated_total_time'] = estimated_total
                    #Projected balance = total estimated time - time taken
                    data['remaining_time'] = max(0, estimated_total - elapsed_time)
        else:
            data['elapsed_time'] = 0
            data['remaining_time'] = 300  #Default 5 Minutes
            data['estimated_total_time'] = 300

        return data

class MemoryStateManager:
    """Memory Status Manager"""

    def __init__(self):
        self._tasks: Dict[str, TaskState] = {}
        #Use threading. Lock instead of asyncio. Lock to avoid a cycle of incidents.
        #When performing analysis in an online pool, create a new cycle of events, asyncio.Lock will cause
        #"is found to a different event loop"
        self._lock = threading.Lock()
        self._websocket_manager = None

    def set_websocket_manager(self, websocket_manager):
        """Setup WebSocket Manager"""
        self._websocket_manager = websocket_manager
        
    async def create_task(
        self,
        task_id: str,
        user_id: str,
        stock_code: str,
        parameters: Optional[Dict[str, Any]] = None,
        stock_name: Optional[str] = None,
    ) -> TaskState:
        """Other Organiser"""
        with self._lock:
            #Calculate the total estimated time
            estimated_duration = self._calculate_estimated_duration(parameters or {})

            task_state = TaskState(
                task_id=task_id,
                user_id=user_id,
                stock_code=stock_code,
                stock_name=stock_name,
                status=TaskStatus.PENDING,
                start_time=datetime.now(),
                parameters=parameters or {},
                estimated_duration=estimated_duration,
                message="任务已创建，等待执行..."
            )
            self._tasks[task_id] = task_state
            logger.info(f"Other Organiser{task_id}")
            logger.info(f"Total estimated duration:{estimated_duration:.1f}sec ({estimated_duration/60:.1f}minutes)")
            logger.info(f"Current RAM number of tasks:{len(self._tasks)}")
            logger.info(f"Memory Manager Example ID:{id(self)}")
            return task_state

    def _calculate_estimated_duration(self, parameters: Dict[str, Any]) -> float:
        """Estimated total length based on analytical parameters (sec)"""
        #Basic time (sec) - Environmental readiness, configuration, etc.
        base_time = 60

        #Get analytical parameters
        research_depth = parameters.get('research_depth', '标准')
        selected_analysts = parameters.get('selected_analysts', [])
        llm_provider = parameters.get('llm_provider', 'dashscope')

        #Research depth map
        depth_map = {"快速": 1, "标准": 2, "深度": 3}
        d = depth_map.get(research_depth, 2)

        #Base time per analyst (based on real test data)
        analyst_base_time = {
            1: 180,  #Rapid analysis: approximately 3 minutes per analyst
            2: 360,  #Standard analysis: approximately 6 minutes per analyst
            3: 600   #Depth analysis: approximately 10 minutes per analyst
        }.get(d, 360)

        analyst_time = len(selected_analysts) * analyst_base_time

        #Model speed effect (based on actual tests)
        model_multiplier = {
            'dashscope': 1.0,  #Alibri's speed is right.
            'deepseek': 0.7,   #DeepSeek is faster.
            'google': 1.3      #Google's slow.
        }.get(llm_provider, 1.0)

        #Study depth additional effects (tool call complexity)
        depth_multiplier = {
            1: 0.8,  #Quick analysis, fewer tools to call
            2: 1.0,  #Standard analysis, standard tool call
            3: 1.3   #Depth analysis, more tools to call and reason
        }.get(d, 1.0)

        total_time = (base_time + analyst_time) * model_multiplier * depth_multiplier
        return total_time

    async def update_task_status(
        self,
        task_id: str,
        status: TaskStatus,
        progress: Optional[int] = None,
        message: Optional[str] = None,
        current_step: Optional[str] = None,
        result_data: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None
    ) -> bool:
        """Update Task Status"""
        with self._lock:
            if task_id not in self._tasks:
                logger.warning(f"Mission does not exist:{task_id}")
                return False
            
            task = self._tasks[task_id]
            task.status = status
            
            if progress is not None:
                task.progress = progress
            if message is not None:
                task.message = message
            if current_step is not None:
                task.current_step = current_step
            if result_data is not None:
                #Debug: Check saved to memory
                logger.info(f"[EMORY] Save result data to memory:{task_id}")
                logger.info(f"[EMORY] result data:{list(result_data.keys()) if result_data else 'None'}")
                logger.info(f"[EMORY]{bool(result_data.get('decision')) if result_data else False}")
                if result_data and result_data.get('decision'):
                    logger.info(f"[EMORY] content:{result_data['decision']}")

                task.result_data = result_data
            if error_message is not None:
                task.error_message = error_message
                
            #Set the end time if the task is completed or failed
            if status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                task.end_time = datetime.now()
                if task.start_time:
                    task.execution_time = (task.end_time - task.start_time).total_seconds()
            
            logger.info(f"Update mission status:{task_id} -> {status.value} ({progress}%)")

            #Send status update to WebSocket
            if self._websocket_manager:
                try:
                    progress_update = {
                        "type": "progress_update",
                        "task_id": task_id,
                        "status": status.value,
                        "progress": task.progress,
                        "message": task.message,
                        "current_step": task.current_step,
                        "timestamp": datetime.now().isoformat()
                    }
                    #Step forward, no waiting for completion
                    asyncio.create_task(
                        self._websocket_manager.send_progress_update(task_id, progress_update)
                    )
                except Exception as e:
                    logger.warning(f"WebSocket failed:{e}")

            return True
    
    async def get_task(self, task_id: str) -> Optional[TaskState]:
        """Get Task Status"""
        with self._lock:
            logger.debug(f"Other Organiser{task_id}")
            logger.debug(f"Current RAM number of tasks:{len(self._tasks)}")
            logger.debug(f"List of tasks in memory:{list(self._tasks.keys())}")
            task = self._tasks.get(task_id)
            if task:
                logger.debug(f"Found a mission:{task_id}")
            else:
                logger.debug(f"No missions found:{task_id}")
            return task
    
    async def get_task_dict(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get Task Status (Dictionary Format)"""
        task = await self.get_task(task_id)
        return task.to_dict() if task else None
    
    async def list_all_tasks(
        self,
        status: Optional[TaskStatus] = None,
        limit: int = 20,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Other Organiser"""
        with self._lock:
            tasks = []
            for task in self._tasks.values():
                if status is None or task.status == status:
                    item = task.to_dict()
                    #Compatible front field
                    if 'stock_name' not in item or not item.get('stock_name'):
                        item['stock_name'] = None
                    tasks.append(item)

            #Sort in reverse at start time
            tasks.sort(key=lambda x: x.get('start_time', ''), reverse=True)

            #Page Break
            return tasks[offset:offset + limit]

    async def list_user_tasks(
        self,
        user_id: str,
        status: Optional[TaskStatus] = None,
        limit: int = 20,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Other Organiser"""
        with self._lock:
            tasks = []
            for task in self._tasks.values():
                if task.user_id == user_id:
                    if status is None or task.status == status:
                        item = task.to_dict()
                        #Compatible front field
                        if 'stock_name' not in item or not item.get('stock_name'):
                            item['stock_name'] = None
                        tasks.append(item)

            #Sort in reverse at start time
            tasks.sort(key=lambda x: x.get('start_time', ''), reverse=True)

            #Page Break
            return tasks[offset:offset + limit]
    
    async def delete_task(self, task_id: str) -> bool:
        """Delete Task"""
        with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                logger.info(f"Delete mission:{task_id}")
                return True
            return False
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Access to statistical information"""
        with self._lock:
            total_tasks = len(self._tasks)
            status_counts = {}
            
            for task in self._tasks.values():
                status = task.status.value
                status_counts[status] = status_counts.get(status, 0) + 1
            
            return {
                "total_tasks": total_tasks,
                "status_distribution": status_counts,
                "running_tasks": status_counts.get("running", 0),
                "completed_tasks": status_counts.get("completed", 0),
                "failed_tasks": status_counts.get("failed", 0)
            }
    
    async def cleanup_old_tasks(self, max_age_hours: int = 24) -> int:
        """Clear old tasks"""
        with self._lock:
            cutoff_time = datetime.now().timestamp() - (max_age_hours * 3600)
            tasks_to_remove = []

            for task_id, task in self._tasks.items():
                if task.start_time and task.start_time.timestamp() < cutoff_time:
                    if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                        tasks_to_remove.append(task_id)

            for task_id in tasks_to_remove:
                del self._tasks[task_id]

            logger.info(f"It's clean.{len(tasks_to_remove)}An old assignment.")
            return len(tasks_to_remove)

    async def cleanup_zombie_tasks(self, max_running_hours: int = 2) -> int:
        """Clean-up of zombie missions (long running missions)

Args:
max running hours: Maximum running time (hours), longer than which running task will be marked as failure

Returns:
Number of tasks cleared
"""
        with self._lock:
            cutoff_time = datetime.now().timestamp() - (max_running_hours * 3600)
            zombie_tasks = []

            for task_id, task in self._tasks.items():
                #Check if it's a long run job
                if task.status in [TaskStatus.RUNNING, TaskStatus.PENDING]:
                    if task.start_time and task.start_time.timestamp() < cutoff_time:
                        zombie_tasks.append(task_id)

            #Mark Zombie Task as Failed
            for task_id in zombie_tasks:
                task = self._tasks[task_id]
                task.status = TaskStatus.FAILED
                task.end_time = datetime.now()
                task.error_message = f"任务超时（运行时间超过 {max_running_hours} 小时）"
                task.message = "任务已超时，自动标记为失败"
                task.progress = 0

                if task.start_time:
                    task.execution_time = (task.end_time - task.start_time).total_seconds()

                logger.warning(f"The zombie mission has been marked as a failure:{task_id}(Run time:{task.execution_time:.1f}sec)")

            if zombie_tasks:
                logger.info(f"It's clean.{len(zombie_tasks)}A zombie mission.")

            return len(zombie_tasks)

    async def remove_task(self, task_id: str) -> bool:
        """Remove Tasks From Memory

Args:
task id: task ID

Returns:
Delete successfully
"""
        with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                logger.info(f"The task 🗑️ was deleted from the memory:{task_id}")
                return True
            else:
                logger.warning(f"The mission does not exist in memory:{task_id}")
                return False

#Global Examples
_memory_state_manager = None

def get_memory_state_manager() -> MemoryStateManager:
    """Fetch memory status manager instance"""
    global _memory_state_manager
    if _memory_state_manager is None:
        _memory_state_manager = MemoryStateManager()
    return _memory_state_manager
