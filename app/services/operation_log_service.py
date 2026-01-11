"""Operation log service
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from bson import ObjectId

from app.core.database import get_mongo_db_async
from app.models.operationlog_models import (
    OperationLogCreate,
    OperationLogResponse,
    OperationLogQuery,
    OperationLogStats,
    convert_objectid_to_str,
    ActionType
)
from app.utils.timezone import now_tz

logger = logging.getLogger("webapi")


class OperationLogService:
    """Operation log service"""
    
    def __init__(self):
        self.collection_name = "operation_logs"
    
    async def create_log(
        self,
        user_id: str,
        username: str,
        log_data: OperationLogCreate,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None
    ) -> str:
        """Create Operations Log"""
        try:
            db = get_mongo_db_async()

            #Build Log Document
            #🔥 With live data (without time zone information), MongoDB will be stored as it is and will not be converted to UTC
            current_time = now_tz().replace(tzinfo=None)  #Remove time zone information, keep local time values
            log_doc = {
                "user_id": user_id,
                "username": username,
                "action_type": log_data.action_type,
                "action": log_data.action,
                "details": log_data.details or {},
                "success": log_data.success,
                "error_message": log_data.error_message,
                "duration_ms": log_data.duration_ms,
                "ip_address": ip_address or log_data.ip_address,
                "user_agent": user_agent or log_data.user_agent,
                "session_id": log_data.session_id,
                "timestamp": current_time,  #give date, MongoDB store as it is
                "created_at": current_time  #give date, MongoDB store as it is
            }
            
            #Insert Database
            result = await db[self.collection_name].insert_one(log_doc)
            
            logger.info(f"The operation log has been recorded:{username} - {log_data.action}")
            return str(result.inserted_id)
            
        except Exception as e:
            logger.error(f"Failed to create operation log:{e}")
            raise Exception(f"创建操作日志失败: {str(e)}")
    
    async def get_logs(self, query: OperationLogQuery) -> Tuple[List[OperationLogResponse], int]:
        """Get Operations Log List"""
        try:
            db = get_mongo_db_async()
            
            #Build query conditions
            filter_query = {}
            
            #Time Range Filter
            if query.start_date or query.end_date:
                time_filter = {}
                if query.start_date:
                    #Process the time zone, remove the Z suffix and resolve it directly
                    start_str = query.start_date.replace('Z', '')
                    time_filter["$gte"] = datetime.fromisoformat(start_str)
                if query.end_date:
                    #Process the time zone, remove the Z suffix and resolve it directly
                    end_str = query.end_date.replace('Z', '')
                    time_filter["$lte"] = datetime.fromisoformat(end_str)
                filter_query["timestamp"] = time_filter
            
            #Operation type filter
            if query.action_type:
                filter_query["action_type"] = query.action_type
            
            #Successful Status Filter
            if query.success is not None:
                filter_query["success"] = query.success
            
            #User Filter
            if query.user_id:
                filter_query["user_id"] = query.user_id
            
            #Keyword Search
            if query.keyword:
                filter_query["$or"] = [
                    {"action": {"$regex": query.keyword, "$options": "i"}},
                    {"username": {"$regex": query.keyword, "$options": "i"}},
                    {"details.stock_symbol": {"$regex": query.keyword, "$options": "i"}}
                ]
            
            #Total acquisitions
            total = await db[self.collection_name].count_documents(filter_query)
            
            #Page Break Query
            skip = (query.page - 1) * query.page_size
            cursor = db[self.collection_name].find(filter_query).sort("timestamp", -1).skip(skip).limit(query.page_size)
            
            logs = []
            async for doc in cursor:
                doc = convert_objectid_to_str(doc)
                logs.append(OperationLogResponse(**doc))

            logger.info(f"Access operation log: Total ={total}returns ={len(logs)}")
            return logs, total
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            raise Exception(f"获取操作日志失败: {str(e)}")
    
    async def get_stats(self, days: int = 30) -> OperationLogStats:
        """Get Operations Log Statistics"""
        try:
            db = get_mongo_db_async()
            
            #Time frame (using Chinese time zone)
            start_date = now_tz() - timedelta(days=days)
            time_filter = {"timestamp": {"$gte": start_date}}
            
            #Basic statistics
            total_logs = await db[self.collection_name].count_documents(time_filter)
            success_logs = await db[self.collection_name].count_documents({**time_filter, "success": True})
            failed_logs = total_logs - success_logs
            success_rate = (success_logs / total_logs * 100) if total_logs > 0 else 0
            
            #Operation type distribution
            action_type_pipeline = [
                {"$match": time_filter},
                {"$group": {"_id": "$action_type", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            action_type_cursor = db[self.collection_name].aggregate(action_type_pipeline)
            action_type_distribution = {}
            async for doc in action_type_cursor:
                action_type_distribution[doc["_id"]] = doc["count"]
            
            #Hourly distribution statistics
            hourly_pipeline = [
                {"$match": time_filter},
                {
                    "$group": {
                        "_id": {"$hour": "$timestamp"},
                        "count": {"$sum": 1}
                    }
                },
                {"$sort": {"_id": 1}}
            ]
            hourly_cursor = db[self.collection_name].aggregate(hourly_pipeline)
            hourly_distribution = []
            hourly_data = {i: 0 for i in range(24)}  #Initialization 24 hours
            
            async for doc in hourly_cursor:
                hourly_data[doc["_id"]] = doc["count"]
            
            for hour, count in hourly_data.items():
                hourly_distribution.append({
                    "hour": f"{hour:02d}:00",
                    "count": count
                })
            
            stats = OperationLogStats(
                total_logs=total_logs,
                success_logs=success_logs,
                failed_logs=failed_logs,
                success_rate=round(success_rate, 2),
                action_type_distribution=action_type_distribution,
                hourly_distribution=hourly_distribution
            )
            
            logger.info(f"Operation log statistics: Total ={total_logs}, success ={success_rate:.1f}%")
            return stats
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            raise Exception(f"获取操作日志统计失败: {str(e)}")
    
    async def clear_logs(self, days: Optional[int] = None, action_type: Optional[str] = None) -> Dict[str, Any]:
        """Empty Operations Log"""
        try:
            db = get_mongo_db_async()
            
            #Build Delete Condition
            delete_filter = {}
            
            if days is not None:
                #Delete N-Day Logs only
                cutoff_date = datetime.now() - timedelta(days=days)
                delete_filter["timestamp"] = {"$lt": cutoff_date}
            
            if action_type:
                #Remove only the specified type of log
                delete_filter["action_type"] = action_type
            
            #Execute Delete
            result = await db[self.collection_name].delete_many(delete_filter)
            
            logger.info(f"Clear operation log: deleted{result.deleted_count}Notes")
            
            return {
                "deleted_count": result.deleted_count,
                "filter": delete_filter
            }
            
        except Exception as e:
            logger.error(f"Clear operation log failed:{e}")
            raise Exception(f"清空操作日志失败: {str(e)}")
    
    async def get_log_by_id(self, log_id: str) -> Optional[OperationLogResponse]:
        """Get Operations Log from ID"""
        try:
            db = get_mongo_db_async()

            doc = await db[self.collection_name].find_one({"_id": ObjectId(log_id)})
            if not doc:
                return None

            doc = convert_objectid_to_str(doc)
            return OperationLogResponse(**doc)

        except Exception as e:
            logger.error(f"Failed to get operation log details:{e}")
            return None


#Examples of global services
_operation_log_service: Optional[OperationLogService] = None


def get_operation_log_service() -> OperationLogService:
    """Get Operations Log Service Examples"""
    global _operation_log_service
    if _operation_log_service is None:
        _operation_log_service = OperationLogService()
    return _operation_log_service


#Easy Functions
async def log_operation(
    user_id: str,
    username: str,
    action_type: str,
    action: str,
    details: Optional[Dict[str, Any]] = None,
    success: bool = True,
    error_message: Optional[str] = None,
    duration_ms: Optional[int] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
    session_id: Optional[str] = None
) -> str:
    """A simple function to record operations logs"""
    service = get_operation_log_service()
    log_data = OperationLogCreate(
        action_type=action_type,
        action=action,
        details=details,
        success=success,
        error_message=error_message,
        duration_ms=duration_ms,
        ip_address=ip_address,
        user_agent=user_agent,
        session_id=session_id
    )
    return await service.create_log(user_id, username, log_data, ip_address, user_agent)
