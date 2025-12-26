"""Operation log data model
"""

from datetime import datetime
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field, field_serializer
from bson import ObjectId


class OperationLogCreate(BaseModel):
    """Create Operations Log Request"""
    action_type: str = Field(..., description="操作类型")
    action: str = Field(..., description="操作描述")
    details: Optional[Dict[str, Any]] = Field(None, description="详细信息")
    success: bool = Field(True, description="是否成功")
    error_message: Optional[str] = Field(None, description="错误信息")
    duration_ms: Optional[int] = Field(None, description="操作耗时(毫秒)")
    ip_address: Optional[str] = Field(None, description="IP地址")
    user_agent: Optional[str] = Field(None, description="用户代理")
    session_id: Optional[str] = Field(None, description="会话ID")


class OperationLogResponse(BaseModel):
    """Operation log response"""
    id: str = Field(..., description="日志ID")
    user_id: str = Field(..., description="用户ID")
    username: str = Field(..., description="用户名")
    action_type: str = Field(..., description="操作类型")
    action: str = Field(..., description="操作描述")
    details: Optional[Dict[str, Any]] = Field(None, description="详细信息")
    success: bool = Field(..., description="是否成功")
    error_message: Optional[str] = Field(None, description="错误信息")
    duration_ms: Optional[int] = Field(None, description="操作耗时(毫秒)")
    ip_address: Optional[str] = Field(None, description="IP地址")
    user_agent: Optional[str] = Field(None, description="用户代理")
    session_id: Optional[str] = Field(None, description="会话ID")
    timestamp: datetime = Field(..., description="操作时间")
    created_at: datetime = Field(..., description="创建时间")

    @field_serializer('timestamp', 'created_at')
    def serialize_datetime(self, dt: datetime, _info) -> Optional[str]:
        """Sequenced datetime in ISO 8601 format, retaining time zone information"""
        if dt:
            return dt.isoformat()
        return None


class OperationLogQuery(BaseModel):
    """Operation log query parameters"""
    page: int = Field(1, ge=1, description="页码")
    page_size: int = Field(20, ge=1, le=100, description="每页数量")
    start_date: Optional[str] = Field(None, description="开始日期")
    end_date: Optional[str] = Field(None, description="结束日期")
    action_type: Optional[str] = Field(None, description="操作类型")
    success: Optional[bool] = Field(None, description="是否成功")
    keyword: Optional[str] = Field(None, description="关键词搜索")
    user_id: Optional[str] = Field(None, description="用户ID")


class OperationLogListResponse(BaseModel):
    """Operation Log List Response"""
    success: bool = Field(True, description="是否成功")
    data: Dict[str, Any] = Field(..., description="响应数据")
    message: str = Field("操作成功", description="响应消息")


class OperationLogStats(BaseModel):
    """Operation log statistics"""
    total_logs: int = Field(..., description="总日志数")
    success_logs: int = Field(..., description="成功日志数")
    failed_logs: int = Field(..., description="失败日志数")
    success_rate: float = Field(..., description="成功率")
    action_type_distribution: Dict[str, int] = Field(..., description="操作类型分布")
    hourly_distribution: List[Dict[str, Any]] = Field(..., description="小时分布")


class OperationLogStatsResponse(BaseModel):
    """Operation log statistical response"""
    success: bool = Field(True, description="是否成功")
    data: OperationLogStats = Field(..., description="统计数据")
    message: str = Field("获取统计信息成功", description="响应消息")


class ClearLogsRequest(BaseModel):
    """Clear Log Request"""
    days: Optional[int] = Field(None, description="保留最近N天的日志，不传则清空所有")
    action_type: Optional[str] = Field(None, description="只清空指定类型的日志")


class ClearLogsResponse(BaseModel):
    """Clear Log Response"""
    success: bool = Field(True, description="是否成功")
    data: Dict[str, Any] = Field(..., description="清空结果")
    message: str = Field("清空日志成功", description="响应消息")


#Operating type constant
class ActionType:
    """Operating type constant"""
    STOCK_ANALYSIS = "stock_analysis"
    CONFIG_MANAGEMENT = "config_management"
    CACHE_OPERATION = "cache_operation"
    DATA_IMPORT = "data_import"
    DATA_EXPORT = "data_export"
    SYSTEM_SETTINGS = "system_settings"
    USER_LOGIN = "user_login"
    USER_LOGOUT = "user_logout"
    USER_MANAGEMENT = "user_management"  #Add User Management Operation Type 🔧
    DATABASE_OPERATION = "database_operation"
    SCREENING = "screening"
    REPORT_GENERATION = "report_generation"


#Operation Type Map
ACTION_TYPE_NAMES = {
    ActionType.STOCK_ANALYSIS: "股票分析",
    ActionType.CONFIG_MANAGEMENT: "配置管理",
    ActionType.CACHE_OPERATION: "缓存操作",
    ActionType.DATA_IMPORT: "数据导入",
    ActionType.DATA_EXPORT: "数据导出",
    ActionType.SYSTEM_SETTINGS: "系统设置",
    ActionType.USER_LOGIN: "用户登录",
    ActionType.USER_LOGOUT: "用户登出",
    ActionType.USER_MANAGEMENT: "用户管理",  #Add user management type name 🔧
    ActionType.DATABASE_OPERATION: "数据库操作",
    ActionType.SCREENING: "股票筛选",
    ActionType.REPORT_GENERATION: "报告生成",
}


def convert_objectid_to_str(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Convert ObjectId from MongoDB document to string"""
    if doc and "_id" in doc:
        doc["id"] = str(doc["_id"])
        del doc["_id"]
    return doc
