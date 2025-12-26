"""User Data Model
"""

from datetime import datetime, timezone
from app.utils.timezone import now_tz
from typing import Optional, Dict, Any, Annotated, List
from pydantic import BaseModel, Field, BeforeValidator, PlainSerializer, ConfigDict, field_serializer
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import core_schema
from bson import ObjectId


def validate_object_id(v: Any) -> ObjectId:
    """AuthenticateObjectID"""
    if isinstance(v, ObjectId):
        return v
    if isinstance(v, str):
        if ObjectId.is_valid(v):
            return ObjectId(v)
    raise ValueError("Invalid ObjectId")


def serialize_object_id(v: ObjectId) -> str:
    """SequencingObjectID as string"""
    return str(v)


#Create Custom ObjectId Type
PyObjectId = Annotated[
    ObjectId,
    BeforeValidator(validate_object_id),
    PlainSerializer(serialize_object_id, return_type=str),
]


class UserPreferences(BaseModel):
    """User preferences"""
    #Analysis preferences
    default_market: str = "A股"
    default_depth: str = "3"  #Level 1-5, level 3, standard analysis (recommended)
    default_analysts: List[str] = Field(default_factory=lambda: ["市场分析师", "基本面分析师"])
    auto_refresh: bool = True
    refresh_interval: int = 30  #sec

    #Appearance Settings
    ui_theme: str = "light"
    sidebar_width: int = 240

    #Languages and regions
    language: str = "zh-CN"

    #Notification Settings
    notifications_enabled: bool = True
    email_notifications: bool = False
    desktop_notifications: bool = True
    analysis_complete_notification: bool = True
    system_maintenance_notification: bool = True


class FavoriteStock(BaseModel):
    """Select Unit Information"""
    stock_code: str = Field(..., description="股票代码")
    stock_name: str = Field(..., description="股票名称")
    market: str = Field(..., description="市场类型")
    added_at: datetime = Field(default_factory=now_tz, description="添加时间")
    tags: List[str] = Field(default_factory=list, description="用户标签")
    notes: str = Field(default="", description="用户备注")
    alert_price_high: Optional[float] = Field(None, description="价格上限提醒")
    alert_price_low: Optional[float] = Field(None, description="价格下限提醒")


class User(BaseModel):
    """User Model"""
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    username: str = Field(..., min_length=3, max_length=50)
    email: str = Field(..., pattern=r'^[^@]+@[^@]+\.[^@]+$')
    hashed_password: str
    is_active: bool = True
    is_verified: bool = False
    is_admin: bool = False
    created_at: datetime = Field(default_factory=now_tz)
    updated_at: datetime = Field(default_factory=now_tz)
    last_login: Optional[datetime] = None
    preferences: UserPreferences = Field(default_factory=UserPreferences)
    
    #Quotas and restrictions
    daily_quota: int = 1000
    concurrent_limit: int = 3
    
    #Statistical information
    total_analyses: int = 0
    successful_analyses: int = 0
    failed_analyses: int = 0

    #Select Units
    favorite_stocks: List[FavoriteStock] = Field(default_factory=list, description="用户自选股列表")
    
    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)


class UserCreate(BaseModel):
    """Create user request model"""
    username: str = Field(..., min_length=3, max_length=50)
    email: str = Field(..., pattern=r'^[^@]+@[^@]+\.[^@]+$')
    password: str = Field(..., min_length=6, max_length=100)


class UserUpdate(BaseModel):
    """Update user request model"""
    email: Optional[str] = Field(None, pattern=r'^[^@]+@[^@]+\.[^@]+$')
    preferences: Optional[UserPreferences] = None
    daily_quota: Optional[int] = None
    concurrent_limit: Optional[int] = None


class UserResponse(BaseModel):
    """User Response Model"""
    id: str
    username: str
    email: str
    is_active: bool
    is_verified: bool
    created_at: datetime
    last_login: Optional[datetime]
    preferences: UserPreferences
    daily_quota: int
    concurrent_limit: int
    total_analyses: int
    successful_analyses: int
    failed_analyses: int

    @field_serializer('created_at', 'last_login')
    def serialize_datetime(self, dt: Optional[datetime], _info) -> Optional[str]:
        """Sequenced datetime in ISO 8601 format, retaining time zone information"""
        if dt:
            return dt.isoformat()
        return None


class UserLogin(BaseModel):
    """User login request model"""
    username: str
    password: str


class UserSession(BaseModel):
    """User Session Model"""
    session_id: str
    user_id: str
    created_at: datetime
    expires_at: datetime
    last_activity: datetime
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None

    @field_serializer('created_at', 'expires_at', 'last_activity')
    def serialize_datetime(self, dt: Optional[datetime], _info) -> Optional[str]:
        """Sequenced datetime in ISO 8601 format, retaining time zone information"""
        if dt:
            return dt.isoformat()
        return None


class TokenResponse(BaseModel):
    """Token Response Model"""
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    refresh_token: Optional[str] = None
    user: UserResponse
