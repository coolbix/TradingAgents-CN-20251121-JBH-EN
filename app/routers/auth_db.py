"""Database-based authentication route - improved version
Alternative to the original configuration-based authentication mechanism
"""

import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Header, Request
from pydantic import BaseModel

from app.services.auth_service import AuthService
from app.services.user_service import user_service
from app.models.user import UserCreate, UserUpdate
from app.services.operation_log_service import log_operation
from app.models.operation_log import ActionType

#Try Import Log Manager
try:
    from tradingagents.utils.logging_manager import get_logger
except ImportError:
    #Use standard log if import failed
    import logging
    def get_logger(name: str) -> logging.Logger:
        return logging.getLogger(name)

logger = get_logger('auth_db')

#Harmonized Response Format
class ApiResponse(BaseModel):
    success: bool = True
    data: dict = {}
    message: str = ""

router = APIRouter()

class LoginRequest(BaseModel):
    username: str
    password: str

class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    user: dict

class RefreshTokenRequest(BaseModel):
    refresh_token: str

class RefreshTokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int

class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str

class ResetPasswordRequest(BaseModel):
    username: str
    new_password: str

class CreateUserRequest(BaseModel):
    username: str
    email: str
    password: str
    is_admin: bool = False

async def get_current_user(authorization: Optional[str] = Header(default=None)) -> dict:
    """Get Current User Information"""
    logger.debug(f"The authentication check begins.")
    logger.debug(f"📋 Authorization header: {authorization[:50] if authorization else 'None'}...")

    if not authorization:
        logger.warning("No Authorizehead.")
        raise HTTPException(status_code=401, detail="No authorization header")

    if not authorization.lower().startswith("bearer "):
        logger.warning(f"Autoration header error:{authorization[:20]}...")
        raise HTTPException(status_code=401, detail="Invalid authorization format")

    token = authorization.split(" ", 1)[1]
    logger.debug(f"Token length extracted:{len(token)}")
    logger.debug(f"Token top 20:{token[:20]}...")

    token_data = AuthService.verify_token(token)
    logger.debug(f"Token:{token_data is not None}")

    if not token_data:
        logger.warning("Token authentication failed")
        raise HTTPException(status_code=401, detail="Invalid token")

    #Get user information from the database
    user = await user_service.get_user_by_username(token_data.sub)
    if not user:
        logger.warning(f"The user does not exist:{token_data.sub}")
        raise HTTPException(status_code=401, detail="User not found")

    if not user.is_active:
        logger.warning(f"User disabled:{token_data.sub}")
        raise HTTPException(status_code=401, detail="User is inactive")

    logger.debug(f"Could not close temporary folder: %s{token_data.sub}")

    #Returns complete user information, including preferences
    return {
        "id": str(user.id),
        "username": user.username,
        "email": user.email,
        "name": user.username,
        "is_admin": user.is_admin,
        "roles": ["admin"] if user.is_admin else ["user"],
        "preferences": user.preferences.model_dump() if user.preferences else {}
    }

@router.post("/login")
async def login(payload: LoginRequest, request: Request):
    """User Login"""
    start_time = time.time()

    #Get Client Information
    ip_address = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent", "")

    logger.info(f"Login request - username:{payload.username}, IP: {ip_address}")

    try:
        #Validate input
        if not payload.username or not payload.password:
            logger.warning(f"Could not close temporary folder: %s")
            await log_operation(
                user_id="unknown",
                username=payload.username or "unknown",
                action_type=ActionType.USER_LOGIN,
                action="用户登录",
                details={"reason": "用户名和密码不能为空"},
                success=False,
                error_message="用户名和密码不能为空",
                duration_ms=int((time.time() - start_time) * 1000),
                ip_address=ip_address,
                user_agent=user_agent
            )
            raise HTTPException(status_code=400, detail="用户名和密码不能为空")

        logger.info(f"Could not close temporary folder: %s{payload.username}")

        #Use database authentication
        user = await user_service.authenticate_user(payload.username, payload.password)

        logger.info(f"Authentication results: user={'Existence' if user else 'does not exist'}")

        if not user:
            logger.warning(f"Could not close temporary folder: %s{payload.username}")
            await log_operation(
                user_id="unknown",
                username=payload.username,
                action_type=ActionType.USER_LOGIN,
                action="用户登录",
                details={"reason": "用户名或密码错误"},
                success=False,
                error_message="用户名或密码错误",
                duration_ms=int((time.time() - start_time) * 1000),
                ip_address=ip_address,
                user_agent=user_agent
            )
            raise HTTPException(status_code=401, detail="用户名或密码错误")

        #Generate token
        token = AuthService.create_access_token(sub=user.username)
        refresh_token = AuthService.create_access_token(sub=user.username, expires_delta=60*60*24*7)  #7 days valid

        #Login Login Success Log
        await log_operation(
            user_id=str(user.id),
            username=user.username,
            action_type=ActionType.USER_LOGIN,
            action="用户登录",
            details={"login_method": "password"},
            success=True,
            duration_ms=int((time.time() - start_time) * 1000),
            ip_address=ip_address,
            user_agent=user_agent
        )

        return {
            "success": True,
            "data": {
                "access_token": token,
                "refresh_token": refresh_token,
                "expires_in": 60 * 60,
                "user": {
                    "id": str(user.id),
                    "username": user.username,
                    "email": user.email,
                    "name": user.username,
                    "is_admin": user.is_admin
                }
            },
            "message": "登录成功"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"It's an anomaly.{e}")
        await log_operation(
            user_id="unknown",
            username=payload.username or "unknown",
            action_type=ActionType.USER_LOGIN,
            action="用户登录",
            details={"error": str(e)},
            success=False,
            error_message=f"系统错误: {str(e)}",
            duration_ms=int((time.time() - start_time) * 1000),
            ip_address=ip_address,
            user_agent=user_agent
        )
        raise HTTPException(status_code=500, detail="登录过程中发生系统错误")

@router.post("/refresh")
async def refresh_token(payload: RefreshTokenRequest):
    """Refresh Access Cards"""
    try:
        logger.debug(f"Request received from refresh token")
        logger.debug(f"Refresh token length:{len(payload.refresh_token) if payload.refresh_token else 0}")

        if not payload.refresh_token:
            logger.warning("Refresh token is empty.")
            raise HTTPException(status_code=401, detail="Refresh token is required")

        #Verify refresh token
        token_data = AuthService.verify_token(payload.refresh_token)
        logger.debug(f"Token:{token_data is not None}")

        if not token_data:
            logger.warning("Refresh token authentication failed")
            raise HTTPException(status_code=401, detail="Invalid refresh token")

        #Verify if user still exists and active
        user = await user_service.get_user_by_username(token_data.sub)
        if not user or not user.is_active:
            logger.warning(f"The user does not exist or is disabled:{token_data.sub}")
            raise HTTPException(status_code=401, detail="User not found or inactive")

        logger.debug(f"Token's confirmed. User:{token_data.sub}")

        #Generate new tokens
        new_token = AuthService.create_access_token(sub=token_data.sub)
        new_refresh_token = AuthService.create_access_token(sub=token_data.sub, expires_delta=60*60*24*7)

        logger.debug(f"New token was successfully created.")

        return {
            "success": True,
            "data": {
                "access_token": new_token,
                "refresh_token": new_refresh_token,
                "expires_in": 60 * 60
            },
            "message": "Token刷新成功"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Refresh token handles anomalies:{str(e)}")
        raise HTTPException(status_code=401, detail=f"Token refresh failed: {str(e)}")

@router.post("/logout")
async def logout(request: Request, user: dict = Depends(get_current_user)):
    """User Logout"""
    start_time = time.time()

    #Get Client Information
    ip_address = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent", "")

    try:
        #Logout Log
        await log_operation(
            user_id=user["id"],
            username=user["username"],
            action_type=ActionType.USER_LOGOUT,
            action="用户登出",
            details={"logout_method": "manual"},
            success=True,
            duration_ms=int((time.time() - start_time) * 1000),
            ip_address=ip_address,
            user_agent=user_agent
        )

        return {
            "success": True,
            "data": {},
            "message": "登出成功"
        }
    except Exception as e:
        logger.error(f"Logout log failed:{e}")
        return {
            "success": True,
            "data": {},
            "message": "登出成功"
        }

@router.get("/me")
async def me(user: dict = Depends(get_current_user)):
    """Get Current User Information"""
    return {
        "success": True,
        "data": user,
        "message": "获取用户信息成功"
    }

@router.put("/me")
async def update_me(
    payload: dict,
    user: dict = Depends(get_current_user)
):
    """Update current user information"""
    try:
        from app.models.user import UserUpdate, UserPreferences

        #Build Update Data
        update_data = {}

        #Update Mailbox
        if "email" in payload:
            update_data["email"] = payload["email"]

        #Update preferences (support partial updates)
        if "preferences" in payload:
            #Fetch current preferences
            current_prefs = user.get("preferences", {})

            #Merge new preferences
            merged_prefs = {**current_prefs, **payload["preferences"]}

            #Create UserPreferences Object
            update_data["preferences"] = UserPreferences(**merged_prefs)

        #Update to preferences if language settings exist Medium
        if "language" in payload:
            if "preferences" not in update_data:
                #Fetch current preferences
                current_prefs = user.get("preferences", {})
                update_data["preferences"] = UserPreferences(**current_prefs)
            update_data["preferences"].language = payload["language"]

        #Update to preferences (if needed)
        #Note: Time zones are usually system-level settings, not user-level settings

        #Call service update users
        user_update = UserUpdate(**update_data)
        updated_user = await user_service.update_user(user["username"], user_update)

        if not updated_user:
            raise HTTPException(status_code=400, detail="更新失败，邮箱可能已被使用")

        #Returns updated user information
        return {
            "success": True,
            "data": updated_user.model_dump(by_alias=True),
            "message": "用户信息更新成功"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"更新用户信息失败: {str(e)}")

@router.post("/change-password")
async def change_password(
    payload: ChangePasswordRequest,
    request: Request,
    user: dict = Depends(get_current_user)
):
    """Change Password"""
    try:
        #Change password using database services
        success = await user_service.change_password(
            user["username"], 
            payload.old_password, 
            payload.new_password
        )
        
        if not success:
            raise HTTPException(status_code=400, detail="旧密码错误")

        return {
            "success": True,
            "data": {},
            "message": "密码修改成功"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Change password failed:{e}")
        raise HTTPException(status_code=500, detail=f"修改密码失败: {str(e)}")

@router.post("/reset-password")
async def reset_password(
    payload: ResetPasswordRequest,
    request: Request,
    user: dict = Depends(get_current_user)
):
    """Reset password (administrator operation)"""
    try:
        #Inspection Permissions
        if not user.get("is_admin", False):
            raise HTTPException(status_code=403, detail="权限不足")

        #Reset Password
        success = await user_service.reset_password(payload.username, payload.new_password)
        
        if not success:
            raise HTTPException(status_code=404, detail="用户不存在")

        return {
            "success": True,
            "data": {},
            "message": f"用户 {payload.username} 的密码已重置"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Reset password failed:{e}")
        raise HTTPException(status_code=500, detail=f"重置密码失败: {str(e)}")

@router.post("/create-user")
async def create_user(
    payload: CreateUserRequest,
    request: Request,
    user: dict = Depends(get_current_user)
):
    """Create User (Administrator Operations)"""
    try:
        #Inspection Permissions
        if not user.get("is_admin", False):
            raise HTTPException(status_code=403, detail="权限不足")

        #Create User
        user_create = UserCreate(
            username=payload.username,
            email=payload.email,
            password=payload.password
        )
        
        new_user = await user_service.create_user(user_create)
        
        if not new_user:
            raise HTTPException(status_code=400, detail="用户名或邮箱已存在")

        #If needed to set as administrator
        if payload.is_admin:
            from pymongo import MongoClient
            from app.core.config import settings
            client = MongoClient(settings.MONGO_URI)
            db = client[settings.MONGO_DB]
            db.users.update_one(
                {"username": payload.username},
                {"$set": {"is_admin": True}}
            )

        return {
            "success": True,
            "data": {
                "id": str(new_user.id),
                "username": new_user.username,
                "email": new_user.email,
                "is_admin": payload.is_admin
            },
            "message": f"用户 {payload.username} 创建成功"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Synchronising {e}")
        raise HTTPException(status_code=500, detail=f"创建用户失败: {str(e)}")

@router.get("/users")
async def list_users(
    skip: int = 0,
    limit: int = 100,
    user: dict = Depends(get_current_user)
):
    """Get User List (Administrator Operations)"""
    try:
        #Inspection Permissions
        if not user.get("is_admin", False):
            raise HTTPException(status_code=403, detail="权限不足")

        users = await user_service.list_users(skip=skip, limit=limit)
        
        return {
            "success": True,
            "data": {
                "users": [user.model_dump() for user in users],
                "total": len(users)
            },
            "message": "获取用户列表成功"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=f"获取用户列表失败: {str(e)}")
