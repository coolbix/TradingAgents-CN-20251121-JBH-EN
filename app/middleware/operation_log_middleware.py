"""Operation log record middle
Autorecord user API Operations Log
"""

import time
import json
import logging
from typing import Optional, Dict, Any
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from app.services.operation_log_service import log_operation
from app.models.operationlog_models import ActionType

logger = logging.getLogger("webapi")

#Global Switches: Whether or not to enable operation log records (can be controlled dynamically by the system)
OPLOG_ENABLED: bool = True

def set_operation_log_enabled(flag: bool) -> None:
    global OPLOG_ENABLED
    OPLOG_ENABLED = bool(flag)



class OperationLogMiddleware(BaseHTTPMiddleware):
    """Operation log record middle"""

    def __init__(self, app, skip_paths: Optional[list] = None):
        super().__init__(app)
        #Skip the log path
        self.skip_paths = skip_paths or [
            "/health",
            "/healthz",
            "/readyz",
            "/favicon.ico",
            "/docs",
            "/redoc",
            "/openapi.json",
            "/api/stream/",  #SSE streams are not recorded
            "/api/system/logs/",  #Operation log API does not record itself
        ]

        #Map Path to Operation Type
        self.path_action_mapping = {
            "/api/analysis/": ActionType.STOCK_ANALYSIS,
            "/api/screening/": ActionType.SCREENING,
            "/api/config/": ActionType.CONFIG_MANAGEMENT,
            "/api/system/database/": ActionType.DATABASE_OPERATION,
            "/api/auth/login": ActionType.USER_LOGIN,
            "/api/auth/logout": ActionType.USER_LOGOUT,
            "/api/auth/change-password": ActionType.USER_MANAGEMENT,  #Add Changes to Password Operations
            "/api/reports/": ActionType.REPORT_GENERATION,
        }

    async def dispatch(self, request: Request, call_next):
        #Check if skipping records is required
        if self._should_skip_logging(request):
            return await call_next(request)

        #Record start time
        start_time = time.time()

        #Access to requested information
        method = request.method
        path = request.url.path
        ip_address = self._get_client_ip(request)
        user_agent = request.headers.get("user-agent", "")

        #Access to user information (if certified)
        user_info = await self._get_user_info(request)

        #Processing of requests
        response = await call_next(request)

        #Time-consuming calculation
        duration_ms = int((time.time() - start_time) * 1000)

        #Step Record Operations Log
        if user_info:
            try:
                await self._log_operation(
                    user_info=user_info,
                    method=method,
                    path=path,
                    response=response,
                    duration_ms=duration_ms,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    request=request
                )
            except Exception as e:
                logger.error(f"Log operation log failed:{e}")

        return response

    def _should_skip_logging(self, request: Request) -> bool:
        """Judge whether logs should be skipped"""
        #Skip directly when global closing
        if not OPLOG_ENABLED:
            return True

        path = request.url.path

        #Check Skip Path
        for skip_path in self.skip_paths:
            if path.startswith(skip_path):
                return True

        #Only record API requests
        if not path.startswith("/api/"):
            return True

        #Record specific HTTP methods only
        if request.method not in ["POST", "PUT", "DELETE", "PATCH"]:
            return True

        return False

    def _get_client_ip(self, request: Request) -> str:
        """Get Client IP Address"""
        #Check Agent
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()

        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip

        #Use direct connection IP
        if request.client:
            return request.client.host

        return "unknown"

    async def _get_user_info(self, request: Request) -> Optional[Dict[str, Any]]:
        """Get User Information"""
        try:
            #Fetch user information from request status (with authentication intermediate settings)
            if hasattr(request.state, "user"):
                return request.state.user

            #Try to decipher user information from Authorize head
            auth_header = request.headers.get("authorization")
            if auth_header and auth_header.startswith("Bearer "):
                token = auth_header.split(" ", 1)[1]

                #Authenticate token using AuthService
                from app.services.auth_service import AuthService
                token_data = AuthService.verify_token(token)

                if token_data:
                    #Return user information (open-source only admin users)
                    return {
                        "id": "admin",
                        "username": "admin",
                        "name": "管理员",
                        "is_admin": True,
                        "roles": ["admin"]
                    }

            return None
        except Exception as e:
            logger.debug(f"Could not close temporary folder: %s{e}")
            return None

    def _get_action_type(self, path: str) -> str:
        """Get Operations Type From Path"""
        for path_prefix, action_type in self.path_action_mapping.items():
            if path.startswith(path_prefix):
                return action_type

        return ActionType.SYSTEM_SETTINGS  #Default Type

    def _get_action_description(self, method: str, path: str, request: Request) -> str:
        """Generate Operation Description"""
        #Basic description
        action_map = {
            "POST": "创建",
            "PUT": "更新",
            "PATCH": "修改",
            "DELETE": "删除"
        }

        action_verb = action_map.get(method, method)

        #Generate more specific descriptions by path
        if "/analysis/" in path:
            if "single" in path:
                return f"{action_verb}单股分析任务"
            elif "batch" in path:
                return f"{action_verb}批量分析任务"
            else:
                return f"{action_verb}分析任务"

        elif "/screening/" in path:
            return f"{action_verb}股票筛选"

        elif "/config/" in path:
            if "llm" in path:
                return f"{action_verb}大模型配置"
            elif "datasource" in path:
                return f"{action_verb}数据源配置"
            else:
                return f"{action_verb}系统配置"

        elif "/database/" in path:
            if "backup" in path:
                return f"{action_verb}数据库备份"
            elif "cleanup" in path:
                return f"{action_verb}数据库清理"
            else:
                return f"{action_verb}数据库操作"

        elif "/auth/" in path:
            if "login" in path:
                return "用户登录"
            elif "logout" in path:
                return "用户登出"
            elif "change-password" in path:
                return "修改密码"
            else:
                return f"{action_verb}认证操作"

        else:
            return f"{action_verb} {path}"

    async def _log_operation(
        self,
        user_info: Dict[str, Any],
        method: str,
        path: str,
        response: Response,
        duration_ms: int,
        ip_address: str,
        user_agent: str,
        request: Request
    ):
        """Log Operations Log"""
        try:
            #Judge whether the operation was successful
            success = 200 <= response.status_code < 400

            #Get Operations Type and Description
            action_type = self._get_action_type(path)
            action = self._get_action_description(method, path, request)

            #Build Details
            details = {
                "method": method,
                "path": path,
                "status_code": response.status_code,
                "query_params": dict(request.query_params) if request.query_params else None,
            }

            #Get the wrong information (if any)
            error_message = None
            if not success:
                error_message = f"HTTP {response.status_code}"

            #Log Operations Log
            await log_operation(
                user_id=user_info.get("id", ""),
                username=user_info.get("username", "unknown"),
                action_type=action_type,
                action=action,
                details=details,
                success=success,
                error_message=error_message,
                duration_ms=duration_ms,
                ip_address=ip_address,
                user_agent=user_agent,
                session_id=user_info.get("session_id")
            )

        except Exception as e:
            logger.error(f"Log operation log failed:{e}")


#Easy function: Manual recording of operational logs
async def manual_log_operation(
    request: Request,
    user_info: Dict[str, Any],
    action_type: str,
    action: str,
    details: Optional[Dict[str, Any]] = None,
    success: bool = True,
    error_message: Optional[str] = None,
    duration_ms: Optional[int] = None
):
    """Manual Log Operations Log"""
    try:
        ip_address = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "")

        await log_operation(
            user_id=user_info.get("id", ""),
            username=user_info.get("username", "unknown"),
            action_type=action_type,
            action=action,
            details=details,
            success=success,
            error_message=error_message,
            duration_ms=duration_ms,
            ip_address=ip_address,
            user_agent=user_agent,
            session_id=user_info.get("session_id")
        )
    except Exception as e:
        logger.error(f"Manual recording operation log failed:{e}")
