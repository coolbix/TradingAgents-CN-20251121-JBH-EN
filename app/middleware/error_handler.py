"""Error Processing Middle
"""

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import logging
import traceback
from typing import Callable

logger = logging.getLogger(__name__)


class ErrorHandlerMiddleware(BaseHTTPMiddleware):
    """Global error processing middle"""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        try:
            response = await call_next(request)
            return response
        except Exception as exc:
            return await self.handle_error(request, exc)
    
    async def handle_error(self, request: Request, exc: Exception) -> JSONResponse:
        """Deal with anomalies and return standardized errors Response"""
        
        #Get Request ID
        request_id = getattr(request.state, "request_id", "unknown")
        
        #Log Error Log
        logger.error(
            f"Request abnormal - ID:{request_id}, "
            f"Path:{request.url.path}, "
            f"Methodology:{request.method}, "
            f"Unusual:{str(exc)}",
            exc_info=True
        )
        
        #Returns different bugs according to unusual type Response
        if isinstance(exc, ValueError):
            return JSONResponse(
                status_code=400,
                content={
                    "error": {
                        "code": "VALIDATION_ERROR",
                        "message": str(exc),
                        "request_id": request_id
                    }
                }
            )
        
        elif isinstance(exc, PermissionError):
            return JSONResponse(
                status_code=403,
                content={
                    "error": {
                        "code": "PERMISSION_DENIED",
                        "message": "权限不足",
                        "request_id": request_id
                    }
                }
            )
        
        elif isinstance(exc, FileNotFoundError):
            return JSONResponse(
                status_code=404,
                content={
                    "error": {
                        "code": "RESOURCE_NOT_FOUND",
                        "message": "请求的资源不存在",
                        "request_id": request_id
                    }
                }
            )
        
        else:
            #Unknown anomaly
            return JSONResponse(
                status_code=500,
                content={
                    "error": {
                        "code": "INTERNAL_SERVER_ERROR",
                        "message": "服务器内部错误，请稍后重试",
                        "request_id": request_id
                    }
                }
            )
