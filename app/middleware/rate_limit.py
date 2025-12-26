"""Intermediate Speed Limit
Prevent API abuse and achieve user and end point speed limits
"""

from fastapi import Request, Response, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
import logging
from typing import Callable, Dict, Optional
from core.redis_client import get_redis_service, RedisKeys

logger = logging.getLogger(__name__)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Intermediate Speed Limit"""
    
    def __init__(self, app, default_rate_limit: int = 100):
        super().__init__(app)
        self.default_rate_limit = default_rate_limit
        
        #Rate limit configuration for different ends
        self.endpoint_limits = {
            "/api/analysis/single": 10,      #Single unit analysis: 10 per minute
            "/api/analysis/batch": 5,        #Batch analysis: 5 per minute
            "/api/screening/filter": 20,     #Stock screening: 20 times a minute
            "/api/auth/login": 5,            #Login: 5 times a minute
            "/api/auth/register": 3,         #Registration: 3 times a minute
        }
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        #Skip health checks and static resources
        if request.url.path.startswith(("/api/health", "/docs", "/redoc", "/openapi.json")):
            return await call_next(request)
        
        #Get User ID (if certified)
        user_id = getattr(request.state, "user_id", None)
        if not user_id:
            #Use IP address for uncertified users
            user_id = f"ip:{request.client.host}" if request.client else "unknown"
        
        #Check speed limit
        try:
            await self.check_rate_limit(user_id, request.url.path)
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"Rate limit check failed:{exc}")
            #If Redis is not available, allow permission.
        
        return await call_next(request)
    
    async def check_rate_limit(self, user_id: str, endpoint: str):
        """Check speed limit"""
        redis_service = get_redis_service()
        
        #Retrieving peer speed limit
        rate_limit = self.endpoint_limits.get(endpoint, self.default_rate_limit)
        
        #Build Redis Key
        rate_key = RedisKeys.USER_RATE_LIMIT.format(
            user_id=user_id,
            endpoint=endpoint.replace("/", "_")
        )
        
        #Get the current count
        current_count = await redis_service.increment_with_ttl(rate_key, ttl=60)
        
        #Check if the limit is exceeded
        if current_count > rate_limit:
            logger.warning(
                f"Rate limit trigger - user:{user_id}, "
                f"End:{endpoint}, "
                f"Current count:{current_count}, "
                f"Limits:{rate_limit}"
            )
            
            raise HTTPException(
                status_code=429,
                detail={
                    "error": {
                        "code": "RATE_LIMIT_EXCEEDED",
                        "message": f"请求过于频繁，请稍后重试",
                        "rate_limit": rate_limit,
                        "current_count": current_count,
                        "reset_time": 60
                    }
                }
            )
        
        logger.debug(
            f"Speed limit check pass - user:{user_id}, "
            f"End:{endpoint}, "
            f"Current count:{current_count}/{rate_limit}"
        )


class QuotaMiddleware(BaseHTTPMiddleware):
    """Medium daily quota"""
    
    def __init__(self, app, daily_quota: int = 1000):
        super().__init__(app)
        self.daily_quota = daily_quota
        
        #Endpoint to include quota
        self.quota_endpoints = {
            "/api/analysis/single",
            "/api/analysis/batch",
            "/api/screening/filter"
        }
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        #Check only the end points that require quotas
        if request.url.path not in self.quota_endpoints:
            return await call_next(request)
        
        #Get User ID
        user_id = getattr(request.state, "user_id", None)
        if not user_id:
            #Uncertified users are not subject to quota
            return await call_next(request)
        
        #Check daily quota
        try:
            await self.check_daily_quota(user_id)
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"Quota check failed:{exc}")
            #If Redis is not available, allow permission.
        
        return await call_next(request)
    
    async def check_daily_quota(self, user_id: str):
        """Check daily quota"""
        import datetime
        
        redis_service = get_redis_service()
        
        #Can not open message
        today = datetime.date.today().isoformat()
        
        #Build Redis Key
        quota_key = RedisKeys.USER_DAILY_QUOTA.format(
            user_id=user_id,
            date=today
        )
        
        #Get Usage Today
        current_usage = await redis_service.increment_with_ttl(quota_key, ttl=86400)  #TTL 24 hours
        
        #Check if the quota is exceeded
        if current_usage > self.daily_quota:
            logger.warning(
                f"Daily quota exceeding - user:{user_id}, "
                f"Used today:{current_usage}, "
                f"Quota:{self.daily_quota}"
            )
            
            raise HTTPException(
                status_code=429,
                detail={
                    "error": {
                        "code": "DAILY_QUOTA_EXCEEDED",
                        "message": "今日配额已用完，请明天再试",
                        "daily_quota": self.daily_quota,
                        "current_usage": current_usage,
                        "reset_date": today
                    }
                }
            )
        
        logger.debug(
            f"Quota check passed - User:{user_id}, "
            f"Used today:{current_usage}/{self.daily_quota}"
        )
