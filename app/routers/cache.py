"""Cache Management Route
Provide Cache Statistics, Clean-up, etc.
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional
from datetime import datetime, timedelta

from app.routers.auth_db import get_current_user
from app.core.response import ok
from tradingagents.utils.logging_manager import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/cache", tags=["cache"])


@router.get("/stats")
async def get_cache_stats(current_user: dict = Depends(get_current_user)):
    """Get cache statistical information

Returns:
dict: Cache Statistics
"""
    try:
        from tradingagents.dataflows.cache import get_cache
        
        cache = get_cache()
        
        #Get Cache Statistics
        stats = cache.get_cache_stats()
        
        logger.info(f"User{current_user['username']}Get Cache Statistics")
        
        return ok(
            data={
                "totalFiles": stats.get('total_files', 0),
                "totalSize": stats.get('total_size', 0),  #Bytes
                "maxSize": 1024 * 1024 * 1024,  # 1GB
                "stockDataCount": stats.get('stock_data_count', 0),
                "newsDataCount": stats.get('news_count', 0),
                "analysisDataCount": stats.get('fundamentals_count', 0)
            },
            message="获取缓存统计成功"
        )
        
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(
            status_code=500,
            detail=f"获取缓存统计失败: {str(e)}"
        )


@router.delete("/cleanup")
async def cleanup_old_cache(
    days: int = Query(7, ge=1, le=30, description="清理多少天前的缓存"),
    current_user: dict = Depends(get_current_user)
):
    """Clear Expired Cache

Args:
Days: Clear how many days ago's caches

Returns:
dict: Cleanup result
"""
    try:
        from tradingagents.dataflows.cache import get_cache
        
        cache = get_cache()
        
        #Clear Expired Cache
        cache.clear_old_cache(days)
        
        logger.info(f"User{current_user['username']}It's clean.{days}Day before Cache")
        
        return ok(
            data={"days": days},
            message=f"已清理 {days} 天前的缓存"
        )
        
    except Exception as e:
        logger.error(f"Clearing cache failed:{e}")
        raise HTTPException(
            status_code=500,
            detail=f"清理缓存失败: {str(e)}"
        )


@router.delete("/clear")
async def clear_all_cache(current_user: dict = Depends(get_current_user)):
    """Clear all caches

Returns:
dict: Cleanup result
"""
    try:
        from tradingagents.dataflows.cache import get_cache

        cache = get_cache()

        #Clear all caches (clean up all expired and unexpired caches)
        #Use clear old cache(0) to clear all caches
        cache.clear_old_cache(0)

        logger.warning(f"User{current_user['username']}Clear all caches.")

        return ok(
            data={},
            message="所有缓存已清空"
        )

    except Exception as e:
        logger.error(f"Clear cache failed:{e}")
        raise HTTPException(
            status_code=500,
            detail=f"清空缓存失败: {str(e)}"
        )


@router.get("/details")
async def get_cache_details(
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    current_user: dict = Depends(get_current_user)
):
    """Get Cache Details List

Args:
Page: Page Number
Page size: Number per page

Returns:
dict: Cache Details List
"""
    try:
        from tradingagents.dataflows.cache import get_cache
        
        cache = get_cache()
        
        #Fetch Cache Details
        #Note: This approach may need to be achieved in the cache class
        try:
            details = cache.get_cache_details(page=page, page_size=page_size)
        except AttributeError:
            #Return empty list if cache class does not achieve this method
            details = {
                "items": [],
                "total": 0,
                "page": page,
                "page_size": page_size
            }
        
        logger.info(f"User{current_user['username']}Get Cache Details{page})")
        
        return ok(
            data=details,
            message="获取缓存详情成功"
        )
        
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(
            status_code=500,
            detail=f"获取缓存详情失败: {str(e)}"
        )


@router.get("/backend-info")
async def get_cache_backend_info(current_user: dict = Depends(get_current_user)):
    """Fetch Cache Backend Information

Returns:
dict: Cachebackend Configuration Information
"""
    try:
        from tradingagents.dataflows.cache import get_cache
        
        cache = get_cache()
        
        #Get Backend Information
        try:
            backend_info = cache.get_cache_backend_info()
        except AttributeError:
            #If the cache class does not achieve this method, return basic information
            backend_info = {
                "system": "file",
                "primary_backend": "file",
                "fallback_enabled": False
            }
        
        logger.info(f"User{current_user['username']}Fetch Cache Backend Information")
        
        return ok(
            data=backend_info,
            message="获取缓存后端信息成功"
        )
        
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(
            status_code=500,
            detail=f"获取缓存后端信息失败: {str(e)}"
        )

