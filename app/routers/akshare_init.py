"""AKShare Data Initializing API Routes
Provision of Web interface for the initialization and management of AKShare data
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel, Field

from app.core.database import get_mongo_db_async
from app.worker.akshare_init_service import get_akshare_init_service
from app.worker.akshare_sync_service import get_akshare_sync_service
from app.routers.auth_db import get_current_user
from app.utils.timezone import now_tz

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/akshare-init", tags=["AKShare初始化"])

#Global Task Status Storage
_initialization_status = {
    "is_running": False,
    "current_task": None,
    "start_time": None,
    "progress": None,
    "result": None
}


class InitializationRequest(BaseModel):
    """Initialization request model"""
    historical_days: int = Field(default=365, ge=1, le=3650, description="历史数据天数")
    force: bool = Field(default=False, description="是否强制重新初始化")
    skip_if_exists: bool = Field(default=True, description="如果数据存在是否跳过")


class SyncRequest(BaseModel):
    """Synchronise Request Model"""
    force_update: bool = Field(default=False, description="是否强制更新")
    symbols: Optional[list] = Field(default=None, description="指定股票代码列表")


@router.get("/status")
async def get_database_status():
    """Get database status

    Returns:
        Database status information
    """
    try:
        db = get_mongo_db_async()
        
        #Check Basic Information
        basic_count = await db.stock_basic_info.count_documents({})
        extended_count = await db.stock_basic_info.count_documents({
            "full_symbol": {"$exists": True},
            "market_info": {"$exists": True}
        })
        
        #Get Update Time
        latest_basic = await db.stock_basic_info.find_one(
            {}, sort=[("updated_at", -1)]
        )
        
        #Check Line Data
        quotes_count = await db.market_quotes.count_documents({})
        latest_quotes = await db.market_quotes.find_one(
            {}, sort=[("updated_at", -1)]
        )
        
        #Data quality assessment
        data_quality = "excellent"
        if basic_count == 0:
            data_quality = "empty"
        elif extended_count / basic_count < 0.5:
            data_quality = "poor"
        elif extended_count / basic_count < 0.9:
            data_quality = "good"
        
        return {
            "success": True,
            "data": {
                "basic_info": {
                    "total_count": basic_count,
                    "extended_count": extended_count,
                    "coverage_rate": round(extended_count / basic_count * 100, 2) if basic_count > 0 else 0,
                    "latest_update": latest_basic.get("updated_at") if latest_basic else None
                },
                "market_quotes": {
                    "total_count": quotes_count,
                    "latest_update": latest_quotes.get("updated_at") if latest_quotes else None
                },
                "data_quality": data_quality,
                "check_time": now_tz()
            },
            "message": "数据库状态检查完成"
        }
        
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=f"获取数据库状态失败: {str(e)}")


@router.get("/connection-test")
async def test_akshare_connection():
    """Testing AKShare Connection

    Returns:
        Connection Test Results
    """
    try:
        service = await get_akshare_sync_service()
        connected = await service.provider.test_connection()
        
        result = {
            "connected": connected,
            "test_time": now_tz()
        }
        
        if connected:
            #Test to get list of shares
            try:
                stock_list = await service.provider.get_stock_list()
                result["stock_count"] = len(stock_list) if stock_list else 0
                result["sample_stocks"] = stock_list[:5] if stock_list else []
            except Exception as e:
                result["stock_list_error"] = str(e)
        
        return {
            "success": True,
            "data": result,
            "message": "AKShare连接测试完成"
        }
        
    except Exception as e:
        logger.error(f"AKShare connection test failed:{e}")
        raise HTTPException(status_code=500, detail=f"连接测试失败: {str(e)}")


@router.post("/start-full")
async def start_full_initialization(
    request: InitializationRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """Start full data initialization

    Args:
        request parameters
        Background tasks: Backstage Job Manager
        current user: Current user information

    Returns:
        Initialise Start Results
    """
    global _initialization_status
    
    if _initialization_status["is_running"]:
        raise HTTPException(status_code=400, detail="初始化任务正在运行中")
    
    try:
        #Set Task Status
        _initialization_status.update({
            "is_running": True,
            "current_task": "full_initialization",
            "start_time": now_tz(),
            "progress": {"current_step": "准备中", "completed_steps": 0, "total_steps": 6},
            "result": None
        })
        
        #Start Backstage Task
        background_tasks.add_task(
            _run_full_initialization_background,
            request.historical_days,
            not request.skip_if_exists
        )
        
        return {
            "success": True,
            "data": {
                "task_id": "full_initialization",
                "start_time": _initialization_status["start_time"],
                "parameters": {
                    "historical_days": request.historical_days,
                    "force": not request.skip_if_exists
                }
            },
            "message": "完整初始化任务已启动，请使用 /initialization-status 查看进度"
        }
        
    except Exception as e:
        _initialization_status["is_running"] = False
        logger.error(f"Starting full initialization failed:{e}")
        raise HTTPException(status_code=500, detail=f"启动初始化失败: {str(e)}")


@router.post("/start-basic-sync")
async def start_basic_sync(
    request: SyncRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """Other Organiser

    Args:
        request parameters
        Background tasks: Backstage Job Manager
        current user: Current user information

    Returns:
        Sync Start Results
    """
    global _initialization_status
    
    if _initialization_status["is_running"]:
        raise HTTPException(status_code=400, detail="同步任务正在运行中")
    
    try:
        #Set Task Status
        _initialization_status.update({
            "is_running": True,
            "current_task": "basic_sync",
            "start_time": now_tz(),
            "progress": {"current_step": "同步基础信息", "completed_steps": 0, "total_steps": 1},
            "result": None
        })
        
        #Start Backstage Task
        background_tasks.add_task(
            _run_basic_sync_background,
            request.force_update
        )
        
        return {
            "success": True,
            "data": {
                "task_id": "basic_sync",
                "start_time": _initialization_status["start_time"],
                "parameters": {
                    "force_update": request.force_update
                }
            },
            "message": "基础信息同步任务已启动"
        }
        
    except Exception as e:
        _initialization_status["is_running"] = False
        logger.error(f"Synchronising {e}")
        raise HTTPException(status_code=500, detail=f"启动同步失败: {str(e)}")


@router.get("/initialization-status")
async def get_initialization_status():
    """Get Initialised Task Status

    Returns:
        Current Task Status
    """
    global _initialization_status
    
    return {
        "success": True,
        "data": {
            "is_running": _initialization_status["is_running"],
            "current_task": _initialization_status["current_task"],
            "start_time": _initialization_status["start_time"],
            "progress": _initialization_status["progress"],
            "result": _initialization_status["result"],
            "duration": (
                (now_tz() - _initialization_status["start_time"]).total_seconds()
                if _initialization_status["start_time"] else 0
            )
        },
        "message": "任务状态获取成功"
    }


@router.post("/stop")
async def stop_initialization(current_user: dict = Depends(get_current_user)):
    """Stop the current initialization task

    Args:
        current user: Current user information

    Returns:
        Stop result
    """
    global _initialization_status
    
    if not _initialization_status["is_running"]:
        raise HTTPException(status_code=400, detail="没有正在运行的任务")
    
    try:
        #Reset Task Status
        _initialization_status.update({
            "is_running": False,
            "current_task": None,
            "start_time": None,
            "progress": None,
            "result": {"stopped": True, "stop_time": datetime.utcnow()}
        })
        
        return {
            "success": True,
            "data": {
                "stopped": True,
                "stop_time": datetime.utcnow()
            },
            "message": "初始化任务已停止"
        }
        
    except Exception as e:
        logger.error(f"Failed to stop initializing task:{e}")
        raise HTTPException(status_code=500, detail=f"停止任务失败: {str(e)}")


async def _run_full_initialization_background(historical_days: int, force: bool):
    """Backstage run full initialization"""
    global _initialization_status
    
    try:
        service = await get_akshare_init_service()
        result = await service.run_full_initialization(
            historical_days=historical_days,
            skip_if_exists=not force
        )
        
        _initialization_status.update({
            "is_running": False,
            "result": result
        })
        
        logger.info(f"Complete initialization of backstage tasks completed:{result}")
        
    except Exception as e:
        _initialization_status.update({
            "is_running": False,
            "result": {"success": False, "error": str(e)}
        })
        logger.error(f"Could not close temporary folder: %s{e}")


async def _run_basic_sync_background(force_update: bool):
    """Synchronise Basic Information for Backstage"""
    global _initialization_status
    
    try:
        service = await get_akshare_sync_service()
        result = await service.sync_stock_basic_info(force_update=force_update)
        
        _initialization_status.update({
            "is_running": False,
            "result": result
        })
        
        logger.info(f"Basic information synchronization backstage tasks completed:{result}")
        
    except Exception as e:
        _initialization_status.update({
            "is_running": False,
            "result": {"success": False, "error": str(e)}
        })
        logger.error(f"Synchronising folder failed:{e}")
