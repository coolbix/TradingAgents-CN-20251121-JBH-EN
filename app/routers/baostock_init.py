#!/usr/bin/env python3
"""BaoStock Initialize API Route
RESTful API interface for the initialization of BaoStock data
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from app.worker.baostock_init_service import BaoStockInitService
from app.worker.baostock_sync_service import BaoStockSyncService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/baostock-init", tags=["BaoStock初始化"])

#Global Status Management
_initialization_status = {
    "is_running": False,
    "current_task": None,
    "stats": None,
    "start_time": None,
    "last_update": None
}


class InitializationRequest(BaseModel):
    """Initialization request model"""
    historical_days: int = Field(default=365, ge=1, le=3650, description="历史数据天数")
    force: bool = Field(default=False, description="是否强制重新初始化")


class InitializationResponse(BaseModel):
    """Initialized Response Model"""
    success: bool
    message: str
    task_id: Optional[str] = None
    data: Optional[Dict[str, Any]] = None


@router.get("/status", response_model=Dict[str, Any])
async def get_database_status():
    """Get database status"""
    try:
        service = BaoStockInitService()
        status = await service.check_database_status()
        
        return {
            "success": True,
            "data": status,
            "message": "数据库状态获取成功"
        }
        
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=f"获取数据库状态失败: {e}")


@router.get("/connection-test", response_model=Dict[str, Any])
async def test_baostock_connection():
    """Test BaoStock Connection"""
    try:
        service = BaoStockSyncService()
        connected = await service.provider.test_connection()
        
        return {
            "success": connected,
            "data": {
                "connected": connected,
                "test_time": datetime.now().isoformat()
            },
            "message": "BaoStock连接正常" if connected else "BaoStock连接失败"
        }
        
    except Exception as e:
        logger.error(f"BaoStock connection test failed:{e}")
        raise HTTPException(status_code=500, detail=f"连接测试失败: {e}")


@router.post("/start-full", response_model=InitializationResponse)
async def start_full_initialization(
    request: InitializationRequest,
    background_tasks: BackgroundTasks
):
    """Start Full Initialization"""
    global _initialization_status
    
    if _initialization_status["is_running"]:
        raise HTTPException(
            status_code=409, 
            detail="初始化任务正在运行中，请等待完成后再试"
        )
    
    try:
        #Generate Task ID
        task_id = f"baostock_full_init_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        #Update Status
        _initialization_status.update({
            "is_running": True,
            "current_task": "full_initialization",
            "stats": None,
            "start_time": datetime.now(),
            "last_update": datetime.now()
        })
        
        #Start Backstage Task
        background_tasks.add_task(
            _run_full_initialization_task,
            request.historical_days,
            request.force,
            task_id
        )
        
        return InitializationResponse(
            success=True,
            message="完整初始化任务已启动",
            task_id=task_id,
            data={
                "historical_days": request.historical_days,
                "force": request.force,
                "estimated_duration": "30-60分钟"
            }
        )
        
    except Exception as e:
        _initialization_status["is_running"] = False
        logger.error(f"Starting full initialization failed:{e}")
        raise HTTPException(status_code=500, detail=f"启动初始化失败: {e}")


@router.post("/start-basic", response_model=InitializationResponse)
async def start_basic_initialization(background_tasks: BackgroundTasks):
    """Start Basic Initialization"""
    global _initialization_status
    
    if _initialization_status["is_running"]:
        raise HTTPException(
            status_code=409,
            detail="初始化任务正在运行中，请等待完成后再试"
        )
    
    try:
        #Generate Task ID
        task_id = f"baostock_basic_init_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        #Update Status
        _initialization_status.update({
            "is_running": True,
            "current_task": "basic_initialization",
            "stats": None,
            "start_time": datetime.now(),
            "last_update": datetime.now()
        })
        
        #Start Backstage Task
        background_tasks.add_task(_run_basic_initialization_task, task_id)
        
        return InitializationResponse(
            success=True,
            message="基础初始化任务已启动",
            task_id=task_id,
            data={
                "estimated_duration": "10-20分钟"
            }
        )
        
    except Exception as e:
        _initialization_status["is_running"] = False
        logger.error(f"Starting base initialization failed:{e}")
        raise HTTPException(status_code=500, detail=f"启动初始化失败: {e}")


@router.get("/initialization-status", response_model=Dict[str, Any])
async def get_initialization_status():
    """Get Initialization"""
    global _initialization_status
    
    try:
        status = _initialization_status.copy()
        
        #Calculate running time
        if status["start_time"]:
            if status["is_running"]:
                duration = (datetime.now() - status["start_time"]).total_seconds()
            else:
                duration = (status["last_update"] - status["start_time"]).total_seconds() if status["last_update"] else 0
            status["duration"] = duration
        
        #Format Statistical Information
        if status["stats"]:
            stats = status["stats"]
            status["progress"] = {
                "completed_steps": stats.completed_steps,
                "total_steps": stats.total_steps,
                "current_step": stats.current_step,
                "progress_percent": (stats.completed_steps / stats.total_steps) * 100
            }
            status["data_summary"] = {
                "basic_info_count": stats.basic_info_count,
                "quotes_count": stats.quotes_count,
                "historical_records": stats.historical_records,
                "financial_records": stats.financial_records,
                "error_count": len(stats.errors)
            }
        
        return {
            "success": True,
            "data": status,
            "message": "状态获取成功"
        }
        
    except Exception as e:
        logger.error(f"Failed to get initialization:{e}")
        raise HTTPException(status_code=500, detail=f"获取状态失败: {e}")


@router.post("/stop", response_model=Dict[str, Any])
async def stop_initialization():
    """Stop Initialising Tasks"""
    global _initialization_status
    
    if not _initialization_status["is_running"]:
        return {
            "success": True,
            "message": "没有正在运行的初始化任务",
            "data": {"was_running": False}
        }
    
    try:
        #Update Status
        _initialization_status.update({
            "is_running": False,
            "current_task": None,
            "last_update": datetime.now()
        })
        
        return {
            "success": True,
            "message": "初始化任务已停止",
            "data": {"was_running": True}
        }
        
    except Exception as e:
        logger.error(f"Failed to stop initializing task:{e}")
        raise HTTPException(status_code=500, detail=f"停止任务失败: {e}")


async def _run_full_initialization_task(historical_days: int, force: bool, task_id: str):
    """Run a full initial task"""
    global _initialization_status
    
    try:
        logger.info(f"Starting the BaoStock mission:{task_id}")
        
        service = BaoStockInitService()
        stats = await service.full_initialization(
            historical_days=historical_days,
            force=force
        )
        
        #Update Status
        _initialization_status.update({
            "is_running": False,
            "stats": stats,
            "last_update": datetime.now()
        })
        
        if stats.completed_steps == stats.total_steps:
            logger.info(f"BaoStock complete initialization mission completed:{task_id}")
        else:
            logger.warning(f"BaoStock was partially completed:{task_id}")
        
    except Exception as e:
        logger.error(f"BaoStock failed:{task_id}, Error:{e}")
        _initialization_status.update({
            "is_running": False,
            "last_update": datetime.now()
        })


async def _run_basic_initialization_task(task_id: str):
    """Operation Base Initialisation Task"""
    global _initialization_status
    
    try:
        logger.info(f"Starting the BaoStock Foundation Initialization mission:{task_id}")
        
        service = BaoStockInitService()
        stats = await service.basic_initialization()
        
        #Update Status
        _initialization_status.update({
            "is_running": False,
            "stats": stats,
            "last_update": datetime.now()
        })
        
        if stats.completed_steps == stats.total_steps:
            logger.info(f"The initialization of BaoStock Foundation was completed:{task_id}")
        else:
            logger.warning(f"Part of BaoStock Foundation Initialization was completed:{task_id}")
        
    except Exception as e:
        logger.error(f"The initialization of BaoStock Foundation failed:{task_id}, Error:{e}")
        _initialization_status.update({
            "is_running": False,
            "last_update": datetime.now()
        })


@router.get("/service-status", response_model=Dict[str, Any])
async def get_service_status():
    """Get BaoStock Service Status"""
    try:
        service = BaoStockSyncService()
        status = await service.check_service_status()
        
        return {
            "success": True,
            "data": status,
            "message": "服务状态获取成功"
        }
        
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=f"获取服务状态失败: {e}")
