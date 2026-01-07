"""Database management API routers
"""

import logging
import json
import os
from datetime import datetime
from typing import Dict, Any, List
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from fastapi.responses import FileResponse
from pydantic import BaseModel

from app.routers.auth_db import get_current_user
from app.core.database import get_mongo_db_async, get_redis_client_async
from app.services.database_service import DatabaseService

router = APIRouter(prefix="/database", tags=["数据库管理"])
logger = logging.getLogger("webapi")

#Request Model
class BackupRequest(BaseModel):
    """Backup Request"""
    name: str
    collections: List[str] = []  #Empty list means all backup collections

class ImportRequest(BaseModel):
    """Import Request"""
    collection: str
    format: str = "json"  # json, csv
    overwrite: bool = False

class ExportRequest(BaseModel):
    """Export request"""
    collections: List[str] = []  #Empty list means export of all collections
    format: str = "json"  # json, csv
    sanitize: bool = False  #Whether or not to be allergic (clean sensitive fields for demonstration systems)

#Response model
class DatabaseStatusResponse(BaseModel):
    """Database status response"""
    mongodb: Dict[str, Any]
    redis: Dict[str, Any]

class DatabaseStatsResponse(BaseModel):
    """Database statistical response"""
    total_collections: int
    total_documents: int
    total_size: int
    collections: List[Dict[str, Any]]

class BackupResponse(BaseModel):
    """Backup Response"""
    id: str
    name: str
    size: int
    created_at: str
    collections: List[str]

#Examples of database services
database_service = DatabaseService()

@router.get("/status")
async def get_database_status(
    current_user: dict = Depends(get_current_user)
):
    """Get database connection status"""
    try:
        logger.info(f"User 🔍{current_user['username']}Request Database Status")
        status_info = await database_service.get_database_status()
        return {
            "success": True,
            "message": "获取数据库状态成功",
            "data": status_info
        }
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取数据库状态失败: {str(e)}"
        )

@router.get("/stats")
async def get_database_stats(
    current_user: dict = Depends(get_current_user)
):
    """Access to database statistics"""
    try:
        logger.info(f"User 📊{current_user['username']}Request database statistics")
        stats = await database_service.get_database_stats()
        return {
            "success": True,
            "message": "获取数据库统计成功",
            "data": stats
        }
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取数据库统计失败: {str(e)}"
        )

@router.post("/test")
async def test_database_connections(
    current_user: dict = Depends(get_current_user)
):
    """Test database connection"""
    try:
        logger.info(f"User 🧪{current_user['username']}Test database connection")
        results = await database_service.test_connections()
        return {
            "success": True,
            "message": "数据库连接测试完成",
            "data": results
        }
    except Exception as e:
        logger.error(f"Test database connection failed:{e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"测试数据库连接失败: {str(e)}"
        )

@router.post("/backup")
async def create_backup(
    request: BackupRequest,
    current_user: dict = Depends(get_current_user)
):
    """Create database backup"""
    try:
        logger.info(f"User 💾{current_user['username']}Create backup:{request.name}")
        backup_info = await database_service.create_backup(
            name=request.name,
            collections=request.collections,
            user_id=current_user['id']
        )
        return {
            "success": True,
            "message": "备份创建成功",
            "data": backup_info
        }
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"创建备份失败: {str(e)}"
        )

@router.get("/backups")
async def list_backups(
    current_user: dict = Depends(get_current_user)
):
    """Get Backup List"""
    try:
        logger.info(f"User 📋{current_user['username']}Get Backup List")
        backups = await database_service.list_backups()
        return {
            "success": True,
            "data": backups
        }
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取备份列表失败: {str(e)}"
        )

@router.post("/import")
async def import_data(
    file: UploadFile = File(...),
    collection: str = "imported_data",
    format: str = "json",
    overwrite: bool = False,
    current_user: dict = Depends(get_current_user)
):
    """Organisation"""
    try:
        logger.info(f"User 📥{current_user['username']}Import data to group:{collection}")
        logger.info(f"File name:{file.filename}")
        logger.info(f"Format:{format}")
        logger.info(f"Overwrite mode:{overwrite}")

        #Read File Contents
        content = await file.read()
        logger.info(f"File size:{len(content)}Bytes")

        result = await database_service.import_data(
            content=content,
            collection=collection,
            format=format,
            overwrite=overwrite,
            filename=file.filename
        )

        logger.info(f"Import succeeded:{result}")

        return {
            "success": True,
            "message": "数据导入成功",
            "data": result
        }
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"导入数据失败: {str(e)}"
        )

@router.post("/export")
async def export_data(
    request: ExportRequest,
    current_user: dict = Depends(get_current_user)
):
    """Export Data"""
    try:
        sanitize_info = "（脱敏模式）" if request.sanitize else ""
        logger.info(f"User 📤{current_user['username']}Export Data{sanitize_info}")

        file_path = await database_service.export_data(
            collections=request.collections,
            format=request.format,
            sanitize=request.sanitize
        )

        return FileResponse(
            path=file_path,
            filename=os.path.basename(file_path),
            media_type='application/octet-stream'
        )
    except Exception as e:
        logger.error(f"Export data failed:{e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"导出数据失败: {str(e)}"
        )

@router.delete("/backups/{backup_id}")
async def delete_backup(
    backup_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Remove Backup"""
    try:
        logger.info(f"User 🗑️{current_user['username']}Delete backup:{backup_id}")
        await database_service.delete_backup(backup_id)
        return {
            "success": True,
            "message": "备份删除成功"
        }
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除备份失败: {str(e)}"
        )

@router.post("/cleanup")
async def cleanup_old_data(
    days: int = 30,
    current_user: dict = Depends(get_current_user)
):
    """Clear old data"""
    try:
        logger.info(f"User 🧹{current_user['username']}Clear{days}Day-to-day data")
        result = await database_service.cleanup_old_data(days)
        return {
            "success": True,
            "message": f"清理完成，删除了 {result['deleted_count']} 条记录",
            "data": result
        }
    except Exception as e:
        logger.error(f"Synchronising {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"清理数据失败: {str(e)}"
        )

@router.post("/cleanup/analysis")
async def cleanup_analysis_results(
    days: int = 30,
    current_user: dict = Depends(get_current_user)
):
    """Clean up outdated analysis"""
    try:
        logger.info(f"User 🧹{current_user['username']}Clear{days}The results of the analysis,")
        result = await database_service.cleanup_analysis_results(days)
        return {
            "success": True,
            "message": f"分析结果清理完成，删除了 {result['deleted_count']} 条记录",
            "data": result
        }
    except Exception as e:
        logger.error(f"Cleanup analysis failed:{e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"清理分析结果失败: {str(e)}"
        )

@router.post("/cleanup/logs")
async def cleanup_operation_logs(
    days: int = 90,
    current_user: dict = Depends(get_current_user)
):
    """Clear Operations Log"""
    try:
        logger.info(f"User 🧹{current_user['username']}Clear{days}Operation log of the sky")
        result = await database_service.cleanup_operation_logs(days)
        return {
            "success": True,
            "message": f"操作日志清理完成，删除了 {result['deleted_count']} 条记录",
            "data": result
        }
    except Exception as e:
        logger.error(f"Cleanup operation log failed:{e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"清理操作日志失败: {str(e)}"
        )
