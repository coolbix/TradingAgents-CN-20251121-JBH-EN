"""Logs manage API route
Provide log query, filter and export functions
"""

import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.routers.auth_db import get_current_user
from app.services.log_export_service import get_log_export_service

router = APIRouter(prefix="/system-logs", tags=["系统日志"])
logger = logging.getLogger("webapi")


#Request Model
class LogReadRequest(BaseModel):
    """Log Read Request"""
    filename: str = Field(..., description="日志文件名")
    lines: int = Field(default=1000, ge=1, le=10000, description="读取行数")
    level: Optional[str] = Field(default=None, description="日志级别过滤")
    keyword: Optional[str] = Field(default=None, description="关键词过滤")
    start_time: Optional[str] = Field(default=None, description="开始时间（ISO格式）")
    end_time: Optional[str] = Field(default=None, description="结束时间（ISO格式）")


class LogExportRequest(BaseModel):
    """Log Export Request"""
    filenames: Optional[List[str]] = Field(default=None, description="要导出的文件名列表（空表示全部）")
    level: Optional[str] = Field(default=None, description="日志级别过滤")
    start_time: Optional[str] = Field(default=None, description="开始时间（ISO格式）")
    end_time: Optional[str] = Field(default=None, description="结束时间（ISO格式）")
    format: str = Field(default="zip", description="导出格式：zip, txt")


#Response model
class LogFileInfo(BaseModel):
    """Log File Information"""
    name: str
    path: str
    size: int
    size_mb: float
    modified_at: str
    type: str


class LogContentResponse(BaseModel):
    """Log Response"""
    filename: str
    lines: List[str]
    stats: dict


class LogStatisticsResponse(BaseModel):
    """Log Statistical Response"""
    total_files: int
    total_size_mb: float
    error_files: int
    recent_errors: List[str]
    log_types: dict


@router.get("/files", response_model=List[LogFileInfo])
async def list_log_files(
    current_user: dict = Depends(get_current_user)
):
    """Fetch list of all log files

    Returns basic information for log files, including file name, size, change time, etc.
    """
    try:
        logger.info(f"User {current_user['username']}Query log file list")
        
        service = get_log_export_service()
        files = service.list_log_files()
        
        return files
        
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=f"获取日志文件列表失败: {str(e)}")


@router.post("/read", response_model=LogContentResponse)
async def read_log_file(
    request: LogReadRequest,
    current_user: dict = Depends(get_current_user)
):
    """Read log file contents

    Support filter conditions:
    - Lines: Number of lines read (starting at the end)
    -level: Log Level (ERRO, WARNING, INFO, DEBUG)
    -keyword: Keyword Search
    -start time/end time:
    """
    try:
        logger.info(f"User {current_user['username']}Read log files:{request.filename}")
        
        service = get_log_export_service()
        content = service.read_log_file(
            filename=request.filename,
            lines=request.lines,
            level=request.level,
            keyword=request.keyword,
            start_time=request.start_time,
            end_time=request.end_time
        )
        
        return content
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=f"读取日志文件失败: {str(e)}")


@router.post("/export")
async def export_logs(
    request: LogExportRequest,
    current_user: dict = Depends(get_current_user)
):
    """Export Log File

    Support export format:
    -zip: Compressed package (recommended)
    -txt: Merged text files

    Support filter conditions:
    -filenames: Specify the file to export
    -level: log level filter
    -start time/end time: timescale filter
    """
    try:
        logger.info(f"User {current_user['username']}Export Log File")
        
        service = get_log_export_service()
        export_path = service.export_logs(
            filenames=request.filenames,
            level=request.level,
            start_time=request.start_time,
            end_time=request.end_time,
            format=request.format
        )
        
        #Return File Download
        import os
        filename = os.path.basename(export_path)
        media_type = "application/zip" if request.format == "zip" else "text/plain"
        
        return FileResponse(
            path=export_path,
            filename=filename,
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=f"导出日志文件失败: {str(e)}")


@router.get("/statistics", response_model=LogStatisticsResponse)
async def get_log_statistics(
    days: int = Query(default=7, ge=1, le=30, description="统计最近几天的日志"),
    current_user: dict = Depends(get_current_user)
):
    """Get Log Statistics

    Returns the latest N-day log statistics, including:
    - Number and total size of files
    - Number of error logs
    - Recent error.
    - Distribution of log type
    """
    try:
        logger.info(f"User {current_user['username']}Query log statistics")
        
        service = get_log_export_service()
        stats = service.get_log_statistics(days=days)
        
        return stats
        
    except Exception as e:
        logger.error(f"Can not get folder: %s: %s{e}")
        raise HTTPException(status_code=500, detail=f"获取日志统计失败: {str(e)}")


@router.delete("/files/{filename}")
async def delete_log_file(
    filename: str,
    current_user: dict = Depends(get_current_user)
):
    """Delete Log File

    Note: This operation cannot be restored. Please be careful.
    """
    try:
        logger.warning(f"User {current_user['username']}Delete log file:{filename}")
        
        service = get_log_export_service()
        file_path = service.log_dir / filename
        
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="日志文件不存在")
        
        #Security check: only delete .log files is allowed
        if not filename.endswith('.log') and not '.log.' in filename:
            raise HTTPException(status_code=400, detail="只能删除日志文件")
        
        file_path.unlink()
        
        return {
            "success": True,
            "message": f"日志文件已删除: {filename}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=f"删除日志文件失败: {str(e)}")

