#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Time job management route
Provides time task queries, pauses, recovery, manual triggers, etc.
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel

from app.routers.auth_db import get_current_user
from app.services.scheduler_service import get_scheduler_service, SchedulerService
from app.core.response import ok

router = APIRouter(prefix="/api/scheduler", tags=["scheduler"])


class JobTriggerRequest(BaseModel):
    """Manually trigger task request"""
    job_id: str
    kwargs: Optional[Dict[str, Any]] = None


class JobUpdateRequest(BaseModel):
    """Update Task Request"""
    job_id: str
    enabled: Optional[bool] = None
    cron: Optional[str] = None


class JobMetadataUpdateRequest(BaseModel):
    """Update task metadata request"""
    display_name: Optional[str] = None
    description: Optional[str] = None


@router.get("/jobs")
async def list_jobs(
    user: dict = Depends(get_current_user),
    service: SchedulerService = Depends(get_scheduler_service)
):
    """Can not open message

Returns:
Other Organiser
"""
    try:
        jobs = await service.list_jobs()
        return ok(data=jobs, message=f"获取到 {len(jobs)} 个定时任务")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取任务列表失败: {str(e)}")


@router.put("/jobs/{job_id}/metadata")
async def update_job_metadata_route(
    job_id: str,
    request: JobMetadataUpdateRequest,
    user: dict = Depends(get_current_user),
    service: SchedulerService = Depends(get_scheduler_service)
):
    """Update task metadata (trigger name and comment)

Args:
Job id: Task ID
request for updates

Returns:
Operation Results
"""
    #Check administrator privileges
    if not user.get("is_admin"):
        raise HTTPException(status_code=403, detail="仅管理员可以更新任务元数据")

    try:
        success = await service.update_job_metadata(
            job_id,
            display_name=request.display_name,
            description=request.description
        )
        if success:
            return ok(message=f"任务 {job_id} 元数据已更新")
        else:
            raise HTTPException(status_code=400, detail=f"更新任务 {job_id} 元数据失败")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新任务元数据失败: {str(e)}")


@router.get("/jobs/{job_id}")
async def get_job_detail(
    job_id: str,
    user: dict = Depends(get_current_user),
    service: SchedulerService = Depends(get_scheduler_service)
):
    """Can not open message

Args:
Job id: Task ID

Returns:
Task details
"""
    try:
        job = await service.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"任务 {job_id} 不存在")
        return ok(data=job, message="获取任务详情成功")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取任务详情失败: {str(e)}")


@router.post("/jobs/{job_id}/pause")
async def pause_job(
    job_id: str,
    user: dict = Depends(get_current_user),
    service: SchedulerService = Depends(get_scheduler_service)
):
    """Pause Task

Args:
Job id: Task ID

Returns:
Operation Results
"""
    #Check administrator privileges
    if not user.get("is_admin"):
        raise HTTPException(status_code=403, detail="仅管理员可以暂停任务")
    
    try:
        success = await service.pause_job(job_id)
        if success:
            return ok(message=f"任务 {job_id} 已暂停")
        else:
            raise HTTPException(status_code=400, detail=f"暂停任务 {job_id} 失败")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"暂停任务失败: {str(e)}")


@router.post("/jobs/{job_id}/resume")
async def resume_job(
    job_id: str,
    user: dict = Depends(get_current_user),
    service: SchedulerService = Depends(get_scheduler_service)
):
    """Resume Mission

Args:
Job id: Task ID

Returns:
Operation Results
"""
    #Check administrator privileges
    if not user.get("is_admin"):
        raise HTTPException(status_code=403, detail="仅管理员可以恢复任务")
    
    try:
        success = await service.resume_job(job_id)
        if success:
            return ok(message=f"任务 {job_id} 已恢复")
        else:
            raise HTTPException(status_code=400, detail=f"恢复任务 {job_id} 失败")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"恢复任务失败: {str(e)}")


@router.post("/jobs/{job_id}/trigger")
async def trigger_job(
    job_id: str,
    user: dict = Depends(get_current_user),
    service: SchedulerService = Depends(get_scheduler_service),
    force: bool = Query(False, description="是否强制执行（跳过交易时间检查等）")
):
    """Manually trigger mission execution

Args:
Job id: Task ID
force: enforcement ( Skip transaction time check, etc.), default False

Returns:
Operation Results
"""
    #Check administrator privileges
    if not user.get("is_admin"):
        raise HTTPException(status_code=403, detail="仅管理员可以手动触发任务")

    try:
        #Pass force parameters for specific tasks
        kwargs = {}
        if force and job_id in ["tushare_quotes_sync", "akshare_quotes_sync"]:
            kwargs["force"] = True

        success = await service.trigger_job(job_id, kwargs=kwargs)
        if success:
            message = f"任务 {job_id} 已触发执行"
            if force:
                message += "（强制模式）"
            return ok(message=message)
        else:
            raise HTTPException(status_code=400, detail=f"触发任务 {job_id} 失败")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"触发任务失败: {str(e)}")


@router.get("/jobs/{job_id}/history")
async def get_job_history(
    job_id: str,
    limit: int = Query(20, ge=1, le=100, description="返回数量限制"),
    offset: int = Query(0, ge=0, description="偏移量"),
    user: dict = Depends(get_current_user),
    service: SchedulerService = Depends(get_scheduler_service)
):
    """Get Task Execution History

Args:
Job id: Task ID
Limited number of returns
offset: offset

Returns:
Mission performance history
"""
    try:
        history = await service.get_job_history(job_id, limit=limit, offset=offset)
        total = await service.count_job_history(job_id)
        
        return ok(
            data={
                "history": history,
                "total": total,
                "limit": limit,
                "offset": offset
            },
            message=f"获取到 {len(history)} 条执行记录"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取执行历史失败: {str(e)}")


@router.get("/history")
async def get_all_history(
    limit: int = Query(50, ge=1, le=200, description="返回数量限制"),
    offset: int = Query(0, ge=0, description="偏移量"),
    job_id: Optional[str] = Query(None, description="任务ID过滤"),
    status: Optional[str] = Query(None, description="状态过滤: success/failed"),
    user: dict = Depends(get_current_user),
    service: SchedulerService = Depends(get_scheduler_service)
):
    """Get all tasks executed history

Args:
Limited number of returns
offset: offset
job id: Task ID filter
status: status filter

Returns:
History of all assignments
"""
    try:
        history = await service.get_all_history(
            limit=limit,
            offset=offset,
            job_id=job_id,
            status=status
        )
        total = await service.count_all_history(job_id=job_id, status=status)
        
        return ok(
            data={
                "history": history,
                "total": total,
                "limit": limit,
                "offset": offset
            },
            message=f"获取到 {len(history)} 条执行记录"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取执行历史失败: {str(e)}")


@router.get("/stats")
async def get_scheduler_stats(
    user: dict = Depends(get_current_user),
    service: SchedulerService = Depends(get_scheduler_service)
):
    """Get statistics from the scheduler

Returns:
Scheduler statistical information, including total tasks, number of active tasks, number of suspended tasks, etc.
"""
    try:
        stats = await service.get_stats()
        return ok(data=stats, message="获取统计信息成功")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取统计信息失败: {str(e)}")


@router.get("/health")
async def scheduler_health_check(
    user: dict = Depends(get_current_user),
    service: SchedulerService = Depends(get_scheduler_service)
):
    """Dispatch health check

Returns:
Scheduler health status
"""
    try:
        health = await service.health_check()
        return ok(data=health, message="调度器运行正常")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"健康检查失败: {str(e)}")


@router.get("/executions")
async def get_job_executions(
    user: dict = Depends(get_current_user),
    service: SchedulerService = Depends(get_scheduler_service),
    job_id: Optional[str] = Query(None, description="任务ID过滤"),
    status: Optional[str] = Query(None, description="状态过滤（success/failed/missed/running）"),
    is_manual: Optional[bool] = Query(None, description="是否手动触发（true=手动，false=自动，None=全部）"),
    limit: int = Query(50, ge=1, le=200, description="返回数量限制"),
    offset: int = Query(0, ge=0, description="偏移量")
):
    """Get Task Execution History

Args:
job id: Task ID filter (optional)
status: status filter (optional)
is manual: Manually triggered (optional)
Limited number of returns
offset: offset

Returns:
Execute History List
"""
    try:
        executions = await service.get_job_executions(
            job_id=job_id,
            status=status,
            is_manual=is_manual,
            limit=limit,
            offset=offset
        )
        total = await service.count_job_executions(job_id=job_id, status=status, is_manual=is_manual)
        return ok(data={
            "items": executions,
            "total": total,
            "limit": limit,
            "offset": offset
        }, message=f"获取到 {len(executions)} 条执行记录")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取执行历史失败: {str(e)}")


@router.get("/jobs/{job_id}/executions")
async def get_single_job_executions(
    job_id: str,
    user: dict = Depends(get_current_user),
    service: SchedulerService = Depends(get_scheduler_service),
    status: Optional[str] = Query(None, description="状态过滤（success/failed/missed/running）"),
    is_manual: Optional[bool] = Query(None, description="是否手动触发（true=手动，false=自动，None=全部）"),
    limit: int = Query(50, ge=1, le=200, description="返回数量限制"),
    offset: int = Query(0, ge=0, description="偏移量")
):
    """Can not open message

Args:
Job id: Task ID
status: status filter (optional)
is manual: Manually triggered (optional)
Limited number of returns
offset: offset

Returns:
Execute History List
"""
    try:
        executions = await service.get_job_executions(
            job_id=job_id,
            status=status,
            is_manual=is_manual,
            limit=limit,
            offset=offset
        )
        total = await service.count_job_executions(job_id=job_id, status=status, is_manual=is_manual)
        return ok(data={
            "items": executions,
            "total": total,
            "limit": limit,
            "offset": offset
        }, message=f"获取到 {len(executions)} 条执行记录")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取执行历史失败: {str(e)}")


@router.get("/jobs/{job_id}/execution-stats")
async def get_job_execution_stats(
    job_id: str,
    user: dict = Depends(get_current_user),
    service: SchedulerService = Depends(get_scheduler_service)
):
    """Access to statistical information on mandate implementation

Args:
Job id: Task ID

Returns:
Statistical information
"""
    try:
        stats = await service.get_job_execution_stats(job_id)
        return ok(data=stats, message="获取统计信息成功")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取统计信息失败: {str(e)}")


@router.post("/executions/{execution_id}/cancel")
async def cancel_execution(
    execution_id: str,
    user: dict = Depends(get_current_user),
    service: SchedulerService = Depends(get_scheduler_service)
):
    """Mandate execution cancelled/terminated

(a) For ongoing tasks, the demarking is set;
For quit but still running in the database, directly marked as failed

Args:
Exection id: Execute Record ID (MongoDB id)

Returns:
Operation Results
"""
    try:
        success = await service.cancel_job_execution(execution_id)
        if success:
            return ok(message="已设置取消标记，任务将在下次检查时停止")
        else:
            raise HTTPException(status_code=400, detail="取消任务失败")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"取消任务失败: {str(e)}")


@router.post("/executions/{execution_id}/mark-failed")
async def mark_execution_failed(
    execution_id: str,
    reason: str = Query("用户手动标记为失败", description="失败原因"),
    user: dict = Depends(get_current_user),
    service: SchedulerService = Depends(get_scheduler_service)
):
    """Mark execution record as a failed state

Used to process outgoing but still running tasks in the database

Args:
Exection id: Execute Record ID (MongoDB id)
Reason for failure

Returns:
Operation Results
"""
    try:
        success = await service.mark_execution_as_failed(execution_id, reason)
        if success:
            return ok(message="已标记为失败状态")
        else:
            raise HTTPException(status_code=400, detail="标记失败")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"标记失败: {str(e)}")


@router.delete("/executions/{execution_id}")
async def delete_execution(
    execution_id: str,
    user: dict = Depends(get_current_user),
    service: SchedulerService = Depends(get_scheduler_service)
):
    """Delete Execution Record

Args:
Exection id: Execute Record ID (MongoDB id)

Returns:
Operation Results
"""
    try:
        success = await service.delete_execution(execution_id)
        if success:
            return ok(message="执行记录已删除")
        else:
            raise HTTPException(status_code=400, detail="删除失败")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除执行记录失败: {str(e)}")
