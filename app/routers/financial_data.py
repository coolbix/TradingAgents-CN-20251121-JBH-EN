#!/usr/bin/env python3
"""Financial data API route
Provide financial data queries and synchronized management interfaces
"""
import logging
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field

from app.worker.financial_data_sync_service import get_financial_sync_service
from app.services.financial_data_service import get_financial_data_service
from app.core.response import ok

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/financial-data", tags=["财务数据"])


#== sync, corrected by elderman == @elder man

class FinancialSyncRequest(BaseModel):
    """Financial Data Synchronization Request"""
    symbols: Optional[List[str]] = Field(None, description="股票代码列表，为空则同步所有股票")
    data_sources: Optional[List[str]] = Field(
        ["tushare", "akshare", "baostock"], 
        description="数据源列表"
    )
    report_types: Optional[List[str]] = Field(
        ["quarterly"], 
        description="报告类型列表 (quarterly/annual)"
    )
    batch_size: int = Field(50, description="批处理大小", ge=1, le=200)
    delay_seconds: float = Field(1.0, description="API调用延迟秒数", ge=0.1, le=10.0)


class SingleStockSyncRequest(BaseModel):
    """Single stock financial data synchronization request"""
    symbol: str = Field(..., description="股票代码")
    data_sources: Optional[List[str]] = Field(
        ["tushare", "akshare", "baostock"], 
        description="数据源列表"
    )



#== sync, corrected by elderman == @elder man

@router.get("/query/{symbol}", summary="查询股票财务数据")
async def query_financial_data(
    symbol: str,
    report_period: Optional[str] = Query(None, description="报告期筛选 (YYYYMMDD)"),
    data_source: Optional[str] = Query(None, description="数据源筛选"),
    report_type: Optional[str] = Query(None, description="报告类型筛选"),
    limit: Optional[int] = Query(10, description="限制返回数量", ge=1, le=100)
) -> dict:
    """Search for stock financial data

- **symbol**: stock code (mandatory)
- **report period**: Screening for reporting period, format YYYMMDD
- **data source**: data source filter (tushare/akshare/baostock)
-**report type**: Report type filter (quarterly/annual)
-**Limit**: Limit number of returns, default 10
"""
    try:
        service = await get_financial_data_service()
        
        results = await service.get_financial_data(
            symbol=symbol,
            report_period=report_period,
            data_source=data_source,
            report_type=report_type,
            limit=limit
        )
        
        return ok(data={
                "symbol": symbol,
                "count": len(results),
                "financial_data": results
            },
            message=f"查询到 {len(results)} 条财务数据"
        )
        
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"查询财务数据失败: {str(e)}")


@router.get("/latest/{symbol}", summary="获取最新财务数据")
async def get_latest_financial_data(
    symbol: str,
    data_source: Optional[str] = Query(None, description="数据源筛选")
) -> dict:
    """Obtain updated financial data on equities

- **symbol**: stock code (mandatory)
- **data source**: data source filter (tushare/akshare/baostock)
"""
    try:
        service = await get_financial_data_service()
        
        result = await service.get_latest_financial_data(
            symbol=symbol,
            data_source=data_source
        )
        
        if result:
            return ok(data=result,
                message="获取最新财务数据成功"
            )
        else:
            return ok(success=False, data=None,
                message="未找到财务数据"
            )
        
    except Exception as e:
        logger.error(f"Failed to obtain latest financial data{symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"获取最新财务数据失败: {str(e)}")


@router.get("/statistics", summary="获取财务数据统计")
async def get_financial_statistics() -> dict:
    """Access to financial data statistics

Return financial data statistics from various data sources, including:
- Total records.
- Total stocks
- Statistics grouped by data source and type of report
"""
    try:
        service = await get_financial_data_service()
        
        stats = await service.get_financial_statistics()
        
        return ok(data=stats,
            message="获取财务数据统计成功"
        )
        
    except Exception as e:
        logger.error(f"Access to financial data statistics failed:{e}")
        raise HTTPException(status_code=500, detail=f"获取财务数据统计失败: {str(e)}")


@router.post("/sync/start", summary="启动财务数据同步")
async def start_financial_sync(
    request: FinancialSyncRequest,
    background_tasks: BackgroundTasks
) -> dict:
    """Other Organiser

Support configuration:
- List of stock codes (sync all stocks if empty)
- Data source selection
- Selection of types of reports
- Batch size and delay settings
"""
    try:
        service = await get_financial_sync_service()
        
        #Synchronise Tasks in Backstage
        background_tasks.add_task(
            _execute_financial_sync,
            service,
            request
        )
        
        return ok(data={
                "task_started": True,
                "config": request.dict()
            },
            message="财务数据同步任务已启动"
        )
        
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=f"启动财务数据同步失败: {str(e)}")


@router.post("/sync/single", summary="同步单只股票财务数据")
async def sync_single_stock_financial(
    request: SingleStockSyncRequest
) -> dict:
    """Synchronize single equity financial data

- **symbol**: stock code (mandatory)
-**data sources**: list of data sources, default for all data sources
"""
    try:
        service = await get_financial_sync_service()
        
        results = await service.sync_single_stock(
            symbol=request.symbol,
            data_sources=request.data_sources
        )
        
        success_count = sum(1 for success in results.values() if success)
        total_count = len(results)
        
        return ok(
            success=success_count > 0,
            data={
                "symbol": request.symbol,
                "results": results,
                "success_count": success_count,
                "total_count": total_count
            },
            message=f"单股票财务数据同步完成: {success_count}/{total_count} 成功"
        )
        
    except Exception as e:
        logger.error(f"Unsync of single stock financial data failed{request.symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"单股票财务数据同步失败: {str(e)}")


@router.get("/sync/statistics", summary="获取同步统计信息")
async def get_sync_statistics() -> dict:
    """Obtain financial data synchronized statistical information

Returns synchronized statistics from data sources, including records, shares, etc.
"""
    try:
        service = await get_financial_sync_service()
        
        stats = await service.get_sync_statistics()
        
        return ok(data=stats,
            message="获取同步统计信息成功"
        )
        
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=f"获取同步统计信息失败: {str(e)}")


@router.get("/health", summary="财务数据服务健康检查")
async def health_check() -> dict:
    """Health screening of financial data services

Check service status and database connection
"""
    try:
        #Initial status of inspection services
        service = await get_financial_data_service()
        sync_service = await get_financial_sync_service()
        
        #Simple database connection test
        stats = await service.get_financial_statistics()
        
        return ok(data={
                "service_status": "healthy",
                "database_connected": True,
                "total_records": stats.get("total_records", 0),
                "total_symbols": stats.get("total_symbols", 0)
            },
            message="财务数据服务运行正常"
        )
        
    except Exception as e:
        logger.error(f"The FDS health check failed:{e}")
        return ok(success=False, data={
                "service_status": "unhealthy",
                "error": str(e)
            },
            message="财务数据服务异常"
        )


#== sync, corrected by elderman == @elder man

async def _execute_financial_sync(
    service: Any,
    request: FinancialSyncRequest
):
    """Perform financial data synchronization back-office tasks"""
    try:
        logger.info(f"🚀starts the process of synchronizing financial data:{request.dict()}")
        
        results = await service.sync_financial_data(
            symbols=request.symbols,
            data_sources=request.data_sources,
            report_types=request.report_types,
            batch_size=request.batch_size,
            delay_seconds=request.delay_seconds
        )
        
        #Overall statistical results
        total_success = sum(stats.success_count for stats in results.values())
        total_symbols = sum(stats.total_symbols for stats in results.values())
        
        logger.info(f"Synchronization of financial data completed:{total_success}/{total_symbols}Success")
        
        #Here you can add a notification logic, e-mail or message.
        
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")


#Import datetime for time stamp
from datetime import datetime
