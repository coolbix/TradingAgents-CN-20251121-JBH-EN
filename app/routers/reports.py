"""Analytical reports manage API routers
"""
import os
import json
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from .auth_db import get_current_user
from ..core.database import get_mongo_db
from ..utils.timezone import to_config_tz
import logging

logger = logging.getLogger("webapi")

#Stock Name Cache
_stock_name_cache = {}

def get_stock_name(stock_code: str) -> str:
    """Get stock names
Priority: Cache - > MongoDB (data source priority) - > Default return stock code
"""
    global _stock_name_cache

    #Check Cache
    if stock_code in _stock_name_cache:
        return _stock_name_cache[stock_code]

    try:
        #Fetch stock names from MongoDB
        from ..core.database import get_mongo_db_sync
        from ..core.unified_config import UnifiedConfigManager

        db = get_mongo_db_sync()
        code6 = str(stock_code).zfill(6)

        #🔥Query by data source priority
        config = UnifiedConfigManager()
        data_source_configs = config.get_data_source_configs()

        #Extract enabled data sources in order of priority
        enabled_sources = [
            ds.type.lower() for ds in data_source_configs
            if ds.enabled and ds.type.lower() in ['tushare', 'akshare', 'baostock']
        ]

        if not enabled_sources:
            enabled_sources = ['tushare', 'akshare', 'baostock']

        #Query by Data Source Priority
        stock_info = None
        for data_source in enabled_sources:
            stock_info = db.stock_basic_info.find_one(
                {"$or": [{"symbol": code6}, {"code": code6}], "source": data_source}
            )
            if stock_info:
                logger.debug(f"Using data sources{data_source}Get stock names{code6}")
                break

        #Try without source condition query (compatible with old data) if all data sources are missing
        if not stock_info:
            stock_info = db.stock_basic_info.find_one(
                {"$or": [{"symbol": code6}, {"code": code6}]}
            )
            if stock_info:
                logger.warning(f"⚠️ Use old data (no source field) for stock names{code6}")

        if stock_info and stock_info.get("name"):
            stock_name = stock_info["name"]
            _stock_name_cache[stock_code] = stock_name
            return stock_name

        #If not found, return the stock code.
        _stock_name_cache[stock_code] = stock_code
        return stock_code

    except Exception as e:
        logger.warning(f"Failed to get stock name{stock_code}: {e}")
        return stock_code


#Unified Build Report Query: Support  id (ObjectID) / anallysis id / task id
def _build_report_query(report_id: str) -> Dict[str, Any]:
    ors = [
        {"analysis_id": report_id},
        {"task_id": report_id},
    ]
    try:
        from bson import ObjectId
        ors.append({"_id": ObjectId(report_id)})
    except Exception:
        pass
    return {"$or": ors}

router = APIRouter(prefix="/api/reports", tags=["reports"])

class ReportFilter(BaseModel):
    """Report filter parameters"""
    search_keyword: Optional[str] = None
    market_filter: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    stock_code: Optional[str] = None
    report_type: Optional[str] = None

class ReportListResponse(BaseModel):
    """Report List Response"""
    reports: List[Dict[str, Any]]
    total: int
    page: int
    page_size: int

@router.get("/list", response_model=Dict[str, Any])
async def get_reports_list(
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    search_keyword: Optional[str] = Query(None, description="搜索关键词"),
    market_filter: Optional[str] = Query(None, description="市场筛选（A股/港股/美股）"),
    start_date: Optional[str] = Query(None, description="开始日期"),
    end_date: Optional[str] = Query(None, description="结束日期"),
    stock_code: Optional[str] = Query(None, description="股票代码"),
    user: dict = Depends(get_current_user)
):
    """Get List of Analytical Reports"""
    try:
        logger.info(f"Can not get folder: %s: %s{user['id']}, page number={page}, per page ={page_size}market ={market_filter}")

        db = get_mongo_db()

        #Build query conditions
        query = {}

        #Search keywords
        if search_keyword:
            query["$or"] = [
                {"stock_symbol": {"$regex": search_keyword, "$options": "i"}},
                {"analysis_id": {"$regex": search_keyword, "$options": "i"}},
                {"summary": {"$regex": search_keyword, "$options": "i"}}
            ]

        #Market screening
        if market_filter:
            query["market_type"] = market_filter

        #Stock code filter
        if stock_code:
            query["stock_symbol"] = stock_code

        #Date range filter
        if start_date or end_date:
            date_query = {}
            if start_date:
                date_query["$gte"] = start_date
            if end_date:
                date_query["$lte"] = end_date
            query["analysis_date"] = date_query

        logger.info(f"Other Organiser{query}")

        #Total calculated
        total = await db.analysis_reports.count_documents(query)

        #Page Break Query
        skip = (page - 1) * page_size
        cursor = db.analysis_reports.find(query).sort("created_at", -1).skip(skip).limit(page_size)

        reports = []
        async for doc in cursor:
            #Convert to the format required for the front end
            stock_code = doc.get("stock_symbol", "")
            #🔥 Prefer to the name of the stock stored in MongoDB or, if not, query
            stock_name = doc.get("stock_name")
            if not stock_name:
                stock_name = get_stock_name(stock_code)

            #🔥 Market type of acquisition, if not extrapolated by stock code
            market_type = doc.get("market_type")
            if not market_type:
                from tradingagents.utils.stock_utils import StockUtils
                market_info = StockUtils.get_market_info(stock_code)
                market_type_map = {
                    "china_a": "A股",
                    "hong_kong": "港股",
                    "us": "美股",
                    "unknown": "A股"
                }
                market_type = market_type_map.get(market_info.get("market", "unknown"), "A股")

            #Fetch creation time (UTC time in database, requiring conversion to UTC+8)
            created_at = doc.get("created_at", datetime.utcnow())
            created_at_tz = to_config_tz(created_at)  #Convert to UTC+8 and add time zone information

            report = {
                "id": str(doc["_id"]),
                "analysis_id": doc.get("analysis_id", ""),
                "title": f"{stock_name}({stock_code}) 分析报告",
                "stock_code": stock_code,
                "stock_name": stock_name,
                "market_type": market_type,  #Add market-type fields
                "model_info": doc.get("model_info", "Unknown"),  #Add Model Information Fields
                "type": "single",  #It's mainly a single analysis.
                "format": "markdown",  #Main Format
                "status": doc.get("status", "completed"),
                "created_at": created_at_tz.isoformat() if created_at_tz else str(created_at),
                "analysis_date": doc.get("analysis_date", ""),
                "analysts": doc.get("analysts", []),
                "research_depth": doc.get("research_depth", 1),
                "summary": doc.get("summary", ""),
                "file_size": len(str(doc.get("reports", {}))),  #Estimated Size
                "source": doc.get("source", "unknown"),
                "task_id": doc.get("task_id", "")
            }
            reports.append(report)

        logger.info(f"Other Organiser{total}returns ={len(reports)}")

        return {
            "success": True,
            "data": {
                "reports": reports,
                "total": total,
                "page": page,
                "page_size": page_size
            },
            "message": "报告列表获取成功"
        }

    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{report_id}/detail")
async def get_report_detail(
    report_id: str,
    user: dict = Depends(get_current_user)
):
    """Access to report details"""
    try:
        logger.info(f"For more information:{report_id}")

        db = get_mongo_db()

        #Support for Objective Id / anallysis id / task id
        query = _build_report_query(report_id)
        doc = await db.analysis_reports.find_one(query)

        if not doc:
            #End: Restore details of the report from analysis tasks.result
            logger.info(f"It was not found in analysis reports, trying to recover from analysis tasks:{report_id}")
            tasks_doc = await db.analysis_tasks.find_one(
                {"$or": [{"task_id": report_id}, {"result.analysis_id": report_id}]},
                {"result": 1, "task_id": 1, "stock_code": 1, "created_at": 1, "completed_at": 1}
            )
            if not tasks_doc or not tasks_doc.get("result"):
                raise HTTPException(status_code=404, detail="报告不存在")

            r = tasks_doc["result"] or {}
            created_at = tasks_doc.get("created_at")
            updated_at = tasks_doc.get("completed_at") or created_at

            #Conversion time zone: UTC time in database converted to UTC+8
            created_at_tz = to_config_tz(created_at)
            updated_at_tz = to_config_tz(updated_at)

            def to_iso(x):
                if hasattr(x, "isoformat"):
                    return x.isoformat()
                return x or ""

            stock_symbol = r.get("stock_symbol", r.get("stock_code", tasks_doc.get("stock_code", "")))
            stock_name = r.get("stock_name")
            if not stock_name:
                stock_name = get_stock_name(stock_symbol)

            report = {
                "id": tasks_doc.get("task_id", report_id),
                "analysis_id": r.get("analysis_id", ""),
                "stock_symbol": stock_symbol,
                "stock_name": stock_name,  #Add stock name field 🔥
                "model_info": r.get("model_info", "Unknown"),  #Add Model Information Fields
                "analysis_date": r.get("analysis_date", ""),
                "status": r.get("status", "completed"),
                "created_at": to_iso(created_at_tz),
                "updated_at": to_iso(updated_at_tz),
                "analysts": r.get("analysts", []),
                "research_depth": r.get("research_depth", 1),
                "summary": r.get("summary", ""),
                "reports": r.get("reports", {}),
                "source": "analysis_tasks",
                "task_id": tasks_doc.get("task_id", report_id),
                "recommendation": r.get("recommendation", ""),
                "confidence_score": r.get("confidence_score", 0.0),
                "risk_level": r.get("risk_level", "中等"),
                "key_points": r.get("key_points", []),
                "execution_time": r.get("execution_time", 0),
                "tokens_used": r.get("tokens_used", 0)
            }
        else:
            #Convert to detailed format (analysis reports hit)
            stock_symbol = doc.get("stock_symbol", "")
            stock_name = doc.get("stock_name")
            if not stock_name:
                stock_name = get_stock_name(stock_symbol)

            #Retrieving time (UTC time in database, requiring conversion to UTC+8)
            created_at = doc.get("created_at", datetime.utcnow())
            updated_at = doc.get("updated_at", datetime.utcnow())

            #Conversion time zone: UTC time in database converted to UTC+8
            created_at_tz = to_config_tz(created_at)
            updated_at_tz = to_config_tz(updated_at)

            report = {
                "id": str(doc["_id"]),
                "analysis_id": doc.get("analysis_id", ""),
                "stock_symbol": stock_symbol,
                "stock_name": stock_name,  #Add stock name field 🔥
                "model_info": doc.get("model_info", "Unknown"),  #Add Model Information Fields
                "analysis_date": doc.get("analysis_date", ""),
                "status": doc.get("status", "completed"),
                "created_at": created_at_tz.isoformat() if created_at_tz else str(created_at),
                "updated_at": updated_at_tz.isoformat() if updated_at_tz else str(updated_at),
                "analysts": doc.get("analysts", []),
                "research_depth": doc.get("research_depth", 1),
                "summary": doc.get("summary", ""),
                "reports": doc.get("reports", {}),
                "source": doc.get("source", "unknown"),
                "task_id": doc.get("task_id", ""),
                "recommendation": doc.get("recommendation", ""),
                "confidence_score": doc.get("confidence_score", 0.0),
                "risk_level": doc.get("risk_level", "中等"),
                "key_points": doc.get("key_points", []),
                "execution_time": doc.get("execution_time", 0),
                "tokens_used": doc.get("tokens_used", 0)
            }

        return {
            "success": True,
            "data": report,
            "message": "报告详情获取成功"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{report_id}/content/{module}")
async def get_report_module_content(
    report_id: str,
    module: str,
    user: dict = Depends(get_current_user)
):
    """Get the contents of the specific module of the report"""
    try:
        logger.info(f"For the report module:{report_id}/{module}")

        db = get_mongo_db()

        #Query report (multiple ID support)
        query = _build_report_query(report_id)
        doc = await db.analysis_reports.find_one(query)

        if not doc:
            raise HTTPException(status_code=404, detail="报告不存在")

        reports = doc.get("reports", {})

        if module not in reports:
            raise HTTPException(status_code=404, detail=f"模块 {module} 不存在")

        content = reports[module]

        return {
            "success": True,
            "data": {
                "module": module,
                "content": content,
                "content_type": "markdown" if isinstance(content, str) else "json"
            },
            "message": "模块内容获取成功"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{report_id}")
async def delete_report(
    report_id: str,
    user: dict = Depends(get_current_user)
):
    """Delete Report"""
    try:
        logger.info(f"Delete the report:{report_id}")

        db = get_mongo_db()

        #Query report (multiple ID support)
        query = _build_report_query(report_id)
        result = await db.analysis_reports.delete_one(query)

        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="报告不存在")

        logger.info(f"Successfully deleted report:{report_id}")

        return {
            "success": True,
            "message": "报告删除成功"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete report failed:{e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{report_id}/download")
async def download_report(
    report_id: str,
    format: str = Query("markdown", description="下载格式: markdown, json, pdf, docx"),
    user: dict = Depends(get_current_user)
):
    """Download Report

Supported format:
- markdown: Markdown format (default)
- json: JSON format (with complete data)
-docx: Word Document Formatting (needs pandoc)
-pdf: PDF format (needs pandoc and PDF engines)
"""
    try:
        logger.info(f"Downloading report:{report_id}, format:{format}")

        db = get_mongo_db()

        #Query report (multiple ID support)
        query = _build_report_query(report_id)
        doc = await db.analysis_reports.find_one(query)

        if not doc:
            raise HTTPException(status_code=404, detail="报告不存在")

        stock_symbol = doc.get("stock_symbol", "unknown")
        analysis_date = doc.get("analysis_date", datetime.now().strftime("%Y-%m-%d"))

        if format == "json":
            #JSON format download
            content = json.dumps(doc, ensure_ascii=False, indent=2, default=str)
            filename = f"{stock_symbol}_{analysis_date}_report.json"
            media_type = "application/json"

            #Return File Stream
            def generate():
                yield content.encode('utf-8')

            return StreamingResponse(
                generate(),
                media_type=media_type,
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )

        elif format == "markdown":
            #Markdown download
            reports = doc.get("reports", {})
            content_parts = []

            #Add Title
            content_parts.append(f"# {stock_symbol} 分析报告")
            content_parts.append(f"**分析日期**: {analysis_date}")
            content_parts.append(f"**分析师**: {', '.join(doc.get('analysts', []))}")
            content_parts.append(f"**研究深度**: {doc.get('research_depth', 1)}")
            content_parts.append("")

            #Add Summary
            if doc.get("summary"):
                content_parts.append("## 执行摘要")
                content_parts.append(doc["summary"])
                content_parts.append("")

            #Add module contents
            for module_name, module_content in reports.items():
                if isinstance(module_content, str) and module_content.strip():
                    content_parts.append(f"## {module_name}")
                    content_parts.append(module_content)
                    content_parts.append("")

            content = "\n".join(content_parts)
            filename = f"{stock_symbol}_{analysis_date}_report.md"
            media_type = "text/markdown"

            #Return File Stream
            def generate():
                yield content.encode('utf-8')

            return StreamingResponse(
                generate(),
                media_type=media_type,
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )

        elif format == "docx":
            #Word Document Format Download
            from app.utils.report_exporter import report_exporter

            if not report_exporter.pandoc_available:
                raise HTTPException(
                    status_code=400,
                    detail="Word 导出功能不可用。请安装 pandoc: pip install pypandoc"
                )

            try:
                #Generate Word Document
                docx_content = report_exporter.generate_docx_report(doc)
                filename = f"{stock_symbol}_{analysis_date}_report.docx"

                #Return File Stream
                def generate():
                    yield docx_content

                return StreamingResponse(
                    generate(),
                    media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    headers={"Content-Disposition": f"attachment; filename={filename}"}
                )
            except Exception as e:
                logger.error(f"Could not close temporary folder: %s{e}")
                raise HTTPException(status_code=500, detail=f"Word 文档生成失败: {str(e)}")

        elif format == "pdf":
            #PDF format download
            from app.utils.report_exporter import report_exporter

            if not report_exporter.pandoc_available:
                raise HTTPException(
                    status_code=400,
                    detail="PDF 导出功能不可用。请安装 pandoc 和 PDF 引擎（wkhtmltopdf 或 LaTeX）"
                )

            try:
                #Generate PDF documents
                pdf_content = report_exporter.generate_pdf_report(doc)
                filename = f"{stock_symbol}_{analysis_date}_report.pdf"

                #Return File Stream
                def generate():
                    yield pdf_content

                return StreamingResponse(
                    generate(),
                    media_type="application/pdf",
                    headers={"Content-Disposition": f"attachment; filename={filename}"}
                )
            except Exception as e:
                logger.error(f"Could not close temporary folder: %s{e}")
                raise HTTPException(status_code=500, detail=f"PDF 文档生成失败: {str(e)}")

        else:
            raise HTTPException(status_code=400, detail=f"不支持的下载格式: {format}")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"The download report failed:{e}")
        raise HTTPException(status_code=500, detail=str(e))
