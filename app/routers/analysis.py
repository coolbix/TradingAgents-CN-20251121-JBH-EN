"""Stock analysis API route
Enhanced version to support functions such as prioritization, progress tracking, task management
"""

from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging
import time
import uuid
import asyncio

from app.routers.auth_db import get_current_user
from app.services.queue_service import get_queue_service, QueueService
from app.services.analysis_service import get_analysis_service
from app.services.simple_analysis_service import get_simple_analysis_service
from app.services.websocket_manager import get_websocket_manager
from app.models.analysis_models import (
    SingleAnalysisRequest, BatchAnalysisRequest, AnalysisParameters,
    AnalysisTaskResponse, AnalysisBatchResponse, AnalysisHistoryQuery
)

router = APIRouter()
logger = logging.getLogger("webapi")

#Compatibility: retention of original request model
class SingleAnalyzeRequest(BaseModel):
    symbol: str
    parameters: dict = Field(default_factory=dict)

class BatchAnalyzeRequest(BaseModel):
    symbols: List[str]
    parameters: dict = Field(default_factory=dict)
    title: str = Field(default="批量分析", description="批次标题")
    description: Optional[str] = Field(None, description="批次描述")

#New API Peer
@router.post("/single", response_model=Dict[str, Any])
async def submit_single_analysis(
    request: SingleAnalysisRequest,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user)
):
    """Submit a single unit analysis task - Use Background Tasks to walk"""
    try:
        logger.info(f"Request for unit analysis.")
        logger.info(f"User information:{user}")
        logger.info(f"Data requested:{request}")

        #Create task logs immediately and return without waiting for execution
        analysis_service = get_simple_analysis_service()
        result = await analysis_service.create_analysis_task(user["id"], request)

        #Extracting variables to avoid closure problems
        task_id = result["task_id"]
        user_id = user["id"]

        #Defines a packing function to run an odd task
        async def run_analysis_task():
            """Packaging function: Run analytical tasks backstage"""
            try:
                logger.info(f"[Background Task]{task_id}")
                logger.info(f"📝 [BackgroundTask] task_id={task_id}, user_id={user_id}")
                logger.info(f"📝 [BackgroundTask] request={request}")

                #Reaccess service examples to ensure correct context
                logger.info(f"[Background Task]")
                service = get_simple_analysis_service()
                logger.info(f"[Background Task]{id(service)}")

                logger.info(f"- [Background Task]")
                await service.execute_analysis_background(
                    task_id,
                    user_id,
                    request
                )
                logger.info(f"[Background Task]{task_id}")
            except Exception as e:
                logger.error(f"[Background Task]{task_id}, Error:{e}", exc_info=True)

        #Use Background Tasks to perform a walk job
        background_tasks.add_task(run_analysis_task)

        logger.info(f"The analysis mission has been launched in the background:{result}")

        return {
            "success": True,
            "data": result,
            "message": "分析任务已在后台启动"
        }
    except Exception as e:
        logger.error(f"❌ failed to submit a single unit analysis mission:{e}")
        raise HTTPException(status_code=400, detail=str(e))


#Test route - Verify that route is correctly registered
@router.get("/test-route")
async def test_route():
    """Tests if route is working"""
    logger.info("Test route has been transferred!")
    return {"message": "测试路由工作正常", "timestamp": time.time()}

@router.get("/tasks/{task_id}/status", response_model=Dict[str, Any])
async def get_task_status_new(
    task_id: str,
    user: dict = Depends(get_current_user)
):
    """Get analytical task status (new step realization)"""
    try:
        logger.info(f"[NEW ROUTE]{task_id}")
        logger.info(f"[NEW ROUTE]{user}")

        analysis_service = get_simple_analysis_service()
        logger.info(f"[NEW ROUTE]{id(analysis_service)}")

        result = await analysis_service.get_task_status(task_id)
        logger.info(f"[NEW ROUTE]{result is not None}")

        if result:
            return {
                "success": True,
                "data": result,
                "message": "任务状态获取成功"
            }
        else:
            #No memory found, trying to find from MongoDB
            logger.info(f"[STATUS] RAM was not found, trying to find from MongoDB:{task_id}")

            from app.core.database import get_mongo_db_async
            db = get_mongo_db_async()

            #First look from anallysis tasks collection (ongoing tasks)
            task_result = await db.analysis_tasks.find_one({"task_id": task_id})

            if task_result:
                logger.info(f"[STATUS]{task_id}")

                #Construct state response (ongoing task)
                status = task_result.get("status", "pending")
                progress = task_result.get("progress", 0)

                #Calculating Time Information
                start_time = task_result.get("started_at") or task_result.get("created_at")
                current_time = datetime.utcnow()
                elapsed_time = 0
                if start_time:
                    elapsed_time = (current_time - start_time).total_seconds()

                status_data = {
                    "task_id": task_id,
                    "status": status,
                    "progress": progress,
                    "message": f"任务{status}中...",
                    "current_step": status,
                    "start_time": start_time,
                    "end_time": task_result.get("completed_at"),
                    "elapsed_time": elapsed_time,
                    "remaining_time": 0,  #Unable to estimate accurately
                    "estimated_total_time": 0,
                    "symbol": task_result.get("symbol") or task_result.get("stock_code"),
                    "stock_code": task_result.get("symbol") or task_result.get("stock_code"),  #Compatible Fields
                    "stock_symbol": task_result.get("symbol") or task_result.get("stock_code"),
                    "source": "mongodb_tasks"  #Mark data sources
                }

                return {
                    "success": True,
                    "data": status_data,
                    "message": "任务状态获取成功（从任务记录恢复）"
                }

            #If analysis tasks is not found, find from analysis reports collection (work done)
            mongo_result = await db.analysis_reports.find_one({"task_id": task_id})

            if mongo_result:
                logger.info(f"[STATUS]{task_id}")

                #Construct state response (simulate completed tasks)
                #Compute time information for completed tasks
                start_time = mongo_result.get("created_at")
                end_time = mongo_result.get("updated_at")
                elapsed_time = 0
                if start_time and end_time:
                    elapsed_time = (end_time - start_time).total_seconds()

                status_data = {
                    "task_id": task_id,
                    "status": "completed",
                    "progress": 100,
                    "message": "分析完成（从历史记录恢复）",
                    "current_step": "completed",
                    "start_time": start_time,
                    "end_time": end_time,
                    "elapsed_time": elapsed_time,
                    "remaining_time": 0,
                    "estimated_total_time": elapsed_time,  #The total time taken to complete the task is the time taken.
                    "stock_code": mongo_result.get("stock_symbol"),
                    "stock_symbol": mongo_result.get("stock_symbol"),
                    "analysts": mongo_result.get("analysts", []),
                    "research_depth": mongo_result.get("research_depth", "快速"),
                    "source": "mongodb_reports"  #Mark data sources
                }

                return {
                    "success": True,
                    "data": status_data,
                    "message": "任务状态获取成功（从历史记录恢复）"
                }
            else:
                logger.warning(f"[STATUS]{task_id} trace={task_id}")
                raise HTTPException(status_code=404, detail="任务不存在")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tasks/{task_id}/result", response_model=Dict[str, Any])
async def get_task_result(
    task_id: str,
    user: dict = Depends(get_current_user)
):
    """Get analytical mission results"""
    try:
        logger.info(f"[RESULT]{task_id}")
        logger.info(f"[RESULT] User:{user}")

        analysis_service = get_simple_analysis_service()
        task_status = await analysis_service.get_task_status(task_id)

        result_data = None

        if task_status and task_status.get('status') == 'completed':
            #Get result data from memory
            result_data = task_status.get('result_data')
            logger.info(f"[RESULT]")

            #Debugging: Checking the data structure in memory
            if result_data:
                logger.info(f"[RESULT] Memory data keys:{list(result_data.keys())}")
                logger.info(f"[RESULT]{bool(result_data.get('decision'))}")
                logger.info(f"[RESULT]{len(result_data.get('summary', ''))}")
                logger.info(f"[RESULT] Recommissioning:{len(result_data.get('recommendation', ''))}")
                if result_data.get('decision'):
                    decision = result_data['decision']
                    logger.info(f"[RESULT] Memory content: action={decision.get('action')}, target_price={decision.get('target_price')}")
            else:
                logger.warning(f"[RESULT] Result data is empty")

        if not result_data:
            #No memory found, trying to find from MongoDB
            logger.info(f"[RESULT] not found in memory, trying to find from MongoDB:{task_id}")

            from app.core.database import get_mongo_db_async
            db = get_mongo_db_async()

            #Find from anallysis reports collection (prior to tag id matching)
            mongo_result = await db.analysis_reports.find_one({"task_id": task_id})

            if not mongo_result:
                #Compatibility with old data: old records may not be available, but analysis id exists in analysis tasks.result
                tasks_doc_for_id = await db.analysis_tasks.find_one({"task_id": task_id}, {"result.analysis_id": 1})
                analysis_id = tasks_doc_for_id.get("result", {}).get("analysis_id") if tasks_doc_for_id else None
                if analysis_id:
                    logger.info(f"[RESULT] Query under anallysis id reports:{analysis_id}")
                    mongo_result = await db.analysis_reports.find_one({"analysis_id": analysis_id})

            if mongo_result:
                logger.info(f"[RESULT]{task_id}")

                #Direct use of MongoDB data structure (consistent with web directory)
                result_data = {
                    "analysis_id": mongo_result.get("analysis_id"),
                    "stock_symbol": mongo_result.get("stock_symbol"),
                    "stock_code": mongo_result.get("stock_symbol"),  #Compatibility
                    "analysis_date": mongo_result.get("analysis_date"),
                    "summary": mongo_result.get("summary", ""),
                    "recommendation": mongo_result.get("recommendation", ""),
                    "confidence_score": mongo_result.get("confidence_score", 0.0),
                    "risk_level": mongo_result.get("risk_level", "中等"),
                    "key_points": mongo_result.get("key_points", []),
                    "execution_time": mongo_result.get("execution_time", 0),
                    "tokens_used": mongo_result.get("tokens_used", 0),
                    "analysts": mongo_result.get("analysts", []),
                    "research_depth": mongo_result.get("research_depth", "快速"),
                    "reports": mongo_result.get("reports", {}),
                    "created_at": mongo_result.get("created_at"),
                    "updated_at": mongo_result.get("updated_at"),
                    "status": mongo_result.get("status", "completed"),
                    "decision": mongo_result.get("decision", {}),
                    "source": "mongodb"  #Mark data sources
                }

                #Add Debug Information
                logger.info(f"[RESULT] MongoDB data structure:{list(result_data.keys())}")
                logger.info(f"[RESULT] MongoDB length:{len(result_data['summary'])}")
                logger.info(f"[RESULT] MongoDB length:{len(result_data['recommendation'])}")
                logger.info(f"[RESULT] MongoDB field:{bool(result_data.get('decision'))}")
                if result_data.get('decision'):
                    decision = result_data['decision']
                    logger.info(f"[RESULT] MongoDB content: action={decision.get('action')}, target_price={decision.get('target_price')}, confidence={decision.get('confidence')}")
            else:
                #Bottom: result field in analysis tasks collection
                tasks_doc = await db.analysis_tasks.find_one(
                    {"task_id": task_id},
                    {"result": 1, "symbol": 1, "stock_code": 1, "created_at": 1, "completed_at": 1}
                )
                if tasks_doc and tasks_doc.get("result"):
                    r = tasks_doc["result"] or {}
                    logger.info("[RESULT]")
                    #Get stock code (prefer symbol)
                    symbol = (tasks_doc.get("symbol") or tasks_doc.get("stock_code") or
                             r.get("stock_symbol") or r.get("stock_code"))
                    result_data = {
                        "analysis_id": r.get("analysis_id"),
                        "stock_symbol": symbol,
                        "stock_code": symbol,  #Compatible Fields
                        "analysis_date": r.get("analysis_date"),
                        "summary": r.get("summary", ""),
                        "recommendation": r.get("recommendation", ""),
                        "confidence_score": r.get("confidence_score", 0.0),
                        "risk_level": r.get("risk_level", "中等"),
                        "key_points": r.get("key_points", []),
                        "execution_time": r.get("execution_time", 0),
                        "tokens_used": r.get("tokens_used", 0),
                        "analysts": r.get("analysts", []),
                        "research_depth": r.get("research_depth", "快速"),
                        "reports": r.get("reports", {}),
                        "state": r.get("state", {}),
                        "detailed_analysis": r.get("detailed_analysis", {}),
                        "created_at": tasks_doc.get("created_at"),
                        "updated_at": tasks_doc.get("completed_at"),
                        "status": r.get("status", "completed"),
                        "decision": r.get("decision", {}),
                        "source": "analysis_tasks"  #Data source tags
                    }

        if not result_data:
            logger.warning(f"[RESULT] None of the data sources found results:{task_id}")
            raise HTTPException(status_code=404, detail="分析结果不存在")

        if not result_data:
            raise HTTPException(status_code=404, detail="分析结果不存在")

        #Process reports fields - if no reports fields are available, try loading them first from the file system and then extract them from the state
        if 'reports' not in result_data or not result_data['reports']:
            import os
            from pathlib import Path

            stock_symbol = result_data.get('stock_symbol') or result_data.get('stock_code')
            #anallysis date may be a date or time stamp string, only part of the date here
            analysis_date_raw = result_data.get('analysis_date')
            analysis_date = str(analysis_date_raw)[:10] if analysis_date_raw else None

            loaded_reports = {}
            try:
                #1) Try to read from a location specified by TRADINGAGAGENTS RESULTS DIR
                base_env = os.getenv('TRADINGAGENTS_RESULTS_DIR')
                project_root = Path.cwd()
                if base_env:
                    base_path = Path(base_env)
                    if not base_path.is_absolute():
                        base_path = project_root / base_env
                else:
                    base_path = project_root / 'results'

                candidate_dirs = []
                if stock_symbol and analysis_date:
                    candidate_dirs.append(base_path / stock_symbol / analysis_date / 'reports')
                #2) Compatible with other save paths
                if stock_symbol and analysis_date:
                    candidate_dirs.append(project_root / 'data' / 'analysis_results' / stock_symbol / analysis_date / 'reports')
                    candidate_dirs.append(project_root / 'data' / 'analysis_results' / 'detailed' / stock_symbol / analysis_date / 'reports')

                for d in candidate_dirs:
                    if d.exists() and d.is_dir():
                        for f in d.glob('*.md'):
                            try:
                                content = f.read_text(encoding='utf-8')
                                if content and content.strip():
                                    loaded_reports[f.stem] = content.strip()
                            except Exception:
                                pass
                if loaded_reports:
                    result_data['reports'] = loaded_reports
                    #Try to complete from the same name report if the Summary / notification is missing
                    if not result_data.get('summary') and loaded_reports.get('summary'):
                        result_data['summary'] = loaded_reports.get('summary')
                    if not result_data.get('recommendation') and loaded_reports.get('recommendation'):
                        result_data['recommendation'] = loaded_reports.get('recommendation')
                    logger.info(f"[RESULT]{len(loaded_reports)}Reports:{list(loaded_reports.keys())}")
            except Exception as fs_err:
                logger.warning(f"[RESULT] Failed to load report from file system:{fs_err}")

            if 'reports' not in result_data or not result_data['reports']:
                logger.info(f"[RESULT] reports field missing, trying to extract from state")

                #Extract report from state
                reports = {}
                state = result_data.get('state', {})

                if isinstance(state, dict):
                    #Define all possible reporting fields
                    report_fields = [
                        'market_report',
                        'sentiment_report',
                        'news_report',
                        'fundamentals_report',
                        'investment_plan',
                        'trader_investment_plan',
                        'final_trade_decision'
                    ]

                    #Extract report from state
                    for field in report_fields:
                        value = state.get(field, "")
                        if isinstance(value, str) and len(value.strip()) > 10:
                            reports[field] = value.strip()

                    #Addressing the status of the research team debate report
                    investment_debate_state = state.get('investment_debate_state', {})
                    if isinstance(investment_debate_state, dict):
                        #Extracting the history of multiple researchers
                        bull_content = investment_debate_state.get('bull_history', "")
                        if isinstance(bull_content, str) and len(bull_content.strip()) > 10:
                            reports['bull_researcher'] = bull_content.strip()

                        #Extracting the history of empty researchers
                        bear_content = investment_debate_state.get('bear_history', "")
                        if isinstance(bear_content, str) and len(bear_content.strip()) > 10:
                            reports['bear_researcher'] = bear_content.strip()

                        #Decision-making by extracting research managers
                        judge_decision = investment_debate_state.get('judge_decision', "")
                        if isinstance(judge_decision, str) and len(judge_decision.strip()) > 10:
                            reports['research_team_decision'] = judge_decision.strip()

                    #Process risk management team debate status report
                    risk_debate_state = state.get('risk_debate_state', {})
                    if isinstance(risk_debate_state, dict):
                        #Extracting the history of radical analysts
                        risky_content = risk_debate_state.get('risky_history', "")
                        if isinstance(risky_content, str) and len(risky_content.strip()) > 10:
                            reports['risky_analyst'] = risky_content.strip()

                        #Extract conservative analyst history
                        safe_content = risk_debate_state.get('safe_history', "")
                        if isinstance(safe_content, str) and len(safe_content.strip()) > 10:
                            reports['safe_analyst'] = safe_content.strip()

                        #Extract neutral analyst history
                        neutral_content = risk_debate_state.get('neutral_history', "")
                        if isinstance(neutral_content, str) and len(neutral_content.strip()) > 10:
                            reports['neutral_analyst'] = neutral_content.strip()

                        #Decision-making by Portfolio Manager
                        risk_decision = risk_debate_state.get('judge_decision', "")
                        if isinstance(risk_decision, str) and len(risk_decision.strip()) > 10:
                            reports['risk_management_decision'] = risk_decision.strip()

                    logger.info(f"[RESULT]{len(reports)}Reports:{list(reports.keys())}")
                    result_data['reports'] = reports
                else:
                    logger.warning(f"[RESULT] state field is not a dictionary type:{type(state)}")

        #Ensure that all contents in reports fields are string type
        if 'reports' in result_data and result_data['reports']:
            reports = result_data['reports']
            if isinstance(reports, dict):
                #Ensure that each report is string and not empty
                cleaned_reports = {}
                for key, value in reports.items():
                    if isinstance(value, str) and value.strip():
                        #Make sure the string is not empty
                        cleaned_reports[key] = value.strip()
                    elif value is not None:
                        #Convert to string if not string
                        str_value = str(value).strip()
                        if str_value:  #Save only non-empty strings
                            cleaned_reports[key] = str_value
                    #Skip the report if the value is notone or empty string

                result_data['reports'] = cleaned_reports
                logger.info(f"[RESULT] Clean up reports field, including{len(cleaned_reports)}Effective report")

                #Set to empty words if cleanup is not reported General
                if not cleaned_reports:
                    logger.warning(f"[RESULT] No valid reports after cleanup")
                    result_data['reports'] = {}
            else:
                logger.warning(f"[RESULT] returns fields are not dictionary types:{type(reports)}")
                result_data['reports'] = {}

        #Complete key fields: recommission/summary/key points
        try:
            reports = result_data.get('reports', {}) or {}
            decision = result_data.get('decision', {}) or {}

            #Decision-making in summary or report
            if not result_data.get('recommendation'):
                rec_candidates = []
                if isinstance(decision, dict) and decision.get('action'):
                    parts = [
                        f"操作: {decision.get('action')}",
                        f"目标价: {decision.get('target_price')}" if decision.get('target_price') else None,
                        f"置信度: {decision.get('confidence')}" if decision.get('confidence') is not None else None
                    ]
                    rec_candidates.append("；".join([p for p in parts if p]))
                #From the report.
                for k in ['final_trade_decision', 'investment_plan']:
                    v = reports.get(k)
                    if isinstance(v, str) and len(v.strip()) > 10:
                        rec_candidates.append(v.strip())
                if rec_candidates:
                    #Maximum amount of information (maximum)
                    result_data['recommendation'] = max(rec_candidates, key=len)[:2000]

            #Summary generated from several reports
            if not result_data.get('summary'):
                sum_candidates = []
                for k in ['market_report', 'fundamentals_report', 'sentiment_report', 'news_report']:
                    v = reports.get(k)
                    if isinstance(v, str) and len(v.strip()) > 50:
                        sum_candidates.append(v.strip())
                if sum_candidates:
                    result_data['summary'] = ("\n\n".join(sum_candidates))[:3000]

            #Key points Bottom
            if not result_data.get('key_points'):
                kp = []
                if isinstance(decision, dict):
                    if decision.get('action'):
                        kp.append(f"操作建议: {decision.get('action')}")
                    if decision.get('target_price'):
                        kp.append(f"目标价: {decision.get('target_price')}")
                    if decision.get('confidence') is not None:
                        kp.append(f"置信度: {decision.get('confidence')}")
                #Intercept the first few points from the reports
                for k in ['investment_plan', 'final_trade_decision']:
                    v = reports.get(k)
                    if isinstance(v, str) and len(v.strip()) > 10:
                        kp.append(v.strip()[:120])
                if kp:
                    result_data['key_points'] = kp[:5]
        except Exception as fill_err:
            logger.warning(f"There was an error completing the key field:{fill_err}")


        #Further: extrapolated and completed from detailed analysis
        try:
            if not result_data.get('summary') or not result_data.get('recommendation') or not result_data.get('reports'):
                da = result_data.get('detailed_analysis')
                #If reports are still empty, insert an original detailed analysis to facilitate the front-end "see report details"
                if (not result_data.get('reports')) and isinstance(da, str) and len(da.strip()) > 20:
                    result_data['reports'] = {'detailed_analysis': da.strip()}
                elif (not result_data.get('reports')) and isinstance(da, dict) and da:
                    #Place long dictionary text entries in reports
                    extracted = {}
                    for k, v in da.items():
                        if isinstance(v, str) and len(v.strip()) > 20:
                            extracted[k] = v.strip()
                    if extracted:
                        result_data['reports'] = extracted

                #Completing Summary
                if not result_data.get('summary'):
                    if isinstance(da, str) and da.strip():
                        result_data['summary'] = da.strip()[:3000]
                    elif isinstance(da, dict) and da:
                        #Take the longest text as summary
                        texts = [v.strip() for v in da.values() if isinstance(v, str) and v.strip()]
                        if texts:
                            result_data['summary'] = max(texts, key=len)[:3000]

                #Completing
                if not result_data.get('recommendation'):
                    rec = None
                    if isinstance(da, str):
                        #A simple keyword-based extraction of paragraphs containing recommendations
                        import re
                        m = re.search(r'(投资建议|建议|结论)[:：]?\s*(.+)', da)
                        if m:
                            rec = m.group(0)
                    elif isinstance(da, dict):
                        for key in ['final_trade_decision', 'investment_plan', '结论', '建议']:
                            v = da.get(key)
                            if isinstance(v, str) and len(v.strip()) > 10:
                                rec = v.strip()
                                break
                    if rec:
                        result_data['recommendation'] = rec[:2000]
        except Exception as da_err:
            logger.warning(f"[RESULT]{da_err}")

        #Strict data formatting and validation
        def safe_string(value, default=""):
            """Convert safely to string"""
            if value is None:
                return default
            if isinstance(value, str):
                return value
            return str(value)

        def safe_number(value, default=0):
            """Convert safely to a number"""
            if value is None:
                return default
            if isinstance(value, (int, float)):
                return value
            try:
                return float(value)
            except (ValueError, TypeError):
                return default

        def safe_list(value, default=None):
            """Convert safely to List"""
            if default is None:
                default = []
            if value is None:
                return default
            if isinstance(value, list):
                return value
            return default

        def safe_dict(value, default=None):
            """Convert safely to dictionary"""
            if default is None:
                default = {}
            if value is None:
                return default
            if isinstance(value, dict):
                return value
            return default

        #Debugging: Check the rest data before the final build
        logger.info(f"[FINAL] Before building the final result, result data:{list(result_data.keys())}")
        logger.info(f"[FINAL] Result data has a description:{bool(result_data.get('decision'))}")
        if result_data.get('decision'):
            logger.info(f"[FINAL] content:{result_data['decision']}")

        #Build strictly validated result data
        final_result_data = {
            "analysis_id": safe_string(result_data.get("analysis_id"), "unknown"),
            "stock_symbol": safe_string(result_data.get("stock_symbol"), "UNKNOWN"),
            "stock_code": safe_string(result_data.get("stock_code"), "UNKNOWN"),
            "analysis_date": safe_string(result_data.get("analysis_date"), "2025-08-20"),
            "summary": safe_string(result_data.get("summary"), "分析摘要暂无"),
            "recommendation": safe_string(result_data.get("recommendation"), "投资建议暂无"),
            "confidence_score": safe_number(result_data.get("confidence_score"), 0.0),
            "risk_level": safe_string(result_data.get("risk_level"), "中等"),
            "key_points": safe_list(result_data.get("key_points")),
            "execution_time": safe_number(result_data.get("execution_time"), 0),
            "tokens_used": safe_number(result_data.get("tokens_used"), 0),
            "analysts": safe_list(result_data.get("analysts")),
            "research_depth": safe_string(result_data.get("research_depth"), "快速"),
            "detailed_analysis": safe_dict(result_data.get("detailed_analysis")),
            "state": safe_dict(result_data.get("state")),
            #Critical fixation: add the decision field!
            "decision": safe_dict(result_data.get("decision"))
        }

        #Special handling of reports fields - ensure that every report is a valid string
        reports_data = safe_dict(result_data.get("reports"))
        validated_reports = {}

        for report_key, report_content in reports_data.items():
            #Make sure the report key is a string
            safe_key = safe_string(report_key, "unknown_report")

            #Ensure that reporting is not empty string
            if report_content is None:
                validated_content = "报告内容暂无"
            elif isinstance(report_content, str):
                validated_content = report_content.strip() if report_content.strip() else "报告内容为空"
            else:
                validated_content = str(report_content).strip() if str(report_content).strip() else "报告内容格式错误"

            validated_reports[safe_key] = validated_content

        final_result_data["reports"] = validated_reports

        logger.info(f"[RESULT]{task_id}")
        logger.info(f"[RESULT]{len(final_result_data.get('reports', {}))}Report")

        #Debugging: Checking data for final return
        logger.info(f"[FINAL] Final return data keys:{list(final_result_data.keys())}")
        logger.info(f"[FINAL] There's finally a deciment:{bool(final_result_data.get('decision'))}")
        if final_result_data.get('decision'):
            logger.info(f"[FINAL] Final content:{final_result_data['decision']}")

        return {
            "success": True,
            "data": final_result_data,
            "message": "分析结果获取成功"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[RESULT]{e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/tasks/all", response_model=Dict[str, Any])
async def list_all_tasks(
    user: dict = Depends(get_current_user),
    status: Optional[str] = Query(None, description="任务状态过滤"),
    limit: int = Query(20, ge=1, le=100, description="返回数量限制"),
    offset: int = Query(0, ge=0, description="偏移量")
):
    """Other Organiser"""
    try:
        logger.info(f"Other Organiser")

        tasks = await get_simple_analysis_service().list_all_tasks(
            status=status,
            limit=limit,
            offset=offset
        )

        return {
            "success": True,
            "data": {
                "tasks": tasks,
                "total": len(tasks),
                "limit": limit,
                "offset": offset
            },
            "message": "任务列表获取成功"
        }

    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tasks", response_model=Dict[str, Any])
async def list_user_tasks(
    user: dict = Depends(get_current_user),
    status: Optional[str] = Query(None, description="任务状态过滤"),
    limit: int = Query(20, ge=1, le=100, description="返回数量限制"),
    offset: int = Query(0, ge=0, description="偏移量")
):
    """Other Organiser"""
    try:
        logger.info(f"Other Organiser{user['id']}")

        tasks = await get_simple_analysis_service().list_user_tasks(
            user_id=user["id"],
            status=status,
            limit=limit,
            offset=offset
        )

        return {
            "success": True,
            "data": {
                "tasks": tasks,
                "total": len(tasks),
                "limit": limit,
                "offset": offset
            },
            "message": "任务列表获取成功"
        }

    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/batch", response_model=Dict[str, Any])
async def submit_batch_analysis(
    request: BatchAnalysisRequest,
    user: dict = Depends(get_current_user)
):
    """Submission of batch analysis tasks (real concurrent execution)

    Attention: Don't use Background Tasks because it's carried out in a string!
    Change to asyncio.create task for real simultaneous execution.
    """
    try:
        logger.info(f"[volume analysis]{request.title}")

        simple_service = get_simple_analysis_service()
        batch_id = str(uuid.uuid4())
        task_ids: List[str] = []
        mapping: List[Dict[str, str]] = []

        #Retrieving list of stock codes (old field compatible)
        stock_symbols = request.get_symbols()
        logger.info(f"[Bulk Analysis] List of stock codes:{stock_symbols}")

        #Validate list of stock codes
        if not stock_symbols:
            raise ValueError("股票代码列表不能为空")

        #(up to 10)
        MAX_BATCH_SIZE = 10
        if len(stock_symbols) > MAX_BATCH_SIZE:
            raise ValueError(f"批量分析最多支持 {MAX_BATCH_SIZE} 个股票，当前提交了 {len(stock_symbols)} 个")

        #Create single share analysis task for each stock
        for i, symbol in enumerate(stock_symbols):
            logger.info(f"[volume analysis]{i+1}/{len(stock_symbols)}Mission:{symbol}")

            single_req = SingleAnalysisRequest(
                symbol=symbol,
                stock_code=symbol,  #Compatible Fields
                parameters=request.parameters
            )

            try:
                create_res = await simple_service.create_analysis_task(user["id"], single_req)
                task_id = create_res.get("task_id")
                if not task_id:
                    raise RuntimeError(f"创建任务失败：未返回task_id (symbol={symbol})")
                task_ids.append(task_id)
                mapping.append({"symbol": symbol, "stock_code": symbol, "task_id": task_id})
                logger.info(f"[volume analysis] Created task:{task_id} - {symbol}")
            except Exception as create_error:
                logger.error(f"❌ [Bulk analysis] Create job failed:{symbol}, Error:{create_error}", exc_info=True)
                raise

        #Use asyncio. Create task to achieve true simultaneous implementation
        #Don't use Background Tasks because it's a serial execution.
        async def run_concurrent_analysis():
            """And all the analytical tasks."""
            tasks = []
            for i, symbol in enumerate(stock_symbols):
                task_id = task_ids[i]
                single_req = SingleAnalysisRequest(
                    symbol=symbol,
                    stock_code=symbol,
                    parameters=request.parameters
                )

                #Create a different task
                async def run_single_analysis(tid: str, req: SingleAnalysisRequest, uid: str):
                    try:
                        logger.info(f"[Submission]{tid} - {req.stock_code}")
                        await simple_service.execute_analysis_background(tid, uid, req)
                        logger.info(f"Implementation of:{tid}")
                    except Exception as e:
                        logger.error(f"[Same mission]{tid}, Error:{e}", exc_info=True)

                #Other Organiser
                task = asyncio.create_task(run_single_analysis(task_id, single_req, user["id"]))
                tasks.append(task)
                logger.info(f"✅ [volume analysis] Created and given missions:{task_id} - {symbol}")

            #Waiting for all tasks to be completed (no resistance)
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info(f"All missions completed: watch id={batch_id}")

        #Starting and delivering tasks backstage (not awaiting completion)
        asyncio.create_task(run_concurrent_analysis())
        logger.info(f"[volume analysis]{len(task_ids)}A side mission.")

        return {
            "success": True,
            "data": {
                "batch_id": batch_id,
                "total_tasks": len(task_ids),
                "task_ids": task_ids,
                "mapping": mapping,
                "status": "submitted"
            },
            "message": f"批量分析任务已提交，共{len(task_ids)}个股票，正在并发执行"
        }
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))

#Compatibility: retain original endpoint
@router.post("/analyze")
async def analyze_single(
    req: SingleAnalyzeRequest,
    user: dict = Depends(get_current_user),
    svc: QueueService = Depends(get_queue_service)
):
    """Single unit analysis (compatibility endpoints)"""
    try:
        task_id = await svc.enqueue_task(
            user_id=user["id"],
            symbol=req.symbol,
            params=req.parameters
        )
        return {"task_id": task_id, "status": "queued"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/analyze/batch")
async def analyze_batch(
    req: BatchAnalyzeRequest,
    user: dict = Depends(get_current_user),
    svc: QueueService = Depends(get_queue_service)
):
    """Batch analysis (compatibility endpoints)"""
    try:
        batch_id, submitted = await svc.create_batch(
            user_id=user["id"],
            symbols=req.symbols,
            params=req.parameters
        )
        return {"batch_id": batch_id, "submitted": submitted}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/batches/{batch_id}")
async def get_batch(batch_id: str, user: dict = Depends(get_current_user), svc: QueueService = Depends(get_queue_service)):
    b = await svc.get_batch(batch_id)
    if not b or b.get("user") != user["id"]:
        raise HTTPException(status_code=404, detail="batch not found")
    return b

#Tasks and batch queries peer
#Note: This route has been moved to /tasks/  FT 0 /status to avoid conflict.
# @router.get("/tasks/{task_id}")
# async def get_task(
#     task_id: str,
#     user: dict = Depends(get_current_user),
#     svc: QueueService = Depends(get_queue_service)
# ):
#"Get Task Details."
#     t = await svc.get_task(task_id)
#     if not t or t.get("user") != user["id"]:
#Rice HTTPException (status code=404, detail= "no mission exists")
#     return t

#The original path has been replaced by a new one.
# @router.get("/tasks/{task_id}/status")
# async def get_task_status_old(
#     task_id: str,
#     user: dict = Depends(get_current_user)
# ):
#"Acquire mission status and progress."
#     try:
#         status = await get_analysis_service().get_task_status(task_id)
#         if not status:
#Rice HTTPException (status code=404, detail= "no mission exists")
#         return {
#             "success": True,
#             "data": status
#         }
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))

@router.post("/tasks/{task_id}/cancel")
async def cancel_task(
    task_id: str,
    user: dict = Depends(get_current_user),
    svc: QueueService = Depends(get_queue_service)
):
    """Cancel Task"""
    try:
        #Authentication of task ownership
        task = await svc.get_task(task_id)
        if not task or task.get("user") != user["id"]:
            raise HTTPException(status_code=404, detail="任务不存在")

        success = await svc.cancel_task(task_id)
        if success:
            return {"success": True, "message": "任务已取消"}
        else:
            raise HTTPException(status_code=400, detail="取消任务失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/user/queue-status")
async def get_user_queue_status(
    user: dict = Depends(get_current_user),
    svc: QueueService = Depends(get_queue_service)
):
    """Get User Queue Status"""
    try:
        status = await svc.get_user_queue_status(user["id"])
        return {
            "success": True,
            "data": status
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/user/history")
async def get_user_analysis_history(
    user: dict = Depends(get_current_user),
    status: Optional[str] = Query(None, description="任务状态过滤"),
    start_date: Optional[str] = Query(None, description="开始日期，YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期，YYYY-MM-DD"),
    symbol: Optional[str] = Query(None, description="股票代码"),
    stock_code: Optional[str] = Query(None, description="股票代码(已废弃,使用symbol)"),
    market_type: Optional[str] = Query(None, description="市场类型"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页大小")
):
    """Access user analysis history (support base filter and page break)"""
    try:
        #Get user  Other Organiser
        raw_tasks = await get_simple_analysis_service().list_user_tasks(
            user_id=user["id"],
            status=status,
            limit=page_size,
            offset=(page - 1) * page_size
        )

        #Basic screening
        from datetime import datetime
        def in_date_range(t: Optional[str]) -> bool:
            if not t:
                return True
            try:
                dt = datetime.fromisoformat(t.replace('Z', '+00:00')) if 'Z' in t else datetime.fromisoformat(t)
            except Exception:
                return True
            ok = True
            if start_date:
                try:
                    ok = ok and (dt.date() >= datetime.fromisoformat(start_date).date())
                except Exception:
                    pass
            if end_date:
                try:
                    ok = ok and (dt.date() <= datetime.fromisoformat(end_date).date())
                except Exception:
                    pass
            return ok

        #Retrieving query stock codes (old compatible fields)
        query_symbol = symbol or stock_code

        filtered = []
        for x in raw_tasks:
            if query_symbol:
                task_symbol = x.get("symbol") or x.get("stock_code") or x.get("stock_symbol")
                if task_symbol not in [query_symbol]:
                    continue
            #Market type provisionally judged within parameters (if any)
            if market_type:
                params = x.get("parameters") or {}
                if params.get("market_type") != market_type:
                    continue
            #Timescale (use start time or created at)
            t = x.get("start_time") or x.get("created_at")
            if not in_date_range(t):
                continue
            filtered.append(x)

        return {
            "success": True,
            "data": {
                "tasks": filtered,
                "total": len(filtered),
                "page": page,
                "page_size": page_size
            },
            "message": "历史查询成功"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

#WebSocket Peer
@router.websocket("/ws/task/{task_id}")
async def websocket_task_progress(websocket: WebSocket, task_id: str):
    """WebSocket Endpoint: Get Task Progress Real Time"""
    import json
    websocket_manager = get_websocket_manager()

    try:
        await websocket_manager.connect(websocket, task_id)

        #Can not open message
        await websocket.send_text(json.dumps({
            "type": "connection_established",
            "task_id": task_id,
            "message": "WebSocket 连接已建立"
        }))

        #Keep Connection Active
        while True:
            try:
                #Receive heart beat messages from client
                data = await websocket.receive_text()
                #Can process messages sent by client
                logger.debug(f"WebSocket.{data}")
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.warning(f"WebSocket message processing error:{e}")
                break

    except WebSocketDisconnect:
        logger.info(f"WebSocket client disconnected:{task_id}")
    except Exception as e:
        logger.error(f"WebSocket connection error:{e}")
    finally:
        await websocket_manager.disconnect(websocket, task_id)

#Other Organiser
@router.get("/tasks/{task_id}/details")
async def get_task_details(
    task_id: str,
    user: dict = Depends(get_current_user),
    svc: QueueService = Depends(get_queue_service)
):
    """Get task details (use different paths to avoid conflict)"""
    t = await svc.get_task(task_id)
    if not t or t.get("user") != user["id"]:
        raise HTTPException(status_code=404, detail="任务不存在")
    return t


#== sync, corrected by elderman == @elder man

@router.get("/admin/zombie-tasks")
async def get_zombie_tasks(
    max_running_hours: int = Query(default=2, ge=1, le=72, description="最大运行时长（小时）"),
    user: dict = Depends(get_current_user)
):
    """Fetch Zombie Job List (administrator only)

    Zombie missions: long-term in procrastination/runing/ping
    """
    #Check administrator privileges
    if user.get("username") != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可访问")

    try:
        svc = get_simple_analysis_service()
        zombie_tasks = await svc.get_zombie_tasks(max_running_hours)

        return {
            "success": True,
            "data": zombie_tasks,
            "total": len(zombie_tasks),
            "max_running_hours": max_running_hours
        }
    except Exception as e:
        logger.error(f"The mission to get zombies failed:{e}")
        raise HTTPException(status_code=500, detail=f"获取僵尸任务失败: {str(e)}")


@router.post("/admin/cleanup-zombie-tasks")
async def cleanup_zombie_tasks(
    max_running_hours: int = Query(default=2, ge=1, le=72, description="最大运行时长（小时）"),
    user: dict = Depends(get_current_user)
):
    """Clean-up of zombie missions (administrators only)

    Could not close temporary folder: %s
    """
    #Check administrator privileges
    if user.get("username") != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可访问")

    try:
        svc = get_simple_analysis_service()
        result = await svc.cleanup_zombie_tasks(max_running_hours)

        return {
            "success": True,
            "data": result,
            "message": f"已清理 {result.get('total_cleaned', 0)} 个僵尸任务"
        }
    except Exception as e:
        logger.error(f"The mission failed:{e}")
        raise HTTPException(status_code=500, detail=f"清理僵尸任务失败: {str(e)}")


@router.post("/tasks/{task_id}/mark-failed")
async def mark_task_as_failed(
    task_id: str,
    user: dict = Depends(get_current_user)
):
    """Could not close temporary folder: %s

    For manual cleansing of stuck tasks
    """
    try:
        svc = get_simple_analysis_service()

        #Update Task Status in Memory
        from app.services.memory_state_manager import TaskStatus
        await svc.memory_manager.update_task_status(
            task_id=task_id,
            status=TaskStatus.FAILED,
            message="手动标记为失败",
            error_message="用户手动标记为失败"
        )

        #Update task status in MongoDB
        from app.core.database import get_mongo_db_async
        from datetime import datetime
        db = get_mongo_db_async()

        result = await db.analysis_tasks.update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "status": "failed",
                    "last_error": "用户手动标记为失败",
                    "completed_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }
            }
        )

        if result.modified_count > 0:
            logger.info(f"Mission{task_id}Marked as Failed")
            return {
                "success": True,
                "message": "任务已标记为失败"
            }
        else:
            logger.warning(f"Mission{task_id}Not found or failed")
            return {
                "success": True,
                "message": "任务未找到或已是失败状态"
            }
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=f"标记任务失败: {str(e)}")


@router.delete("/tasks/{task_id}")
async def delete_task(
    task_id: str,
    user: dict = Depends(get_current_user)
):
    """Other Organiser

    Remove task records from memory and database
    """
    try:
        svc = get_simple_analysis_service()

        #Remove Tasks From Memory
        await svc.memory_manager.remove_task(task_id)

        #Remove Tasks From MongoDB
        from app.core.database import get_mongo_db_async
        db = get_mongo_db_async()

        result = await db.analysis_tasks.delete_one({"task_id": task_id})

        if result.deleted_count > 0:
            logger.info(f"Mission{task_id}Deleted")
            return {
                "success": True,
                "message": "任务已删除"
            }
        else:
            logger.warning(f"Mission{task_id}Not found")
            return {
                "success": True,
                "message": "任务未找到"
            }
    except Exception as e:
        logger.error(f"Delete mission failed:{e}")
        raise HTTPException(status_code=500, detail=f"删除任务失败: {str(e)}")