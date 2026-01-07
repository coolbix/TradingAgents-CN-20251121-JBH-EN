"""
Multi-source synchronization API routes
Provides endpoints for multi-source stock data synchronization
"""
import asyncio
import logging
from typing import Dict, List, Optional, Any, Union
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.services.multi_source_basics_sync_service import get_multi_source_sync_service
from app.services.data_sources.manager import DataSourceManager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sync/multi-source", tags=["Multi-Source Sync"])


class SyncRequest(BaseModel):
    """Synchronise Request Model"""
    force: bool = False
    preferred_sources: Optional[List[str]] = None


class SyncResponse(BaseModel):
    """Synchronized Response Model"""
    success: bool
    message: str
    data: Union[Dict[str, Any], List[Any], Any]


class DataSourceStatus(BaseModel):
    """Data Source Status Model"""
    name: str
    priority: int
    available: bool
    description: str


@router.get("/sources/status")
async def get_data_sources_status():
    """Obtain status of all data sources"""
    try:
        manager = DataSourceManager()
        available_adapters = manager.get_available_adapters()
        all_adapters = manager.adapters

        status_list = []
        for adapter in all_adapters:
            is_available = adapter in available_adapters

            #Description according to data source type
            descriptions = {
                "tushare": "专业金融数据API，提供高质量的A股数据和财务指标",
                "akshare": "开源金融数据库，提供基础的股票信息",
                "baostock": "免费开源的证券数据平台，提供历史数据"
            }

            status_item = {
                "name": adapter.name,
                "priority": adapter.priority,
                "available": is_available,
                "description": descriptions.get(adapter.name, f"{adapter.name}数据源")
            }

            #Add Token Source Information (Tushare only)
            if adapter.name == "tushare" and is_available and hasattr(adapter, 'get_token_source'):
                token_source = adapter.get_token_source()
                if token_source:
                    status_item["token_source"] = token_source
                    if token_source == 'database':
                        status_item["description"] += " (Token来源: 数据库)"
                    elif token_source == 'env':
                        status_item["description"] += " (Token来源: .env)"

            status_list.append(status_item)

        return SyncResponse(
            success=True,
            message="Data sources status retrieved successfully",
            data=status_list
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get data sources status: {str(e)}")


@router.get("/sources/current")
async def get_current_data_source():
    """Access to data sources currently in use (highest priority available)"""
    try:
        manager = DataSourceManager()
        available_adapters = manager.get_available_adapters()

        if not available_adapters:
            return SyncResponse(
                success=False,
                message="No available data sources",
                data={"name": None, "priority": None}
            )

        #Access to the highest-priority available data sources (higher priority figures)
        current_adapter = max(available_adapters, key=lambda x: x.priority)

        #Description according to data source type
        descriptions = {
            "tushare": "专业金融数据API",
            "akshare": "开源金融数据库",
            "baostock": "免费证券数据平台"
        }

        result = {
            "name": current_adapter.name,
            "priority": current_adapter.priority,
            "description": descriptions.get(current_adapter.name, current_adapter.name)
        }

        #Add Token Source Information (Tushare only)
        if current_adapter.name == "tushare" and hasattr(current_adapter, 'get_token_source'):
            token_source = current_adapter.get_token_source()
            if token_source:
                result["token_source"] = token_source
                if token_source == 'database':
                    result["token_source_display"] = "数据库配置"
                elif token_source == 'env':
                    result["token_source_display"] = ".env 配置"

        return SyncResponse(
            success=True,
            message="Current data source retrieved successfully",
            data=result
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get current data source: {str(e)}")


@router.get("/status")
async def get_sync_status():
    """Get Multidata Source Sync Status"""
    try:
        service = get_multi_source_sync_service()
        status = await service.get_status()
        
        return SyncResponse(
            success=True,
            message="Status retrieved successfully",
            data=status
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get sync status: {str(e)}")


@router.post("/stock_basics/run")
async def run_stock_basics_sync(
    force: bool = Query(False, description="是否强制运行同步"),
    preferred_sources: Optional[str] = Query(None, description="优先使用的数据源，用逗号分隔")
):
    """Run multiple data source stock base information sync"""
    try:
        service = get_multi_source_sync_service()

        #Parsing Priority Data Sources
        sources_list = None
        if preferred_sources and isinstance(preferred_sources, str):
            sources_list = [s.strip() for s in preferred_sources.split(",") if s.strip()]

        #Run Synchronization (Sync execution with 10 minutes running past the front end)
        result = await service.run_full_sync(force=force, preferred_sources=sources_list)

        #To judge success.
        success = result.get("status") in ["success", "success_with_errors"]
        message = "Synchronization completed successfully"

        if result.get("status") == "success_with_errors":
            message = f"Synchronization completed with {result.get('errors', 0)} errors"
        elif result.get("status") == "failed":
            message = f"Synchronization failed: {result.get('message', 'Unknown error')}"
            success = False
        elif result.get("status") == "running":
            message = "Synchronization is already running"

        return SyncResponse(
            success=success,
            message=message,
            data=result
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run synchronization: {str(e)}")


async def _test_single_adapter(adapter) -> dict:
    """Test the connectivity of individual data source adapters
    Lightweight connectivity tests only, not complete data
    """
    result = {
        "name": adapter.name,
        "priority": adapter.priority,
        "available": False,
        "message": "连接失败"
    }

    #Connectivity test timeout (sec)
    test_timeout = 10

    try:
        #Test connectivity - Force reconnection to use the latest configuration
        logger.info(f"Test{adapter.name}Connectivity{test_timeout}Seconds...")

        try:
            #For Tushare, mandatory reconnection to use the latest database configuration
            if adapter.name == "tushare" and hasattr(adapter, '_provider'):
                logger.info(f"Force{adapter.name}Reconnect to use the latest configuration...")
                provider = adapter._provider
                if provider:
                    #Reset Connection Status
                    provider.connected = False
                    provider.token_source = None
                    #Reconnect
                    await asyncio.wait_for(
                        asyncio.to_thread(provider.connect_sync),
                        timeout=test_timeout
                    )

            #Run in the online pool is aviable() check
            is_available = await asyncio.wait_for(
                asyncio.to_thread(adapter.is_available),
                timeout=test_timeout
            )

            if is_available:
                result["available"] = True

                #Get Token Source (Tushare only)
                token_source = None
                if adapter.name == "tushare" and hasattr(adapter, 'get_token_source'):
                    token_source = adapter.get_token_source()

                if token_source == 'database':
                    result["message"] = "✅ 连接成功 (Token来源: 数据库)"
                    result["token_source"] = "database"
                elif token_source == 'env':
                    result["message"] = "✅ 连接成功 (Token来源: .env)"
                    result["token_source"] = "env"
                else:
                    result["message"] = "✅ 连接成功"

                logger.info(f"✅ {adapter.name}Connectivity test successful. Token source:{token_source}")
            else:
                result["available"] = False
                result["message"] = "❌ 数据源不可用"
                logger.warning(f"⚠️ {adapter.name}Not Available")
        except asyncio.TimeoutError:
            result["available"] = False
            result["message"] = f"❌ 连接超时 ({test_timeout}秒)"
            logger.warning(f"⚠️ {adapter.name}Connection timed out")
        except Exception as e:
            result["available"] = False
            result["message"] = f"❌ 连接失败: {str(e)}"
            logger.error(f"❌ {adapter.name}Connection failed:{e}")

    except Exception as e:
        result["available"] = False
        result["message"] = f"❌ 测试异常: {str(e)}"
        logger.error(f"Test{adapter.name}Synchronising folder{e}")

    return result


class TestSourceRequest(BaseModel):
    """Test data source request"""
    source_name: str | None = None


@router.post("/test-sources")
async def test_data_sources(request: TestSourceRequest = TestSourceRequest()):
    """Test connectivity of data sources

    Parameters:
        - source name: Optional, specify the name of the data source to be tested. If not specified, test all data sources

    Lightweight connectivity tests only, not complete data
    - Test timeout: 10 seconds.
    - Get only one data check connection.
    - Rapid returns.
    """
    try:
        manager = DataSourceManager()
        all_adapters = manager.adapters

        #Fetch data source name from the request
        source_name = request.source_name
        logger.info(f"Request for testing received, source name={source_name}")

        #If a data source name is specified, only the data source is tested
        if source_name:
            adapters_to_test = [a for a in all_adapters if a.name.lower() == source_name.lower()]
            if not adapters_to_test:
                raise HTTPException(
                    status_code=400,
                    detail=f"Data source '{source_name}' not found"
                )
            logger.info(f"Start testing data sources:{source_name}")
        else:
            adapters_to_test = all_adapters
            logger.info(f"Start testing.{len(all_adapters)}Connectivity of a data source...")

        #Combination test adapter (executed in backstage thread)
        test_tasks = [_test_single_adapter(adapter) for adapter in adapters_to_test]
        test_results = await asyncio.gather(*test_tasks, return_exceptions=True)

        #Deal with anomalies
        final_results = []
        for i, result in enumerate(test_results):
            if isinstance(result, Exception):
                logger.error(f"Test adaptor{adapters_to_test[i].name}Synchronising folder{result}")
                final_results.append({
                    "name": adapters_to_test[i].name,
                    "priority": adapters_to_test[i].priority,
                    "available": False,
                    "message": f"❌ 测试异常: {str(result)}"
                })
            else:
                final_results.append(result)

        #Statistical results
        available_count = sum(1 for r in final_results if r.get("available"))
        if source_name:
            logger.info(f"Data source ✅{source_name}Test complete:{'Available' if available_count > 0 else 'Not Available'}")
        else:
            logger.info(f"Data source connectivity tests completed:{available_count}/{len(final_results)}Available")

        return SyncResponse(
            success=True,
            message=f"Tested {len(final_results)} data sources, {available_count} available",
            data={"test_results": final_results}
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error testing data source:{e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to test data sources: {str(e)}")


@router.get("/recommendations")
async def get_sync_recommendations():
    """Recommendations for accessing data sources"""
    try:
        manager = DataSourceManager()
        available_adapters = manager.get_available_adapters()
        
        recommendations = {
            "primary_source": None,
            "fallback_sources": [],
            "suggestions": [],
            "warnings": []
        }
        
        if available_adapters:
            #Recommended the highest priority available data source as the primary data source
            primary = available_adapters[0]
            recommendations["primary_source"] = {
                "name": primary.name,
                "priority": primary.priority,
                "reason": "Highest priority available data source"
            }
            
            #Other available data sources as backup
            for adapter in available_adapters[1:]:
                recommendations["fallback_sources"].append({
                    "name": adapter.name,
                    "priority": adapter.priority
                })
        
        #Generate recommendations
        if not available_adapters:
            recommendations["warnings"].append("No data sources are available. Please check your configuration.")
        elif len(available_adapters) == 1:
            recommendations["suggestions"].append("Consider configuring additional data sources for redundancy.")
        else:
            recommendations["suggestions"].append(f"You have {len(available_adapters)} data sources available, which provides good redundancy.")
        
        #Recommendations for specific data sources
        tushare_available = any(a.name == "tushare" for a in available_adapters)
        if not tushare_available:
            recommendations["suggestions"].append("Consider configuring Tushare for the most comprehensive financial data.")
        
        return SyncResponse(
            success=True,
            message="Recommendations generated successfully",
            data=recommendations
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate recommendations: {str(e)}")


@router.get("/history")
async def get_sync_history(
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(10, ge=1, le=50, description="每页大小"),
    status: Optional[str] = Query(None, description="状态筛选")
):
    """Get Synchronized History"""
    try:
        from app.core.database import get_mongo_db_async
        db = get_mongo_db_async()

        #Build query conditions
        query = {"job": "stock_basics_multi_source"}
        if status:
            query["status"] = status

        #Calculating Skipped Records
        skip = (page - 1) * page_size

        #Query History
        cursor = db.sync_status.find(query).sort("started_at", -1).skip(skip).limit(page_size)
        history_records = await cursor.to_list(length=page_size)

        #Total acquisitions
        total = await db.sync_status.count_documents(query)

        #Clear  id fields in the record
        for record in history_records:
            record.pop("_id", None)

        return SyncResponse(
            success=True,
            message="History retrieved successfully",
            data={
                "records": history_records,
                "total": total,
                "page": page,
                "page_size": page_size,
                "has_more": skip + len(history_records) < total
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get sync history: {str(e)}")


@router.delete("/cache")
async def clear_sync_cache():
    """Empty associated caches"""
    try:
        service = get_multi_source_sync_service()

        #Empty Sync Status Cache
        cleared_items = 0

        #1. Empty sync status
        try:
            from app.core.database import get_mongo_db_async
            db = get_mongo_db_async()

            #Remove Sync Status Record
            result = await db.sync_status.delete_many({"job": "stock_basics_multi_source"})
            cleared_items += result.deleted_count

            #Reset Service Status
            service._running = False

        except Exception as e:
            logger.warning(f"Failed to clear sync status cache: {e}")

        #2. Clearing data source caches, if any
        try:
            manager = DataSourceManager()
            #Here you can add the specific cache cleanup logic of the data source.
            #The current data source adapter does not have a persistent cache, so skip
        except Exception as e:
            logger.warning(f"Failed to clear data source cache: {e}")

        return SyncResponse(
            success=True,
            message=f"Cache cleared successfully, {cleared_items} items removed",
            data={"cleared": True, "items_cleared": cleared_items}
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to clear cache: {str(e)}")
