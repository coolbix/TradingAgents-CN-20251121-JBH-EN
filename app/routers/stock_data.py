"""Stock data API route - based on extended data model
Provide standardized stock data access interfaces
"""
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import status

from app.routers.auth_db import get_current_user
from app.services.stock_data_service import get_stock_data_service
from app.models import (
    StockBasicInfoResponse,
    MarketQuotesResponse,
    StockListResponse,
    StockBasicInfoExtended,
    MarketQuotesExtended,
    MarketType
)

router = APIRouter(prefix="/api/stock-data", tags=["股票数据"])


@router.get("/basic-info/{symbol}", response_model=StockBasicInfoResponse)
async def get_stock_basic_info(
    symbol: str,
    current_user: dict = Depends(get_current_user)
):
    """Access to basic stock information

    Args:
        Symbol: Stock code

    Returns:
        StockBasicInfoResponse: Stock Basic Information with Extended Fields
    """
    try:
        service = get_stock_data_service()
        stock_info = await service.get_stock_basic_info(symbol)

        if not stock_info:
            return StockBasicInfoResponse(
                success=False,
                message=f"未找到股票代码 {symbol} 的基础信息"
            )

        return StockBasicInfoResponse(
            success=True,
            data=stock_info,
            message="获取成功"
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取股票基础信息失败: {str(e)}"
        )


@router.get("/quotes/{symbol}", response_model=MarketQuotesResponse)
async def get_market_quotes(
    symbol: str,
    current_user: dict = Depends(get_current_user)
):
    """Get Real Time Line Data

    Args:
        Symbol: Stock code

    Returns:
        MarketQuotesReponse: Real-time line data with extended fields
    """
    try:
        service = get_stock_data_service()
        quotes = await service.get_market_quotes(symbol)

        if not quotes:
            return MarketQuotesResponse(
                success=False,
                message=f"未找到股票代码 {symbol} 的行情数据"
            )

        return MarketQuotesResponse(
            success=True,
            data=quotes,
            message="获取成功"
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取实时行情失败: {str(e)}"
        )


@router.get("/list", response_model=StockListResponse)
async def get_stock_list(
    market: Optional[str] = Query(None, description="市场筛选"),
    industry: Optional[str] = Query(None, description="行业筛选"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页大小"),
    current_user: dict = Depends(get_current_user)
):
    """Get Stock List

    Args:
        market: Market Filter (optional)
        Industry filter (optional)
        Page: Page Number (from 1)
        page size: per page size (1-100)

    Returns:
        StockListResponse: Stocklist data
    """
    try:
        service = get_stock_data_service()
        stock_list = await service.get_stock_list(
            market=market,
            industry=industry,
            page=page,
            page_size=page_size
        )
        
        #Calculating the total (simplified and achieved, actually separately)
        total = len(stock_list)
        
        return StockListResponse(
            success=True,
            data=stock_list,
            total=total,
            page=page,
            page_size=page_size,
            message="获取成功"
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取股票列表失败: {str(e)}"
        )


@router.get("/combined/{symbol}")
async def get_combined_stock_data(
    symbol: str,
    current_user: dict = Depends(get_current_user)
):
    """Access to comprehensive stock data (basic information + real time lines)

    Args:
        symbol: stock code

    Returns:
        dict: Comprehensive data containing basic information and real time patterns
    """
    try:
        service = get_stock_data_service()

        #Parallel access to basic information and situational data
        import asyncio
        basic_info_task = service.get_stock_basic_info(symbol)
        quotes_task = service.get_market_quotes(symbol)

        basic_info, quotes = await asyncio.gather(
            basic_info_task,
            quotes_task,
            return_exceptions=True
        )

        #Deal with anomalies
        if isinstance(basic_info, Exception):
            basic_info = None
        if isinstance(quotes, Exception):
            quotes = None

        if not basic_info and not quotes:
            return {
                "success": False,
                "message": f"未找到股票代码 {symbol} 的任何数据"
            }

        return {
            "success": True,
            "data": {
                "basic_info": basic_info.dict() if basic_info else None,
                "quotes": quotes.dict() if quotes else None,
                "symbol": symbol,
                "timestamp": quotes.updated_at if quotes else None
            },
            "message": "获取成功"
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取股票综合数据失败: {str(e)}"
        )


@router.get("/search")
async def search_stocks(
    keyword: str = Query(..., min_length=1, description="搜索关键词"),
    limit: int = Query(10, ge=1, le=50, description="返回数量限制"),
    current_user: dict = Depends(get_current_user)
):
    """Search stocks

    Args:
        Keyword: Search for keywords (stock code or name)
        Limited number of returns

    Returns:
        dict: Search results
    """
    try:
        from app.core.database import get_mongo_db
        from app.core.unified_config import UnifiedConfigManager

        db = get_mongo_db()
        collection = db.stock_basic_info

        #Access source priority configuration
        config = UnifiedConfigManager()
        data_source_configs = await config.get_data_source_configs_async()

        #Extract enabled data sources in order of priority
        enabled_sources = [
            ds.type.lower() for ds in data_source_configs
            if ds.enabled and ds.type.lower() in ['tushare', 'akshare', 'baostock']
        ]

        if not enabled_sources:
            enabled_sources = ['tushare', 'akshare', 'baostock']

        preferred_source = enabled_sources[0] if enabled_sources else 'tushare'

        #Build search conditions
        search_conditions = []

        #If it's a six-digit number, match it by code.
        if keyword.isdigit() and len(keyword) == 6:
            search_conditions.append({"symbol": keyword})
        else:
            #Match by name blur
            search_conditions.append({"name": {"$regex": keyword, "$options": "i"}})
            #If you include numbers, try code matching.
            if any(c.isdigit() for c in keyword):
                search_conditions.append({"symbol": {"$regex": keyword}})

        #Add data source filter 🔥: only the highest priority data source is asked
        query = {
            "$and": [
                {"$or": search_conditions},
                {"source": preferred_source}
            ]
        }

        #Execute Search
        cursor = collection.find(query, {"_id": 0}).limit(limit)

        results = await cursor.to_list(length=limit)

        #Data standardization
        service = get_stock_data_service()
        standardized_results = []
        for doc in results:
            standardized_doc = service._standardize_basic_info(doc)
            standardized_results.append(standardized_doc)

        return {
            "success": True,
            "data": standardized_results,
            "total": len(standardized_results),
            "keyword": keyword,
            "source": preferred_source,  #Return data source
            "message": "搜索完成"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"搜索股票失败: {str(e)}"
        )


@router.get("/markets")
async def get_market_summary(
    current_user: dict = Depends(get_current_user)
):
    """Overview of access markets

    Returns:
        Dict: Stock count by market
    """
    try:
        from app.core.database import get_mongo_db

        db = get_mongo_db()
        collection = db.stock_basic_info

        #Number of stocks by market
        pipeline = [
            {
                "$group": {
                    "_id": "$market",
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {"count": -1}
            }
        ]

        cursor = collection.aggregate(pipeline)
        market_stats = await cursor.to_list(length=None)

        #Total statistics
        total_count = await collection.count_documents({})

        return {
            "success": True,
            "data": {
                "total_stocks": total_count,
                "market_breakdown": market_stats,
                "supported_markets": ["CN"],  #Current Supported Market
                "last_updated": None  #Update time from data
            },
            "message": "获取成功"
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取市场概览失败: {str(e)}"
        )


@router.get("/sync-status/quotes")
async def get_quotes_sync_status(
    current_user: dict = Depends(get_current_user)
):
    """Get Real Time Line Sync Status

    Returns:
        == sync, corrected by elderman ==
        "Message": "Access."
        ♪ I'm sorry ♪
    """
    try:
        from app.services.quotes_ingestion_service import QuotesIngestionService

        service = QuotesIngestionService()
        status_data = await service.get_sync_status()

        return {
            "success": True,
            "data": status_data,
            "message": "获取成功"
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取同步状态失败: {str(e)}"
        )
