"""Multimarket Stock API Route
Support for a unified query interface for Unit A, the Port Unit and the United States Unit

Function:
1. Cross-market equity information search
2. Multi-data source priority queries
3. Harmonized response format

Path prefix: /api/markets
"""
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Depends, HTTPException, status, Query
import logging

from app.routers.auth_db import get_current_user
from app.core.database import get_mongo_db
from app.core.response import ok
from app.services.unified_stock_service import UnifiedStockService

logger = logging.getLogger("webapi")

router = APIRouter(prefix="/markets", tags=["multi-market"])


@router.get("", response_model=dict)
async def get_supported_markets(current_user: dict = Depends(get_current_user)):
    """List of markets to obtain support

Returns:
FMT 0,
I don't know.

♪ I'm sorry ♪
♪ I'm sorry ♪
"""
    markets = [
        {
            "code": "CN",
            "name": "A股",
            "name_en": "China A-Shares",
            "currency": "CNY",
            "timezone": "Asia/Shanghai",
            "trading_hours": "09:30-15:00"
        },
        {
            "code": "HK",
            "name": "港股",
            "name_en": "Hong Kong Stocks",
            "currency": "HKD",
            "timezone": "Asia/Hong_Kong",
            "trading_hours": "09:30-16:00"
        },
        {
            "code": "US",
            "name": "美股",
            "name_en": "US Stocks",
            "currency": "USD",
            "timezone": "America/New_York",
            "trading_hours": "09:30-16:00 EST"
        }
    ]
    
    return ok(data={"markets": markets})


@router.get("/{market}/stocks/search", response_model=dict)
async def search_stocks(
    market: str,
    q: str = Query(..., description="搜索关键词（代码或名称）"),
    limit: int = Query(20, ge=1, le=100, description="返回结果数量"),
    current_user: dict = Depends(get_current_user)
):
    """Search for stocks (support multi-market)

Args:
Market type (CN/HK/US)
q: Search keyword
Number of returns

Returns:
FMT 0 
I don't know.
"Total": 1
♪ I'm sorry ♪
♪ I'm sorry ♪
"""
    market = market.upper()
    if market not in ["CN", "HK", "US"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"不支持的市场类型: {market}"
        )
    
    db = get_mongo_db()
    service = UnifiedStockService(db)
    
    try:
        results = await service.search_stocks(market, q, limit)
        return ok(data={
            "stocks": results,
            "total": len(results)
        })
    except Exception as e:
        logger.error(f"Search for stock failed: market={market}, q={q}, error={e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"搜索失败: {str(e)}"
        )


@router.get("/{market}/stocks/{code}/info", response_model=dict)
async def get_stock_info(
    market: str,
    code: str,
    source: Optional[str] = Query(None, description="指定数据源（可选）"),
    current_user: dict = Depends(get_current_user)
):
    """Access to basic stock information (support to multi-market, multi-data sources)

Args:
Market type (CN/HK/US)
code: stock code
source: specify the data source (optional, not assigned automatic selection by priority)

Returns:
FMT 0 
♪ I'm sorry ♪
"""
    market = market.upper()
    if market not in ["CN", "HK", "US"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"不支持的市场类型: {market}"
        )
    
    db = get_mongo_db()
    service = UnifiedStockService(db)
    
    try:
        stock_info = await service.get_stock_info(market, code, source)
        
        if not stock_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到股票: {market}:{code}"
            )
        
        return ok(data=stock_info)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{market}, code={code}, error={e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取股票信息失败: {str(e)}"
        )


@router.get("/{market}/stocks/{code}/quote", response_model=dict)
async def get_stock_quote(
    market: str,
    code: str,
    current_user: dict = Depends(get_current_user)
):
    """Access to real-time equity (support to multiple markets)

Args:
Market type (CN/HK/US)
code: stock code

Returns:
FMT 0 
♪ I'm sorry ♪
"""
    market = market.upper()
    if market not in ["CN", "HK", "US"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"不支持的市场类型: {market}"
        )
    
    db = get_mongo_db()
    service = UnifiedStockService(db)
    
    try:
        quote = await service.get_stock_quote(market, code)
        
        if not quote:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到股票行情: {market}:{code}"
            )
        
        return ok(data=quote)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"♪ I can't get it ♪{market}, code={code}, error={e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取股票行情失败: {str(e)}"
        )


@router.get("/{market}/stocks/{code}/daily", response_model=dict)
async def get_stock_daily_quotes(
    market: str,
    code: str,
    start_date: Optional[str] = Query(None, description="开始日期 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="结束日期 (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000, description="返回记录数"),
    current_user: dict = Depends(get_current_user)
):
    """Acquisition of stock history K-line data (support to multi-market)

Args:
Market type (CN/HK/US)
code: stock code
Start date: Start date
End date: End date
Other Organiser

Returns:
FMT 0,
I don't know.
I don't know.
"Total": 100
♪ I'm sorry ♪
♪ I'm sorry ♪
"""
    market = market.upper()
    if market not in ["CN", "HK", "US"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"不支持的市场类型: {market}"
        )
    
    db = get_mongo_db()
    service = UnifiedStockService(db)
    
    try:
        quotes = await service.get_daily_quotes(
            market, code, start_date, end_date, limit
        )
        
        return ok(data={
            "code": code,
            "market": market,
            "quotes": quotes,
            "total": len(quotes)
        })
    except Exception as e:
        logger.error(f"Getting history line failed: market={market}, code={code}, error={e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取历史K线失败: {str(e)}"
        )

