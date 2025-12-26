"""Self-selected units manage API routers
"""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
import logging

from app.routers.auth_db import get_current_user
from app.models.user import User, FavoriteStock
from app.services.favorites_service import favorites_service
from app.core.response import ok

logger = logging.getLogger("webapi")

router = APIRouter(prefix="/favorites", tags=["自选股管理"])


class AddFavoriteRequest(BaseModel):
    """Add Selected Unit Request"""
    stock_code: str
    stock_name: str
    market: str = "A股"
    tags: List[str] = []
    notes: str = ""
    alert_price_high: Optional[float] = None
    alert_price_low: Optional[float] = None


class UpdateFavoriteRequest(BaseModel):
    """Update the self-selected unit request"""
    tags: Optional[List[str]] = None
    notes: Optional[str] = None
    alert_price_high: Optional[float] = None
    alert_price_low: Optional[float] = None


class FavoriteStockResponse(BaseModel):
    """Self-selected Unit Response"""
    stock_code: str
    stock_name: str
    market: str
    added_at: str
    tags: List[str]
    notes: str
    alert_price_high: Optional[float]
    alert_price_low: Optional[float]
    #Real time data
    current_price: Optional[float] = None
    change_percent: Optional[float] = None
    volume: Optional[int] = None


@router.get("/", response_model=dict)
async def get_favorites(
    current_user: dict = Depends(get_current_user)
):
    """Fetch user selection list"""
    try:
        favorites = await favorites_service.get_user_favorites(current_user["id"])
        return ok(favorites)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取自选股失败: {str(e)}"
        )


@router.post("/", response_model=dict)
async def add_favorite(
    request: AddFavoriteRequest,
    current_user: dict = Depends(get_current_user)
):
    """Add stocks to selected shares"""
    import logging
    logger = logging.getLogger("webapi")

    try:
        logger.info(f"📝 Add a selection request: user id={current_user['id']}, stock_code={request.stock_code}, stock_name={request.stock_name}")

        #Check for presence
        is_fav = await favorites_service.is_favorite(current_user["id"], request.stock_code)
        logger.info(f"Check if there is:{is_fav}")

        if is_fav:
            logger.warning(f"The shares have been selected:{request.stock_code}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="该股票已在自选股中"
            )

        #Add to Selected Unit
        logger.info(f"Starting to add selected shares...")
        success = await favorites_service.add_favorite(
            user_id=current_user["id"],
            stock_code=request.stock_code,
            stock_name=request.stock_name,
            market=request.market,
            tags=request.tags,
            notes=request.notes,
            alert_price_high=request.alert_price_high,
            alert_price_low=request.alert_price_low
        )

        logger.info(f"Add: result={success}")

        if success:
            return ok({"stock_code": request.stock_code}, "添加成功")
        else:
            logger.error(f"Could not close temporary folder: %s")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="添加失败"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Add a selection anomaly:{type(e).__name__}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"添加自选股失败: {str(e)}"
        )


@router.put("/{stock_code}", response_model=dict)
async def update_favorite(
    stock_code: str,
    request: UpdateFavoriteRequest,
    current_user: dict = Depends(get_current_user)
):
    """Update information on selected units"""
    try:
        success = await favorites_service.update_favorite(
            user_id=current_user["id"],
            stock_code=stock_code,
            tags=request.tags,
            notes=request.notes,
            alert_price_high=request.alert_price_high,
            alert_price_low=request.alert_price_low
        )

        if success:
            return ok({"stock_code": stock_code}, "更新成功")
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="自选股不存在"
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新自选股失败: {str(e)}"
        )


@router.delete("/{stock_code}", response_model=dict)
async def remove_favorite(
    stock_code: str,
    current_user: dict = Depends(get_current_user)
):
    """Remove shares from selected shares"""
    try:
        success = await favorites_service.remove_favorite(current_user["id"], stock_code)

        if success:
            return ok({"stock_code": stock_code}, "移除成功")
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="自选股不存在"
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"移除自选股失败: {str(e)}"
        )


@router.get("/check/{stock_code}", response_model=dict)
async def check_favorite(
    stock_code: str,
    current_user: dict = Depends(get_current_user)
):
    """Check if the stock is in the selected stock"""
    try:
        is_favorite = await favorites_service.is_favorite(current_user["id"], stock_code)
        return ok({"stock_code": stock_code, "is_favorite": is_favorite})
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"检查自选股状态失败: {str(e)}"
        )


@router.get("/tags", response_model=dict)
async def get_user_tags(
    current_user: dict = Depends(get_current_user)
):
    """Get all tags used by users"""
    try:
        tags = await favorites_service.get_user_tags(current_user["id"])
        return ok(tags)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取标签失败: {str(e)}"
        )


class SyncFavoritesRequest(BaseModel):
    """Synchronise real-time requests for self-selected units"""
    data_source: str = "tushare"  # tushare/akshare


@router.post("/sync-realtime", response_model=dict)
async def sync_favorites_realtime(
    request: SyncFavoritesRequest,
    current_user: dict = Depends(get_current_user)
):
    """Sync Self-Select Real-Time Line

- **data source**: data source (tushare/akshare)
"""
    try:
        logger.info(f"Synchronize the self-selected unit real-time status: user id={current_user['id']}, data_source={request.data_source}")

        #Fetch user selection list
        favorites = await favorites_service.get_user_favorites(current_user["id"])

        if not favorites:
            logger.info("The user has no self-selected shares.")
            return ok({
                "total": 0,
                "success_count": 0,
                "failed_count": 0,
                "message": "没有自选股需要同步"
            })

        #Extract stock code list
        symbols = [fav.get("stock_code") or fav.get("symbol") for fav in favorites]
        symbols = [s for s in symbols if s]  #Filter empty values

        logger.info(f"The shares that need to be synchronized:{len(symbols)}Just--{symbols}")

        #Select sync service according to data source
        if request.data_source == "tushare":
            from app.worker.tushare_sync_service import get_tushare_sync_service
            service = await get_tushare_sync_service()
        elif request.data_source == "akshare":
            from app.worker.akshare_sync_service import get_akshare_sync_service
            service = await get_akshare_sync_service()
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"不支持的数据源: {request.data_source}"
            )

        if not service:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"{request.data_source} 服务不可用"
            )

        #Sync Real Time Line
        logger.info(f"Call{request.data_source}Sync Service...")
        sync_result = await service.sync_realtime_quotes(
            symbols=symbols,
            force=True  #Enforcement, skip transaction time check
        )

        success_count = sync_result.get("success_count", 0)
        failed_count = sync_result.get("failed_count", 0)

        logger.info(f"✅Self-selected units completed in real time sync: successful{success_count}/{len(symbols)}Only")

        return ok({
            "total": len(symbols),
            "success_count": success_count,
            "failed_count": failed_count,
            "symbols": symbols,
            "data_source": request.data_source,
            "message": f"同步完成: 成功 {success_count} 只，失败 {failed_count} 只"
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ >Syncing self-selected units in real-time mode failed:{e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"同步失败: {str(e)}"
        )
