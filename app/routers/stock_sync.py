"""Stock data synchronize API route
Support the synchronization of historical and financial data for individual or bulk stocks
"""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from app.routers.auth_db import get_current_user
from app.core.response import ok
from app.core.database import get_mongo_db_async
from app.worker.tushare_sync_service import get_tushare_sync_service
from app.worker.akshare_sync_service import get_akshare_sync_service
from app.worker.financial_data_sync_service import get_financial_sync_service
import logging
import asyncio
from datetime import datetime, timedelta

logger = logging.getLogger("webapi")

router = APIRouter(prefix="/api/stock-sync", tags=["股票数据同步"])


async def _sync_latest_to_market_quotes(symbol: str) -> None:
    """Synchronise the latest data from stock daily quotes to market quotes

    Smart judgement logic:
    - Not overrided if updated data (trade date update) are available in market quotes
    - Update if data are not available or older

    Args:
        Symbol: Stock code (6 bits)
    """
    db = get_mongo_db_async()
    symbol6 = str(symbol).zfill(6)

    #Get the latest data from stock daily quotes
    latest_doc = await db.stock_daily_quotes.find_one(
        {"symbol": symbol6},
        sort=[("trade_date", -1)]
    )

    if not latest_doc:
        logger.warning(f"⚠️ {symbol6}: stock daily quotes not available")
        return

    historical_trade_date = latest_doc.get("trade_date")

    #Check if there are any updated data in market quotes
    existing_quote = await db.market_quotes.find_one({"code": symbol6})

    if existing_quote:
        existing_trade_date = existing_quote.get("trade_date")

        #If data dates are updated or the same in market guetes, do not overwrite
        if existing_trade_date and historical_trade_date:
            #Compare date string (format: YYY-MM-DD or YYYMMDD)
            existing_date_str = str(existing_trade_date).replace("-", "")
            historical_date_str = str(historical_trade_date).replace("-", "")

            if existing_date_str >= historical_date_str:
                #Do not cover (avoid coverage of real-time data with historical data) when the date is the same or updated 🔥
                logger.info(
                    f"⏭️ {symbol6}: date of data in market quotes > = date of historical data"
                    f"(market_quotes: {existing_trade_date}, historical: {historical_trade_date}) Skip Overwrite"
                )
                return

    #Extract required fields
    quote_data = {
        "code": symbol6,
        "symbol": symbol6,
        "close": latest_doc.get("close"),
        "open": latest_doc.get("open"),
        "high": latest_doc.get("high"),
        "low": latest_doc.get("low"),
        "volume": latest_doc.get("volume"),  #Unit converted
        "amount": latest_doc.get("amount"),  #Unit converted
        "pct_chg": latest_doc.get("pct_chg"),
        "pre_close": latest_doc.get("pre_close"),
        "trade_date": latest_doc.get("trade_date"),
        "updated_at": datetime.utcnow()
    }

    #Log: Record the amount of synchronous trade
    logger.info(
        f"[Sync to markt quotes]{symbol6} - "
        f"volume={quote_data['volume']}, amount={quote_data['amount']}, trade_date={quote_data['trade_date']}"
    )

    #Update market quotes
    await db.market_quotes.update_one(
        {"code": symbol6},
        {"$set": quote_data},
        upsert=True
    )


class SingleStockSyncRequest(BaseModel):
    """Single stock sync request"""
    symbol: str = Field(..., description="股票代码（6位）")
    sync_realtime: bool = Field(False, description="是否同步实时行情")
    sync_historical: bool = Field(True, description="是否同步历史数据")
    sync_financial: bool = Field(True, description="是否同步财务数据")
    sync_basic: bool = Field(False, description="是否同步基础数据")
    data_source: str = Field("tushare", description="数据源: tushare/akshare")
    days: int = Field(30, description="历史数据天数", ge=1, le=3650)


class BatchStockSyncRequest(BaseModel):
    """BatchSync Request"""
    symbols: List[str] = Field(..., description="股票代码列表")
    sync_historical: bool = Field(True, description="是否同步历史数据")
    sync_financial: bool = Field(True, description="是否同步财务数据")
    sync_basic: bool = Field(False, description="是否同步基础数据")
    data_source: str = Field("tushare", description="数据源: tushare/akshare")
    days: int = Field(30, description="历史数据天数", ge=1, le=3650)


@router.post("/single")
async def sync_single_stock(
    request: SingleStockSyncRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """Synchronize historical data, financial data and real-time behaviour of individual equities

    -**symbol**: stock code (6 bits)
    -**sync realtime**: Sync real-time lines
    -**sync historic**: Synchronization of historical data
    -**sync financial**: Synchronization of financial data
    - **data source**: data source (tushare/akshare)
    - **days**: days of historical data
    """
    try:
        logger.info(f"Commencing single stocks:{request.symbol}(Data source:{request.data_source})")

        result = {
            "symbol": request.symbol,
            "realtime_sync": None,
            "historical_sync": None,
            "financial_sync": None,
            "basic_sync": None
        }

        #Sync Real Time Line
        if request.sync_realtime:
            try:
                #🔥 Real-time business synchronization for individual equities: priority for AKshare (avoiding Tushare interface limitations)
                actual_data_source = request.data_source
                if request.data_source == "tushare":
                    logger.info(f"💡 Real-time line sync for individual equities, automatically switching to AKShare data source (avoiding Tushare interface limitations)")
                    actual_data_source = "akshare"

                if actual_data_source == "tushare":
                    service = await get_tushare_sync_service()
                elif actual_data_source == "akshare":
                    service = await get_akshare_sync_service()
                else:
                    raise ValueError(f"不支持的数据源: {actual_data_source}")

                #Sync Real Time Lines (sync specified shares only)
                realtime_result = await service.sync_realtime_quotes(
                    symbols=[request.symbol],
                    force=True  #Enforcement, skip transaction time check
                )

                #If the AKShare sync fails, back to Tushare full sync
                if actual_data_source == "akshare" and realtime_result.get("success_count", 0) == 0:
                    logger.warning(f"AKShare sync failed. Back to Tushare Full Sync")
                    logger.info(f"Tushare only supports full-scale synchronization, which will synchronize the real-time performance of all equities")

                    tushare_service = await get_tushare_sync_service()
                    if tushare_service:
                        #Use Tushare FullSync (no symbols specified, sync all shares)
                        realtime_result = await tushare_service.sync_realtime_quotes(
                            symbols=None,  #Full Sync
                            force=True
                        )
                        logger.info(f"Tushare complete synchronised:{realtime_result.get('success_count', 0)}Only")
                    else:
                        logger.error(f"Tushare service is not available and cannot be withdrawn")
                        realtime_result["fallback_failed"] = True

                success = realtime_result.get("success_count", 0) > 0

                #If you switch data sources, indicate in message
                message = f"实时行情同步{'成功' if success else '失败'}"
                if request.data_source == "tushare" and actual_data_source == "akshare":
                    message += "（已自动切换到 AKShare 数据源）"

                result["realtime_sync"] = {
                    "success": success,
                    "message": message,
                    "data_source_used": actual_data_source  #returns the actual data source
                }
                logger.info(f"✅ {request.symbol}Real-time line sync completed:{success}")

            except Exception as e:
                logger.error(f"❌ {request.symbol}Synchronising {e}")
                result["realtime_sync"] = {
                    "success": False,
                    "error": str(e)
                }
        
        #Sync Historical Data
        if request.sync_historical:
            try:
                if request.data_source == "tushare":
                    service = await get_tushare_sync_service()
                elif request.data_source == "akshare":
                    service = await get_akshare_sync_service()
                else:
                    raise ValueError(f"不支持的数据源: {request.data_source}")

                #Calculate Date Range
                end_date = datetime.now().strftime('%Y-%m-%d')
                start_date = (datetime.now() - timedelta(days=request.days)).strftime('%Y-%m-%d')

                #Sync Historical Data
                hist_result = await service.sync_historical_data(
                    symbols=[request.symbol],
                    start_date=start_date,
                    end_date=end_date,
                    incremental=False
                )

                result["historical_sync"] = {
                    "success": hist_result.get("success_count", 0) > 0,
                    "records": hist_result.get("total_records", 0),
                    "message": f"同步了 {hist_result.get('total_records', 0)} 条历史记录"
                }
                logger.info(f"✅ {request.symbol}Synchronization of historical data:{hist_result.get('total_records', 0)}Notes")

                #Synchronize the latest historical data to market quotes
                if hist_result.get("success_count", 0) > 0:
                    try:
                        await _sync_latest_to_market_quotes(request.symbol)
                        logger.info(f"✅ {request.symbol}Latest data synchronized to market quotes")
                    except Exception as e:
                        logger.warning(f"⚠️ {request.symbol}Could not close temporary folder: %s{e}")

                #🔥 [disabled] if no real-time line is ticked, but automatically synchronizes real-time line during transaction time
                #User feedback: not wishing to automatically synchronize real-time lines, should strictly follow user selection
                # if not request.sync_realtime:
                #     from app.utils.trading_time import is_trading_time
                #     if is_trading_time():
                #Logger.info (f "📊 FMT 0 AutoSync Real Time Lines during Current Transactions")
                #         try:
                #             realtime_result = await service.sync_realtime_quotes(
                #                 symbols=[request.symbol],
                #                 force=True
                #             )
                #             if realtime_result.get("success_count", 0) > 0:
                #logger.info (f "✅  FMT 0 0 AutoSync Success))
                #                 result["realtime_sync"] = {
                #                     "success": True,
                #"Message": "AutoSync in Real Time"
                #                 }
                #         except Exception as e:
                #Logger.warning (f"⚠️  FMT 0 AutoSync failed:   FMT 1 ")

            except Exception as e:
                logger.error(f"❌ {request.symbol}Synchronising folder{e}")
                result["historical_sync"] = {
                    "success": False,
                    "error": str(e)
                }
        
        #Sync Financial Data
        if request.sync_financial:
            try:
                financial_service = await get_financial_sync_service()
                
                #Sync Financial Data
                fin_result = await financial_service.sync_single_stock(
                    symbol=request.symbol,
                    data_sources=[request.data_source]
                )
                
                success = fin_result.get(request.data_source, False)
                result["financial_sync"] = {
                    "success": success,
                    "message": "财务数据同步成功" if success else "财务数据同步失败"
                }
                logger.info(f"✅ {request.symbol}Financial data synchronized:{success}")
                
            except Exception as e:
                logger.error(f"❌ {request.symbol}Could not close temporary folder: %s{e}")
                result["financial_sync"] = {
                    "success": False,
                    "error": str(e)
                }

        #Sync Basic Data
        if request.sync_basic:
            try:
                #Synchronize basic data for individual equities
                #Reference to the logic of achievement for Basics sync service
                if request.data_source == "tushare":
                    from app.services.basics_sync import (
                        fetch_stock_basic_df,
                        find_latest_trade_date,
                        fetch_daily_basic_mv_map,
                        fetch_latest_roe_map,
                    )

                    db = get_mongo_db_async()
                    symbol6 = str(request.symbol).zfill(6)

                    #Step 1: Access to basic stock information
                    stock_df = await asyncio.to_thread(fetch_stock_basic_df)
                    if stock_df is None or stock_df.empty:
                        result["basic_sync"] = {
                            "success": False,
                            "error": "Tushare 返回空数据"
                        }
                    else:
                        #Filter Target Stock
                        stock_row = None
                        for _, row in stock_df.iterrows():
                            ts_code = row.get("ts_code", "")
                            if isinstance(ts_code, str) and ts_code.startswith(symbol6):
                                stock_row = row
                                break

                        if stock_row is None:
                            result["basic_sync"] = {
                                "success": False,
                                "error": f"未找到股票 {symbol6} 的基础信息"
                            }
                        else:
                            #Step 2: Getting up to date on transactions and financial indicators
                            latest_trade_date = await asyncio.to_thread(find_latest_trade_date)
                            daily_data_map = await asyncio.to_thread(fetch_daily_basic_mv_map, latest_trade_date)
                            roe_map = await asyncio.to_thread(fetch_latest_roe_map)

                            #Step 3: Build document (reference logic for Basics sync service)
                            #Get the current time and avoid the domain problem.
                            now_iso = datetime.utcnow().isoformat()

                            name = stock_row.get("name") or ""
                            area = stock_row.get("area") or ""
                            industry = stock_row.get("industry") or ""
                            market = stock_row.get("market") or ""
                            list_date = stock_row.get("list_date") or ""
                            ts_code = stock_row.get("ts_code") or ""

                            #Extract 6-bit code.
                            if isinstance(ts_code, str) and "." in ts_code:
                                code = ts_code.split(".")[0]
                            else:
                                code = symbol6

                            #A judgement exchange.
                            if isinstance(ts_code, str):
                                if ts_code.endswith(".SH"):
                                    sse = "上海证券交易所"
                                elif ts_code.endswith(".SZ"):
                                    sse = "深圳证券交易所"
                                elif ts_code.endswith(".BJ"):
                                    sse = "北京证券交易所"
                                else:
                                    sse = "未知"
                            else:
                                sse = "未知"

                            #Generate full symbol
                            full_symbol = ts_code

                            #Extracting financial indicators
                            daily_metrics = {}
                            if isinstance(ts_code, str) and ts_code in daily_data_map:
                                daily_metrics = daily_data_map[ts_code]

                            #Market value conversion (millions - > billions)
                            total_mv_yi = None
                            circ_mv_yi = None
                            if "total_mv" in daily_metrics:
                                try:
                                    total_mv_yi = float(daily_metrics["total_mv"]) / 10000.0
                                except Exception:
                                    pass
                            if "circ_mv" in daily_metrics:
                                try:
                                    circ_mv_yi = float(daily_metrics["circ_mv"]) / 10000.0
                                except Exception:
                                    pass

                            #Build Document
                            doc = {
                                "code": code,
                                "symbol": code,
                                "name": name,
                                "area": area,
                                "industry": industry,
                                "market": market,
                                "list_date": list_date,
                                "sse": sse,
                                "sec": "stock_cn",
                                "source": "tushare",
                                "updated_at": now_iso,
                                "full_symbol": full_symbol,
                            }

                            #Add Market Value
                            if total_mv_yi is not None:
                                doc["total_mv"] = total_mv_yi
                            if circ_mv_yi is not None:
                                doc["circ_mv"] = circ_mv_yi

                            #Add Valuation Indicator
                            for field in ["pe", "pb", "ps", "pe_ttm", "pb_mrq", "ps_ttm"]:
                                if field in daily_metrics:
                                    doc[field] = daily_metrics[field]

                            #Add ROE
                            if isinstance(ts_code, str) and ts_code in roe_map:
                                roe_val = roe_map[ts_code].get("roe")
                                if roe_val is not None:
                                    doc["roe"] = roe_val

                            #Add Transaction Indicator
                            for field in ["turnover_rate", "volume_ratio"]:
                                if field in daily_metrics:
                                    doc[field] = daily_metrics[field]

                            #Add equity information
                            for field in ["total_share", "float_share"]:
                                if field in daily_metrics:
                                    doc[field] = daily_metrics[field]

                            #Step 4: Update data Library
                            await db.stock_basic_info.update_one(
                                {"code": code, "source": "tushare"},
                                {"$set": doc},
                                upsert=True
                            )

                            result["basic_sync"] = {
                                "success": True,
                                "message": "基础数据同步成功"
                            }
                            logger.info(f"✅ {request.symbol}Basic data synchronized")

                elif request.data_source == "akshare":
                    #Basic Data Synchronization of AKShare Data Source
                    db = get_mongo_db_async()
                    symbol6 = str(request.symbol).zfill(6)

                    #Get AKShare Sync Service
                    service = await get_akshare_sync_service()

                    #Access to basic stock information
                    basic_info = await service.provider.get_stock_basic_info(symbol6)

                    if basic_info:
                        #Convert to Dictionary Format
                        if hasattr(basic_info, 'model_dump'):
                            basic_data = basic_info.model_dump()
                        elif hasattr(basic_info, 'dict'):
                            basic_data = basic_info.dict()
                        else:
                            basic_data = basic_info

                        #Ensure necessary fields
                        basic_data["code"] = symbol6
                        basic_data["symbol"] = symbol6
                        basic_data["source"] = "akshare"
                        basic_data["updated_at"] = datetime.utcnow().isoformat()

                        #Update to Database
                        await db.stock_basic_info.update_one(
                            {"code": symbol6, "source": "akshare"},
                            {"$set": basic_data},
                            upsert=True
                        )

                        result["basic_sync"] = {
                            "success": True,
                            "message": "基础数据同步成功"
                        }
                        logger.info(f"✅ {request.symbol}Basic data synchronized (AKShare)")
                    else:
                        result["basic_sync"] = {
                            "success": False,
                            "error": "未获取到基础数据"
                        }
                else:
                    result["basic_sync"] = {
                        "success": False,
                        "error": f"基础数据同步仅支持 Tushare/AKShare 数据源，当前数据源: {request.data_source}"
                    }

            except Exception as e:
                logger.error(f"❌ {request.symbol}Synchronising folder failed:{e}")
                result["basic_sync"] = {
                    "success": False,
                    "error": str(e)
                }

        #To judge if the whole thing is working.
        overall_success = (
            (not request.sync_realtime or result["realtime_sync"].get("success", False)) and
            (not request.sync_historical or result["historical_sync"].get("success", False)) and
            (not request.sync_financial or result["financial_sync"].get("success", False)) and
            (not request.sync_basic or result["basic_sync"].get("success", False))
        )

        #Add overall success mark to result
        result["overall_success"] = overall_success

        return ok(
            data=result,
            message=f"股票 {request.symbol} 数据同步{'成功' if overall_success else '部分失败'}"
        )
        
    except Exception as e:
        logger.error(f"Synchronising single stock failed:{e}")
        raise HTTPException(status_code=500, detail=f"同步失败: {str(e)}")


@router.post("/batch")
async def sync_batch_stocks(
    request: BatchStockSyncRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """Batch synchronization of historical and financial data on multiple equities

    -**symbols**: list of stock codes
    -**sync historic**: Synchronization of historical data
    -**sync financial**: Synchronization of financial data
    - **data source**: data source (tushare/akshare)
    - **days**: days of historical data
    """
    try:
        logger.info(f"Start batch sync{len(request.symbols)}Stock only (data source:{request.data_source})")
        
        result = {
            "total": len(request.symbols),
            "symbols": request.symbols,
            "historical_sync": None,
            "financial_sync": None,
            "basic_sync": None
        }
        
        #Sync Historical Data
        if request.sync_historical:
            try:
                if request.data_source == "tushare":
                    service = await get_tushare_sync_service()
                elif request.data_source == "akshare":
                    service = await get_akshare_sync_service()
                else:
                    raise ValueError(f"不支持的数据源: {request.data_source}")

                #Calculate Date Range
                end_date = datetime.now().strftime('%Y-%m-%d')
                start_date = (datetime.now() - timedelta(days=request.days)).strftime('%Y-%m-%d')
                
                #Batch Sync Historical Data
                hist_result = await service.sync_historical_data(
                    symbols=request.symbols,
                    start_date=start_date,
                    end_date=end_date,
                    incremental=False
                )
                
                result["historical_sync"] = {
                    "success_count": hist_result.get("success_count", 0),
                    "error_count": hist_result.get("error_count", 0),
                    "total_records": hist_result.get("total_records", 0),
                    "message": f"成功同步 {hist_result.get('success_count', 0)}/{len(request.symbols)} 只股票，共 {hist_result.get('total_records', 0)} 条记录"
                }
                logger.info(f"Batch of historical data synchronised:{hist_result.get('success_count', 0)}/{len(request.symbols)}")
                
            except Exception as e:
                logger.error(f"Batch of historical data sync failed:{e}")
                result["historical_sync"] = {
                    "success_count": 0,
                    "error_count": len(request.symbols),
                    "error": str(e)
                }
        
        #Sync Financial Data
        if request.sync_financial:
            try:
                financial_service = await get_financial_sync_service()
                
                #Batch Sync Financial Data
                fin_results = await financial_service.sync_financial_data(
                    symbols=request.symbols,
                    data_sources=[request.data_source],
                    batch_size=10
                )
                
                source_stats = fin_results.get(request.data_source)
                if source_stats:
                    result["financial_sync"] = {
                        "success_count": source_stats.success_count,
                        "error_count": source_stats.error_count,
                        "total_symbols": source_stats.total_symbols,
                        "message": f"成功同步 {source_stats.success_count}/{source_stats.total_symbols} 只股票的财务数据"
                    }
                else:
                    result["financial_sync"] = {
                        "success_count": 0,
                        "error_count": len(request.symbols),
                        "message": "财务数据同步失败"
                    }
                
                logger.info(f"✅ Batch financial data synchronized:{result['financial_sync']['success_count']}/{len(request.symbols)}")
                
            except Exception as e:
                logger.error(f"Synchronising financial data failed:{e}")
                result["financial_sync"] = {
                    "success_count": 0,
                    "error_count": len(request.symbols),
                    "error": str(e)
                }

        #Sync Basic Data
        if request.sync_basic:
            try:
                #Batch Sync Basic Data
                #Note: Basic Data Synchronization Service currently only supports Tushare data sources
                if request.data_source == "tushare":
                    from tradingagents.dataflows.providers.china.tushare import TushareProvider

                    tushare_provider = TushareProvider()
                    if tushare_provider.is_available():
                        success_count = 0
                        error_count = 0

                        for symbol in request.symbols:
                            try:
                                basic_info = await tushare_provider.get_stock_basic_info(symbol)

                                if basic_info:
                                    #Save to MongoDB
                                    db = get_mongo_db_async()
                                    symbol6 = str(symbol).zfill(6)

                                    #Add the necessary fields
                                    basic_info["code"] = symbol6
                                    basic_info["source"] = "tushare"
                                    basic_info["updated_at"] = datetime.utcnow()

                                    await db.stock_basic_info.update_one(
                                        {"code": symbol6, "source": "tushare"},
                                        {"$set": basic_info},
                                        upsert=True
                                    )

                                    success_count += 1
                                    logger.info(f"✅ {symbol}Basic data sync successfully")
                                else:
                                    error_count += 1
                                    logger.warning(f"⚠️ {symbol}Basic data not obtained")
                            except Exception as e:
                                error_count += 1
                                logger.error(f"❌ {symbol}Synchronising folder failed:{e}")

                        result["basic_sync"] = {
                            "success_count": success_count,
                            "error_count": error_count,
                            "total_symbols": len(request.symbols),
                            "message": f"成功同步 {success_count}/{len(request.symbols)} 只股票的基础数据"
                        }
                        logger.info(f"✅ Batch base data synchronized:{success_count}/{len(request.symbols)}")
                    else:
                        result["basic_sync"] = {
                            "success_count": 0,
                            "error_count": len(request.symbols),
                            "error": "Tushare 数据源不可用"
                        }
                else:
                    result["basic_sync"] = {
                        "success_count": 0,
                        "error_count": len(request.symbols),
                        "error": f"基础数据同步仅支持 Tushare 数据源，当前数据源: {request.data_source}"
                    }

            except Exception as e:
                logger.error(f"Batch base data sync failed:{e}")
                result["basic_sync"] = {
                    "success_count": 0,
                    "error_count": len(request.symbols),
                    "error": str(e)
                }

        #To judge if the whole thing is working.
        hist_success = result["historical_sync"].get("success_count", 0) if request.sync_historical else 0
        fin_success = result["financial_sync"].get("success_count", 0) if request.sync_financial else 0
        basic_success = result["basic_sync"].get("success_count", 0) if request.sync_basic else 0
        total_success = max(hist_success, fin_success, basic_success)

        #Add statistical information to result
        result["total_success"] = total_success
        result["total_symbols"] = len(request.symbols)

        return ok(
            data=result,
            message=f"批量同步完成: {total_success}/{len(request.symbols)} 只股票成功"
        )
        
    except Exception as e:
        logger.error(f"Batch sync failed:{e}")
        raise HTTPException(status_code=500, detail=f"批量同步失败: {str(e)}")


@router.get("/status/{symbol}")
async def get_sync_status(
    symbol: str,
    current_user: dict = Depends(get_current_user)
):
    """Retrieving stock synchronization

    Returns final sync time, data bar, etc.
    """
    try:
        from app.core.database import get_mongo_db_async
        
        db = get_mongo_db_async()
        
        #Query history data final sync time
        hist_doc = await db.historical_data.find_one(
            {"symbol": symbol},
            sort=[("date", -1)]
        )
        
        #Query financial data final sync time
        fin_doc = await db.stock_financial_data.find_one(
            {"symbol": symbol},
            sort=[("updated_at", -1)]
        )
        
        #Number of statistical historical data bars
        hist_count = await db.historical_data.count_documents({"symbol": symbol})
        
        #Statistical financial data bar
        fin_count = await db.stock_financial_data.count_documents({"symbol": symbol})
        
        return ok(data={
            "symbol": symbol,
            "historical_data": {
                "last_sync": hist_doc.get("updated_at") if hist_doc else None,
                "last_date": hist_doc.get("date") if hist_doc else None,
                "total_records": hist_count
            },
            "financial_data": {
                "last_sync": fin_doc.get("updated_at") if fin_doc else None,
                "last_report_period": fin_doc.get("report_period") if fin_doc else None,
                "total_records": fin_count
            }
        })
        
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=f"获取同步状态失败: {str(e)}")

