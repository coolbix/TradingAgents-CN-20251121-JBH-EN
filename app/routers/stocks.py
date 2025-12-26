"""Share details related to API
- Unified response package:   FT 0 
- All endpoints need access.
- Path prefix inmain.py to /api, current path prefix to /stocks
"""
from typing import Optional, Dict, Any, List, Tuple
from fastapi import APIRouter, Depends, HTTPException, status, Query
import logging
import re

from app.routers.auth_db import get_current_user
from app.core.database import get_mongo_db
from app.core.response import ok

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/stocks", tags=["stocks"])


def _zfill_code(code: str) -> str:
    try:
        s = str(code).strip()
        if len(s) == 6 and s.isdigit():
            return s
        return s.zfill(6)
    except Exception:
        return str(code)


def _detect_market_and_code(code: str) -> Tuple[str, str]:
    """Test market type of stock code and standardize code

Args:
code: stock code

Returns:
(market, standardized code): Market type and standardized code
- CN: Unit A (6-digit)
- HK: Port Unit (4-5 digit or HK suffix)
- US: United States shares (letter code)
"""
    code = code.strip().upper()

    #Port Unit: with .HK suffix
    if code.endswith('.HK'):
        return ('HK', code[:-3].zfill(5))  #Remove. HK, complete to five.

    #United States share: pure letters
    if re.match(r'^[A-Z]+$', code):
        return ('US', code)

    #Port Unit: 4-5 figures
    if re.match(r'^\d{4,5}$', code):
        return ('HK', code.zfill(5))  #Five.

    #Unit A: 6 figures
    if re.match(r'^\d{6}$', code):
        return ('CN', code)

    #Default as Unit A
    return ('CN', _zfill_code(code))


@router.get("/{code}/quote", response_model=dict)
async def get_quote(
    code: str,
    force_refresh: bool = Query(False, description="是否强制刷新（跳过缓存）"),
    current_user: dict = Depends(get_current_user)
):
    """Access to real-time equity (support to Unit A/Hong Kong/United States)

Automatic recognition of market type:
- Six bits.
- 4-digit number or. HK Port Unit
- Pure letters.

Parameters:
- Code: Stock code
-Force refresh: forced refreshing (jumping cache)

Return field (data inside, snake name):
-Code, name, market.
- price(clos), change percent(pct chg), amount, prev close (estimate)
- turnover rate, amplitude, substitution ratio
I'm sorry.
"""
    #Test market type
    market, normalized_code = _detect_market_and_code(code)

    #Hong Kong and United States units: use of new services
    if market in ['HK', 'US']:
        from app.services.foreign_stock_service import ForeignStockService

        db = get_mongo_db()  #No need for wait, directly return database object
        service = ForeignStockService(db=db)

        try:
            quote = await service.get_quote(market, normalized_code, force_refresh)
            return ok(data=quote)
        except Exception as e:
            logger.error(f"Access{market}Equities{code}Project failure:{e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"获取行情失败: {str(e)}"
            )

    #Unit A: use of existing logic
    db = get_mongo_db()
    code6 = normalized_code

    #Behave
    q = await db["market_quotes"].find_one({"code": code6}, {"_id": 0})

    #Debugging log: look for query results
    logger.info(f"Other Organiser{code6}")
    if q:
        logger.info(f"Found data: volume={q.get('volume')}, amount={q.get('amount')}, volume_ratio={q.get('volume_ratio')}")
    else:
        logger.info(f"No data found.")

    #🔥 Basic Information - Query by Data Source Priority
    from app.core.unified_config import UnifiedConfigManager
    config = UnifiedConfigManager()
    data_source_configs = await config.get_data_source_configs_async()

    #Extract enabled data sources in order of priority
    enabled_sources = [
        ds.type.lower() for ds in data_source_configs
        if ds.enabled and ds.type.lower() in ['tushare', 'akshare', 'baostock']
    ]

    if not enabled_sources:
        enabled_sources = ['tushare', 'akshare', 'baostock']

    #Search basic information by priority
    b = None
    for src in enabled_sources:
        b = await db["stock_basic_info"].find_one({"code": code6, "source": src}, {"_id": 0})
        if b:
            break

    #Try without source condition query (compatible with old data) if all data sources are missing
    if not b:
        b = await db["stock_basic_info"].find_one({"code": code6}, {"_id": 0})

    if not q and not b:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="未找到该股票的任何信息")

    close = (q or {}).get("close")
    pct = (q or {}).get("pct_chg")
    pre_close_saved = (q or {}).get("pre_close")
    prev_close = pre_close_saved
    if prev_close is None:
        try:
            if close is not None and pct is not None:
                prev_close = round(float(close) / (1.0 + float(pct) / 100.0), 4)
        except Exception:
            prev_close = None

    #🔥 Preferably to turnover rate (real-time data)
    #Retrieved from stock basic info if market quotes are not available (daily data)
    turnover_rate = (q or {}).get("turnover_rate")
    turnover_rate_date = None
    if turnover_rate is None:
        turnover_rate = (b or {}).get("turnover_rate")
        turnover_rate_date = (b or {}).get("trade_date")  #Data from Day
    else:
        turnover_rate_date = (q or {}).get("trade_date")  #From Real Time Data

    #🔥 Calculated amplitude replacement ratio (volume ratio)
    #amplitude = (highest - lowest price) / yesterday 's price x 100%
    amplitude = None
    amplitude_date = None
    try:
        high = (q or {}).get("high")
        low = (q or {}).get("low")
        logger.info(f"Calculated amplitude: high={high}, low={low}, prev_close={prev_close}")
        if high is not None and low is not None and prev_close is not None and prev_close > 0:
            amplitude = round((float(high) - float(low)) / float(prev_close) * 100, 2)
            amplitude_date = (q or {}).get("trade_date")  #From Real Time Data
            logger.info(f"The amplitude calculation was successful:{amplitude}%")
        else:
            logger.warning(f"Data is incomplete and amplitude cannot be calculated")
    except Exception as e:
        logger.warning(f"The amplitude failed:{e}")
        amplitude = None

    data = {
        "code": code6,
        "name": (b or {}).get("name"),
        "market": (b or {}).get("market"),
        "price": close,
        "change_percent": pct,
        "amount": (q or {}).get("amount"),
        "volume": (q or {}).get("volume"),
        "open": (q or {}).get("open"),
        "high": (q or {}).get("high"),
        "low": (q or {}).get("low"),
        "prev_close": prev_close,
        #🔥 Prefer real-time data to downgrade to day data
        "turnover_rate": turnover_rate,
        "amplitude": amplitude,  #New: amplitude (substitution ratio)
        "turnover_rate_date": turnover_rate_date,  #Add: Change rate date
        "amplitude_date": amplitude_date,  #Add: amplitude data date
        "trade_date": (q or {}).get("trade_date"),
        "updated_at": (q or {}).get("updated_at"),
    }

    return ok(data)


@router.get("/{code}/fundamentals", response_model=dict)
async def get_fundamentals(
    code: str,
    source: Optional[str] = Query(None, description="数据源 (tushare/akshare/baostock/multi_source)"),
    force_refresh: bool = Query(False, description="是否强制刷新（跳过缓存）"),
    current_user: dict = Depends(get_current_user)
):
    """Obtain basic snapshots (support Unit A/Hong Kong/US)

Data source priorities:
Stock basic info collection (basic information, valuation indicators)
Stock financial data pool (financial indicators: ROE, liability ratio, etc.)

Parameters:
- Code: Stock code
-source: Data source (optional), default by priority: tushare > multi source > akshare > baostock
-Force refresh: forced refreshing (jumping cache)
"""
    #Test market type
    market, normalized_code = _detect_market_and_code(code)

    #Hong Kong and United States units: use of new services
    if market in ['HK', 'US']:
        from app.services.foreign_stock_service import ForeignStockService

        db = get_mongo_db()  #No need for wait, directly return database object
        service = ForeignStockService(db=db)

        try:
            info = await service.get_basic_info(market, normalized_code, force_refresh)
            return ok(data=info)
        except Exception as e:
            logger.error(f"Access{market}Equities{code}Could not close temporary folder: %s{e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"获取基础信息失败: {str(e)}"
            )

    #Unit A: use of existing logic
    db = get_mongo_db()
    code6 = normalized_code

    #1. Access to basic information (support to data source screening)
    query = {"code": code6}

    if source:
        #Specify data source
        query["source"] = source
        b = await db["stock_basic_info"].find_one(query, {"_id": 0})
        if not b:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到该股票在数据源 {source} 中的基础信息"
            )
    else:
        #🔥 No data sources specified, query by priority
        source_priority = ["tushare", "multi_source", "akshare", "baostock"]
        b = None

        for src in source_priority:
            query_with_source = {"code": code6, "source": src}
            b = await db["stock_basic_info"].find_one(query_with_source, {"_id": 0})
            if b:
                logger.info(f"Using data sources:{src}Query stocks{code6}")
                break

        #Try without source condition query (compatible with old data) if all data sources are missing
        if not b:
            b = await db["stock_basic_info"].find_one({"code": code6}, {"_id": 0})
            if b:
                logger.warning(f"Use old data (no source field):{code6}")

        if not b:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="未找到该股票的基础信息")

    #2. Attempt to obtain updated financial indicators from stock financial data
    #🔥Query by data source priority instead of time stamp to avoid mixing data from different data sources
    financial_data = None
    try:
        #Get Data Source Priority Configuration
        from app.core.unified_config import UnifiedConfigManager
        config = UnifiedConfigManager()
        data_source_configs = await config.get_data_source_configs_async()

        #Extract enabled data sources in order of priority
        enabled_sources = [
            ds.type.lower() for ds in data_source_configs
            if ds.enabled and ds.type.lower() in ['tushare', 'akshare', 'baostock']
        ]

        if not enabled_sources:
            enabled_sources = ['tushare', 'akshare', 'baostock']

        #Query financial data by data source priority
        for data_source in enabled_sources:
            financial_data = await db["stock_financial_data"].find_one(
                {"$or": [{"symbol": code6}, {"code": code6}], "data_source": data_source},
                {"_id": 0},
                sort=[("report_period", -1)]  #Obtain updated data from this data source, in descending order, by reporting period
            )
            if financial_data:
                logger.info(f"Using data sources{data_source}Financial data (reporting period:{financial_data.get('report_period')})")
                break

        if not financial_data:
            logger.warning(f"Not found{code6}Financial data")
    except Exception as e:
        logger.error(f"Failed to obtain financial data:{e}")

    #3. Access to real-time PE/PB (priority for real-time calculations)
    from tradingagents.dataflows.realtime_metrics import get_pe_pb_with_fallback
    import asyncio

    #Perform synchronized real-time calculations in an online pool
    realtime_metrics = await asyncio.to_thread(
        get_pe_pb_with_fallback,
        code6,
        db.client
    )

    #4. Build return data
    #Priority is given to real-time market value down to static market value of stock basic info
    realtime_market_cap = realtime_metrics.get("market_cap")  #Real-time market value (billions of dollars)
    total_mv = realtime_market_cap if realtime_market_cap else b.get("total_mv")

    data = {
        "code": code6,
        "name": b.get("name"),
        "industry": b.get("industry"),  #Industry (e.g. banking, software services)
        "market": b.get("market"),      #Exchanges (e.g. master boards, start-up boards)

        #Board information: use the market field (mainboard/emergence board/screen board/north office, etc.)
        "sector": b.get("market"),

        #Valuation indicator (priority for real-time calculations, downgrade to stock basic info)
        "pe": realtime_metrics.get("pe") or b.get("pe"),
        "pb": realtime_metrics.get("pb") or b.get("pb"),
        "pe_ttm": realtime_metrics.get("pe_ttm") or b.get("pe_ttm"),
        "pb_mrq": realtime_metrics.get("pb_mrq") or b.get("pb_mrq"),

        #🔥 Marketing rate (PS) - Dynamic calculations (using real-time market value)
        "ps": None,
        "ps_ttm": None,

        #PE/PB Data Source Identification
        "pe_source": realtime_metrics.get("source", "unknown"),
        "pe_is_realtime": realtime_metrics.get("is_realtime", False),
        "pe_updated_at": realtime_metrics.get("updated_at"),

        #ROE (prioritized from stock financial data, followed by stock basic info)
        "roe": None,

        #Debt ratio (from stock financial data)
        "debt_ratio": None,

        #Market value: preferential use of real-time market value and downgrade to static market value
        "total_mv": total_mv,
        "circ_mv": b.get("circ_mv"),

        #Market value source identification
        "mv_is_realtime": bool(realtime_market_cap),

        #Transaction indicators (possibly empty)
        "turnover_rate": b.get("turnover_rate"),
        "volume_ratio": b.get("volume_ratio"),

        "updated_at": b.get("updated_at"),
    }

    #5. Drawing from financial data ROE, liability ratio and calculation of PS
    if financial_data:
        #ROE (net asset return)
        if financial_data.get("financial_indicators"):
            indicators = financial_data["financial_indicators"]
            data["roe"] = indicators.get("roe")
            data["debt_ratio"] = indicators.get("debt_to_assets")

        #Try fetching from top floor fields if they are not available
        if data["roe"] is None:
            data["roe"] = financial_data.get("roe")
        if data["debt_ratio"] is None:
            data["debt_ratio"] = financial_data.get("debt_to_assets")

        #Dynamic PS (market rate) - Use of real-time market value
        #Prioritize TTM operating income or, if not, single-stage operating income
        revenue_ttm = financial_data.get("revenue_ttm")
        revenue = financial_data.get("revenue")
        revenue_for_ps = revenue_ttm if revenue_ttm and revenue_ttm > 0 else revenue

        if revenue_for_ps and revenue_for_ps > 0:
            #🔥 Use real-time market value (if any), otherwise use static market value
            if total_mv and total_mv > 0:
                #Business income units: dollars, which need to be converted to billions
                revenue_yi = revenue_for_ps / 100000000
                ps_calculated = total_mv / revenue_yi
                data["ps"] = round(ps_calculated, 2)
                data["ps_ttm"] = round(ps_calculated, 2) if revenue_ttm else None

    #6. Use block basic info if ROE is not available
    if data["roe"] is None:
        data["roe"] = b.get("roe")

    return ok(data)


@router.get("/{code}/kline", response_model=dict)
async def get_kline(
    code: str,
    period: str = "day",
    limit: int = 120,
    adj: str = "none",
    force_refresh: bool = Query(False, description="是否强制刷新（跳过缓存）"),
    current_user: dict = Depends(get_current_user)
):
    """Access to K-line data (support to Unit A/Hong Kong/US)

period: day/week/month/5m/15m/30m/60m
Adj: none/qfq/hfq
source refresh: whether to force refresh (jump cache)

Add function: real-time K-line data on the day
- Time of transaction (09:30: 15:00): real time data from market quotes
- After closing up: check if historical data are available for the day or not from market quotes
"""
    import logging
    from datetime import datetime, timedelta, time as dtime
    from zoneinfo import ZoneInfo
    logger = logging.getLogger(__name__)

    valid_periods = {"day","week","month","5m","15m","30m","60m"}
    if period not in valid_periods:
        raise HTTPException(status_code=400, detail=f"不支持的period: {period}")

    #Test market type
    market, normalized_code = _detect_market_and_code(code)

    #Hong Kong and United States units: use of new services
    if market in ['HK', 'US']:
        from app.services.foreign_stock_service import ForeignStockService

        db = get_mongo_db()  #No need for wait, directly return database object
        service = ForeignStockService(db=db)

        try:
            kline_data = await service.get_kline(market, normalized_code, period, limit, force_refresh)
            return ok(data={
                'code': normalized_code,
                'period': period,
                'items': kline_data,
                'source': 'cache_or_api'
            })
        except Exception as e:
            logger.error(f"Access{market}Equities{code}K-line data failed:{e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"获取K线数据失败: {str(e)}"
            )

    #Unit A: use of existing logic
    code_padded = normalized_code
    adj_norm = None if adj in (None, "none", "", "null") else adj
    items = None
    source = None

    #Periodically Map: Frontend - > MongoDB
    period_map = {
        "day": "daily",
        "week": "weekly",
        "month": "monthly",
        "5m": "5min",
        "15m": "15min",
        "30m": "30min",
        "60m": "60min"
    }
    mongodb_period = period_map.get(period, "daily")

    #Get Current Time (Beijing Time)
    from app.core.config import settings
    tz = ZoneInfo(settings.TIMEZONE)
    now = datetime.now(tz)
    today_str_yyyymmdd = now.strftime("%Y%m%d")  #Format: 20251028 (for query)
    today_str_formatted = now.strftime("%Y-%m-%d")  #Format: 2025-10-28 (for return)

    #1. Prioritize access from the MongoDB cache
    try:
        from tradingagents.dataflows.cache.mongodb_cache_adapter import get_mongodb_cache_adapter
        adapter = get_mongodb_cache_adapter()

        #Calculate Date Range
        end_date = now.strftime("%Y-%m-%d")
        start_date = (now - timedelta(days=limit * 2)).strftime("%Y-%m-%d")

        logger.info(f"Try to get K-line data from MongoDB:{code_padded}, period={period} (MongoDB: {mongodb_period}), limit={limit}")
        df = adapter.get_historical_data(code_padded, start_date, end_date, period=mongodb_period)

        if df is not None and not df.empty:
            #Convert DataFrame as List Format
            items = []
            for _, row in df.tail(limit).iterrows():
                items.append({
                    "time": row.get("trade_date", row.get("date", "")),  #Front-end expectation time field
                    "open": float(row.get("open", 0)),
                    "high": float(row.get("high", 0)),
                    "low": float(row.get("low", 0)),
                    "close": float(row.get("close", 0)),
                    "volume": float(row.get("volume", row.get("vol", 0))),
                    "amount": float(row.get("amount", 0)) if "amount" in row else None,
                })
            source = "mongodb"
            logger.info(f"From MongoDB{len(items)}K-line data")
    except Exception as e:
        logger.warning(f"MongoDB to access line K failed:{e}")

    #2. If MongoDB does not have data, downgrade to external API
    if not items:
        logger.info(f"MongoDB has no data, downgraded to external API")
        try:
            import asyncio
            from app.services.data_sources.manager import DataSourceManager

            mgr = DataSourceManager()
            #Add 10 seconds timeout protection
            items, source = await asyncio.wait_for(
                asyncio.to_thread(mgr.get_kline_with_fallback, code_padded, period, limit, adj_norm),
                timeout=10.0
            )
        except asyncio.TimeoutError:
            logger.error(f"❌ External API Retrieving K-line Timeout (10 seconds)")
            raise HTTPException(status_code=504, detail="获取K线数据超时，请稍后重试")
        except Exception as e:
            logger.error(f"External API access to K-line failed:{e}")
            raise HTTPException(status_code=500, detail=f"获取K线数据失败: {str(e)}")

    #3. Check if real-time data for the day need to be added (for daylight only)
    if period == "day" and items:
        try:
            #Check if the same day data are available in historical data (support both date formats)
            has_today_data = any(
                item.get("time") in [today_str_yyyymmdd, today_str_formatted]
                for item in items
            )

            #Determination of buffer period within transaction time or after closing
            current_time = now.time()
            is_weekday = now.weekday() < 5  #Monday to Friday.

            #Transaction time: 9.30-11.30, 15:00-15:00
            #Post-disbursement buffer period: 15:00-15:30 (ensure collection price)
            is_trading_time = (
                is_weekday and (
                    (dtime(9, 30) <= current_time <= dtime(11, 30)) or
                    (dtime(13, 0) <= current_time <= dtime(15, 30))
                )
            )

            #🔥 Add real-time data only during the trading time or in the buffer period after closing
            #No real-time data added on non-trading days (weeks, holidays)
            should_fetch_realtime = is_trading_time

            if should_fetch_realtime:
                logger.info(f"🔥 trying to get real-time data from the day:{code_padded}(transaction time:{is_trading_time}, the day data are available:{has_today_data})")

                db = get_mongo_db()
                market_quotes_coll = db["market_quotes"]

                #Query real-time lines on the day
                realtime_quote = await market_quotes_coll.find_one({"code": code_padded})

                if realtime_quote:
                    #🔥 Constructs the K-line data for the day (using the unified date format YYY-MM-DD)
                    today_kline = {
                        "time": today_str_formatted,  #Use YYY-MM-DD format, consistent with historical data
                        "open": float(realtime_quote.get("open", 0)),
                        "high": float(realtime_quote.get("high", 0)),
                        "low": float(realtime_quote.get("low", 0)),
                        "close": float(realtime_quote.get("close", 0)),
                        "volume": float(realtime_quote.get("volume", 0)),
                        "amount": float(realtime_quote.get("amount", 0)),
                    }

                    #Replace with the current day data if historical data are available; otherwise add
                    if has_today_data:
                        #Replace the last data (assuming the last is on the same day)
                        items[-1] = today_kline
                        logger.info(f"Replace the current K-line data:{code_padded}")
                    else:
                        #Add to End
                        items.append(today_kline)
                        logger.info(f"Add K-line data for the day:{code_padded}")

                    source = f"{source}+market_quotes"
                else:
                    logger.warning(f"No current data were found in ⚠️ market quotes:{code_padded}")
        except Exception as e:
            logger.warning(f"Could not close temporary folder: %s{e}")

    data = {
        "code": code_padded,
        "period": period,
        "limit": limit,
        "adj": adj if adj else "none",
        "source": source,
        "items": items or []
    }
    return ok(data)


@router.get("/{code}/news", response_model=dict)
async def get_news(code: str, days: int = 30, limit: int = 50, include_announcements: bool = True, current_user: dict = Depends(get_current_user)):
    """Access to news and announcements (support to Unit A, Port Unit, United States Unit)"""
    from app.services.foreign_stock_service import ForeignStockService
    from app.services.news_data_service import get_news_data_service, NewsQueryParams

    #Test for stock type
    market, normalized_code = _detect_market_and_code(code)

    if market == 'US':
        #United States shares: using ForestStockService
        service = ForeignStockService()
        result = await service.get_us_news(normalized_code, days=days, limit=limit)
        return ok(result)
    elif market == 'HK':
        #Port Unit: provisional return data (TODO: Port Unit News)
        data = {
            "code": normalized_code,
            "days": days,
            "limit": limit,
            "source": "none",
            "items": []
        }
        return ok(data)
    else:
        #Unit A: Query method for direct synchronisation services (including intelligent regression logic)
        try:
            logger.info(f"=" * 80)
            logger.info(f"We're getting news.{code}, normalized_code={normalized_code}, days={days}, limit={limit}")

            #Query logic for directly using news data path
            from app.services.news_data_service import get_news_data_service, NewsQueryParams
            from datetime import datetime, timedelta
            from app.worker.akshare_sync_service import get_akshare_sync_service

            service = await get_news_data_service()
            sync_service = await get_akshare_sync_service()

            #Calculate the time frame
            hours_back = days * 24

            #🔥 without setting the start time limit, direct to the latest N-line news
            #Because the news in the database may not be from the last few days, but from history.
            params = NewsQueryParams(
                symbol=normalized_code,
                limit=limit,
                sort_by="publish_time",
                sort_order=-1
            )

            logger.info(f"Query parameters: symbol={params.symbol}, limit={params.limit}(no time limit)")

            #1. Query first from the database
            logger.info(f"Step 1: Search for news from the database...")
            news_list = await service.query_news(params)
            logger.info(f"📊 database query result: returns{len(news_list)}News")

            data_source = "database"

            #2. If data are not available in the database, call sync service
            if not news_list:
                logger.info(f"⚠️ database does not contain news data and calls to sync services to:{normalized_code}")
                try:
                    #🔥 CALLS synchronised services to upload individual stock code lists
                    logger.info(f"Step 2: Call the sync service...")
                    await sync_service.sync_news_data(
                        symbols=[normalized_code],
                        max_news_per_stock=limit,
                        force_update=False,
                        favorites_only=False
                    )

                    #Re-Query
                    logger.info(f"Step 3: Retrieval from the database...")
                    news_list = await service.query_news(params)
                    logger.info(f"📊Research result: returns{len(news_list)}News")
                    data_source = "realtime"

                except Exception as e:
                    logger.error(f"Synchronization service anomaly:{e}", exc_info=True)

            #Convert to old format (compatible frontend)
            logger.info(f"Step 4: Converting data formats...")
            items = []
            for news in news_list:
                #Converts a datetime object to an ISO string
                publish_time = news.get("publish_time", "")
                if isinstance(publish_time, datetime):
                    publish_time = publish_time.isoformat()

                items.append({
                    "title": news.get("title", ""),
                    "source": news.get("source", ""),
                    "time": publish_time,
                    "url": news.get("url", ""),
                    "type": "news",
                    "content": news.get("content", ""),
                    "summary": news.get("summary", "")
                })

            logger.info(f"Conversion complete:{len(items)}News")

            data = {
                "code": normalized_code,
                "days": days,
                "limit": limit,
                "include_announcements": include_announcements,
                "source": data_source,
                "items": items
            }

            logger.info(f"Final return: source={data_source}, items_count={len(items)}")
            logger.info(f"=" * 80)
            return ok(data)

        except Exception as e:
            logger.error(f"Access to news failed:{e}", exc_info=True)
            data = {
                "code": normalized_code,
                "days": days,
                "limit": limit,
                "include_announcements": include_announcements,
                "source": None,
                "items": []
            }
            return ok(data)

