"""Blocked tool function for Tushare:
- Fetch stock basic df: get list of shares (ensure that Tushare is connected)
- find latest trade date: detect the latest available trading day (YYYYMMDD)
- Fetch daily basic mv map: acquisition of a map of basic indicators of the day of the transaction (market value/value/transaction)
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict


def fetch_stock_basic_df():
    """Gets the stock base list (DataFrame format) from Tushare, which requires that it be configured and connected correctly.
Reliance on environmental variables: TUSHARE ENABLED=tru and .env provides TUSHARE TOKEN.

Note: This is a synchronized function that will wait for the Tushare connection to complete.
"""
    import time
    import logging
    from tradingagents.dataflows.providers.china.tushare import get_tushare_provider
    from app.core.config import settings

    logger = logging.getLogger(__name__)

    #Check whether Tushare is enabled
    if not settings.TUSHARE_ENABLED:
        logger.error("Tushare data source is disabled (TUSHARE ENABLED=false)")
        logger.error("Please set TUSHARE ENABLED=true in .env file or use multiple data source sync service")
        raise RuntimeError(
            "Tushare is disabled (TUSHARE_ENABLED=false). "
            "Set TUSHARE_ENABLED=true in .env or use MultiSourceBasicsSyncService."
        )

    provider = get_tushare_provider()

    #Waiting for connection complete (up to 5 seconds)
    max_wait_seconds = 5
    wait_interval = 0.1
    elapsed = 0.0

    logger.info(f"Waiting for Tushare to connect...")
    while not getattr(provider, "connected", False) and elapsed < max_wait_seconds:
        time.sleep(wait_interval)
        elapsed += wait_interval

    #Check connectivity and API availability
    if not getattr(provider, "connected", False) or provider.api is None:
        logger.error(f"Tushare connection failed (waiting){max_wait_seconds}S behind timeout)")
        logger.error(f"Please check:")
        logger.error(f"1. A valid TUSHARE TOKEN is configured in .env files")
        logger.error(f"Tushare Token has not expired and has sufficient points")
        logger.error(f"3. Network connectivity is normal")
        raise RuntimeError(
            f"Tushare not connected after waiting {max_wait_seconds}s. "
            "Check TUSHARE_TOKEN in .env and ensure it's valid."
        )

    logger.info(f"Tushare is connected, starting to get the list of shares...")

    #Directly call Tushare API to DataFrame
    try:
        df = provider.api.stock_basic(
            list_status='L',
            fields='ts_code,symbol,name,area,industry,market,exchange,list_date,is_hs'
        )

        #Enhanced false diagnosis
        if df is None:
            logger.error(f"Tushare API returns None")
            logger.error(f"Possible causes:")
            logger.error(f"Tushare Token is invalid or expired")
            logger.error(f"2. API deficit")
            logger.error(f"3. Network connectivity issues")
            raise RuntimeError("Tushare API returned None. Check token validity and API credits.")

        if hasattr(df, 'empty') and df.empty:
            logger.error(f"Tushare API returns empty DataFrame")
            logger.error(f"Possible causes:")
            logger.error(f"List status='L'parameters may not be correct")
            logger.error(f"2. Tushare data sources are temporarily unavailable")
            logger.error(f"3. API call limits (check points and call frequency)")
            raise RuntimeError("Tushare API returned empty DataFrame. Check API parameters and data availability.")

        logger.info(f"Successfully accessed{len(df)}Stock data")
        return df

    except Exception as e:
        logger.error(f"Call to Tushare API failed:{e}")
        raise RuntimeError(f"Failed to fetch stock basic DataFrame: {e}")


def find_latest_trade_date() -> str:
    """Detection of the latest available trading day (YYYYMMDD).
- A maximum of five days from today;
- If not, back to yesterday's date.
"""
    from tradingagents.dataflows.providers.china.tushare import get_tushare_provider

    provider = get_tushare_provider()
    api = provider.api
    if api is None:
        raise RuntimeError("Tushare API unavailable")

    today = datetime.now()
    for delta in range(0, 6):
        d = (today - timedelta(days=delta)).strftime("%Y%m%d")
        try:
            db = api.daily_basic(trade_date=d, fields="ts_code,total_mv")
            if db is not None and not db.empty:
                return d
        except Exception:
            continue
    return (today - timedelta(days=1)).strftime("%Y%m%d")


def fetch_daily_basic_mv_map(trade_date: str) -> Dict[str, Dict[str, float]]:
    """Maps the basic daily indicators according to the date of transaction.
Overwrite field: Total mv/circ mv/pe/pb/ps/turnover rate/volume ratio/pe ttm/pb mrq/ps ttm
"""
    from tradingagents.dataflows.providers.china.tushare import get_tushare_provider

    provider = get_tushare_provider()
    api = provider.api
    if api is None:
        raise RuntimeError("Tushare API unavailable")

    #Add: ps, ps ttm, total share, float share field
    fields = "ts_code,total_mv,circ_mv,pe,pb,ps,turnover_rate,volume_ratio,pe_ttm,pb_mrq,ps_ttm,total_share,float_share"
    db = api.daily_basic(trade_date=trade_date, fields=fields)

    data_map: Dict[str, Dict[str, float]] = {}
    if db is not None and not db.empty:
        for _, row in db.iterrows():  # type: ignore
            ts_code = row.get("ts_code")
            if ts_code is not None:
                try:
                    metrics = {}
                    #Add: ps, ps ttm, total share, float share to field list
                    for field in [
                        "total_mv",
                        "circ_mv",
                        "pe",
                        "pb",
                        "ps",
                        "turnover_rate",
                        "volume_ratio",
                        "pe_ttm",
                        "pb_mrq",
                        "ps_ttm",
                        "total_share",
                        "float_share",
                    ]:
                        value = row.get(field)
                        if value is not None and str(value).lower() not in ["nan", "none", ""]:
                            metrics[field] = float(value)
                    if metrics:
                        data_map[str(ts_code)] = metrics
                except Exception:
                    pass
    return data_map




def fetch_latest_roe_map() -> Dict[str, Dict[str, float]]:
    """Gets the RoE map (ts code-> FMT 0   ) of the latest available financial period.
Priority is given to finding the first non-empty data in reverse sequence for the latest quarter.
"""
    from tradingagents.dataflows.providers.china.tushare import get_tushare_provider
    from datetime import datetime

    provider = get_tushare_provider()
    api = provider.api
    if api is None:
        raise RuntimeError("Tushare API unavailable")

    #Generate end-of-cycle dates for the most recent financial quarters, format YYYMMDD
    def quarter_ends(now: datetime):
        y = now.year
        q_dates = [
            f"{y}0331",
            f"{y}0630",
            f"{y}0930",
            f"{y}1231",
        ]
        #Include the previous year, increase the probability of success
        py = y - 1
        q_dates_prev = [
            f"{py}1231",
            f"{py}0930",
            f"{py}0630",
            f"{py}0331",
        ]
        #Almost six.
        return q_dates_prev + q_dates

    candidates = quarter_ends(datetime.now())
    data_map: Dict[str, Dict[str, float]] = {}

    for end_date in candidates:
        try:
            df = api.fina_indicator(end_date=end_date, fields="ts_code,end_date,roe")
            if df is not None and not df.empty:
                for _, row in df.iterrows():  # type: ignore
                    ts_code = row.get("ts_code")
                    val = row.get("roe")
                    if ts_code is None or val is None:
                        continue
                    try:
                        v = float(val)
                    except Exception:
                        continue
                    data_map[str(ts_code)] = {"roe": v}
                if data_map:
                    break  #Just find the last issue.
        except Exception:
            continue

    return data_map
