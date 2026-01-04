"""Real-time valuation indicator calculation module
Calculation of indicators such as PE/PB based on real-time experience and financial data
"""
import logging
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


def calculate_realtime_pe_pb(
    symbol: str,
    db_client=None
) -> Optional[Dict[str, Any]]:
    """Calculate dynamics based on real-time patterns and Tushare TTM data

    Calculate logic:
    1. Fetch p ttm from stock basic info (based on yesterday 's closing price)
    2. Inverse TTM net profit = total market value / p ttm
    3. Real-time market value using real-time equity prices
    4. Calculation of dynamic PE TTM = real-time market value / TTM net profit

    Args:
        symbol: 6-bit stock code
        db client: MongoDB client (optional, for simultaneous calls)

    Returns:
        FMT 0 
        None if calculation failed
    """
    try:
        #Obtain database connection (ensure that it is synchronized with client)
        if db_client is None:
            from tradingagents.config.database_manager import get_database_manager
            db_manager = get_database_manager()
            if not db_manager.is_mongodb_available():
                logger.debug("MongoDB is not available to calculate real-time PE/PB")
                return None
            db_client = db_manager.get_mongodb_client()

        #Check if it's an odd client
        #Convert to Synchronize client if it is a walk client
        client_type = type(db_client).__name__
        if 'AsyncIOMotorClient' in client_type or 'Motor' in client_type:
            #This is a walk client. Create a synchronized client
            from pymongo import MongoClient
            from app.core.config import SETTINGS
            logger.debug(f"Distant client detected{client_type}Convert to Sync client")
            db_client = MongoClient(SETTINGS.MONGO_URI)

        db = db_client['tradingagents']
        code6 = str(symbol).zfill(6)

        logger.info(f"[real time PE calculations]{code6}")

        #1. Access to real-time situations (market quotes)
        quote = db.market_quotes.find_one({"code": code6})
        if not quote:
            logger.warning(f"No stocks found{code6}Timeline Data")
            return None

        realtime_price = quote.get("close")
        pre_close = quote.get("pre_close")  #Yesterday's closing price.
        quote_updated_at = quote.get("updated_at", "N/A")

        if not realtime_price or realtime_price <= 0:
            logger.warning(f"⚠️ [real-time PE calculations -- failure]{code6}is invalid:{realtime_price}")
            return None

        logger.info(f"✓ Real-time share price:{realtime_price}Dollar (update:{quote_updated_at})")
        logger.info(f"♪ Yesterday's closing price:{pre_close}Dollar")

        #2. Access to basic information (stock basic info) - access to p ttm and market value data for Tushare
        #🔥 Priority query for Tushare data sources (because only Tushare has pe ttm, total mv, total share)
        logger.info(f"[MongoDB query]{code6}, source=tushare")
        basic_info = db.stock_basic_info.find_one({"code": code6, "source": "tushare"})

        if not basic_info:
            #Diagnosis: See what data sources are in MongoDB
            all_sources = list(db.stock_basic_info.find({"code": code6}, {"source": 1, "_id": 0}))
            logger.warning(f"No Tushare data found")
            logger.warning(f"Data source for the stock in MongoDB:{[s.get('source') for s in all_sources]}")

            #Try querying other data sources if no Tushare data
            basic_info = db.stock_basic_info.find_one({"code": code6})
            if not basic_info:
                logger.warning(f"No stocks found{code6}Basic information")
                logger.warning(f"Recommendation: Run a Tushare data sync task to ensure that stock basic info contains Tushare data")
                return None
            else:
                logger.warning(f"Use of other data sources:{basic_info.get('source', 'unknown')}")
                #If Tushare data are not available, key fields may be missing and go straight to None
                if basic_info.get('source') != 'tushare':
                    logger.warning(f"⚠️ [Dynamic PE Calculator - Failed] Data source{basic_info.get('source')}Does not contain p ttm")
                    logger.warning(f"Available fields:{list(basic_info.keys())}")
                    return None

        #Get p ttm from Tushare (based on yesterday's closing price)
        pe_ttm_tushare = basic_info.get("pe_ttm")
        pe_tushare = basic_info.get("pe")
        pb_tushare = basic_info.get("pb")
        total_mv_yi = basic_info.get("total_mv")  #Total market value (billions of dollars)
        total_share = basic_info.get("total_share")  #Total equity (millions)
        basic_info_updated_at = basic_info.get("updated_at")  #Update Time

        logger.info(f"   ✓ Tushare PE_TTM: {pe_ttm_tushare}Double")
        logger.info(f"   ✓ Tushare PE: {pe_tushare}Double")
        logger.info(f"Tushare total market value:{total_mv_yi}Billions.")
        logger.info(f"Total equity:{total_share}Thousand shares")
        logger.info(f"Update:{basic_info_updated_at}")

        #3. Whether or not to recalculate market value
        #If stock basic info updates after closing today (after 15:00), the data is up to date
        from datetime import datetime, time as dtime
        from zoneinfo import ZoneInfo

        need_recalculate = True
        if basic_info_updated_at:
            #Ensure time with time zone information
            if isinstance(basic_info_updated_at, datetime):
                if basic_info_updated_at.tzinfo is None:
                    basic_info_updated_at = basic_info_updated_at.replace(tzinfo=ZoneInfo("Asia/Shanghai"))

                #Can not open message
                today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
                update_date = basic_info_updated_at.date()
                update_time = basic_info_updated_at.time()

                #If the date of update is today and the time of update is 1500, indicate that the data are the latest data since closing today
                if update_date == today and update_time >= dtime(15, 0):
                    need_recalculate = False
                    logger.info(f"💡stock basic info has been updated after closing today to use its data directly")

        if not need_recalculate:
            #Directly using stock basic info data without recalculation
            logger.info(f"✓ Recent data from stock basic info (no recalculation required)")

            result = {
                "pe": round(pe_tushare, 2) if pe_tushare else None,
                "pb": round(pb_tushare, 2) if pb_tushare else None,
                "pe_ttm": round(pe_ttm_tushare, 2) if pe_ttm_tushare else None,
                "price": round(realtime_price, 2),
                "market_cap": round(total_mv_yi, 2) if total_mv_yi else None,
                "updated_at": quote.get("updated_at"),
                "source": "stock_basic_info_latest",
                "is_realtime": False,
                "note": "使用stock_basic_info收盘后最新数据",
            }

            logger.info(f"✅ [Dynamic PE calculations -- Success]{code6}: PE_TTM={result['pe_ttm']}Double, PB={result['pb']}Double (from stock basic info)")
            return result

        #4. 🔥 Calculating gross equity (needs to judge whether the market value of stock basic info is yesterday or today)
        total_shares_wan = None
        yesterday_mv_yi = None

        #Option 1: Prioritize total share in stock basic info (if any)
        if total_share and total_share > 0:
            total_shares_wan = total_share
            logger.info(f"Use stock basic info.total share:{total_shares_wan:.2f}Thousand shares")

            #Calculated yesterday's market value = gross equity x yesterday's closing price
            if pre_close and pre_close > 0:
                yesterday_mv_yi = (total_shares_wan * pre_close) / 10000
                logger.info(f"✓ Market value yesterday:{total_shares_wan:.2f}10,000 shares x{pre_close:.2f}Dollar / 10000 ={yesterday_mv_yi:.2f}Billions.")
            elif total_mv_yi and total_mv_yi > 0:
                #Use the market value of stock basic info if yesterday 's closing price (assuming yesterday 's)
                yesterday_mv_yi = total_mv_yi
                logger.info(f"No pre close, using stock basic info as the market value for yesterday:{yesterday_mv_yi:.2f}Billions.")
            else:
                #Neither pre close nor total mv yi, uncalculated
                logger.warning(f"Can't get yesterday's market value: pre close={pre_close}, total_mv={total_mv_yi}")
                return None

        #Option 2: reverse equity by pre close (received yesterday)
        elif pre_close and pre_close > 0 and total_mv_yi and total_mv_yi > 0:
            #Key: judgement total mv yi was yesterday or today
            #If stock basic info updates before closing today, total mv yi is the market value of yesterday
            #If the update time is closed today, indicate that total mv yi is the market value of today and needs to be reversed with realtime price

            #Determines whether stock basic info is yesterday's data
            is_yesterday_data = True
            if basic_info_updated_at and isinstance(basic_info_updated_at, datetime):
                if basic_info_updated_at.tzinfo is None:
                    basic_info_updated_at = basic_info_updated_at.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
                today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
                update_date = basic_info_updated_at.date()
                update_time = basic_info_updated_at.time()
                #If the update date is today, and the update time is 1500, it means today's data
                if update_date == today and update_time >= dtime(15, 0):
                    is_yesterday_data = False

            if is_yesterday_data:
                #Total mv yi is yesterday's market value with pre close inverse equity
                total_shares_wan = (total_mv_yi * 10000) / pre_close
                yesterday_mv_yi = total_mv_yi
                logger.info(f"✓ stock basic info is yesterday's data.{total_mv_yi:.2f}Billion dollars/{pre_close:.2f}Dollar ={total_shares_wan:.2f}Thousand shares")
            else:
                #Total mv yi is today's market value, reverse equity with realtime price
                total_shares_wan = (total_mv_yi * 10000) / realtime_price
                yesterday_mv_yi = (total_shares_wan * pre_close) / 10000
                logger.info(f"✓ stock basic info is today's data.{total_mv_yi:.2f}Billion dollars/{realtime_price:.2f}Dollar ={total_shares_wan:.2f}Thousand shares")
                logger.info(f"✓ Market value yesterday:{total_shares_wan:.2f}10,000 shares x{pre_close:.2f}Dollar / 10000 ={yesterday_mv_yi:.2f}Billions.")

        #Option 3: only total mv yi, not pre close (market quotes data incomplete)
        elif total_mv_yi and total_mv_yi > 0:
            #Use realtime price inverse equity, assuming total mv yi is yesterday's market value
            total_shares_wan = (total_mv_yi * 10000) / realtime_price
            yesterday_mv_yi = total_mv_yi
            logger.warning(f"There's no pre close in the list.")
            logger.info(f"• Respond to gross equity with realtime price:{total_mv_yi:.2f}Billion dollars/{realtime_price:.2f}Dollar ={total_shares_wan:.2f}Thousand shares")
            logger.info(f"• The market value of yesterday (assuming):{yesterday_mv_yi:.2f}Billions.")

        #Option 4: If none, it cannot be calculated
        else:
            logger.warning(f"⚠️ [Dynamic PE Calculator - Failed]")
            logger.warning(f"   - total_share: {total_share}")
            logger.warning(f"   - pre_close: {pre_close}")
            logger.warning(f"   - total_mv: {total_mv_yi}")
            return None

        #5. Inverse TTM net profit from Tushare pe ttm (using yesterday ' s market value)

        if not pe_ttm_tushare or pe_ttm_tushare <= 0 or not yesterday_mv_yi or yesterday_mv_yi <= 0:
            logger.warning(f"Can't reverse net profit of TTM: p ttm={pe_ttm_tushare}, yesterday_mv={yesterday_mv_yi}")
            logger.warning(f"It's probably a loss of stock.")
            return None

        #Inverse TTM net profit (billion yuan) = market value of yesterday / PE TTM
        ttm_net_profit_yi = yesterday_mv_yi / pe_ttm_tushare
        logger.info(f"TTM net profit:{yesterday_mv_yi:.2f}Billion dollars/{pe_ttm_tushare:.2f}Double ={ttm_net_profit_yi:.2f}Billions.")

        #6. Calculation of real-time market value (billions of dollars) = gross equity (twice shares) x real-time equity (dollars) / 10000
        realtime_mv_yi = (realtime_price * total_shares_wan) / 10000
        logger.info(f":: Real-time market value:{realtime_price:.2f}Dollar x{total_shares_wan:.2f}Ten thousand shares / 10000 ={realtime_mv_yi:.2f}Billions.")

        #7. Calculation of dynamic PE TTM = real-time market value / TTM net profit
        dynamic_pe_ttm = realtime_mv_yi / ttm_net_profit_yi
        logger.info(f"Dynamic PE TTM calculation:{realtime_mv_yi:.2f}Billion dollars/{ttm_net_profit_yi:.2f}Billion dollars ={dynamic_pe_ttm:.2f}Double")

        #8. Access to financial data (for calculation of PB)
        financial_data = db.stock_financial_data.find_one({"code": code6}, sort=[("report_period", -1)])
        pb = None
        total_equity_yi = None

        if financial_data:
            total_equity = financial_data.get("total_equity")  #Net assets ($)
            if total_equity and total_equity > 0:
                total_equity_yi = total_equity / 100000000  #Convert to Billion Dollars
                pb = realtime_mv_yi / total_equity_yi
                logger.info(f"✓ Dynamic PB calculation:{realtime_mv_yi:.2f}Billion dollars/{total_equity_yi:.2f}Billion dollars ={pb:.2f}Double")
            else:
                logger.warning(f"⚠️PB calculation failed: net asset invalid ({total_equity})")
        else:
            logger.warning(f"No financial data was found to calculate the PB")
            #Use Tushare 's PB as demotion
            if pb_tushare:
                pb = pb_tushare
                logger.info(f"Tushare PB:{pb}Double")

        #9. Build return results
        result = {
            "pe": round(dynamic_pe_ttm, 2),  #Dynamic PE (based on TTM)
            "pb": round(pb, 2) if pb else None,
            "pe_ttm": round(dynamic_pe_ttm, 2),  #Dynamic PE TTM
            "price": round(realtime_price, 2),
            "market_cap": round(realtime_mv_yi, 2),  #Real-time market value (billions of dollars)
            "ttm_net_profit": round(ttm_net_profit_yi, 2),  #TTM net profit (billions of dollars)
            "updated_at": quote.get("updated_at"),
            "source": "realtime_calculated_from_market_quotes",
            "is_realtime": True,
            "note": "基于market_quotes实时股价和pre_close计算",
            "total_shares": round(total_shares_wan, 2),  #Total equity (millions)
            "yesterday_close": round(pre_close, 2) if pre_close else None,  #Yesterday's closing price (reference)
            "tushare_pe_ttm": round(pe_ttm_tushare, 2),  #Tushare PE TTM (reference)
            "tushare_pe": round(pe_tushare, 2) if pe_tushare else None,  #Tushare PE (reference)
        }

        logger.info(f"✅ [Dynamic PE calculations -- Success]{code6}: Dynamic PE TTM={result['pe_ttm']}Double, PB={result['pb']}Double")
        return result
        
    except Exception as e:
        logger.error(f"Equities calculated{symbol}Time-use PE/PB failed:{e}", exc_info=True)
        return None


def validate_pe_pb(pe: Optional[float], pb: Optional[float]) -> bool:
    """Validate whether PE/PB is within reasonable limits

    Args:
        p: Profits
        pb Net market ratio

    Returns:
        Bool: Reasonable
    """
    #PE's reasonable range: -100 to 1000 (permissible negative value because the deficit enterprise PE is negative)
    if pe is not None and (pe < -100 or pe > 1000):
        logger.warning(f"PE anomaly:{pe}")
        return False
    
    #PB reasonable range: 0.1 to 100
    if pb is not None and (pb < 0.1 or pb > 100):
        logger.warning(f"PB anomaly:{pb}")
        return False
    
    return True


def get_pe_pb_with_fallback(
    symbol: str,
    db_client=None
) -> Dict[str, Any]:
    """Get PE/PB, smart downgrade policy

    Policy:
    1. Prefer dynamic PE (based on real-time equity + Tushare TTM net profit)
    If dynamic calculations fail, downgrade to Tushare Static PE (based on yesterday's closing prices)

    Strengths:
    - Dynamic PE reflects real-time stock price changes
    - Use Tushare official TTM net profit (inverse) to avoid single-quarter data errors
    - The calculations are accurate, the logs are detailed.

    Args:
        symbol: 6-bit stock code
        db client: MongoDB client (optional)

    Returns:
        FMT 0 
    """
    logger.info(f"[PE Smart Strategy]{symbol}PE/PB")

    #Prepare database connections
    try:
        if db_client is None:
            from tradingagents.config.database_manager import get_database_manager
            db_manager = get_database_manager()
            if not db_manager.is_mongodb_available():
                logger.error("MongoDB is not available")
                return {}
            db_client = db_manager.get_mongodb_client()

        #Check if it's a walker client
        client_type = type(db_client).__name__
        if 'AsyncIOMotorClient' in client_type or 'Motor' in client_type:
            from pymongo import MongoClient
            from app.core.config import SETTINGS
            logger.debug(f"Distant client detected{client_type}Convert to Sync client")
            db_client = MongoClient(SETTINGS.MONGO_URI)

    except Exception as e:
        logger.error(f"[PE smart policy-failed] Database connection failed:{e}")
        return {}

    #1. Prefer dynamic PE calculations (based on real-time equity + Tushare TTM)
    logger.info("• Trial option 1: Dynamic PE calculations (real-time share price + Tushare TTM net profit)")
    logger.info("Note 💡: Use real-time equity and Tushare official net TTM profits to accurately reflect current valuations")

    realtime_metrics = calculate_realtime_pe_pb(symbol, db_client)
    if realtime_metrics:
        #Validation of data reasonableness
        pe = realtime_metrics.get('pe')
        pb = realtime_metrics.get('pb')
        if validate_pe_pb(pe, pb):
            logger.info(f"Use dynamic PE: PE={pe}, PB={pb}")
            logger.info(f"Data source:{realtime_metrics.get('source')}")
            logger.info(f"TTM net profit:{realtime_metrics.get('ttm_net_profit')}Billions.")
            return realtime_metrics
        else:
            logger.warning(f"⚠️ [PE Smart Strategy - Option 1 Anomalous] Dynamic PE/PB is beyond reasonable range (PE=){pe}, PB={pb})")

    #2. Downgrade to Tushare Static PE (based on yesterday ' s closing price)
    logger.info("Tushare Static PE (based on yesterday's closing price)")
    logger.info("💡 Description: Use Tushare Official PE TTM based on yesterday's closing price")

    try:
        db = db_client['tradingagents']
        code6 = str(symbol).zfill(6)

        #🔥 priority search for Tushare data source
        basic_info = db.stock_basic_info.find_one({"code": code6, "source": "tushare"})
        if not basic_info:
            #Try querying other data sources if no Tushare data
            basic_info = db.stock_basic_info.find_one({"code": code6})

        if basic_info:
            pe_static = basic_info.get("pe")
            pb_static = basic_info.get("pb")
            pe_ttm = basic_info.get("pe_ttm")
            pb_mrq = basic_info.get("pb_mrq")
            updated_at = basic_info.get("updated_at", "N/A")

            if pe_ttm or pe_static or pb_static:
                logger.info(f"[PE smart policy-successful]{pe_static}, PE_TTM={pe_ttm}, PB={pb_static}")
                logger.info(f"└ - Data source: stock basic info (update:{updated_at})")

                return {
                    "pe": pe_static,
                    "pb": pb_static,
                    "pe_ttm": pe_ttm,
                    "pb_mrq": pb_mrq,
                    "source": "daily_basic",
                    "is_realtime": False,
                    "updated_at": updated_at,
                    "note": "使用Tushare最近一个交易日的数据（基于TTM）"
                }

        logger.warning("Tushare static data are not available")

    except Exception as e:
        logger.warning(f"[PE Smart Strategy - Option 2 Aberrant]{e}")

    logger.error(f"Can't get a stock.{symbol}PE/PB")
    return {}

