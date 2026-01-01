from typing import Annotated, Dict
import time
import os
from datetime import datetime

#Import news module (support old and new paths)
try:
    from .news import fetch_top_from_category
except ImportError:
    from .news.reddit import fetch_top_from_category

from .news.google_news import *


from .news.chinese_finance import get_chinese_social_sentiment

#Import Finnhub tool (support old and new paths)

from .providers.us import get_data_in_range


#Import Unified Log System
from tradingagents.utils.logging_init import setup_dataflow_logging

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')
logger = setup_dataflow_logging()

#Import Port Unit Tool
try:
    from .providers.hk.hk_stock import get_hk_stock_data, get_hk_stock_info
    HK_STOCK_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Port Unit tools are not available:{e}")
    HK_STOCK_AVAILABLE = False

#Import AKShare Stock Tool
#Note: Port Unit functions in providers/hk/ directory
try:
    from .providers.hk.improved_hk import get_hk_stock_data_akshare, get_hk_stock_info_akshare
    AKSHARE_HK_AVAILABLE = True
except (ImportError, AttributeError) as e:
    logger.warning(f"The AKShare Port tool is not available:{e}")
    AKSHARE_HK_AVAILABLE = False
    #Define placeholder function
    def get_hk_stock_data_akshare(*args, **kwargs):
        return None
    def get_hk_stock_info_akshare(*args, **kwargs):
        return None


#== sync, corrected by elderman == @elder man

def _get_enabled_hk_data_sources() -> list:
    """Access to user-enabled port unit data source configuration from database

Returns:
list: list of data sources in order of priority, e.g. ['akshare', 'yfinance']
"""
    try:
        #Try reading configurations from the database
        from app.core.database import get_mongo_db_synchronous
        db = get_mongo_db_synchronous()

        #Get the latest active configuration
        config_data = db.system_configs.find_one(
            {"is_active": True},
            sort=[("version", -1)]
        )

        if config_data and config_data.get('data_source_configs'):
            data_source_configs = config_data.get('data_source_configs', [])

            #Filters enabled port stock data sources
            enabled_sources = []
            for ds in data_source_configs:
                if not ds.get('enabled', True):
                    continue

                #Check to support the port stock market (support to the Central English logo)
                market_categories = ds.get('market_categories', [])
                if market_categories:
                    #Support the Port Unit or 'hk stocks'
                    if '港股' not in market_categories and 'hk_stocks' not in market_categories:
                        continue

                #Map data source type
                ds_type = ds.get('type', '').lower()
                if ds_type in ['akshare', 'yfinance', 'finnhub']:
                    enabled_sources.append({
                        'type': ds_type,
                        'priority': ds.get('priority', 0)
                    })

            #Sort by priority (the larger the number, the higher the priority)
            enabled_sources.sort(key=lambda x: x['priority'], reverse=True)

            result = [s['type'] for s in enabled_sources]
            if result:
                logger.info(f"Access to the database:{result}")
                return result
            else:
                logger.warning(f"⚠️ [Hong Kong Unit Data Source] Data source not enabled in database, using default order")
        else:
            logger.warning("⚠️ [port data source] database is not configured, using default order")
    except Exception as e:
        logger.warning(f"Access to the database failed:{e}, using default order")

    #Back to Default Order
    return ['akshare', 'yfinance']


def _get_enabled_us_data_sources() -> list:
    """Read user-enabled US share data source configuration from database

Returns:
list: list of data sources in order of priority, such as ['yfinance', 'finnhub']
"""
    try:
        #Try reading configurations from the database
        from app.core.database import get_mongo_db_synchronous
        db = get_mongo_db_synchronous()

        #Get the latest active configuration
        config_data = db.system_configs.find_one(
            {"is_active": True},
            sort=[("version", -1)]
        )

        if config_data and config_data.get('data_source_configs'):
            data_source_configs = config_data.get('data_source_configs', [])

            #Filtering enabled US stock data sources
            enabled_sources = []
            for ds in data_source_configs:
                if not ds.get('enabled', True):
                    continue

                #Check for support to the United States stock market (support for the Central English logo)
                market_categories = ds.get('market_categories', [])
                if market_categories:
                    #Support 'American shares' or 'us stocks'
                    if '美股' not in market_categories and 'us_stocks' not in market_categories:
                        continue

                #Map data source type
                ds_type = ds.get('type', '').lower()
                if ds_type in ['yfinance', 'finnhub']:
                    enabled_sources.append({
                        'type': ds_type,
                        'priority': ds.get('priority', 0)
                    })

            #Sort by priority (the larger the number, the higher the priority)
            enabled_sources.sort(key=lambda x: x['priority'], reverse=True)

            result = [s['type'] for s in enabled_sources]
            if result:
                logger.info(f"Read from the database:{result}")
                return result
            else:
                logger.warning(f"⚠️ [USE data source] No USE data source enabled in database, using default order")
        else:
            logger.warning("⚠️ [USE data source] database is not configured, using default order")
    except Exception as e:
        logger.warning(f"⚠️ [USE data source] Reading from database failed:{e}, using default order")

    #Back to Default Order
    return ['yfinance', 'finnhub']

#Try importing yfinance-related modules and skip if it fails
try:
    from .providers.us.yfinance import *
    YFIN_AVAILABLE = True
except ImportError as e:
    logger.warning(f"The yfinance tool is not available:{e}")
    YFIN_AVAILABLE = False

try:
    from .technical.stockstats import *
    STOCKSTATS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ Stockstat tools are not available:{e}")
    STOCKSTATS_AVAILABLE = False
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import os
import pandas as pd
from tqdm import tqdm
from openai import OpenAI

#Try importing yfinance, setting it to Noone if it fails
try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError as e:
    logger.warning(f"The yfinance library is not available:{e}")
    yf = None
    YF_AVAILABLE = False
from tradingagents.config.config_manager import config_manager

#Get Data Directory
DATA_DIR = config_manager.get_data_dir()

def get_config():
    """Acquisition configuration (compatible packaging)"""
    return config_manager.load_settings()

def set_config(config):
    """Settings Configuration (compatible packaging)"""
    config_manager.save_settings(config)


def get_finnhub_news(
    ticker: Annotated[
        str,
        "Search query of a company's, e.g. 'AAPL, TSM, etc.",
    ],
    curr_date: Annotated[str, "Current date in yyyy-mm-dd format"],
    look_back_days: Annotated[int, "how many days to look back"],
):
    """
    Retrieve news about a company within a time frame

    Args
        ticker (str): ticker for the company you are interested in
        start_date (str): Start date in yyyy-mm-dd format
        end_date (str): End date in yyyy-mm-dd format
    Returns
        str: dataframe containing the news of the company in the time frame

    """

    start_date = datetime.strptime(curr_date, "%Y-%m-%d")
    before = start_date - relativedelta(days=look_back_days)
    before = before.strftime("%Y-%m-%d")

    result = get_data_in_range(ticker, before, curr_date, "news_data", DATA_DIR)

    if len(result) == 0:
        error_msg = f"⚠️ 无法获取{ticker}的新闻数据 ({before} 到 {curr_date})\n"
        error_msg += f"可能的原因：\n"
        error_msg += f"1. 数据文件不存在或路径配置错误\n"
        error_msg += f"2. 指定日期范围内没有新闻数据\n"
        error_msg += f"3. 需要先下载或更新Finnhub新闻数据\n"
        error_msg += f"建议：检查数据目录配置或重新获取新闻数据"
        logger.debug(f"📰 [DEBUG] {error_msg}")
        return error_msg

    combined_result = ""
    for day, data in result.items():
        if len(data) == 0:
            continue
        for entry in data:
            current_news = (
                "### " + entry["headline"] + f" ({day})" + "\n" + entry["summary"]
            )
            combined_result += current_news + "\n\n"

    return f"## {ticker} News, from {before} to {curr_date}:\n" + str(combined_result)


def get_finnhub_company_insider_sentiment(
    ticker: Annotated[str, "ticker symbol for the company"],
    curr_date: Annotated[
        str,
        "current date of you are trading at, yyyy-mm-dd",
    ],
    look_back_days: Annotated[int, "number of days to look back"],
):
    """
    Retrieve insider sentiment about a company (retrieved from public SEC information) for the past 15 days
    Args:
        ticker (str): ticker symbol of the company
        curr_date (str): current date you are trading on, yyyy-mm-dd
    Returns:
        str: a report of the sentiment in the past 15 days starting at curr_date
    """

    date_obj = datetime.strptime(curr_date, "%Y-%m-%d")
    before = date_obj - relativedelta(days=look_back_days)
    before = before.strftime("%Y-%m-%d")

    data = get_data_in_range(ticker, before, curr_date, "insider_senti", DATA_DIR)

    if len(data) == 0:
        return ""

    result_str = ""
    seen_dicts = []
    for date, senti_list in data.items():
        for entry in senti_list:
            if entry not in seen_dicts:
                result_str += f"### {entry['year']}-{entry['month']}:\nChange: {entry['change']}\nMonthly Share Purchase Ratio: {entry['mspr']}\n\n"
                seen_dicts.append(entry)

    return (
        f"## {ticker} Insider Sentiment Data for {before} to {curr_date}:\n"
        + result_str
        + "The change field refers to the net buying/selling from all insiders' transactions. The mspr field refers to monthly share purchase ratio."
    )


def get_finnhub_company_insider_transactions(
    ticker: Annotated[str, "ticker symbol"],
    curr_date: Annotated[
        str,
        "current date you are trading at, yyyy-mm-dd",
    ],
    look_back_days: Annotated[int, "how many days to look back"],
):
    """
    Retrieve insider transcaction information about a company (retrieved from public SEC information) for the past 15 days
    Args:
        ticker (str): ticker symbol of the company
        curr_date (str): current date you are trading at, yyyy-mm-dd
    Returns:
        str: a report of the company's insider transaction/trading informtaion in the past 15 days
    """

    date_obj = datetime.strptime(curr_date, "%Y-%m-%d")
    before = date_obj - relativedelta(days=look_back_days)
    before = before.strftime("%Y-%m-%d")

    data = get_data_in_range(ticker, before, curr_date, "insider_trans", DATA_DIR)

    if len(data) == 0:
        return ""

    result_str = ""

    seen_dicts = []
    for date, senti_list in data.items():
        for entry in senti_list:
            if entry not in seen_dicts:
                result_str += f"### Filing Date: {entry['filingDate']}, {entry['name']}:\nChange:{entry['change']}\nShares: {entry['share']}\nTransaction Price: {entry['transactionPrice']}\nTransaction Code: {entry['transactionCode']}\n\n"
                seen_dicts.append(entry)

    return (
        f"## {ticker} insider transactions from {before} to {curr_date}:\n"
        + result_str
        + "The change field reflects the variation in share count—here a negative number indicates a reduction in holdings—while share specifies the total number of shares involved. The transactionPrice denotes the per-share price at which the trade was executed, and transactionDate marks when the transaction occurred. The name field identifies the insider making the trade, and transactionCode (e.g., S for sale) clarifies the nature of the transaction. FilingDate records when the transaction was officially reported, and the unique id links to the specific SEC filing, as indicated by the source. Additionally, the symbol ties the transaction to a particular company, isDerivative flags whether the trade involves derivative securities, and currency notes the currency context of the transaction."
    )


def get_simfin_balance_sheet(
    ticker: Annotated[str, "ticker symbol"],
    freq: Annotated[
        str,
        "reporting frequency of the company's financial history: annual / quarterly",
    ],
    curr_date: Annotated[str, "current date you are trading at, yyyy-mm-dd"],
):
    data_path = os.path.join(
        DATA_DIR,
        "fundamental_data",
        "simfin_data_all",
        "balance_sheet",
        "companies",
        "us",
        f"us-balance-{freq}.csv",
    )
    df = pd.read_csv(data_path, sep=";")

    # Convert date strings to datetime objects and remove any time components
    df["Report Date"] = pd.to_datetime(df["Report Date"], utc=True).dt.normalize()
    df["Publish Date"] = pd.to_datetime(df["Publish Date"], utc=True).dt.normalize()

    # Convert the current date to datetime and normalize
    curr_date_dt = pd.to_datetime(curr_date, utc=True).normalize()

    # Filter the DataFrame for the given ticker and for reports that were published on or before the current date
    filtered_df = df[(df["Ticker"] == ticker) & (df["Publish Date"] <= curr_date_dt)]

    # Check if there are any available reports; if not, return a notification
    if filtered_df.empty:
        logger.info(f"No balance sheet available before the given current date.")
        return ""

    # Get the most recent balance sheet by selecting the row with the latest Publish Date
    latest_balance_sheet = filtered_df.loc[filtered_df["Publish Date"].idxmax()]

    # drop the SimFinID column
    latest_balance_sheet = latest_balance_sheet.drop("SimFinId")

    return (
        f"## {freq} balance sheet for {ticker} released on {str(latest_balance_sheet['Publish Date'])[0:10]}: \n"
        + str(latest_balance_sheet)
        + "\n\nThis includes metadata like reporting dates and currency, share details, and a breakdown of assets, liabilities, and equity. Assets are grouped as current (liquid items like cash and receivables) and noncurrent (long-term investments and property). Liabilities are split between short-term obligations and long-term debts, while equity reflects shareholder funds such as paid-in capital and retained earnings. Together, these components ensure that total assets equal the sum of liabilities and equity."
    )


def get_simfin_cashflow(
    ticker: Annotated[str, "ticker symbol"],
    freq: Annotated[
        str,
        "reporting frequency of the company's financial history: annual / quarterly",
    ],
    curr_date: Annotated[str, "current date you are trading at, yyyy-mm-dd"],
):
    data_path = os.path.join(
        DATA_DIR,
        "fundamental_data",
        "simfin_data_all",
        "cash_flow",
        "companies",
        "us",
        f"us-cashflow-{freq}.csv",
    )
    df = pd.read_csv(data_path, sep=";")

    # Convert date strings to datetime objects and remove any time components
    df["Report Date"] = pd.to_datetime(df["Report Date"], utc=True).dt.normalize()
    df["Publish Date"] = pd.to_datetime(df["Publish Date"], utc=True).dt.normalize()

    # Convert the current date to datetime and normalize
    curr_date_dt = pd.to_datetime(curr_date, utc=True).normalize()

    # Filter the DataFrame for the given ticker and for reports that were published on or before the current date
    filtered_df = df[(df["Ticker"] == ticker) & (df["Publish Date"] <= curr_date_dt)]

    # Check if there are any available reports; if not, return a notification
    if filtered_df.empty:
        logger.info(f"No cash flow statement available before the given current date.")
        return ""

    # Get the most recent cash flow statement by selecting the row with the latest Publish Date
    latest_cash_flow = filtered_df.loc[filtered_df["Publish Date"].idxmax()]

    # drop the SimFinID column
    latest_cash_flow = latest_cash_flow.drop("SimFinId")

    return (
        f"## {freq} cash flow statement for {ticker} released on {str(latest_cash_flow['Publish Date'])[0:10]}: \n"
        + str(latest_cash_flow)
        + "\n\nThis includes metadata like reporting dates and currency, share details, and a breakdown of cash movements. Operating activities show cash generated from core business operations, including net income adjustments for non-cash items and working capital changes. Investing activities cover asset acquisitions/disposals and investments. Financing activities include debt transactions, equity issuances/repurchases, and dividend payments. The net change in cash represents the overall increase or decrease in the company's cash position during the reporting period."
    )


def get_simfin_income_statements(
    ticker: Annotated[str, "ticker symbol"],
    freq: Annotated[
        str,
        "reporting frequency of the company's financial history: annual / quarterly",
    ],
    curr_date: Annotated[str, "current date you are trading at, yyyy-mm-dd"],
):
    data_path = os.path.join(
        DATA_DIR,
        "fundamental_data",
        "simfin_data_all",
        "income_statements",
        "companies",
        "us",
        f"us-income-{freq}.csv",
    )
    df = pd.read_csv(data_path, sep=";")

    # Convert date strings to datetime objects and remove any time components
    df["Report Date"] = pd.to_datetime(df["Report Date"], utc=True).dt.normalize()
    df["Publish Date"] = pd.to_datetime(df["Publish Date"], utc=True).dt.normalize()

    # Convert the current date to datetime and normalize
    curr_date_dt = pd.to_datetime(curr_date, utc=True).normalize()

    # Filter the DataFrame for the given ticker and for reports that were published on or before the current date
    filtered_df = df[(df["Ticker"] == ticker) & (df["Publish Date"] <= curr_date_dt)]

    # Check if there are any available reports; if not, return a notification
    if filtered_df.empty:
        logger.info(f"No income statement available before the given current date.")
        return ""

    # Get the most recent income statement by selecting the row with the latest Publish Date
    latest_income = filtered_df.loc[filtered_df["Publish Date"].idxmax()]

    # drop the SimFinID column
    latest_income = latest_income.drop("SimFinId")

    return (
        f"## {freq} income statement for {ticker} released on {str(latest_income['Publish Date'])[0:10]}: \n"
        + str(latest_income)
        + "\n\nThis includes metadata like reporting dates and currency, share details, and a comprehensive breakdown of the company's financial performance. Starting with Revenue, it shows Cost of Revenue and resulting Gross Profit. Operating Expenses are detailed, including SG&A, R&D, and Depreciation. The statement then shows Operating Income, followed by non-operating items and Interest Expense, leading to Pretax Income. After accounting for Income Tax and any Extraordinary items, it concludes with Net Income, representing the company's bottom-line profit or loss for the period."
    )


def get_google_news(
    query: Annotated[str, "Query to search with"],
    curr_date: Annotated[str, "Curr date in yyyy-mm-dd format"],
    look_back_days: Annotated[int, "how many days to look back"] = 7,
) -> str:
    #Check for Unit A
    is_china_stock = False
    if any(code in query for code in ['SH', 'SZ', 'XSHE', 'XSHG']) or query.isdigit() or (len(query) == 6 and query[:6].isdigit()):
        is_china_stock = True
    
    #Try using StockUtils
    try:
        from tradingagents.utils.stock_utils import StockUtils
        market_info = StockUtils.get_market_info(query.split()[0])
        if market_info['is_china']:
            is_china_stock = True
    except Exception:
        #If StockUtils fails, use the simple judgment above.
        pass
    
    #Add a keyword to Unit A query
    if is_china_stock:
        logger.info(f"[Google News]{query}, search in Chinese")
        if '股票' not in query and '股价' not in query and '公司' not in query:
            query = f"{query} 股票 公司 财报 新闻"
    
    query = query.replace(" ", "+")

    start_date = datetime.strptime(curr_date, "%Y-%m-%d")
    before = start_date - relativedelta(days=look_back_days)
    before = before.strftime("%Y-%m-%d")

    logger.info(f"[Google News] Start getting news, querying:{query}, time frame:{before}to{curr_date}")
    news_results = getNewsData(query, before, curr_date)

    news_str = ""

    for news in news_results:
        news_str += (
            f"### {news['title']} (source: {news['source']}) \n\n{news['snippet']}\n\n"
        )

    if len(news_results) == 0:
        logger.warning(f"[Google News] No relevant news.{query}")
        return ""

    logger.info(f"[Google News ]{len(news_results)}News, queries:{query}")
    return f"## {query.replace('+', ' ')} Google News, from {before} to {curr_date}:\n\n{news_str}"


def get_reddit_global_news(
    start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
    look_back_days: Annotated[int, "how many days to look back"],
    max_limit_per_day: Annotated[int, "Maximum number of news per day"],
) -> str:
    """
    Retrieve the latest top reddit news
    Args:
        start_date: Start date in yyyy-mm-dd format
        end_date: End date in yyyy-mm-dd format
    Returns:
        str: A formatted dataframe containing the latest news articles posts on reddit and meta information in these columns: "created_utc", "id", "title", "selftext", "score", "num_comments", "url"
    """

    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    before = start_date - relativedelta(days=look_back_days)
    before = before.strftime("%Y-%m-%d")

    posts = []
    # iterate from start_date to end_date
    curr_date = datetime.strptime(before, "%Y-%m-%d")

    total_iterations = (start_date - curr_date).days + 1
    pbar = tqdm(desc=f"Getting Global News on {start_date}", total=total_iterations)

    while curr_date <= start_date:
        curr_date_str = curr_date.strftime("%Y-%m-%d")
        fetch_result = fetch_top_from_category(
            "global_news",
            curr_date_str,
            max_limit_per_day,
            data_path=os.path.join(DATA_DIR, "reddit_data"),
        )
        posts.extend(fetch_result)
        curr_date += relativedelta(days=1)
        pbar.update(1)

    pbar.close()

    if len(posts) == 0:
        return ""

    news_str = ""
    for post in posts:
        if post["content"] == "":
            news_str += f"### {post['title']}\n\n"
        else:
            news_str += f"### {post['title']}\n\n{post['content']}\n\n"

    return f"## Global News Reddit, from {before} to {curr_date}:\n{news_str}"


def get_reddit_company_news(
    ticker: Annotated[str, "ticker symbol of the company"],
    start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
    look_back_days: Annotated[int, "how many days to look back"],
    max_limit_per_day: Annotated[int, "Maximum number of news per day"],
) -> str:
    """
    Retrieve the latest top reddit news
    Args:
        ticker: ticker symbol of the company
        start_date: Start date in yyyy-mm-dd format
        end_date: End date in yyyy-mm-dd format
    Returns:
        str: A formatted dataframe containing the latest news articles posts on reddit and meta information in these columns: "created_utc", "id", "title", "selftext", "score", "num_comments", "url"
    """

    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    before = start_date - relativedelta(days=look_back_days)
    before = before.strftime("%Y-%m-%d")

    posts = []
    # iterate from start_date to end_date
    curr_date = datetime.strptime(before, "%Y-%m-%d")

    total_iterations = (start_date - curr_date).days + 1
    pbar = tqdm(
        desc=f"Getting Company News for {ticker} on {start_date}",
        total=total_iterations,
    )

    while curr_date <= start_date:
        curr_date_str = curr_date.strftime("%Y-%m-%d")
        fetch_result = fetch_top_from_category(
            "company_news",
            curr_date_str,
            max_limit_per_day,
            ticker,
            data_path=os.path.join(DATA_DIR, "reddit_data"),
        )
        posts.extend(fetch_result)
        curr_date += relativedelta(days=1)

        pbar.update(1)

    pbar.close()

    if len(posts) == 0:
        return ""

    news_str = ""
    for post in posts:
        if post["content"] == "":
            news_str += f"### {post['title']}\n\n"
        else:
            news_str += f"### {post['title']}\n\n{post['content']}\n\n"

    return f"##{ticker} News Reddit, from {before} to {curr_date}:\n\n{news_str}"


def get_stock_stats_indicators_window(
    symbol: Annotated[str, "ticker symbol of the company"],
    indicator: Annotated[str, "technical indicator to get the analysis and report of"],
    curr_date: Annotated[
        str, "The current trading date you are trading on, YYYY-mm-dd"
    ],
    look_back_days: Annotated[int, "how many days to look back"],
    online: Annotated[bool, "to fetch data online or offline"],
) -> str:

    best_ind_params = {
        # Moving Averages
        "close_50_sma": (
            "50 SMA: A medium-term trend indicator. "
            "Usage: Identify trend direction and serve as dynamic support/resistance. "
            "Tips: It lags price; combine with faster indicators for timely signals."
        ),
        "close_200_sma": (
            "200 SMA: A long-term trend benchmark. "
            "Usage: Confirm overall market trend and identify golden/death cross setups. "
            "Tips: It reacts slowly; best for strategic trend confirmation rather than frequent trading entries."
        ),
        "close_10_ema": (
            "10 EMA: A responsive short-term average. "
            "Usage: Capture quick shifts in momentum and potential entry points. "
            "Tips: Prone to noise in choppy markets; use alongside longer averages for filtering false signals."
        ),
        # MACD Related
        "macd": (
            "MACD: Computes momentum via differences of EMAs. "
            "Usage: Look for crossovers and divergence as signals of trend changes. "
            "Tips: Confirm with other indicators in low-volatility or sideways markets."
        ),
        "macds": (
            "MACD Signal: An EMA smoothing of the MACD line. "
            "Usage: Use crossovers with the MACD line to trigger trades. "
            "Tips: Should be part of a broader strategy to avoid false positives."
        ),
        "macdh": (
            "MACD Histogram: Shows the gap between the MACD line and its signal. "
            "Usage: Visualize momentum strength and spot divergence early. "
            "Tips: Can be volatile; complement with additional filters in fast-moving markets."
        ),
        # Momentum Indicators
        "rsi": (
            "RSI: Measures momentum to flag overbought/oversold conditions. "
            "Usage: Apply 70/30 thresholds and watch for divergence to signal reversals. "
            "Tips: In strong trends, RSI may remain extreme; always cross-check with trend analysis."
        ),
        # Volatility Indicators
        "boll": (
            "Bollinger Middle: A 20 SMA serving as the basis for Bollinger Bands. "
            "Usage: Acts as a dynamic benchmark for price movement. "
            "Tips: Combine with the upper and lower bands to effectively spot breakouts or reversals."
        ),
        "boll_ub": (
            "Bollinger Upper Band: Typically 2 standard deviations above the middle line. "
            "Usage: Signals potential overbought conditions and breakout zones. "
            "Tips: Confirm signals with other tools; prices may ride the band in strong trends."
        ),
        "boll_lb": (
            "Bollinger Lower Band: Typically 2 standard deviations below the middle line. "
            "Usage: Indicates potential oversold conditions. "
            "Tips: Use additional analysis to avoid false reversal signals."
        ),
        "atr": (
            "ATR: Averages true range to measure volatility. "
            "Usage: Set stop-loss levels and adjust position sizes based on current market volatility. "
            "Tips: It's a reactive measure, so use it as part of a broader risk management strategy."
        ),
        # Volume-Based Indicators
        "vwma": (
            "VWMA: A moving average weighted by volume. "
            "Usage: Confirm trends by integrating price action with volume data. "
            "Tips: Watch for skewed results from volume spikes; use in combination with other volume analyses."
        ),
        "mfi": (
            "MFI: The Money Flow Index is a momentum indicator that uses both price and volume to measure buying and selling pressure. "
            "Usage: Identify overbought (>80) or oversold (<20) conditions and confirm the strength of trends or reversals. "
            "Tips: Use alongside RSI or MACD to confirm signals; divergence between price and MFI can indicate potential reversals."
        ),
    }

    if indicator not in best_ind_params:
        raise ValueError(
            f"Indicator {indicator} is not supported. Please choose from: {list(best_ind_params.keys())}"
        )

    end_date = curr_date
    curr_date = datetime.strptime(curr_date, "%Y-%m-%d")
    before = curr_date - relativedelta(days=look_back_days)

    if not online:
        # read from YFin data
        data = pd.read_csv(
            os.path.join(
                DATA_DIR,
                f"market_data/price_data/{symbol}-YFin-data-2015-01-01-2025-03-25.csv",
            )
        )
        data["Date"] = pd.to_datetime(data["Date"], utc=True)
        dates_in_df = data["Date"].astype(str).str[:10]

        ind_string = ""
        while curr_date >= before:
            # only do the trading dates
            if curr_date.strftime("%Y-%m-%d") in dates_in_df.values:
                indicator_value = get_stockstats_indicator(
                    symbol, indicator, curr_date.strftime("%Y-%m-%d"), online
                )

                ind_string += f"{curr_date.strftime('%Y-%m-%d')}: {indicator_value}\n"

            curr_date = curr_date - relativedelta(days=1)
    else:
        # online gathering
        ind_string = ""
        while curr_date >= before:
            indicator_value = get_stockstats_indicator(
                symbol, indicator, curr_date.strftime("%Y-%m-%d"), online
            )

            ind_string += f"{curr_date.strftime('%Y-%m-%d')}: {indicator_value}\n"

            curr_date = curr_date - relativedelta(days=1)

    result_str = (
        f"## {indicator} values from {before.strftime('%Y-%m-%d')} to {end_date}:\n\n"
        + ind_string
        + "\n\n"
        + best_ind_params.get(indicator, "No description available.")
    )

    return result_str


def get_stockstats_indicator(
    symbol: Annotated[str, "ticker symbol of the company"],
    indicator: Annotated[str, "technical indicator to get the analysis and report of"],
    curr_date: Annotated[
        str, "The current trading date you are trading on, YYYY-mm-dd"
    ],
    online: Annotated[bool, "to fetch data online or offline"],
) -> str:

    curr_date = datetime.strptime(curr_date, "%Y-%m-%d")
    curr_date = curr_date.strftime("%Y-%m-%d")

    try:
        indicator_value = StockstatsUtils.get_stock_stats(
            symbol,
            indicator,
            curr_date,
            os.path.join(DATA_DIR, "market_data", "price_data"),
            online=online,
        )
    except Exception as e:
        print(
            f"Error getting stockstats indicator data for indicator {indicator} on {curr_date}: {e}"
        )
        return ""

    return str(indicator_value)


def get_YFin_data_window(
    symbol: Annotated[str, "ticker symbol of the company"],
    curr_date: Annotated[str, "Start date in yyyy-mm-dd format"],
    look_back_days: Annotated[int, "how many days to look back"],
) -> str:
    # calculate past days
    date_obj = datetime.strptime(curr_date, "%Y-%m-%d")
    before = date_obj - relativedelta(days=look_back_days)
    start_date = before.strftime("%Y-%m-%d")

    # read in data
    data = pd.read_csv(
        os.path.join(
            DATA_DIR,
            f"market_data/price_data/{symbol}-YFin-data-2015-01-01-2025-03-25.csv",
        )
    )

    # Extract just the date part for comparison
    data["DateOnly"] = data["Date"].str[:10]

    # Filter data between the start and end dates (inclusive)
    filtered_data = data[
        (data["DateOnly"] >= start_date) & (data["DateOnly"] <= curr_date)
    ]

    # Drop the temporary column we created
    filtered_data = filtered_data.drop("DateOnly", axis=1)

    # Set pandas display options to show the full DataFrame
    with pd.option_context(
        "display.max_rows", None, "display.max_columns", None, "display.width", None
    ):
        df_string = filtered_data.to_string()

    return (
        f"## Raw Market Data for {symbol} from {start_date} to {curr_date}:\n\n"
        + df_string
    )


def get_YFin_data_online(
    symbol: Annotated[str, "ticker symbol of the company"],
    start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
    end_date: Annotated[str, "End date in yyyy-mm-dd format"],
):
    #Check if yfinance is available
    if not YF_AVAILABLE or yf is None:
        return "yfinance库不可用，无法获取美股数据"

    datetime.strptime(start_date, "%Y-%m-%d")
    datetime.strptime(end_date, "%Y-%m-%d")

    # Create ticker object
    ticker = yf.Ticker(symbol.upper())

    # Fetch historical data for the specified date range
    data = ticker.history(start=start_date, end=end_date)

    # Check if data is empty
    if data.empty:
        return (
            f"No data found for symbol '{symbol}' between {start_date} and {end_date}"
        )

    # Remove timezone info from index for cleaner output
    if data.index.tz is not None:
        data.index = data.index.tz_localize(None)

    # Round numerical values to 2 decimal places for cleaner display
    numeric_columns = ["Open", "High", "Low", "Close", "Adj Close"]
    for col in numeric_columns:
        if col in data.columns:
            data[col] = data[col].round(2)

    # Convert DataFrame to CSV string
    csv_string = data.to_csv()

    # Add header information
    header = f"# Stock data for {symbol.upper()} from {start_date} to {end_date}\n"
    header += f"# Total records: {len(data)}\n"
    header += f"# Data retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    return header + csv_string


def get_YFin_data(
    symbol: Annotated[str, "ticker symbol of the company"],
    start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
    end_date: Annotated[str, "End date in yyyy-mm-dd format"],
) -> str:
    # read in data
    data = pd.read_csv(
        os.path.join(
            DATA_DIR,
            f"market_data/price_data/{symbol}-YFin-data-2015-01-01-2025-03-25.csv",
        )
    )

    if end_date > "2025-03-25":
        raise Exception(
            f"Get_YFin_Data: {end_date} is outside of the data range of 2015-01-01 to 2025-03-25"
        )

    # Extract just the date part for comparison
    data["DateOnly"] = data["Date"].str[:10]

    # Filter data between the start and end dates (inclusive)
    filtered_data = data[
        (data["DateOnly"] >= start_date) & (data["DateOnly"] <= end_date)
    ]

    # Drop the temporary column we created
    filtered_data = filtered_data.drop("DateOnly", axis=1)

    # remove the index from the dataframe
    filtered_data = filtered_data.reset_index(drop=True)

    return filtered_data


def get_stock_news_openai(ticker, curr_date):
    config = get_config()
    client = OpenAI(base_url=config["backend_url"])

    response = client.responses.create(
        model=config["quick_think_llm"],
        input=[
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": f"Can you search Social Media for {ticker} from 7 days before {curr_date} to {curr_date}? Make sure you only get the data posted during that period.",
                    }
                ],
            }
        ],
        text={"format": {"type": "text"}},
        reasoning={},
        tools=[
            {
                "type": "web_search_preview",
                "user_location": {"type": "approximate"},
                "search_context_size": "low",
            }
        ],
        temperature=1,
        max_output_tokens=4096,
        top_p=1,
        store=True,
    )

    return response.output[1].content[0].text


def get_global_news_openai(curr_date):
    config = get_config()
    client = OpenAI(base_url=config["backend_url"])

    response = client.responses.create(
        model=config["quick_think_llm"],
        input=[
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": f"Can you search global or macroeconomics news from 7 days before {curr_date} to {curr_date} that would be informative for trading purposes? Make sure you only get the data posted during that period.",
                    }
                ],
            }
        ],
        text={"format": {"type": "text"}},
        reasoning={},
        tools=[
            {
                "type": "web_search_preview",
                "user_location": {"type": "approximate"},
                "search_context_size": "low",
            }
        ],
        temperature=1,
        max_output_tokens=4096,
        top_p=1,
        store=True,
    )

    return response.output[1].content[0].text


def get_fundamentals_finnhub(ticker, curr_date):
    """Using Finnhub API to obtain stock fundamentals as an option for OpenAI
Args:
ticker (str): Stock code
curr date(str): Current date in yyyy-mm-dd
Returns:
str: Formatted Basic Data Reports
"""
    try:
        import finnhub
        import os
        #Import Cache Manager (Unified Entry)
        from .cache import get_cache
        cache = get_cache()
        cached_key = cache.find_cached_fundamentals_data(ticker, data_source="finnhub")
        if cached_key:
            cached_data = cache.load_fundamentals_data(cached_key)
            if cached_data:
                logger.debug(f"[DEBUG] Loading Finnhub fundamental data from cache:{ticker}")
                return cached_data
        
        #Get Finnhub API Key
        api_key = os.getenv('FINNHUB_API_KEY')
        if not api_key:
            return "错误：未配置FINNHUB_API_KEY环境变量"
        
        #Initialize Finnhub Client
        finnhub_client = finnhub.Client(api_key=api_key)
        
        logger.debug(f"[DEBUG]{ticker}Basic Data...")
        
        #Access to basic financial data
        try:
            basic_financials = finnhub_client.company_basic_financials(ticker, 'all')
        except Exception as e:
            logger.error(f"[DBUG] Finnhub's basic financial data has failed:{str(e)}")
            basic_financials = None
        
        #Access to company profiles
        try:
            company_profile = finnhub_client.company_profile2(symbol=ticker)
        except Exception as e:
            logger.error(f"[DBUG] Finnhub profile failed:{str(e)}")
            company_profile = None
        
        #Data on access to benefits
        try:
            earnings = finnhub_client.company_earnings(ticker, limit=4)
        except Exception as e:
            logger.error(f"[DBUG] Finnhub revenue data access failed:{str(e)}")
            earnings = None
        
        #Formatting Reports
        report = f"# {ticker} 基本面分析报告（Finnhub数据源）\n\n"
        report += f"**数据获取时间**: {curr_date}\n"
        report += f"**数据来源**: Finnhub API\n\n"
        
        #Corporate profile component
        if company_profile:
            report += "## 公司概况\n"
            report += f"- **公司名称**: {company_profile.get('name', 'N/A')}\n"
            report += f"- **行业**: {company_profile.get('finnhubIndustry', 'N/A')}\n"
            report += f"- **国家**: {company_profile.get('country', 'N/A')}\n"
            report += f"- **货币**: {company_profile.get('currency', 'N/A')}\n"
            report += f"- **市值**: {company_profile.get('marketCapitalization', 'N/A')} 百万美元\n"
            report += f"- **流通股数**: {company_profile.get('shareOutstanding', 'N/A')} 百万股\n\n"
        
        #Basic financial indicators
        if basic_financials and 'metric' in basic_financials:
            metrics = basic_financials['metric']
            report += "## 关键财务指标\n"
            report += "| 指标 | 数值 |\n"
            report += "|------|------|\n"
            
            #Valuation indicators
            if 'peBasicExclExtraTTM' in metrics:
                report += f"| 市盈率 (PE) | {metrics['peBasicExclExtraTTM']:.2f} |\n"
            if 'psAnnual' in metrics:
                report += f"| 市销率 (PS) | {metrics['psAnnual']:.2f} |\n"
            if 'pbAnnual' in metrics:
                report += f"| 市净率 (PB) | {metrics['pbAnnual']:.2f} |\n"
            
            #Profitability indicators
            if 'roeTTM' in metrics:
                report += f"| 净资产收益率 (ROE) | {metrics['roeTTM']:.2f}% |\n"
            if 'roaTTM' in metrics:
                report += f"| 总资产收益率 (ROA) | {metrics['roaTTM']:.2f}% |\n"
            if 'netProfitMarginTTM' in metrics:
                report += f"| 净利润率 | {metrics['netProfitMarginTTM']:.2f}% |\n"
            
            #Financial health indicators
            if 'currentRatioAnnual' in metrics:
                report += f"| 流动比率 | {metrics['currentRatioAnnual']:.2f} |\n"
            if 'totalDebt/totalEquityAnnual' in metrics:
                report += f"| 负债权益比 | {metrics['totalDebt/totalEquityAnnual']:.2f} |\n"
            
            report += "\n"
        
        #Income history
        if earnings:
            report += "## 收益历史\n"
            report += "| 季度 | 实际EPS | 预期EPS | 差异 |\n"
            report += "|------|---------|---------|------|\n"
            for earning in earnings[:4]:  #Show the last 4 quarters
                actual = earning.get('actual', 'N/A')
                estimate = earning.get('estimate', 'N/A')
                period = earning.get('period', 'N/A')
                surprise = earning.get('surprise', 'N/A')
                report += f"| {period} | {actual} | {estimate} | {surprise} |\n"
            report += "\n"
        
        #Description of data availability
        report += "## 数据说明\n"
        report += "- 本报告使用Finnhub API提供的官方财务数据\n"
        report += "- 数据来源于公司财报和SEC文件\n"
        report += "- TTM表示过去12个月数据\n"
        report += "- Annual表示年度数据\n\n"
        
        if not basic_financials and not company_profile and not earnings:
            report += "⚠️ **警告**: 无法获取该股票的基本面数据，可能原因：\n"
            report += "- 股票代码不正确\n"
            report += "- Finnhub API限制\n"
            report += "- 该股票暂无基本面数据\n"
        
        #Save to Cache
        if report and len(report) > 100:  #Cache only when the report has actual content
            cache.save_fundamentals_data(ticker, report, data_source="finnhub")
        
        logger.debug(f"[DBUG] Finnhub Basic data acquisition completed, report length:{len(report)}")
        return report
        
    except ImportError:
        return "错误：未安装finnhub-python库，请运行: pip install finnhub-python"
    except Exception as e:
        logger.error(f"[DBUG] Finnhub fundamental data acquisition failed:{str(e)}")
        return f"Finnhub基本面数据获取失败: {str(e)}"


def get_fundamentals_openai(ticker, curr_date):
    """Acquisition of U.S. stock fundamentals, automatic selection and downgrading using data source manager

Supported data sources (priority by database configuration):
- Alpha Vantage: Basic and news data (high accuracy)
- yfinance: Stock prices and basic information (free of charge)
- Finnhub: Alternative data source
- OpenAI: Search basic information using AI (needs configuration)

Priority read from database data groupings (market category id=us stocks')

Args:
ticker (str): Stock code
curr date(str): Current date in yyyy-mm-dd
Returns:
str: Basic data reporting
"""
    try:
        #Import Cache Manager and Data Source Manager
        from .cache import get_cache
        from .data_source_manager import get_us_data_source_manager, USDataSource

        cache = get_cache()
        us_manager = get_us_data_source_manager()

        #Check Cache - Check by Data Source Priority
        data_source_cache_names = {
            USDataSource.ALPHA_VANTAGE: "alpha_vantage",
            USDataSource.YFINANCE: "yfinance",
            USDataSource.FINNHUB: "finnhub",
        }

        for source in us_manager.available_sources:
            if source == USDataSource.MONGODB:
                continue  #MongoDB cache is handled separately

            cache_name = data_source_cache_names.get(source)
            if cache_name:
                cached_key = cache.find_cached_fundamentals_data(ticker, data_source=cache_name)
                if cached_key:
                    cached_data = cache.load_fundamentals_data(cached_key)
                    if cached_data:
                        logger.info(f"From{cache_name}Cache load basic face data:{ticker}")
                        return cached_data

        #Data source priorities from the database
        priority_order = us_manager._get_data_source_priority_order(ticker)
        logger.info(f"Data source priorities:{[s.value for s in priority_order]}")

        #Try each data source on a priority basis
        for source in priority_order:
            try:
                if source == USDataSource.ALPHA_VANTAGE:
                    result = _get_fundamentals_alpha_vantage(ticker, curr_date, cache)
                    if result:
                        return result

                elif source == USDataSource.YFINANCE:
                    result = _get_fundamentals_yfinance(ticker, curr_date, cache)
                    if result:
                        return result

                elif source == USDataSource.FINNHUB:
                    result = get_fundamentals_finnhub(ticker, curr_date)
                    if result and "❌" not in result:
                        cache.save_fundamentals_data(ticker, result, data_source="finnhub")
                        return result

            except Exception as e:
                logger.warning(f"⚠️ [{source.value}Acquisition failure:{e}, try the next data source")
                continue

        #Special handling: OpenAI (if configured)
        config = get_config()
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if openai_api_key and config.get("backend_url") and config.get("quick_think_llm"):
            backend_url = config.get("backend_url", "")
            if "openai.com" in backend_url:
                try:
                    logger.info(f"📊 [OpenAI] Try to access basic data with OpenAI...")
                    return _get_fundamentals_openai_impl(ticker, curr_date, config, cache)
                except Exception as e:
                    logger.warning(f"[OpenAI]{e}")

        #All data sources failed
        logger.error(f"All data sources failed:{ticker}")
        return f"❌ 获取 {ticker} 基本面数据失败：所有数据源都不可用"

    except Exception as e:
        logger.error(f"[U.S. stock base]{str(e)}")
        return f"❌ 获取 {ticker} 基本面数据失败: {str(e)}"


def _get_fundamentals_alpha_vantage(ticker, curr_date, cache):
    """Fetch basic face data from Alpha Vantage

Args:
ticker: Stock code
Curr date: Current date
Cache: Cache Object

Returns:
str: Basic Data Report, Failed to return None
"""
    try:
        logger.info(f"[Alpha Vantage]{ticker}Basic Data...")
        from .providers.us.alpha_vantage_fundamentals import get_fundamentals as get_av_fundamentals

        result = get_av_fundamentals(ticker, curr_date)

        if result and "Error" not in result and len(result) > 100:
            #Save to Cache
            cache.save_fundamentals_data(ticker, result, data_source="alpha_vantage")
            logger.info(f"[Alpha Vantage]{ticker}")
            return result
        else:
            logger.warning(f"[Alpha Vantage]")
            return None
    except Exception as e:
        logger.warning(f"[Alpha Vantage]{e}")
        return None


def _get_fundamentals_yfinance(ticker, curr_date, cache):
    """Fetch basic facet data from yfinance

Args:
ticker: Stock code
Curr date: Current date
Cache: Cache Object

Returns:
str: Basic Data Report, Failed to return None
"""
    try:
        logger.info(f"[yfinance]{ticker}Basic Data...")
        import yfinance as yf

        ticker_obj = yf.Ticker(ticker.upper())
        info = ticker_obj.info

        if info and len(info) > 5:  #Ensuring availability of actual data
            #Format yfinance data
            result = f"""# {ticker} 基本面数据 (来源: Yahoo Finance)

## 公司信息
- 公司名称: {info.get('longName', 'N/A')}
- 行业: {info.get('industry', 'N/A')}
- 板块: {info.get('sector', 'N/A')}
- 网站: {info.get('website', 'N/A')}

## 估值指标
- 市值: ${info.get('marketCap', 'N/A'):,}
- PE比率: {info.get('trailingPE', 'N/A')}
- 前瞻PE: {info.get('forwardPE', 'N/A')}
- PB比率: {info.get('priceToBook', 'N/A')}
- PS比率: {info.get('priceToSalesTrailing12Months', 'N/A')}

## 财务指标
- 总收入: ${info.get('totalRevenue', 'N/A'):,}
- 毛利润: ${info.get('grossProfits', 'N/A'):,}
- EBITDA: ${info.get('ebitda', 'N/A'):,}
- 每股收益(EPS): ${info.get('trailingEps', 'N/A')}
- 股息率: {info.get('dividendYield', 'N/A')}

## 盈利能力
- 利润率: {info.get('profitMargins', 'N/A')}
- 营业利润率: {info.get('operatingMargins', 'N/A')}
- ROE: {info.get('returnOnEquity', 'N/A')}
- ROA: {info.get('returnOnAssets', 'N/A')}

## 股价信息
- 当前价格: ${info.get('currentPrice', 'N/A')}
- 52周最高: ${info.get('fiftyTwoWeekHigh', 'N/A')}
- 52周最低: ${info.get('fiftyTwoWeekLow', 'N/A')}
- 50日均线: ${info.get('fiftyDayAverage', 'N/A')}
- 200日均线: ${info.get('twoHundredDayAverage', 'N/A')}

## 分析师评级
- 目标价: ${info.get('targetMeanPrice', 'N/A')}
- 推荐评级: {info.get('recommendationKey', 'N/A')}

数据获取时间: {curr_date}
"""
            #Save to Cache
            cache.save_fundamentals_data(ticker, result, data_source="yfinance")
            logger.info(f"[yfinance]{ticker}")
            return result
        else:
            logger.warning(f"[yfinance]")
            return None
    except Exception as e:
        logger.warning(f"[yfinance]{e}")
        return None


def _get_fundamentals_openai_impl(ticker, curr_date, config, cache):
    """OpenAI Basic Data Acquisition Achieved (Internal Functions)

Args:
ticker: Stock code
Curr date: Current date
config: Configure Object
Cache: Cache Object

Returns:
str: Basic data reporting
"""
    try:
        logger.debug(f"[OpenAI]{ticker}Basic Data...")

        client = OpenAI(base_url=config["backend_url"])

        response = client.responses.create(
            model=config["quick_think_llm"],
            input=[
                {
                    "role": "system",
                    "content": [
                        {
                            "type": "input_text",
                            "text": f"Can you search Fundamental for discussions on {ticker} during of the month before {curr_date} to the month of {curr_date}. Make sure you only get the data posted during that period. List as a table, with PE/PS/Cash flow/ etc",
                        }
                    ],
                }
            ],
            text={"format": {"type": "text"}},
            reasoning={},
            tools=[
                {
                    "type": "web_search_preview",
                    "user_location": {"type": "approximate"},
                    "search_context_size": "low",
                }
            ],
            temperature=1,
            max_output_tokens=4096,
            top_p=1,
            store=True,
        )

        result = response.output[1].content[0].text

        #Save to Cache
        if result and len(result) > 100:  #Cache only if the result has actual content
            cache.save_fundamentals_data(ticker, result, data_source="openai")

        logger.info(f"[OpenAI] Basic data acquisition success:{ticker}")
        return result

    except Exception as e:
        logger.error(f"[OpenAI] Basic data acquisition failed:{str(e)}")
        raise  #Throw an anomaly so that the outer layer continues to try other data sources


#== sync, corrected by elderman == @elder man

def get_china_stock_data_tushare(
    ticker: Annotated[str, "中国股票代码，如：000001、600036等"],
    start_date: Annotated[str, "开始日期，格式：YYYY-MM-DD"],
    end_date: Annotated[str, "结束日期，格式：YYYY-MM-DD"]
) -> str:
    """Using Tushare to access Chinese stock A historical data
Redirect to Data source manager to avoid circular calls

Args:
ticker: Stock code
Start date: Start date
End date: End date

Returns:
str: Formatted Stock Data Reports
"""
    try:
        from .data_source_manager import get_data_source_manager

        logger.debug(f"[Tushare]{ticker}Stock Data...")

        #Add detailed stock code tracking log
        logger.info(f"🔍 [Securities Code Tracking] get china stock data tushare to pick up the stock code received: '{ticker}' (type:{type(ticker)})")
        logger.info(f"Redirect to Data source manager.")

        manager = get_data_source_manager()
        return manager.get_china_stock_data_tushare(ticker, start_date, end_date)

    except Exception as e:
        logger.error(f"[Tushare]{e}")
        return f"❌ 获取{ticker}股票数据失败: {e}"


def get_china_stock_info_tushare(
    ticker: Annotated[str, "中国股票代码，如：000001、600036等"]
) -> str:
    """Use Tushare to obtain basic information on China's A-unit
Directly call Tushare adapter to avoid recycling

Args:
ticker: Stock code

Returns:
str: Formatted Basic Information on Stock
"""
    try:
        from .data_source_manager import get_data_source_manager

        logger.debug(f"[Tushare]{ticker}Stock Information...")
        logger.info(f"🔍 [Securities Code Tracking] get  china stock info tushare to pick up the stock code received: '{ticker}' (type:{type(ticker)})")
        logger.info(f"Direct call to Tushare adapter")

        manager = get_data_source_manager()

        #Direct call  get tushare stock info() to avoid recycling
        #Do not call get stock info(), as it will call again()
        info = manager._get_tushare_stock_info(ticker)

        #Format Return String
        if info and isinstance(info, dict):
            return f"""股票代码: {info.get('symbol', ticker)}
股票名称: {info.get('name', '未知')}
所属行业: {info.get('industry', '未知')}
上市日期: {info.get('list_date', '未知')}
交易所: {info.get('exchange', '未知')}"""
        else:
            return f"❌ 未找到{ticker}的股票信息"

    except Exception as e:
        logger.error(f"[Tushare]{e}")
        return f"❌ 获取{ticker}股票信息失败: {e}"


def get_china_stock_fundamentals_tushare(
    ticker: Annotated[str, "中国股票代码，如：000001、600036等"]
) -> str:
    """Acquisition of China Unit A fundamentals data (uniform interface)
Supporting multiple data sources: MongoDB →Tushare →AKshare → Generating

Args:
ticker: Stock code

Returns:
str: Basic analysis reports
"""
    try:
        from .data_source_manager import get_data_source_manager

        logger.debug(f"Access{ticker}Basic Data...")
        logger.info(f"Redirect to Data source manager. Get fundamentals data")

        manager = get_data_source_manager()
        #Support multiple data sources and automatic downgrading using the new unified interface
        return manager.get_fundamentals_data(ticker)

    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        return f"❌ 获取{ticker}基本面数据失败: {e}"


#== sync, corrected by elderman == @elder man

def get_china_stock_data_unified(
    ticker: Annotated[str, "中国股票代码，如：000001、600036等"],
    start_date: Annotated[str, "开始日期，格式：YYYY-MM-DD"],
    end_date: Annotated[str, "结束日期，格式：YYYY-MM-DD"]
) -> str:
    """Integrated China A data access interface
Automatically use configured data sources (default Tushare) to support backup data sources

Args:
ticker: Stock code
Start date: Start date
End date: End date

Returns:
str: Formatted Stock Data Reports
"""
    # Smart date range processing: auto-extension back-to-back days to configuration, processing weekends/leaves Day
    from tradingagents.utils.dataflow_utils import get_trading_date_range
    from app.core.config import get_settings

    original_start_date = start_date
    original_end_date = end_date

    #Number of days to retrieve market analysis from configuration (default 30 days)
    try:
        settings = get_settings()
        lookback_days = settings.MARKET_ANALYST_LOOKBACK_DAYS
        logger.info(f"== sync, corrected by elderman == @elder man")
        logger.info(f"📅 [configuration authentication] Read from the profile:{lookback_days}days")
        logger.info(f"📅 [configuration validation] Configuration source: app.core.config.Settings")
        logger.info(f"Environmental variable: MARKET ANALIST LOOKBACK DAYS={lookback_days}")
    except Exception as e:
        lookback_days = 30  #Default 30 days
        logger.warning(f"⚠️ [configuration authentication] cannot access configuration, using default values:{lookback_days}days")
        logger.warning(f"[configuration authentication] Error details:{e}")

    #Use end date as target date to retroactively specify days
    start_date, end_date = get_trading_date_range(end_date, lookback_days=lookback_days)

    logger.info(f"📅 [smart date] = = = date range calculation = = = = =")
    logger.info(f"[smart date] Original input:{original_start_date}to{original_end_date}")
    logger.info(f"[Intelligent date] Back-to-back days:{lookback_days}days")
    logger.info(f"[Intelligent date]{start_date}to{end_date}")
    logger.info(f"Actual number of days:{(datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days}days")
    logger.info(f"💡 [smart date] Note: Automatically expand the date range to address weekends, holidays and data delays")

    #Record detailed input parameters
    logger.info(f"[Unified interface]",
               extra={
                   'function': 'get_china_stock_data_unified',
                   'ticker': ticker,
                   'start_date': start_date,
                   'end_date': end_date,
                   'event_type': 'unified_data_call_start'
               })

    #Add detailed stock code tracking log
    logger.info(f"🔍 [Securities Code Tracking] get  china stock data unified to pick up the original stock code received: '{ticker}' (type:{type(ticker)})")
    logger.info(f"[Equal code tracking]{len(str(ticker))}")
    logger.info(f"[Equal code tracking]{list(str(ticker))}")

    start_time = time.time()

    try:
        from .data_source_manager import get_china_stock_data_unified

        result = get_china_stock_data_unified(ticker, start_date, end_date)

        #Record detailed output results
        duration = time.time() - start_time
        result_length = len(result) if result else 0
        is_success = result and "❌" not in result and "错误" not in result

        if is_success:
            logger.info(f"✅ [Unified interface] Chinese stock data acquisition success",
                       extra={
                           'function': 'get_china_stock_data_unified',
                           'ticker': ticker,
                           'start_date': start_date,
                           'end_date': end_date,
                           'duration': duration,
                           'result_length': result_length,
                           'result_preview': result[:300] + '...' if result_length > 300 else result,
                           'event_type': 'unified_data_call_success'
                       })
        else:
            logger.warning(f"[Unified interface]",
                          extra={
                              'function': 'get_china_stock_data_unified',
                              'ticker': ticker,
                              'start_date': start_date,
                              'end_date': end_date,
                              'duration': duration,
                              'result_length': result_length,
                              'result_preview': result[:300] + '...' if result_length > 300 else result,
                              'event_type': 'unified_data_call_warning'
                          })

        return result

    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"❌ [Unified Interface] Failed to access stock data:{e}",
                    extra={
                        'function': 'get_china_stock_data_unified',
                        'ticker': ticker,
                        'start_date': start_date,
                        'end_date': end_date,
                        'duration': duration,
                        'error': str(e),
                        'event_type': 'unified_data_call_error'
                    }, exc_info=True)
        return f"❌ 获取{ticker}股票数据失败: {e}"


def get_china_stock_info_unified(
    ticker: Annotated[str, "中国股票代码，如：000001、600036等"]
) -> str:
    """Basic information access interface for Unit A of China
Automatically use configured data sources (default Tushare)

Args:
ticker: Stock code

Returns:
str: Stock Basic Information
"""
    try:
        from .data_source_manager import get_china_stock_info_unified

        logger.info(f"[Uniform interface]{ticker}Basic information...")

        info = get_china_stock_info_unified(ticker)

        if info and info.get('name'):
            result = f"股票代码: {ticker}\n"
            result += f"股票名称: {info.get('name', '未知')}\n"
            result += f"所属地区: {info.get('area', '未知')}\n"
            result += f"所属行业: {info.get('industry', '未知')}\n"
            result += f"上市市场: {info.get('market', '未知')}\n"
            result += f"上市日期: {info.get('list_date', '未知')}\n"
            #Accelerating compliance (if any)
            cp = info.get('current_price')
            pct = info.get('change_pct')
            vol = info.get('volume')
            if cp is not None:
                result += f"当前价格: {cp}\n"
            if pct is not None:
                try:
                    pct_str = f"{float(pct):+.2f}%"
                except Exception:
                    pct_str = str(pct)
                result += f"涨跌幅: {pct_str}\n"
            if vol is not None:
                result += f"成交量: {vol}\n"
            result += f"数据来源: {info.get('source', 'unknown')}\n"

            return result
        else:
            return f"❌ 未能获取{ticker}的基本信息"

    except Exception as e:
        logger.error(f"[Unified interface]{e}")
        return f"❌ 获取{ticker}股票信息失败: {e}"


def switch_china_data_source(
    source: Annotated[str, "数据源名称：tushare, akshare, baostock"]
) -> str:
    """Toggle Chinese stock data sources

Args:
source name

Returns:
st: Switch result
"""
    try:
        from .data_source_manager import get_data_source_manager, ChinaDataSource

        #Map string to count (TDX removed)
        source_mapping = {
            'tushare': ChinaDataSource.TUSHARE,
            'akshare': ChinaDataSource.AKSHARE,
            'baostock': ChinaDataSource.BAOSTOCK,
            #'tdx': ChinaDataSource.TDX # Removed
        }

        if source.lower() not in source_mapping:
            return f"❌ 不支持的数据源: {source}。支持的数据源: {list(source_mapping.keys())}"

        manager = get_data_source_manager()
        target_source = source_mapping[source.lower()]

        if manager.set_current_source(target_source):
            return f"✅ 数据源已切换到: {source}"
        else:
            return f"❌ 数据源切换失败: {source} 不可用"

    except Exception as e:
        logger.error(f"Data source switching failed:{e}")
        return f"❌ 数据源切换失败: {e}"


def get_current_china_data_source() -> str:
    """Get the current Chinese stock data source

Returns:
str: Current data source information
"""
    try:
        from .data_source_manager import get_data_source_manager

        manager = get_data_source_manager()
        current = manager.get_current_source()
        available = manager.available_china_sources

        result = f"当前数据源: {current.value}\n"
        result += f"可用数据源: {[s.value for s in available]}\n"
        result += f"默认数据源: {manager.default_source.value}\n"

        return result

    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        return f"❌ 获取数据源信息失败: {e}"


#== sync, corrected by elderman == @elder man

def get_hk_stock_data_unified(symbol: str, start_date: str = None, end_date: str = None) -> str:
    """Harmonization interface for access to port unit data (selection of data sources according to user configuration)

Args:
Symbol: Port Unit Code (e.g. 0700.HK)
Start date: Start date (YYYYY-MM-DD)
End date: End Date (YYYYY-MM-DD)

Returns:
str: Formatted Port Unit data
"""
    try:
        logger.info(f"Access to Port Unit data:{symbol}")

        #🔧 Smart date range processing: auto-extension back-to-back days to configuration, processing weekends/leaves Day
        from tradingagents.utils.dataflow_utils import get_trading_date_range
        from app.core.config import get_settings

        original_start_date = start_date
        original_end_date = end_date

        #Number of days to retrieve market analysis back from configuration (default 60 days)
        try:
            settings = get_settings()
            lookback_days = settings.MARKET_ANALYST_LOOKBACK_DAYS
            logger.info(f"MARKET ANALIST LOOKBACK DAYS:{lookback_days}days")
        except Exception as e:
            lookback_days = 60  #Default 60 days
            logger.warning(f"⚠️ [Port Unit Configuration Validation] could not access configuration, using default values:{lookback_days}days")
            logger.warning(f"⚠️ [Port Unit Configuration Validation]{e}")

        #Use end date as target date to retroactively specify days
        start_date, end_date = get_trading_date_range(end_date, lookback_days=lookback_days)

        logger.info(f"Original input:{original_start_date}to{original_end_date}")
        logger.info(f"📅 [Hong Kong stock smart date]{lookback_days}days")
        logger.info(f"The results of the calculations are as follows:{start_date}to{end_date}")
        logger.info(f"Actual number of days:{(datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days}days")

        #🔥 Read user-enabled data source configuration from the database
        enabled_sources = _get_enabled_hk_data_sources()

        #Try data sources on a priority basis
        for source in enabled_sources:
            if source == 'akshare' and AKSHARE_HK_AVAILABLE:
                try:
                    logger.info(f"Using AKShare to access port data:{symbol}")
                    result = get_hk_stock_data_akshare(symbol, start_date, end_date)
                    if result and "❌" not in result:
                        logger.info(f"The data of the AKShare Port Unit was obtained successfully:{symbol}")
                        return result
                    else:
                        logger.warning(f"AKShare returns the wrong result and tries the next data Source")
                except Exception as e:
                    logger.error(f"There is no data acquisition in the port of Akshare:{e}, try the next data source")

            elif source == 'yfinance' and HK_STOCK_AVAILABLE:
                try:
                    logger.info(f"Using Yahoo Finance to access port unit data:{symbol}")
                    result = get_hk_stock_data(symbol, start_date, end_date)
                    if result and "❌" not in result:
                        logger.info(f"✅ Yahoo Finance Port Unit data acquisition success:{symbol}")
                        return result
                    else:
                        logger.warning(f"Yahoo Finance returns the wrong result and tries the next data Source")
                except Exception as e:
                    logger.error(f"Yahoo Finance’s data acquisition failed:{e}, try the next data source")

            elif source == 'finnhub':
                try:
                    #Import U.S. stock data provider (support for old and new paths)
                    try:
                        from .providers.us import OptimizedUSDataProvider
                        provider = OptimizedUSDataProvider()
                        get_us_stock_data_cached = provider.get_stock_data
                    except ImportError:
                        from tradingagents.dataflows.providers.us.optimized import get_us_stock_data_cached

                    logger.info(f"Access to Port Unit data by FINNHUB:{symbol}")
                    result = get_us_stock_data_cached(symbol, start_date, end_date)
                    if result and "❌" not in result:
                        logger.info(f"The following data was obtained successfully:{symbol}")
                        return result
                    else:
                        logger.warning(f"FINNHUB returns the wrong result and tries the next data source")
                except Exception as e:
                    logger.error(f"FINNHUB has failed to access data:{e}, try the next data source")

        #All data sources failed
        error_msg = f"❌ 无法获取港股{symbol}数据 - 所有启用的数据源都不可用"
        logger.error(error_msg)
        return error_msg

    except Exception as e:
        logger.error(f"Access to Port Unit data failed:{e}")
        return f"❌ 获取港股{symbol}数据失败: {e}"


def get_hk_stock_info_unified(symbol: str) -> Dict:
    """Unified interface for information on port units (selection of data sources according to user configuration)

Args:
Symbol: Port Unit Code

Returns:
Dict: Port Unit Information
"""
    try:
        #🔥 Read user-enabled data source configuration from the database
        enabled_sources = _get_enabled_hk_data_sources()

        #Try data sources on a priority basis
        for source in enabled_sources:
            if source == 'akshare' and AKSHARE_HK_AVAILABLE:
                try:
                    logger.info(f"Using AKShare to access information about the Port Unit:{symbol}")
                    result = get_hk_stock_info_akshare(symbol)
                    if result and 'error' not in result and not result.get('name', '').startswith('港股'):
                        logger.info(f"AKShare successfully accessed information about the Port Unit:{symbol} -> {result.get('name', 'N/A')}")
                        return result
                    else:
                        logger.warning(f"AKShare returns the default information and tries the next data Source")
                except Exception as e:
                    logger.error(f"A KShare Port has failed to access information:{e}, try the next data source")

            elif source == 'yfinance' and HK_STOCK_AVAILABLE:
                try:
                    logger.info(f"🔄.{symbol}")
                    result = get_hk_stock_info(symbol)
                    if result and 'error' not in result and not result.get('name', '').startswith('港股'):
                        logger.info(f"✅ Yahoo Finance successfully accessed information about the Port Unit:{symbol} -> {result.get('name', 'N/A')}")
                        return result
                    else:
                        logger.warning(f"Yahoo Finance returns the default information and tries the next data Source")
                except Exception as e:
                    logger.error(f"There is no access to information from Yahoo Finance Port Unit:{e}, try the next data source")

        #All data sources failed to return basic information
        logger.warning(f"All enabled data sources failed, using default information:{symbol}")
        return {
            'symbol': symbol,
            'name': f'港股{symbol}',
            'currency': 'HKD',
            'exchange': 'HKG',
            'source': 'fallback'
        }

    except Exception as e:
        logger.error(f"Access to information on the Port Unit failed:{e}")
        return {
            'symbol': symbol,
            'name': f'港股{symbol}',
            'currency': 'HKD',
            'exchange': 'HKG',
            'source': 'error',
            'error': str(e)
        }


def get_stock_data_by_market(symbol: str, start_date: str = None, end_date: str = None) -> str:
    """Automatically select data sources according to stock market type

Args:
symbol: stock code
Start date: Start date
End date: End date

Returns:
str: Formatted Stock Data
"""
    try:
        from tradingagents.utils.stock_utils import StockUtils

        market_info = StockUtils.get_market_info(symbol)

        if market_info['is_china']:
            #China A Unit
            return get_china_stock_data_unified(symbol, start_date, end_date)
        elif market_info['is_hk']:
            #Port Unit
            return get_hk_stock_data_unified(symbol, start_date, end_date)
        else:
            #United States share or other
            #Import U.S. stock data provider (support for old and new paths)
            try:
                from .providers.us import OptimizedUSDataProvider
                provider = OptimizedUSDataProvider()
                return provider.get_stock_data(symbol, start_date, end_date)
            except ImportError:
                from tradingagents.dataflows.providers.us.optimized import get_us_stock_data_cached
                return get_us_stock_data_cached(symbol, start_date, end_date)

    except Exception as e:
        logger.error(f"@❌> Failed to access stock data:{e}")
        return f"❌ 获取股票{symbol}数据失败: {e}"
