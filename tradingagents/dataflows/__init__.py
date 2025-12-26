#Import Basic Module
#Finnhub tool (support for old and new paths)
try:
    from .providers.us import get_data_in_range
except ImportError:
    try:
        from .finnhub_utils import get_data_in_range
    except ImportError:
        get_data_in_range = None

#Import News Module (New Path)
try:
    from .news import getNewsData, fetch_top_from_category
except ImportError:
    #Backcompatibility: trying to import from old paths
    try:
        from .news.google_news import getNewsData
    except ImportError:
        getNewsData = None
    try:
        from .news.reddit import fetch_top_from_category
    except ImportError:
        fetch_top_from_category = None

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')

#Try importing yfinance-related modules (support new and old paths)
try:
    from .providers.us import YFinanceUtils, YFINANCE_AVAILABLE
except ImportError:
    try:
        from .yfin_utils import YFinanceUtils
        YFINANCE_AVAILABLE = True
    except ImportError as e:
        logger.warning(f"The yfinance module is not available:{e}")
        YFinanceUtils = None
        YFINANCE_AVAILABLE = False

#Import technical indicators module (new path)
try:
    from .technical import StockstatsUtils, STOCKSTATS_AVAILABLE
except ImportError as e:
    #Backcompatibility: trying to import from old paths
    try:
        from .technical.stockstats import StockstatsUtils
        STOCKSTATS_AVAILABLE = True
    except ImportError as e:
        logger.warning(f"The stockstats module is not available:{e}")
        StockstatsUtils = None
        STOCKSTATS_AVAILABLE = False

from .interface import (

    # News and sentiment functions
    get_finnhub_news,
    get_finnhub_company_insider_sentiment,
    get_finnhub_company_insider_transactions,
    get_google_news,
    get_reddit_global_news,
    get_reddit_company_news,
    # Financial statements functions
    get_simfin_balance_sheet,
    get_simfin_cashflow,
    get_simfin_income_statements,
    # Technical analysis functions
    get_stock_stats_indicators_window,
    get_stockstats_indicator,
    # Market data functions
    get_YFin_data_window,
    get_YFin_data,
    # Tushare data functions
    get_china_stock_data_tushare,
    get_china_stock_fundamentals_tushare,
    # Unified China data functions (recommended)
    get_china_stock_data_unified,
    get_china_stock_info_unified,
    switch_china_data_source,
    get_current_china_data_source,
    # Hong Kong stock functions
    get_hk_stock_data_unified,
    get_hk_stock_info_unified,
    get_stock_data_by_market,
)

__all__ = [
    # News and sentiment functions
    "get_finnhub_news",
    "get_finnhub_company_insider_sentiment",
    "get_finnhub_company_insider_transactions",
    "get_google_news",
    "get_reddit_global_news",
    "get_reddit_company_news",
    # Financial statements functions
    "get_simfin_balance_sheet",
    "get_simfin_cashflow",
    "get_simfin_income_statements",
    # Technical analysis functions
    "get_stock_stats_indicators_window",
    "get_stockstats_indicator",
    # Market data functions
    "get_YFin_data_window",
    "get_YFin_data",
    # Tushare data functions
    "get_china_stock_data_tushare",
    "get_china_stock_fundamentals_tushare",
    # Unified China data functions
    "get_china_stock_data_unified",
    "get_china_stock_info_unified",
    "switch_china_data_source",
    "get_current_china_data_source",
    # Hong Kong stock functions
    "get_hk_stock_data_unified",
    "get_hk_stock_info_unified",
    "get_stock_data_by_market",
]
