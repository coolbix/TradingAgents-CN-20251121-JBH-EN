"""Unified data source supply package
Organization of data providers by market
"""
from .base_provider import BaseStockDataProvider

#Import China Market Provider (New Path)
try:
    from .china import (
        AKShareProvider,
        TushareProvider,
        BaostockProvider as BaoStockProvider,
        AKSHARE_AVAILABLE,
        TUSHARE_AVAILABLE,
        BAOSTOCK_AVAILABLE
    )
except ImportError:
    #Backcompatibility: trying to import from old paths
    try:
        from .tushare_provider import TushareProvider
    except ImportError:
        TushareProvider = None

    try:
        from .akshare_provider import AKShareProvider
    except ImportError:
        AKShareProvider = None

    try:
        from .baostock_provider import BaoStockProvider
    except ImportError:
        BaoStockProvider = None

    AKSHARE_AVAILABLE = AKShareProvider is not None
    TUSHARE_AVAILABLE = TushareProvider is not None
    BAOSTOCK_AVAILABLE = BaoStockProvider is not None

#Import port unit provider
try:
    from .hk import (
        ImprovedHKStockProvider,
        get_improved_hk_provider,
        HK_PROVIDER_AVAILABLE
    )
except ImportError:
    ImprovedHKStockProvider = None
    get_improved_hk_provider = None
    HK_PROVIDER_AVAILABLE = False

#Import U.S. stock provider
try:
    from .us import (
        YFinanceUtils,
        OptimizedUSDataProvider,
        get_data_in_range,
        YFINANCE_AVAILABLE,
        OPTIMIZED_US_AVAILABLE,
        FINNHUB_AVAILABLE
    )
except ImportError:
    #Backcompatibility: trying to import from old paths
    try:
        from ..yfin_utils import YFinanceUtils
    except ImportError:
        YFinanceUtils = None

    try:
        from ..optimized_us_data import OptimizedUSDataProvider
    except ImportError:
        OptimizedUSDataProvider = None

    try:
        from ..finnhub_utils import get_data_in_range
    except ImportError:
        get_data_in_range = None

    YFINANCE_AVAILABLE = YFinanceUtils is not None
    OPTIMIZED_US_AVAILABLE = OptimizedUSDataProvider is not None
    FINNHUB_AVAILABLE = get_data_in_range is not None

#Other providers (set aside)
try:
    from .yahoo_provider import YahooProvider
except ImportError:
    YahooProvider = None

try:
    from .finnhub_provider import FinnhubProvider
except ImportError:
    FinnhubProvider = None

#TDXProvider removed
# try:
#     from .tdx_provider import TDXProvider
# except ImportError:
#     TDXProvider = None

__all__ = [
    #Base Category
    'BaseStockDataProvider',

    #China Market
    'TushareProvider',
    'AKShareProvider',
    'BaoStockProvider',
    'AKSHARE_AVAILABLE',
    'TUSHARE_AVAILABLE',
    'BAOSTOCK_AVAILABLE',

    #Port Unit
    'ImprovedHKStockProvider',
    'get_improved_hk_provider',
    'HK_PROVIDER_AVAILABLE',

    #United States share
    'YFinanceUtils',
    'OptimizedUSDataProvider',
    'get_data_in_range',
    'YFINANCE_AVAILABLE',
    'OPTIMIZED_US_AVAILABLE',
    'FINNHUB_AVAILABLE',

    #Other (reserved)
    'YahooProvider',
    'FinnhubProvider',
    #'TDXProvider '# Removed
]
