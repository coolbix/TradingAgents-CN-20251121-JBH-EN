"""United States share data provider
Includes US stock data sources like Finnhub, Yahoo Finance
"""

#Import Finnhub Tool
try:
    from .finnhub import get_data_in_range
    FINNHUB_AVAILABLE = True
except ImportError:
    get_data_in_range = None
    FINNHUB_AVAILABLE = False

#Import Yahoo Finance Tool
try:
    from .yfinance import YFinanceUtils
    YFINANCE_AVAILABLE = True
except ImportError:
    YFinanceUtils = None
    YFINANCE_AVAILABLE = False

#Import optimized US share data provider
try:
    from .optimized import OptimizedUSDataProvider
    OPTIMIZED_US_AVAILABLE = True
except ImportError:
    OptimizedUSDataProvider = None
    OPTIMIZED_US_AVAILABLE = False

#Default use optimized provider
DefaultUSProvider = OptimizedUSDataProvider

__all__ = [
    # Finnhub
    'get_data_in_range',
    'FINNHUB_AVAILABLE',

    # Yahoo Finance
    'YFinanceUtils',
    'YFINANCE_AVAILABLE',

    #Optimized provider
    'OptimizedUSDataProvider',
    'OPTIMIZED_US_AVAILABLE',
    'DefaultUSProvider',
]

