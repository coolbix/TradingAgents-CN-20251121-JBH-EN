"""China Market Data Provider
Data sources in Chinese markets, including shares A and Ports
"""

#Import AKShare Provider
try:
    from .akshare import AKShareProvider
    AKSHARE_AVAILABLE = True
except ImportError:
    AKShareProvider = None
    AKSHARE_AVAILABLE = False

#Import Tushare Provider
try:
    from .tushare import TushareProvider
    TUSHARE_AVAILABLE = True
except ImportError:
    TushareProvider = None
    TUSHARE_AVAILABLE = False

#Import Baustock Provider
try:
    from .baostock import BaostockProvider
    BAOSTOCK_AVAILABLE = True
except ImportError:
    BaostockProvider = None
    BAOSTOCK_AVAILABLE = False

#Import Basic Face Snapshot Tool
try:
    from .fundamentals_snapshot import get_fundamentals_snapshot
    FUNDAMENTALS_SNAPSHOT_AVAILABLE = True
except ImportError:
    get_fundamentals_snapshot = None
    FUNDAMENTALS_SNAPSHOT_AVAILABLE = False

__all__ = [
    'AKShareProvider',
    'AKSHARE_AVAILABLE',
    'TushareProvider',
    'TUSHARE_AVAILABLE',
    'BaostockProvider',
    'BAOSTOCK_AVAILABLE',
    'get_fundamentals_snapshot',
    'FUNDAMENTALS_SNAPSHOT_AVAILABLE',
]

