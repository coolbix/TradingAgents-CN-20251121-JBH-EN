"""Port Unit data provider
"""

#Import improved port stock tool
try:
    from .improved_hk import (
        ImprovedHKStockProvider,
        get_improved_hk_provider,
        get_hk_stock_info_improved
    )
    HK_PROVIDER_AVAILABLE = True
except ImportError:
    ImprovedHKStockProvider = None
    get_improved_hk_provider = None
    get_hk_stock_info_improved = None
    HK_PROVIDER_AVAILABLE = False

#Import Port Unit Data Tool
try:
    from .hk_stock import HKStockProvider
    HK_STOCK_AVAILABLE = True
except ImportError:
    HKStockProvider = None
    HK_STOCK_AVAILABLE = False

__all__ = [
    'ImprovedHKStockProvider',
    'get_improved_hk_provider',
    'get_hk_stock_info_improved',
    'HK_PROVIDER_AVAILABLE',
    'HKStockProvider',
    'HK_STOCK_AVAILABLE',
]

