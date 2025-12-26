"""Module on calculation of technical indicators
Provision of computing functions for various technical analysis indicators
"""

#Import stockstats
try:
    from .stockstats import StockstatsUtils
    STOCKSTATS_AVAILABLE = True
except ImportError:
    StockstatsUtils = None
    STOCKSTATS_AVAILABLE = False

__all__ = [
    'StockstatsUtils',
    'STOCKSTATS_AVAILABLE',
]

