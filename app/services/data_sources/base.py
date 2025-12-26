"""
Base classes and shared typing for data source adapters
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict
import pandas as pd


class DataSourceAdapter(ABC):
    """Data Source Adapter Base Category"""

    def __init__(self):
        self._priority: Optional[int] = None  #Dynamic priority, load from database

    @property
    @abstractmethod
    def name(self) -> str:
        """Data Source Name"""
        raise NotImplementedError

    @property
    def priority(self) -> int:
        """Data source priorities (higher the smaller the number)"""
        #Use dynamic priority if there is a dynamic priority; otherwise use default priority
        if self._priority is not None:
            return self._priority
        return self._get_default_priority()

    @abstractmethod
    def _get_default_priority(self) -> int:
        """Get default priority (subcategory achieved)"""
        raise NotImplementedError

    @abstractmethod
    def is_available(self) -> bool:
        """Check for data source availability"""
        raise NotImplementedError

    @abstractmethod
    def get_stock_list(self) -> Optional[pd.DataFrame]:
        """Get Stock List"""
        raise NotImplementedError

    @abstractmethod
    def get_daily_basic(self, trade_date: str) -> Optional[pd.DataFrame]:
        """Access to daily basic financial data"""
        raise NotImplementedError

    @abstractmethod
    def find_latest_trade_date(self) -> Optional[str]:
        """Find Recent Transaction Date"""
        raise NotImplementedError

    #New: Market-wide real-time snapshot (near real-time price/fall/offset) with 6-digit key
    @abstractmethod
    def get_realtime_quotes(self) -> Optional[Dict[str, Dict[str, Optional[float]]]]:
        """returns   FT 0,...}"""
        raise NotImplementedError

    #Add: K-line and News Abstract Interface
    @abstractmethod
    def get_kline(self, code: str, period: str = "day", limit: int = 120, adj: Optional[str] = None):
        """Get K-line and return the list in chronological order: [   FMT 0 ]"""
        raise NotImplementedError

    @abstractmethod
    def get_news(self, code: str, days: int = 2, limit: int = 50, include_announcements: bool = True):
        """Get news/advertisement, type in ['news', 'announcement']"""
        raise NotImplementedError
