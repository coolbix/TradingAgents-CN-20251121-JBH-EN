"""Harmonization of stock data provider base
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, date
import logging
import pandas as pd


class BaseStockDataProvider(ABC):
    """Stock data provider base category
    A uniform interface for all data source providers is defined
    """
    
    def __init__(self, provider_name: str):
        """Initialization data provider

        Args:
            program name: Provider name
        """
        self.provider_name = provider_name
        self.connected = False
        self.logger = logging.getLogger(f"{__name__}.{provider_name}")
    
    #== sync, corrected by elderman == @elder man
    
    @abstractmethod
    async def connect(self) -> bool:
        """Asynchronous connection to Data Source
        NOTE: Tushare uses a Synchonous connection method
        Returns:
            Bool: Successful connection
        """
        pass
    
    async def disconnect(self):
        """Disconnect"""
        self.connected = False
        self.logger.info(f"✅ {self.provider_name}Connection Disconnected")
    
    def is_available(self) -> bool:
        """Check for data source availability"""
        return self.connected
    
    #== sync, corrected by elderman == @elder man
    
    @abstractmethod
    async def get_stock_basic_info(self, symbol: str = None) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """Access to basic stock information

        Args:
            symbol: stock code, take all stocks for empty

        Returns:
            Single stock dictionary or list of shares
        """
        pass
    
    @abstractmethod
    async def get_stock_quotes(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get Real Time Lines

        Args:
            symbol: stock code

        Returns:
            Real Time Line Data Dictionary
        """
        pass
    
    @abstractmethod
    async def get_historical_data(
        self, 
        symbol: str, 
        start_date: Union[str, date], 
        end_date: Union[str, date] = None
    ) -> Optional[pd.DataFrame]:
        """Access to historical data

        Args:
            symbol: stock code
            Start date: Start date
            End date: End date

        Returns:
            DataFrame
        """
        pass
    
    #== sync, corrected by elderman == @elder man
    
    async def get_stock_list(self, market: str = None) -> Optional[List[Dict[str, Any]]]:
        """Get Stock List

        Args:
            Market code (CN/HK/US)

        Returns:
            List of stocks
        """
        return await self.get_stock_basic_info()
    
    async def get_financial_data(self, symbol: str, report_type: str = "annual") -> Optional[Dict[str, Any]]:
        """Access to financial data

        Args:
            symbol: stock code
            Report type: Report type (annual/quarterly)

        Returns:
            Financial data dictionary
        """
        #Default achieves return to None, subclasses can be rewrited
        return None
    
    #== sync, corrected by elderman == @elder man
    
    def standardize_basic_info(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Standardized stock base information

        Args:
            Raw data: raw data

        Returns:
            Standardized data
        """
        #Basic standardized logic
        return {
            "code": raw_data.get("code", raw_data.get("symbol", "")),
            "name": raw_data.get("name", ""),
            "symbol": raw_data.get("symbol", raw_data.get("code", "")),
            "full_symbol": raw_data.get("full_symbol", raw_data.get("ts_code", "")),
            
            #Market information
            "market_info": self._determine_market_info(raw_data),
            
            #Operational information
            "industry": raw_data.get("industry"),
            "area": raw_data.get("area"),
            "list_date": self._format_date_output(raw_data.get("list_date")),
            
            #Metadata
            "data_source": self.provider_name.lower(),
            "data_version": 1,
            "updated_at": datetime.utcnow()
        }
    
    def standardize_quotes(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Standardized real-time behaviour data

        Args:
            Raw data: raw data

        Returns:
            Standardized data
        """
        symbol = raw_data.get("symbol", raw_data.get("code", ""))
        
        return {
            #Base Fields
            "code": symbol,
            "symbol": symbol,
            "full_symbol": raw_data.get("full_symbol", raw_data.get("ts_code", symbol)),
            "market": self._determine_market(raw_data),
            
            #Price data
            "close": self._convert_to_float(raw_data.get("close")),
            "current_price": self._convert_to_float(raw_data.get("current_price", raw_data.get("close"))),
            "open": self._convert_to_float(raw_data.get("open")),
            "high": self._convert_to_float(raw_data.get("high")),
            "low": self._convert_to_float(raw_data.get("low")),
            "pre_close": self._convert_to_float(raw_data.get("pre_close")),
            
            #Change data
            "change": self._convert_to_float(raw_data.get("change")),
            "pct_chg": self._convert_to_float(raw_data.get("pct_chg")),
            
            #Sold data
            "volume": self._convert_to_float(raw_data.get("volume", raw_data.get("vol"))),
            "amount": self._convert_to_float(raw_data.get("amount")),
            
            #Time data
            "trade_date": self._format_date_output(raw_data.get("trade_date")),
            "timestamp": datetime.utcnow(),
            
            #Metadata
            "data_source": self.provider_name.lower(),
            "data_version": 1,
            "updated_at": datetime.utcnow()
        }
    
    #== sync, corrected by elderman == @elder man
    
    def _determine_market_info(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Identification of market information"""
        #Default achieved, subclass can be rewrited
        return {
            "market": "CN",
            "exchange": "UNKNOWN",
            "exchange_name": "未知交易所",
            "currency": "CNY",
            "timezone": "Asia/Shanghai"
        }
    
    def _determine_market(self, raw_data: Dict[str, Any]) -> str:
        """Identification of market codes"""
        market_info = self._determine_market_info(raw_data)
        return market_info.get("market", "CN")
    
    def _convert_to_float(self, value: Any) -> Optional[float]:
        """Convert to Float"""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _format_date_output(self, date_value: Any) -> Optional[str]:
        """Format date into output format (YYYYY-MM-DD)"""
        if not date_value:
            return None
        
        date_str = str(date_value)
        
        #Process YYYMMDD format
        if len(date_str) == 8 and date_str.isdigit():
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        
        #Process other formats
        if isinstance(date_value, (date, datetime)):
            return date_value.strftime('%Y-%m-%d')
        
        return date_str
    
    #== sync, corrected by elderman == @elder man
    
    async def __aenter__(self):
        """Step Context Manager Entry"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Step Context Manager Export"""
        await self.disconnect()
    
    def __repr__(self):
        return f"<{self.__class__.__name__}(name='{self.provider_name}', connected={self.connected})>"
