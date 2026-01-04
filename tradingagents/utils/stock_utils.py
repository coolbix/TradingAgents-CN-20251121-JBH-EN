"""Stock Tool Functions
Provide stock code identification, classification and processing functions
"""

import re
from typing import Dict, Tuple, Optional
from enum import Enum

#Import Unified Log System
from tradingagents.utils.logging_init import get_logger
logger = get_logger("default")


class StockMarket(Enum):
    """Stock market count"""
    CHINA_A = "china_a"      #China A Unit
    HONG_KONG = "hong_kong"  #Port Unit
    US = "us"                #United States share
    UNKNOWN = "unknown"      #Unknown


class StockUtils:
    """Stock Tool Category"""
    
    @staticmethod
    def identify_stock_market(ticker: str) -> StockMarket:
        """Identify the market where the stock code belongs

        Args:
            ticker: Stock code

        Returns:
            Stockmark: Stock market type
        """
        if not ticker:
            return StockMarket.UNKNOWN

        ticker = str(ticker).strip().upper()

        #China Unit A: 6 figures
        if re.match(r'^\d{6}$', ticker):
            return StockMarket.CHINA_A

        #Port Unit: 4-5 digit. HK or 4-5 digit pure (support 0,700.HK, 09988.HK, 00700, 9988)
        if re.match(r'^\d{4,5}\.HK$', ticker) or re.match(r'^\d{4,5}$', ticker):
            return StockMarket.HONG_KONG

        #United States share: 1-5 letters
        if re.match(r'^[A-Z]{1,5}$', ticker):
            return StockMarket.US

        return StockMarket.UNKNOWN
    
    @staticmethod
    def is_china_stock(ticker: str) -> bool:
        """Whether or not it's China A.

        Args:
            ticker: Stock code

        Returns:
            Bool: Is it Chinese Unit A?
        """
        return StockUtils.identify_stock_market(ticker) == StockMarket.CHINA_A
    
    @staticmethod
    def is_hk_stock(ticker: str) -> bool:
        """Determination of Port Unit

        Args:
            ticker: Stock code

        Returns:
            Bool: Is it the Port Unit?
        """
        return StockUtils.identify_stock_market(ticker) == StockMarket.HONG_KONG
    
    @staticmethod
    def is_us_stock(ticker: str) -> bool:
        """To determine whether it's a U.S. stock.

        Args:
            ticker: Stock code

        Returns:
            Bool: Is it American?
        """
        return StockUtils.identify_stock_market(ticker) == StockMarket.US
    
    @staticmethod
    def get_currency_info(ticker: str) -> Tuple[str, str]:
        """Obtain monetary information by stock code

        Args:
            ticker: Stock code

        Returns:
            Tuple[str, st]: (currency name, currency symbol)
        """
        market = StockUtils.identify_stock_market(ticker)
        
        if market == StockMarket.CHINA_A:
            return "人民币", "¥"
        elif market == StockMarket.HONG_KONG:
            return "港币", "HK$"
        elif market == StockMarket.US:
            return "美元", "$"
        else:
            return "未知", "?"
    
    @staticmethod
    def get_data_source(ticker: str) -> str:
        """Obtain recommended data sources from stock code

        Args:
            ticker: Stock code

        Returns:
            str: Data Source Name
        """
        market = StockUtils.identify_stock_market(ticker)
        
        if market == StockMarket.CHINA_A:
            return "china_unified"  #Use of unified Chinese stock data source
        elif market == StockMarket.HONG_KONG:
            return "yahoo_finance"  #The Hong Kong Unit uses Yahoo Finance
        elif market == StockMarket.US:
            return "yahoo_finance"  #USU uses Yahoo Finance
        else:
            return "unknown"
    
    @staticmethod
    def normalize_hk_ticker(ticker: str) -> str:
        """Standardized port unit code format

        Args:
            ticker: Original Port Unit Code

        Returns:
            st: Standardized port unit code
        """
        if not ticker:
            return ticker
            
        ticker = str(ticker).strip().upper()
        
        #Add. HK suffix if 4-5 digits are pure
        if re.match(r'^\d{4,5}$', ticker):
            return f"{ticker}.HK"

        #If it's in the right format, go straight back.
        if re.match(r'^\d{4,5}\.HK$', ticker):
            return ticker
            
        return ticker
    
    @staticmethod
    def get_market_info(ticker: str) -> Dict:
        """Access to stock market details

        Args:
            ticker: Stock code

        Returns:
            Dict: Market Dictionary
        """
        market = StockUtils.identify_stock_market(ticker)
        currency_name, currency_symbol = StockUtils.get_currency_info(ticker)
        data_source = StockUtils.get_data_source(ticker)
        
        market_names = {
            StockMarket.CHINA_A: "中国A股",
            StockMarket.HONG_KONG: "港股",
            StockMarket.US: "美股",
            StockMarket.UNKNOWN: "未知市场"
        }
        
        return {
            "ticker": ticker,
            "market": market.value,
            "market_name": market_names[market],
            "currency_name": currency_name,
            "currency_symbol": currency_symbol,
            "data_source": data_source,
            "is_china": market == StockMarket.CHINA_A,
            "is_hk": market == StockMarket.HONG_KONG,
            "is_us": market == StockMarket.US
        }


#Easy function, maintain backward compatibility
def is_china_stock(ticker: str) -> bool:
    """Determination of China A Unit (reward compatibility)"""
    return StockUtils.is_china_stock(ticker)


def is_hk_stock(ticker: str) -> bool:
    """Determination of Port Unit"""
    return StockUtils.is_hk_stock(ticker)


def is_us_stock(ticker: str) -> bool:
    """To determine whether it's a U.S. stock."""
    return StockUtils.is_us_stock(ticker)


def get_stock_market_info(ticker: str) -> Dict:
    """Access to stock market information"""
    return StockUtils.get_market_info(ticker)
