"""Harmonized definitions of data source codes
All data sources are defined here in terms of code, name, description, etc.

Steps to add a new data source:
Add a new data source code to the DatasourceCode count
Register data source information in DATA SOURCE REGISTRY
3. Implement data source interfaces in corresponding providers
4. Options for updating the front-end data source type (if required)
"""

from enum import Enum
from typing import Dict, List, Optional
from dataclasses import dataclass


class DataSourceCode(str, Enum):
    """Data source encoded entries

Name code:
- Use capital letters and underlineds
- Values with lowercase letters and underlineds
- Keep it simple and clear.
"""
    
    #== sync, corrected by elderman == @elder man
    MONGODB = "mongodb"  #MongoDB database cache (highest priority)
    
    #== sync, corrected by elderman == @elder man
    TUSHARE = "tushare"      #Tushare - Professional A Unit Data
    AKSHARE = "akshare"      #AKShare - Open Source Financial Data (Unit A + Port Unit)
    BAOSTOCK = "baostock"    #BaoStock - Free A stock data
    
    #== sync, corrected by elderman == @elder man
    YFINANCE = "yfinance"         #I'm sorry.
    FINNHUB = "finnhub"           #Finnhub - USE Real-Time Data
    YAHOO_FINANCE = "yahoo_finance"  #Yahoo Finance - Global Stock Data (alias)
    ALPHA_VANTAGE = "alpha_vantage"  #Alpha Vantage - USE Technical Analysis
    IEX_CLOUD = "iex_cloud"       #IEX Cloud - USU Real-Time Data
    
    #== sync, corrected by elderman == @elder man
    #Note: AKShare also supports the Port Unit, as defined above.
    
    #== sync, corrected by elderman == @elder man
    WIND = "wind"        #Wind Wonder - Professional Financial Terminal
    CHOICE = "choice"    #East Wealth Choice - Professional Financial Data
    
    #== sync, corrected by elderman == @elder man
    QUANDL = "quandl"        #Quandl - Economic and Financial Data
    LOCAL_FILE = "local_file"  #Local File Data Source
    CUSTOM = "custom"        #Custom Data Source


@dataclass
class DataSourceInfo:
    """Data Source Information"""
    code: str  #Data Source Encoding
    name: str  #Data Source Name
    display_name: str  #Show Name
    provider: str  #Provider
    description: str  #Description
    supported_markets: List[str]  #Supported markets (a shares, us stocks, hk stocks, etc.)
    requires_api_key: bool  #Need an API key
    is_free: bool  #Is it free?
    official_website: Optional[str] = None  #Official website
    documentation_url: Optional[str] = None  #Document Address
    features: List[str] = None  #Feature List
    
    def __post_init__(self):
        if self.features is None:
            self.features = []


#== sync, corrected by elderman == @elder man
DATA_SOURCE_REGISTRY: Dict[str, DataSourceInfo] = {
    #MongoDB Cache
    DataSourceCode.MONGODB: DataSourceInfo(
        code=DataSourceCode.MONGODB,
        name="MongoDB",
        display_name="MongoDB 缓存",
        provider="MongoDB Inc.",
        description="本地 MongoDB 数据库缓存，最高优先级数据源",
        supported_markets=["a_shares", "us_stocks", "hk_stocks", "crypto", "futures"],
        requires_api_key=False,
        is_free=True,
        features=["本地缓存", "最快速度", "离线可用"],
    ),
    
    # Tushare
    DataSourceCode.TUSHARE: DataSourceInfo(
        code=DataSourceCode.TUSHARE,
        name="Tushare",
        display_name="Tushare",
        provider="Tushare",
        description="专业的A股数据接口，提供高质量的历史数据和实时行情",
        supported_markets=["a_shares"],
        requires_api_key=True,
        is_free=False,  #Free editions are restricted, professional editions are paid.
        official_website="https://tushare.pro",
        documentation_url="https://tushare.pro/document/2",
        features=["历史行情", "实时行情", "财务数据", "基本面数据", "新闻公告"],
    ),
    
    # AKShare
    DataSourceCode.AKSHARE: DataSourceInfo(
        code=DataSourceCode.AKSHARE,
        name="AKShare",
        display_name="AKShare",
        provider="AKFamily",
        description="开源的金融数据接口，支持A股和港股，完全免费",
        supported_markets=["a_shares", "hk_stocks"],
        requires_api_key=False,
        is_free=True,
        official_website="https://akshare.akfamily.xyz",
        documentation_url="https://akshare.akfamily.xyz/introduction.html",
        features=["历史行情", "实时行情", "财务数据", "新闻资讯", "完全免费"],
    ),
    
    # BaoStock
    DataSourceCode.BAOSTOCK: DataSourceInfo(
        code=DataSourceCode.BAOSTOCK,
        name="BaoStock",
        display_name="BaoStock",
        provider="BaoStock",
        description="免费的A股数据接口，提供稳定的历史数据",
        supported_markets=["a_shares"],
        requires_api_key=False,
        is_free=True,
        official_website="http://baostock.com",
        documentation_url="http://baostock.com/baostock/index.php/Python_API%E6%96%87%E6%A1%A3",
        features=["历史行情", "财务数据", "完全免费", "数据稳定"],
    ),
    
    # yfinance
    DataSourceCode.YFINANCE: DataSourceInfo(
        code=DataSourceCode.YFINANCE,
        name="yfinance",
        display_name="yfinance (Yahoo Finance)",
        provider="Yahoo Finance",
        description="Yahoo Finance Python库，支持美股、港股等多个市场，完全免费",
        supported_markets=["us_stocks", "hk_stocks"],
        requires_api_key=False,
        is_free=True,
        official_website="https://finance.yahoo.com",
        documentation_url="https://pypi.org/project/yfinance/",
        features=["历史行情", "实时行情", "技术指标", "全球市场", "完全免费"],
    ),

    # Finnhub
    DataSourceCode.FINNHUB: DataSourceInfo(
        code=DataSourceCode.FINNHUB,
        name="Finnhub",
        display_name="Finnhub",
        provider="Finnhub",
        description="美股实时数据和新闻接口，提供高质量的市场数据",
        supported_markets=["us_stocks"],
        requires_api_key=True,
        is_free=True,  #It's free.
        official_website="https://finnhub.io",
        documentation_url="https://finnhub.io/docs/api",
        features=["实时行情", "历史数据", "新闻资讯", "财务数据", "技术指标"],
    ),
    
    # Yahoo Finance
    DataSourceCode.YAHOO_FINANCE: DataSourceInfo(
        code=DataSourceCode.YAHOO_FINANCE,
        name="Yahoo Finance",
        display_name="Yahoo Finance",
        provider="Yahoo",
        description="全球股票数据接口，支持美股、港股等多个市场",
        supported_markets=["us_stocks", "hk_stocks"],
        requires_api_key=False,
        is_free=True,
        official_website="https://finance.yahoo.com",
        features=["历史行情", "实时行情", "全球市场", "完全免费"],
    ),
    
    # Alpha Vantage
    DataSourceCode.ALPHA_VANTAGE: DataSourceInfo(
        code=DataSourceCode.ALPHA_VANTAGE,
        name="Alpha Vantage",
        display_name="Alpha Vantage",
        provider="Alpha Vantage",
        description="美股技术分析数据接口，提供丰富的技术指标",
        supported_markets=["us_stocks"],
        requires_api_key=True,
        is_free=True,  #It's free.
        official_website="https://www.alphavantage.co",
        documentation_url="https://www.alphavantage.co/documentation",
        features=["技术指标", "历史数据", "外汇数据", "加密货币"],
    ),
    
    # IEX Cloud
    DataSourceCode.IEX_CLOUD: DataSourceInfo(
        code=DataSourceCode.IEX_CLOUD,
        name="IEX Cloud",
        display_name="IEX Cloud",
        provider="IEX Cloud",
        description="美股实时数据接口，提供高质量的市场数据",
        supported_markets=["us_stocks"],
        requires_api_key=True,
        is_free=False,  #Fees required
        official_website="https://iexcloud.io",
        documentation_url="https://iexcloud.io/docs/api",
        features=["实时行情", "历史数据", "财务数据", "新闻资讯"],
    ),
    
    # Wind
    DataSourceCode.WIND: DataSourceInfo(
        code=DataSourceCode.WIND,
        name="Wind",
        display_name="Wind 万得",
        provider="Wind 万得",
        description="专业金融终端，提供全面的金融数据和分析工具",
        supported_markets=["a_shares", "hk_stocks", "us_stocks"],
        requires_api_key=True,
        is_free=False,  #Professional edition fees
        official_website="https://www.wind.com.cn",
        features=["专业数据", "全市场覆盖", "高质量数据", "专业分析"],
    ),
    
    # Choice
    DataSourceCode.CHOICE: DataSourceInfo(
        code=DataSourceCode.CHOICE,
        name="Choice",
        display_name="东方财富 Choice",
        provider="东方财富",
        description="专业金融数据终端，提供全面的A股数据",
        supported_markets=["a_shares"],
        requires_api_key=True,
        is_free=False,  #Professional edition fees
        official_website="http://choice.eastmoney.com",
        features=["专业数据", "A股专注", "高质量数据", "专业分析"],
    ),
    
    # Quandl
    DataSourceCode.QUANDL: DataSourceInfo(
        code=DataSourceCode.QUANDL,
        name="Quandl",
        display_name="Quandl",
        provider="Nasdaq",
        description="经济和金融数据平台，提供全球经济数据",
        supported_markets=["us_stocks"],
        requires_api_key=True,
        is_free=True,  #It's free.
        official_website="https://www.quandl.com",
        documentation_url="https://docs.quandl.com",
        features=["经济数据", "金融数据", "全球覆盖"],
    ),
    
    # Local File
    DataSourceCode.LOCAL_FILE: DataSourceInfo(
        code=DataSourceCode.LOCAL_FILE,
        name="Local File",
        display_name="本地文件",
        provider="本地",
        description="从本地文件读取数据",
        supported_markets=["a_shares", "us_stocks", "hk_stocks"],
        requires_api_key=False,
        is_free=True,
        features=["离线可用", "自定义数据", "完全免费"],
    ),
    
    # Custom
    DataSourceCode.CUSTOM: DataSourceInfo(
        code=DataSourceCode.CUSTOM,
        name="Custom",
        display_name="自定义数据源",
        provider="自定义",
        description="自定义数据源接口",
        supported_markets=["a_shares", "us_stocks", "hk_stocks"],
        requires_api_key=False,
        is_free=True,
        features=["自定义接口", "灵活配置"],
    ),
}


#== sync, corrected by elderman == @elder man

def get_data_source_info(code: str) -> Optional[DataSourceInfo]:
    """Access to data source information

Args:
code: Data source encoding

Returns:
Data source information, return None if not available
"""
    return DATA_SOURCE_REGISTRY.get(code)


def list_all_data_sources() -> List[DataSourceInfo]:
    """List all data sources

Returns:
List of all data sources
"""
    return list(DATA_SOURCE_REGISTRY.values())


def list_data_sources_by_market(market: str) -> List[DataSourceInfo]:
    """List the data sources supporting the specified market

Args:
Market type (a shares, us stocks, hk stocks, etc.)

Returns:
List of data sources supporting the market
"""
    return [
        info for info in DATA_SOURCE_REGISTRY.values()
        if market in info.supported_markets
    ]


def list_free_data_sources() -> List[DataSourceInfo]:
    """List all free data sources

Returns:
Free Data Source List
"""
    return [
        info for info in DATA_SOURCE_REGISTRY.values()
        if info.is_free
    ]


def is_data_source_supported(code: str) -> bool:
    """Check if data sources support

Args:
code: Data source encoding

Returns:
Supported
"""
    return code in DATA_SOURCE_REGISTRY

