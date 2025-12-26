"""Stock Data Model - Based on Existing Pool Extension
Adopt option B: Expand fields on existing pools and maintain backward compatibility
"""
from datetime import datetime, date
from typing import Optional, Dict, Any, List, Literal
from pydantic import BaseModel, Field
from bson import ObjectId


def to_str_id(v: Any) -> str:
    """ObjectId spin string utility function"""
    try:
        if isinstance(v, ObjectId):
            return str(v)
        return str(v)
    except Exception:
        return ""


#Enumeration type definition
MarketType = Literal["CN", "HK", "US"]  #Market type
ExchangeType = Literal["SZSE", "SSE", "SEHK", "NYSE", "NASDAQ"]  #Exchange
StockStatus = Literal["L", "D", "P"]  #Listing status: L-listed D-release P-suspended
CurrencyType = Literal["CNY", "HKD", "USD"]  #Currency type


class MarketInfo(BaseModel):
    """Market Information Structure - Add Fields"""
    market: MarketType = Field(..., description="市场标识")
    exchange: ExchangeType = Field(..., description="交易所代码")
    exchange_name: str = Field(..., description="交易所名称")
    currency: CurrencyType = Field(..., description="交易货币")
    timezone: str = Field(..., description="时区")
    trading_hours: Optional[Dict[str, Any]] = Field(None, description="交易时间")


class TechnicalIndicators(BaseModel):
    """Technical indicator structure - classification extension design"""
    #Trends indicators
    trend: Optional[Dict[str, float]] = Field(None, description="趋势指标")
    #Percussion indicators
    oscillator: Optional[Dict[str, float]] = Field(None, description="震荡指标")
    #Channel indicators
    channel: Optional[Dict[str, float]] = Field(None, description="通道指标")
    #Transaction indicators
    volume: Optional[Dict[str, float]] = Field(None, description="成交量指标")
    #Volatility indicators
    volatility: Optional[Dict[str, float]] = Field(None, description="波动率指标")
    #Custom indicators
    custom: Optional[Dict[str, Any]] = Field(None, description="自定义指标")


class StockBasicInfoExtended(BaseModel):
    """Stock Basic Information Extension Model - based on existing stock basic info collection
Harmonize the use of symbol as the main stock code field
"""
    #= = standardized field (main field) = = =
    symbol: str = Field(..., description="6位股票代码", pattern=r"^\d{6}$")
    full_symbol: str = Field(..., description="完整标准化代码(如 000001.SZ)")
    name: str = Field(..., description="股票名称")

    #= = Compatible Fields (maintain compatibility backwards) = =
    code: Optional[str] = Field(None, description="6位股票代码(已废弃,使用symbol)")

    #== sync, corrected by elderman ==
    area: Optional[str] = Field(None, description="所在地区")
    industry: Optional[str] = Field(None, description="行业")
    market: Optional[str] = Field(None, description="交易所名称")
    list_date: Optional[str] = Field(None, description="上市日期")
    sse: Optional[str] = Field(None, description="板块")
    sec: Optional[str] = Field(None, description="所属板块")
    source: Optional[str] = Field(None, description="数据来源")
    updated_at: Optional[datetime] = Field(None, description="更新时间")

    #Market value fields
    total_mv: Optional[float] = Field(None, description="总市值(亿元)")
    circ_mv: Optional[float] = Field(None, description="流通市值(亿元)")

    #Financial indicators
    pe: Optional[float] = Field(None, description="市盈率")
    pb: Optional[float] = Field(None, description="市净率")
    pe_ttm: Optional[float] = Field(None, description="滚动市盈率")
    pb_mrq: Optional[float] = Field(None, description="最新市净率")
    roe: Optional[float] = Field(None, description="净资产收益率")

    #Transaction indicators
    turnover_rate: Optional[float] = Field(None, description="换手率%")
    volume_ratio: Optional[float] = Field(None, description="量比")

    #== sync, corrected by elderman ==
    name_en: Optional[str] = Field(None, description="英文名称")
    
    #Add Market Information
    market_info: Optional[MarketInfo] = Field(None, description="市场信息")
    
    #New Standard Field
    board: Optional[str] = Field(None, description="板块标准化")
    industry_code: Optional[str] = Field(None, description="行业代码")
    sector: Optional[str] = Field(None, description="所属板块标准化（GICS行业）")
    delist_date: Optional[str] = Field(None, description="退市日期")
    status: Optional[StockStatus] = Field(None, description="上市状态")
    is_hs: Optional[bool] = Field(None, description="是否沪深港通标的")

    #New equity information
    total_shares: Optional[float] = Field(None, description="总股本")
    float_shares: Optional[float] = Field(None, description="流通股本")

    #Hong Kong stock specific field
    lot_size: Optional[int] = Field(None, description="每手股数（港股特有）")

    #Currency field
    currency: Optional[CurrencyType] = Field(None, description="交易货币")
    
    #Version Control
    data_version: Optional[int] = Field(None, description="数据版本")
    
    class Config:
        #Allows extra fields to maintain backward compatibility
        extra = "allow"
        #Example Data
        json_schema_extra = {
            "example": {
                #Standardized Fields
                "symbol": "000001",
                "full_symbol": "000001.SZ",
                "name": "平安银行",

                #Basic information
                "area": "深圳",
                "industry": "银行",
                "market": "深圳证券交易所",
                "sse": "主板",
                "total_mv": 2500.0,
                "pe": 5.2,
                "pb": 0.8,

                #Expand Fields
                "market_info": {
                    "market": "CN",
                    "exchange": "SZSE",
                    "exchange_name": "深圳证券交易所",
                    "currency": "CNY",
                    "timezone": "Asia/Shanghai"
                },
                "status": "L",
                "data_version": 1
            }
        }


class MarketQuotesExtended(BaseModel):
    """Real-time line extension model - based on existing market quotes collection
Harmonize the use of symbol as the main stock code field
"""
    #= = standardized field (main field) = = =
    symbol: str = Field(..., description="6位股票代码", pattern=r"^\d{6}$")
    full_symbol: Optional[str] = Field(None, description="完整标准化代码")
    market: Optional[MarketType] = Field(None, description="市场标识")

    #= = Compatible Fields (maintain compatibility backwards) = =
    code: Optional[str] = Field(None, description="6位股票代码(已废弃,使用symbol)")

    #♪ Line fields ♪
    close: Optional[float] = Field(None, description="收盘价")
    pct_chg: Optional[float] = Field(None, description="涨跌幅%")
    amount: Optional[float] = Field(None, description="成交额")
    open: Optional[float] = Field(None, description="开盘价")
    high: Optional[float] = Field(None, description="最高价")
    low: Optional[float] = Field(None, description="最低价")
    pre_close: Optional[float] = Field(None, description="前收盘价")
    trade_date: Optional[str] = Field(None, description="交易日期")
    updated_at: Optional[datetime] = Field(None, description="更新时间")
    
    #Add line field
    current_price: Optional[float] = Field(None, description="当前价格(与close相同)")
    change: Optional[float] = Field(None, description="涨跌额")
    volume: Optional[float] = Field(None, description="成交量")
    turnover_rate: Optional[float] = Field(None, description="换手率")
    volume_ratio: Optional[float] = Field(None, description="量比")
    
    #Five.
    bid_prices: Optional[List[float]] = Field(None, description="买1-5价")
    bid_volumes: Optional[List[float]] = Field(None, description="买1-5量")
    ask_prices: Optional[List[float]] = Field(None, description="卖1-5价")
    ask_volumes: Optional[List[float]] = Field(None, description="卖1-5量")
    
    #Timetamp
    timestamp: Optional[datetime] = Field(None, description="行情时间戳")
    
    #Data source and version
    data_source: Optional[str] = Field(None, description="数据来源")
    data_version: Optional[int] = Field(None, description="数据版本")
    
    class Config:
        extra = "allow"
        json_schema_extra = {
            "example": {
                #Standardized Fields
                "symbol": "000001",
                "full_symbol": "000001.SZ",
                "market": "CN",

                #Line field
                "close": 12.65,
                "pct_chg": 1.61,
                "amount": 1580000000,
                "open": 12.50,
                "high": 12.80,
                "low": 12.30,
                "trade_date": "2024-01-15",

                #Expand Fields
                "current_price": 12.65,
                "change": 0.20,
                "volume": 125000000
            }
        }


#Response model for database operations
class StockBasicInfoResponse(BaseModel):
    """Equities Basic Information API Response Model"""
    success: bool = True
    data: Optional[StockBasicInfoExtended] = None
    message: str = ""


class MarketQuotesResponse(BaseModel):
    """Real-time API Response Model"""
    success: bool = True
    data: Optional[MarketQuotesExtended] = None
    message: str = ""


class StockListResponse(BaseModel):
    """Stock List API Response Model"""
    success: bool = True
    data: Optional[List[StockBasicInfoExtended]] = None
    total: int = 0
    page: int = 1
    page_size: int = 20
    message: str = ""
