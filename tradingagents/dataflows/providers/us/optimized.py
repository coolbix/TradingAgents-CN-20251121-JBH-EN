#!/usr/bin/env python3
"""Optimized US stock data acquisition tool
Integrated cache strategy, fewer API calls, faster response
"""

import os
import time
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from typing import Optional, Dict, Any
import yfinance as yf
import pandas as pd

#Import Cache Manager (support new and old paths)
try:
    from ...cache import StockDataCache
    def get_cache():
        return StockDataCache()
except ImportError:
    from ...cache_manager import get_cache

#Import Configuration (support for old and new paths)
try:
    from ...config import get_config
except ImportError:
    def get_config():
        return {}

from tradingagents.config.runtime_settings import get_float, get_timezone_name
#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')


class OptimizedUSDataProvider:
    """Optimized U.S. stock data provider - integration cache and API restriction processing"""

    def __init__(self):
        self.cache = get_cache()
        self.config = get_config()
        self.last_api_call = 0
        self.min_api_interval = get_float("TA_US_MIN_API_INTERVAL_SECONDS", "ta_us_min_api_interval_seconds", 1.0)

        #🔥 Initialization data source manager (read configuration from database)
        try:
            from tradingagents.dataflows.data_source_manager import USDataSourceManager
            self.us_manager = USDataSourceManager()
            logger.info(f"U.S. stock data source manager successfully initiated")
        except Exception as e:
            logger.warning(f"The initialization of the USE data source manager failed:{e}, the default order will be used")
            self.us_manager = None

        logger.info(f"📊 Optimization of U.S. stock data provider initialised")

    def _wait_for_rate_limit(self):
        """Waiting for API Limit"""
        current_time = time.time()
        time_since_last_call = current_time - self.last_api_call

        if time_since_last_call < self.min_api_interval:
            wait_time = self.min_api_interval - time_since_last_call
            logger.info(f"API limits waiting{wait_time:.1f}s...")
            time.sleep(wait_time)

        self.last_api_call = time.time()

    def get_stock_data(self, symbol: str, start_date: str, end_date: str,
                      force_refresh: bool = False) -> str:
        """Get US share data - Prioritize Cache

Args:
symbol: stock code
Start date: Start date (YYYYY-MM-DD)
End date: End Date (YYYYY-MM-DD)
source refresh: whether to forcibly refresh the cache

Returns:
Formatted stock data string
"""
        logger.info(f"For US stock data:{symbol} ({start_date}Present.{end_date})")

        #Check Cache (unless mandatory refreshing)
        if not force_refresh:
            #Find caches by data source priority
            from ...data_source_manager import get_us_data_source_manager, USDataSource
            us_manager = get_us_data_source_manager()

            #Get data source priorities
            priority_order = us_manager._get_data_source_priority_order(symbol)

            #Data Source Name Map
            source_name_mapping = {
                USDataSource.ALPHA_VANTAGE: "alpha_vantage",
                USDataSource.YFINANCE: "yfinance",
                USDataSource.FINNHUB: "finnhub",
            }

            #Find caches in order of priority
            for source in priority_order:
                if source == USDataSource.MONGODB:
                    continue  #MongoDB cache is handled separately

                source_name = source_name_mapping.get(source)
                if source_name:
                    cache_key = self.cache.find_cached_stock_data(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date,
                        data_source=source_name
                    )

                    if cache_key:
                        cached_data = self.cache.load_stock_data(cache_key)
                        if cached_data:
                            logger.info(f"[Data source: Cache-{source_name}Loading US share data from cache:{symbol}")
                            return cached_data

        #Cache uncut, fetch from API - use data source manager priority order
        formatted_data = None
        data_source = None

        #🔥 Get priority order from the data source manager
        if self.us_manager:
            try:
                source_priority = self.us_manager._get_data_source_priority_order(symbol)
                logger.info(f"Read from the database:{[s.value for s in source_priority]}")
            except Exception as e:
                logger.warning(f"Access to data source priority failed:{e}, using default order")
                source_priority = None
        else:
            source_priority = None

        #Use default order if no priority is configured
        if not source_priority:
            #Default order: yfinance > alpha vantage > Finnishhub
            from tradingagents.dataflows.data_source_manager import USDataSource
            source_priority = [USDataSource.YFINANCE, USDataSource.ALPHA_VANTAGE, USDataSource.FINNHUB]
            logger.info(f"The default order is used:{[s.value for s in source_priority]}")

        #Try data sources on a priority basis
        for source in source_priority:
            try:
                source_name = source.value
                logger.info(f"[Data source: API call--{source_name.upper()}Try from{source_name.upper()}Access to data:{symbol}")
                self._wait_for_rate_limit()

                #Call different methods according to data source type
                if source_name == 'finnhub':
                    formatted_data = self._get_data_from_finnhub(symbol, start_date, end_date)
                elif source_name == 'alpha_vantage':
                    formatted_data = self._get_data_from_alpha_vantage(symbol, start_date, end_date)
                elif source_name == 'yfinance':
                    formatted_data = self._get_data_from_yfinance(symbol, start_date, end_date)
                else:
                    logger.warning(f"Unknown data source type:{source_name}")
                    continue

                if formatted_data and "❌" not in formatted_data:
                    data_source = source_name
                    logger.info(f"✅ [Data source: API call successful]{source_name.upper()}] {source_name.upper()}Data acquisition success:{symbol}")
                    break  #Successfully capture data, jump out of loop
                else:
                    logger.warning(f"[Data source: API failed --{source_name.upper()}] {source_name.upper()}Data acquisition failed, try next data source")
                    formatted_data = None

            except Exception as e:
                logger.error(f"[Data source: API anomaly--{source.value.upper()}] {source.value.upper()}API call failed:{e}")
                formatted_data = None
                continue  #Try Next Data Source

        #If all the configured data sources fail, try the backup.
        if not formatted_data:
            try:
                #Test for stock type
                from tradingagents.utils.stock_utils import StockUtils
                market_info = StockUtils.get_market_info(symbol)

                if market_info['is_hk']:
                    #Port Unit prioritizes AKShare data source
                    logger.info(f"🇭🇰 [Data Source: API Call-AKShare]{symbol}")
                    try:
                        from tradingagents.dataflows.interface import get_hk_stock_data_unified
                        hk_data_text = get_hk_stock_data_unified(symbol, start_date, end_date)

                        if hk_data_text and "❌" not in hk_data_text:
                            formatted_data = hk_data_text
                            data_source = "akshare_hk"
                            logger.info(f"✅ [Data source: API call success-AKShare] AKShare Port data acquisition success:{symbol}")
                        else:
                            raise Exception("AKShare港股数据获取失败")

                    except Exception as e:
                        logger.error(f"⚠️ [data source: API failed -- AKShare] AKShare Port failed to access data:{e}")
                        #Alternative: Yahoo Finance
                        logger.info(f"🔄 [Data Source: API Call-Yahoo Finance Standby]{symbol}")

                        self._wait_for_rate_limit()
                        ticker = yf.Ticker(symbol)  #Maintenance of port unit code
                        data = ticker.history(start=start_date, end=end_date)

                        if not data.empty:
                            formatted_data = self._format_stock_data(symbol, data, start_date, end_date)
                            data_source = "yfinance_hk"
                            logger.info(f"✅ [Data source: API call successful -- Yahoo Finance] Yahoo Finance Port Unit data acquisition success:{symbol}")
                        else:
                            logger.error(f"❌ [Data Source: API Failed -- Yahoo Finance] Yahoo Finance Port Unit data is empty:{symbol}")
                else:
                    #USU uses Yahoo Finance
                    logger.info(f"[Data Source: API Call-Yahoo Finance]{symbol}")
                    self._wait_for_rate_limit()

                    #Getting data
                    ticker = yf.Ticker(symbol.upper())
                    data = ticker.history(start=start_date, end=end_date)

                    if data.empty:
                        error_msg = f"未找到股票 '{symbol}' 在 {start_date} 到 {end_date} 期间的数据"
                        logger.error(f"❌ [Data Source: API Failed -- Yahoo Finance]{error_msg}")
                    else:
                        #Formatting Data
                        formatted_data = self._format_stock_data(symbol, data, start_date, end_date)
                        data_source = "yfinance"
                        logger.info(f"✅ [Data Source: API Call Success -- Yahoo Finance] Yahoo Finance US stock data acquisition success:{symbol}")

            except Exception as e:
                logger.error(f"Data acquisition failed:{e}")
                formatted_data = None

        #If all APIs fail, generate backup data.
        if not formatted_data:
            error_msg = "所有美股数据源都不可用"
            logger.error(f"[Data source: All API failed]{error_msg}")
            logger.warning(f"⚠️ [data source: backup data] Generate secondary data:{symbol}")
            return self._generate_fallback_data(symbol, start_date, end_date, error_msg)

        #Save to Cache
        self.cache.save_stock_data(
            symbol=symbol,
            data=formatted_data,
            start_date=start_date,
            end_date=end_date,
            data_source=data_source
        )

        logger.info(f"[Data source:{data_source}Data cached:{symbol}")
        return formatted_data

    def _format_stock_data(self, symbol: str, data: pd.DataFrame,
                          start_date: str, end_date: str) -> str:
        """Format stock data as string"""

        #Remove Timezone Information
        if data.index.tz is not None:
            data.index = data.index.tz_localize(None)

        #Rounded values
        numeric_columns = ["Open", "High", "Low", "Close", "Adj Close"]
        for col in numeric_columns:
            if col in data.columns:
                data[col] = data[col].round(2)

        #Access to up-to-date price and statistical information
        latest_price = data['Close'].iloc[-1]
        price_change = data['Close'].iloc[-1] - data['Close'].iloc[0]
        price_change_pct = (price_change / data['Close'].iloc[0]) * 100

        #🔥 Calculates functions using harmonized technical indicators
        #Close, High, Low
        from tradingagents.tools.analysis.indicators import add_all_indicators
        data = add_all_indicators(data, close_col='Close', high_col='High', low_col='Low')

        #Access to up-to-date technology indicators
        latest = data.iloc[-1]

        #Format Output
        result = f"""# {symbol} 美股数据分析

## 📊 基本信息
- 股票代码: {symbol}
- 数据期间: {start_date} 至 {end_date}
- 数据条数: {len(data)}条
- 最新价格: ${latest_price:.2f}
- 期间涨跌: ${price_change:+.2f} ({price_change_pct:+.2f}%)

## 📈 价格统计
- 期间最高: ${data['High'].max():.2f}
- 期间最低: ${data['Low'].min():.2f}
- 平均成交量: {data['Volume'].mean():,.0f}

## 🔍 技术指标（最新值）
**移动平均线**:
- MA5: ${latest['ma5']:.2f}
- MA10: ${latest['ma10']:.2f}
- MA20: ${latest['ma20']:.2f}
- MA60: ${latest['ma60']:.2f}

**MACD指标**:
- DIF: {latest['macd_dif']:.2f}
- DEA: {latest['macd_dea']:.2f}
- MACD: {latest['macd']:.2f}

**RSI指标**:
- RSI(14): {latest['rsi']:.2f}

**布林带**:
- 上轨: ${latest['boll_upper']:.2f}
- 中轨: ${latest['boll_mid']:.2f}
- 下轨: ${latest['boll_lower']:.2f}

## 📋 最近5日数据
{data[['Open', 'High', 'Low', 'Close', 'Volume']].tail().to_string()}

数据来源: Yahoo Finance API
更新时间: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y-%m-%d %H:%M:%S')}
"""

        return result

    def _try_get_old_cache(self, symbol: str, start_date: str, end_date: str) -> Optional[str]:
        """Try to obtain expired cache data as backup"""
        try:
            #Find any associated caches without TTL
            for metadata_file in self.cache.metadata_dir.glob(f"*_meta.json"):
                try:
                    import json
                    with open(metadata_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)

                    if (metadata.get('symbol') == symbol and
                        metadata.get('data_type') == 'stock_data' and
                        metadata.get('market_type') == 'us'):

                        cache_key = metadata_file.stem.replace('_meta', '')
                        cached_data = self.cache.load_stock_data(cache_key)
                        if cached_data:
                            return cached_data + "\n\n⚠️ 注意: 使用的是过期缓存数据"
                except Exception:
                    continue
        except Exception:
            pass

        return None

    def _get_data_from_finnhub(self, symbol: str, start_date: str, end_date: str) -> str:
        """Obtain stock data from FINNHUB API"""
        try:
            import finnhub
            import os
            from datetime import datetime, timedelta


            #Get API Keys
            api_key = os.getenv('FINNHUB_API_KEY')
            if not api_key:
                return None

            client = finnhub.Client(api_key=api_key)

            #Get live quotes
            quote = client.quote(symbol.upper())
            if not quote or 'c' not in quote:
                return None

            #Access to corporate information
            profile = client.company_profile2(symbol=symbol.upper())
            company_name = profile.get('name', symbol.upper()) if profile else symbol.upper()

            #Formatting Data
            current_price = quote.get('c', 0)
            change = quote.get('d', 0)
            change_percent = quote.get('dp', 0)

            formatted_data = f"""# {symbol.upper()} 美股数据分析

## 📊 实时行情
- 股票名称: {company_name}
- 当前价格: ${current_price:.2f}
- 涨跌额: ${change:+.2f}
- 涨跌幅: {change_percent:+.2f}%
- 开盘价: ${quote.get('o', 0):.2f}
- 最高价: ${quote.get('h', 0):.2f}
- 最低价: ${quote.get('l', 0):.2f}
- 前收盘: ${quote.get('pc', 0):.2f}
- 更新时间: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y-%m-%d %H:%M:%S')}

## 📈 数据概览
- 数据期间: {start_date} 至 {end_date}
- 数据来源: FINNHUB API (实时数据)
- 当前价位相对位置: {((current_price - quote.get('l', current_price)) / max(quote.get('h', current_price) - quote.get('l', current_price), 0.01) * 100):.1f}%
- 日内振幅: {((quote.get('h', 0) - quote.get('l', 0)) / max(quote.get('pc', 1), 0.01) * 100):.2f}%

生成时间: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y-%m-%d %H:%M:%S')}
"""

            return formatted_data

        except Exception as e:
            logger.error(f"FINNHUB data acquisition failed:{e}")
            return None

    def _get_data_from_yfinance(self, symbol: str, start_date: str, end_date: str) -> str:
        """Get stock data from Yahoo Finance API"""
        try:
            #Getting data
            ticker = yf.Ticker(symbol.upper())
            data = ticker.history(start=start_date, end=end_date)

            if data.empty:
                error_msg = f"未找到股票 '{symbol}' 在 {start_date} 到 {end_date} 期间的数据"
                logger.error(f"Yahoo Finance is empty:{error_msg}")
                return None

            #Formatting Data
            formatted_data = self._format_stock_data(symbol, data, start_date, end_date)
            return formatted_data

        except Exception as e:
            logger.error(f"Yahoo Finance data acquisition failed:{e}")
            return None

    def _get_data_from_alpha_vantage(self, symbol: str, start_date: str, end_date: str) -> str:
        """Fetch stock data from Alpha Vantage API"""
        try:
            from tradingagents.dataflows.providers.us.alpha_vantage_common import get_api_key
            import requests
            from datetime import datetime

            #Get API Key
            api_key = get_api_key()
            if not api_key:
                logger.warning("Alpha Vantage API Key is not configured")
                return None

            #Call Alpha Vantage API (TIME SERIES DAILY)
            url = f"https://www.alphavantage.co/query"
            params = {
                "function": "TIME_SERIES_DAILY",
                "symbol": symbol.upper(),
                "apikey": api_key,
                "outputsize": "full"  #Get complete historical data
            }

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data_json = response.json()

            #Check error
            if "Error Message" in data_json:
                logger.error(f"Alpha Vantage API error:{data_json['Error Message']}")
                return None

            if "Note" in data_json:
                logger.warning(f"Alpha Vantaage API restrictions:{data_json['Note']}")
                return None

            #Parsing time series data
            time_series = data_json.get("Time Series (Daily)", {})
            if not time_series:
                logger.error("Alpha Vantage returns data empty")
                return None

            #Convert to DataFrame
            df = pd.DataFrame.from_dict(time_series, orient='index')
            df.index = pd.to_datetime(df.index)
            df = df.sort_index()

            #Rename Column
            df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            df = df.astype(float)

            #Filter Date Range
            df = df[(df.index >= start_date) & (df.index <= end_date)]

            if df.empty:
                logger.error(f"Alpha Vantage data is empty within specified dates")
                return None

            #Formatting Data
            formatted_data = self._format_stock_data(symbol, df, start_date, end_date)
            return formatted_data

        except Exception as e:
            logger.error(f"Alpha Vantage data acquisition failed:{e}")
            return None

    def _generate_fallback_data(self, symbol: str, start_date: str, end_date: str, error_msg: str) -> str:
        """Generate backup data"""
        return f"""# {symbol} 美股数据获取失败

## ❌ 错误信息
{error_msg}

## 📊 模拟数据（仅供演示）
- 股票代码: {symbol}
- 数据期间: {start_date} 至 {end_date}
- 最新价格: ${random.uniform(100, 300):.2f}
- 模拟涨跌: {random.uniform(-5, 5):+.2f}%

## ⚠️ 重要提示
由于API限制或网络问题，无法获取实时数据。
建议稍后重试或检查网络连接。

生成时间: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y-%m-%d %H:%M:%S')}
"""


#Global Examples
_us_data_provider = None

def get_optimized_us_data_provider() -> OptimizedUSDataProvider:
    """Example of obtaining global United States stock data provider"""
    global _us_data_provider
    if _us_data_provider is None:
        _us_data_provider = OptimizedUSDataProvider()
    return _us_data_provider


def get_us_stock_data_cached(symbol: str, start_date: str, end_date: str,
                           force_refresh: bool = False) -> str:
    """A convenient function to access U.S. stock data

Args:
symbol: stock code
Start date: Start date (YYYYY-MM-DD)
End date: End Date (YYYYY-MM-DD)
source refresh: whether to forcibly refresh the cache

Returns:
Formatted stock data string
"""
    # Smart date range processing: auto-extension back-to-back days to configuration, processing weekends/leaves Day
    from tradingagents.utils.dataflow_utils import get_trading_date_range
    from app.core.config import get_settings
    from datetime import datetime

    original_start_date = start_date
    original_end_date = end_date

    #Number of days to retrieve market analysis back from configuration (default 60 days)
    try:
        settings = get_settings()
        lookback_days = settings.MARKET_ANALYST_LOOKBACK_DAYS
        logger.info(f"MARKET ANALIST LOOKBACK DAYS:{lookback_days}days")
    except Exception as e:
        lookback_days = 60  #Default 60 days
        logger.warning(f"Can not get configuration with default:{lookback_days}days")
        logger.warning(f"[United States shattering ]{e}")

    #Use end date as target date to retroactively specify days
    start_date, end_date = get_trading_date_range(end_date, lookback_days=lookback_days)

    logger.info(f"Original input:{original_start_date}to{original_end_date}")
    logger.info(f"Backtracking days:{lookback_days}days")
    logger.info(f"The results of the calculations are as follows:{start_date}to{end_date}")
    logger.info(f"[United States stock smart date]{(datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days}days")

    provider = get_optimized_us_data_provider()
    return provider.get_stock_data(symbol, start_date, end_date, force_refresh)
