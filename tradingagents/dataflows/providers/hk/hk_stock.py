"""Port Unit data acquisition tool
Provide port unit data acquisition, processing and cache functionality
"""

import pandas as pd
import numpy as np
import yfinance as yf
import time
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from tradingagents.config.runtime_settings import get_timezone_name

import os

from tradingagents.config.runtime_settings import get_float, get_int
#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')



class HKStockProvider:
    """Port Unit data provider"""

    def __init__(self):
        """Initialization Port Unit data provider"""
        self.last_request_time = 0
        self.min_request_interval = get_float("TA_HK_MIN_REQUEST_INTERVAL_SECONDS", "ta_hk_min_request_interval_seconds", 2.0)
        self.timeout = get_int("TA_HK_TIMEOUT_SECONDS", "ta_hk_timeout_seconds", 60)
        self.max_retries = get_int("TA_HK_MAX_RETRIES", "ta_hk_max_retries", 3)
        self.rate_limit_wait = get_int("TA_HK_RATE_LIMIT_WAIT_SECONDS", "ta_hk_rate_limit_wait_seconds", 60)

        logger.info(f"Initialization of the data provider for the Port Unit")

    def _wait_for_rate_limit(self):
        """Waiting Rate Limit"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last_request
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def get_stock_data(self, symbol: str, start_date: str = None, end_date: str = None) -> Optional[pd.DataFrame]:
        """Access to historical data on port units

        Args:
            Symbol: Port Unit Code (e.g. 0700.HK)
            Start date: Start date (YYYYY-MM-DD)
            End date: End Date (YYYYY-MM-DD)

        Returns:
            DataFrame: Stock history data
        """
        try:
            #Standardized port unit code
            symbol = self._normalize_hk_symbol(symbol)

            #Set Default Date
            if not end_date:
                end_date = datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y-%m-%d')
            if not start_date:
                start_date = (datetime.now(ZoneInfo(get_timezone_name())) - timedelta(days=365)).strftime('%Y-%m-%d')

            logger.info(f"Access to Port Unit data:{symbol} ({start_date}Present.{end_date})")

            #Retry data many times
            for attempt in range(self.max_retries):
                try:
                    self._wait_for_rate_limit()

                    #Use yfinance to get data
                    ticker = yf.Ticker(symbol)
                    data = ticker.history(
                        start=start_date,
                        end=end_date,
                        timeout=self.timeout
                    )

                    if not data.empty:
                        #Data preprocessing
                        data = data.reset_index()
                        data['Symbol'] = symbol

                        logger.info(f"Port Unit data acquisition success:{symbol}, {len(data)}Notes")
                        return data
                    else:
                        logger.warning(f"Port Unit data is empty:{symbol}(Trying){attempt + 1}/{self.max_retries})")

                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"❌ Port Unit data acquisition failed (attempted){attempt + 1}/{self.max_retries}): {error_msg}")

                    #Check for frequency limit error
                    if "Rate limited" in error_msg or "Too Many Requests" in error_msg:
                        if attempt < self.max_retries - 1:
                            logger.info(f"Frequency limit detected, waiting{self.rate_limit_wait}Seconds...")
                            time.sleep(self.rate_limit_wait)
                        else:
                            logger.error(f"Frequency limit, skip retry")
                            break
                    else:
                        if attempt < self.max_retries - 1:
                            time.sleep(2 ** attempt)  #Index evading

            logger.error(f"❌ Port Unit data acquisition failed:{symbol}")
            return None

        except Exception as e:
            logger.error(f"Port Unit data acquisition anomaly:{e}")
            return None

    def get_stock_info(self, symbol: str) -> Dict[str, Any]:
        """Access to basic information on port units

        Args:
            Symbol: Port Unit Code

        Returns:
            Dict: Stock Basic Information
        """
        try:
            symbol = self._normalize_hk_symbol(symbol)

            logger.info(f"For information about the Port Unit:{symbol}")

            self._wait_for_rate_limit()

            ticker = yf.Ticker(symbol)
            info = ticker.info

            if info and 'symbol' in info:
                return {
                    'symbol': symbol,
                    'name': info.get('longName', info.get('shortName', f'港股{symbol}')),
                    'currency': info.get('currency', 'HKD'),
                    'exchange': info.get('exchange', 'HKG'),
                    'market_cap': info.get('marketCap'),
                    'sector': info.get('sector'),
                    'industry': info.get('industry'),
                    'source': 'yfinance_hk'
                }
            else:
                return {
                    'symbol': symbol,
                    'name': f'港股{symbol}',
                    'currency': 'HKD',
                    'exchange': 'HKG',
                    'source': 'yfinance_hk'
                }

        except Exception as e:
            logger.error(f"Access to information on the Port Unit failed:{e}")
            return {
                'symbol': symbol,
                'name': f'港股{symbol}',
                'currency': 'HKD',
                'exchange': 'HKG',
                'source': 'unknown',
                'error': str(e)
            }

    def get_real_time_price(self, symbol: str) -> Optional[Dict]:
        """Real-time prices for the acquisition of port shares

        Args:
            Symbol: Port Unit Code

        Returns:
            Dict: Real-time price information
        """
        try:
            symbol = self._normalize_hk_symbol(symbol)

            self._wait_for_rate_limit()

            ticker = yf.Ticker(symbol)

            #Access to up-to-date historical data (1 day)
            data = ticker.history(period="1d", timeout=self.timeout)

            if not data.empty:
                latest = data.iloc[-1]
                return {
                    'symbol': symbol,
                    'price': latest['Close'],
                    'open': latest['Open'],
                    'high': latest['High'],
                    'low': latest['Low'],
                    'volume': latest['Volume'],
                    'timestamp': data.index[-1].strftime('%Y-%m-%d %H:%M:%S'),
                    'currency': 'HKD'
                }
            else:
                return None

        except Exception as e:
            logger.error(f"❌ has failed to obtain real-time prices for the Port stock:{e}")
            return None

    def _normalize_hk_symbol(self, symbol: str) -> str:
        """Standardized port unit code format

        Yahoo Finance Expected Format: 0700.HK (4-digit)
        Enter a possible format: 007000, 700, 07000.HK, 007000.HK

        Args:
            Symbol: Original Port Unit Code

        Returns:
            str: Standardized Port Unit Code (format: 0700.HK)
        """
        if not symbol:
            return symbol

        symbol = str(symbol).strip().upper()

        #If you already have a.HK suffix, remove it.
        if symbol.endswith('.HK'):
            symbol = symbol[:-3]

        #If it's a pure number, it's standardised at 4 digits.
        if symbol.isdigit():
            #Remove pilot 0 and complete it to four.
            clean_code = symbol.lstrip('0') or '0'  #If it's all zeros, keep one zero.
            normalized_code = clean_code.zfill(4)
            return f"{normalized_code}.HK"

        return symbol

    def format_stock_data(self, symbol: str, data: pd.DataFrame, start_date: str, end_date: str) -> str:
        """Formatting Port Unit data into text format (including technical indicators)

        Args:
            symbol: stock code
            Data: Stock data DataFrame
            Start date: Start date
            End date: End date

        Returns:
            str: Formatted stock data text (including technical indicators)
        """
        if data is None or data.empty:
            return f"❌ 无法获取港股 {symbol} 的数据"

        try:
            original_data_count = len(data)
            logger.info(f"[Hong Kong Unit Technical Indicators]{original_data_count}Article")

            #Access to basic stock information
            stock_info = self.get_stock_info(symbol)
            stock_name = stock_info.get('name', f'港股{symbol}')

            #Ensure that data are sorted by date
            if 'Date' in data.columns:
                data = data.sort_values('Date')
            else:
                data = data.sort_index()

            #Calculate moving average lines
            data['ma5'] = data['Close'].rolling(window=5, min_periods=1).mean()
            data['ma10'] = data['Close'].rolling(window=10, min_periods=1).mean()
            data['ma20'] = data['Close'].rolling(window=20, min_periods=1).mean()
            data['ma60'] = data['Close'].rolling(window=60, min_periods=1).mean()

            #Calculating RSI (relative strength and weakness indicator)
            delta = data['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
            rs = gain / (loss.replace(0, np.nan))
            data['rsi'] = 100 - (100 / (1 + rs))

            #Compute MCD
            ema12 = data['Close'].ewm(span=12, adjust=False).mean()
            ema26 = data['Close'].ewm(span=26, adjust=False).mean()
            data['macd_dif'] = ema12 - ema26
            data['macd_dea'] = data['macd_dif'].ewm(span=9, adjust=False).mean()
            data['macd'] = (data['macd_dif'] - data['macd_dea']) * 2

            #Calculating Brink Belts
            data['boll_mid'] = data['Close'].rolling(window=20, min_periods=1).mean()
            std = data['Close'].rolling(window=20, min_periods=1).std()
            data['boll_upper'] = data['boll_mid'] + 2 * std
            data['boll_lower'] = data['boll_mid'] - 2 * std

            #Only the last 3-5 days of data are retained for display (reduce token consumption)
            display_rows = min(5, len(data))
            display_data = data.tail(display_rows)
            latest_data = data.iloc[-1]

            #🔍 [Debug log] Prints raw data and technical indicators for the last five days
            logger.info(f"🔍 [Hong Kong Unit Technical Indicators Details] = = = most recent ={display_rows}Number of transactions")
            for i, (idx, row) in enumerate(display_data.iterrows(), 1):
                date_str = row.get('Date', idx.strftime('%Y-%m-%d') if hasattr(idx, 'strftime') else str(idx))
                logger.info(f"🔍 [Hong Kong Unit Technical Indicators Details]{i}Day{date_str}):")
                logger.info(f"Price: Open ={row.get('Open', 0):.2f}High{row.get('High', 0):.2f}Low ={row.get('Low', 0):.2f}, received ={row.get('Close', 0):.2f}")
                logger.info(f"   MA: MA5={row.get('ma5', 0):.2f}, MA10={row.get('ma10', 0):.2f}, MA20={row.get('ma20', 0):.2f}, MA60={row.get('ma60', 0):.2f}")
                logger.info(f"   MACD: DIF={row.get('macd_dif', 0):.4f}, DEA={row.get('macd_dea', 0):.4f}, MACD={row.get('macd', 0):.4f}")
                logger.info(f"   RSI: {row.get('rsi', 0):.2f}")
                logger.info(f"BOLL: Up{row.get('boll_upper', 0):.2f}, Medium ={row.get('boll_mid', 0):.2f}, below{row.get('boll_lower', 0):.2f}")

            logger.info(f"🔍 [Hong Kong Unit Technical Indicators Detailed] = = = data end= = = = =")

            #Formatting output contains all technical indicators reconciled Read
            result = f"📊 {stock_name}({symbol}) - 港股技术分析数据\n"
            result += "=" * 60 + "\n\n"

            #Basic information
            result += "📈 基本信息\n"
            result += f"   代码: {symbol}\n"
            result += f"   名称: {stock_name}\n"
            result += f"   货币: 港币 (HKD)\n"
            result += f"   交易所: 香港交易所 (HKG)\n"
            result += f"   数据期间: {start_date} 至 {end_date}\n"
            result += f"   交易天数: {len(data)}天\n\n"

            #Recent prices
            latest_price = latest_data['Close']
            result += "💰 最新价格\n"
            result += f"   收盘价: HK${latest_price:.2f}\n"
            result += f"   开盘价: HK${latest_data['Open']:.2f}\n"
            result += f"   最高价: HK${latest_data['High']:.2f}\n"
            result += f"   最低价: HK${latest_data['Low']:.2f}\n"
            result += f"   成交量: {latest_data['Volume']:,.0f}股\n\n"

            #Move average line
            result += "📊 移动平均线 (MA)\n"
            ma5 = latest_data['ma5']
            ma10 = latest_data['ma10']
            ma20 = latest_data['ma20']
            ma60 = latest_data['ma60']

            if not pd.isna(ma5):
                ma5_diff = ((latest_price - ma5) / ma5) * 100
                ma5_pos = "上方" if latest_price > ma5 else "下方"
                result += f"   MA5: HK${ma5:.2f} (价格在MA5{ma5_pos} {abs(ma5_diff):.2f}%)\n"

            if not pd.isna(ma10):
                ma10_diff = ((latest_price - ma10) / ma10) * 100
                ma10_pos = "上方" if latest_price > ma10 else "下方"
                result += f"   MA10: HK${ma10:.2f} (价格在MA10{ma10_pos} {abs(ma10_diff):.2f}%)\n"

            if not pd.isna(ma20):
                ma20_diff = ((latest_price - ma20) / ma20) * 100
                ma20_pos = "上方" if latest_price > ma20 else "下方"
                result += f"   MA20: HK${ma20:.2f} (价格在MA20{ma20_pos} {abs(ma20_diff):.2f}%)\n"

            if not pd.isna(ma60):
                ma60_diff = ((latest_price - ma60) / ma60) * 100
                ma60_pos = "上方" if latest_price > ma60 else "下方"
                result += f"   MA60: HK${ma60:.2f} (价格在MA60{ma60_pos} {abs(ma60_diff):.2f}%)\n"

            #Organisation
            if not pd.isna(ma5) and not pd.isna(ma10) and not pd.isna(ma20):
                if ma5 > ma10 > ma20:
                    result += "   ✅ 均线呈多头排列\n\n"
                elif ma5 < ma10 < ma20:
                    result += "   ⚠️ 均线呈空头排列\n\n"
                else:
                    result += "   ➡️ 均线排列混乱\n\n"
            else:
                result += "\n"

            #MACD indicators
            result += "📉 MACD指标\n"
            macd_dif = latest_data['macd_dif']
            macd_dea = latest_data['macd_dea']
            macd = latest_data['macd']

            if not pd.isna(macd_dif) and not pd.isna(macd_dea):
                result += f"   DIF: {macd_dif:.4f}\n"
                result += f"   DEA: {macd_dea:.4f}\n"
                result += f"   MACD柱: {macd:.4f} ({'多头' if macd > 0 else '空头'})\n"

                #MACD gold fork/dead fork test
                if len(data) > 1:
                    prev_dif = data.iloc[-2]['macd_dif']
                    prev_dea = data.iloc[-2]['macd_dea']
                    curr_dif = latest_data['macd_dif']
                    curr_dea = latest_data['macd_dea']

                    if not pd.isna(prev_dif) and not pd.isna(prev_dea):
                        if prev_dif <= prev_dea and curr_dif > curr_dea:
                            result += "   ⚠️ MACD金叉信号（DIF上穿DEA）\n\n"
                        elif prev_dif >= prev_dea and curr_dif < curr_dea:
                            result += "   ⚠️ MACD死叉信号（DIF下穿DEA）\n\n"
                        else:
                            result += "\n"
                    else:
                        result += "\n"
                else:
                    result += "\n"
            else:
                result += "   数据不足，无法计算MACD\n\n"

            #RSI indicators
            result += "📊 RSI指标\n"
            rsi = latest_data['rsi']

            if not pd.isna(rsi):
                result += f"   RSI(14): {rsi:.2f}"
                if rsi >= 70:
                    result += " (超买区域)\n\n"
                elif rsi <= 30:
                    result += " (超卖区域)\n\n"
                elif rsi >= 60:
                    result += " (接近超买区域)\n\n"
                elif rsi <= 40:
                    result += " (接近超卖区域)\n\n"
                else:
                    result += " (中性区域)\n\n"
            else:
                result += "   数据不足，无法计算RSI\n\n"

            #Blinks.
            result += "📐 布林带 (BOLL)\n"
            boll_upper = latest_data['boll_upper']
            boll_mid = latest_data['boll_mid']
            boll_lower = latest_data['boll_lower']

            if not pd.isna(boll_upper) and not pd.isna(boll_mid) and not pd.isna(boll_lower):
                result += f"   上轨: HK${boll_upper:.2f}\n"
                result += f"   中轨: HK${boll_mid:.2f}\n"
                result += f"   下轨: HK${boll_lower:.2f}\n"

                #Calculates where the price is in the boolean belt
                boll_width = boll_upper - boll_lower
                if boll_width > 0:
                    boll_position = ((latest_price - boll_lower) / boll_width) * 100
                    result += f"   价格位置: {boll_position:.1f}%"

                    if boll_position >= 90:
                        result += " (接近上轨)\n\n"
                    elif boll_position <= 10:
                        result += " (接近下轨)\n\n"
                    else:
                        result += "\n\n"
                else:
                    result += "\n"
            else:
                result += "   数据不足，无法计算布林带\n\n"

            #Recent transaction date data
            result += "📅 最近交易日数据\n"
            for _, row in display_data.iterrows():
                if 'Date' in row:
                    date_str = row['Date'].strftime('%Y-%m-%d')
                else:
                    date_str = row.name.strftime('%Y-%m-%d')

                result += f"   {date_str}: "
                result += f"开盘HK${row['Open']:.2f}, "
                result += f"收盘HK${row['Close']:.2f}, "
                result += f"最高HK${row['High']:.2f}, "
                result += f"最低HK${row['Low']:.2f}, "
                result += f"成交量{row['Volume']:,.0f}\n"

            result += "\n数据来源: Yahoo Finance (港股)\n"

            logger.info(f"✅ [Hong Kong Unit Technical Indicators]{display_rows}Day data")

            return result

        except Exception as e:
            logger.error(f"❌ Formatting Port Unit data failed:{e}", exc_info=True)
            return f"❌ 港股数据格式化失败: {symbol}"


#Examples of global providers
_hk_provider = None

def get_hk_stock_provider() -> HKStockProvider:
    """Access to global port unit provider examples"""
    global _hk_provider
    if _hk_provider is None:
        _hk_provider = HKStockProvider()
    return _hk_provider


def get_hk_stock_data(symbol: str, start_date: str = None, end_date: str = None) -> str:
    """Easy function to access port stock data

    Args:
        Symbol: Port Unit Code
        Start date: Start date
        End date: End date

    Returns:
        str: Formatted Port Unit data
    """
    provider = get_hk_stock_provider()
    data = provider.get_stock_data(symbol, start_date, end_date)
    return provider.format_stock_data(symbol, data, start_date, end_date)


def get_hk_stock_info(symbol: str) -> Dict:
    """A convenient function to access information on port shares

    Args:
        Symbol: Port Unit Code

    Returns:
        Dict: Port Unit Information
    """
    provider = get_hk_stock_provider()
    return provider.get_stock_info(symbol)
