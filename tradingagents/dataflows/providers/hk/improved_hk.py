#!/usr/bin/env python3
"""Improved port unit data acquisition tool
Addressing API speed limits and data access issues
"""

import time
import json
import os
import pandas as pd
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

from tradingagents.config.runtime_settings import get_int
#Import Unified Log System
from tradingagents.utils.logging_init import get_logger
logger = get_logger("default")

#Add: using a unified directory configuration
try:
    from utils.data_config import get_cache_dir
except Exception:
    #Back: data/cache/hk under project root
    def get_cache_dir(subdir: Optional[str] = None, create: bool = True):
        base = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'cache')
        if subdir:
            base = os.path.join(base, subdir)
        if create:
            os.makedirs(base, exist_ok=True)
        return base


class ImprovedHKStockProvider:
    """Improved port unit data provider"""
    
    def __init__(self):
        #Write cache files to a unified data cache directory to avoid contamination of the root directory
        hk_cache_dir = get_cache_dir('hk')
        if hasattr(hk_cache_dir, 'joinpath'):  # Path
            self.cache_file = str(hk_cache_dir.joinpath('hk_stock_cache.json'))
        else:  # str
            self.cache_file = os.path.join(hk_cache_dir, 'hk_stock_cache.json')

        self.cache_ttl = get_int("TA_HK_CACHE_TTL_SECONDS", "ta_hk_cache_ttl_seconds", 3600 * 24)
        self.rate_limit_wait = get_int("TA_HK_RATE_LIMIT_WAIT_SECONDS", "ta_hk_rate_limit_wait_seconds", 5)
        self.last_request_time = 0

        #Internal port name map (avoid API call)
        self.hk_stock_names = {
            #I'm calling.
            '0700.HK': '腾讯控股', '0700': '腾讯控股', '00700': '腾讯控股',
            
            #Telecommunications operator
            '0941.HK': '中国移动', '0941': '中国移动', '00941': '中国移动',
            '0762.HK': '中国联通', '0762': '中国联通', '00762': '中国联通',
            '0728.HK': '中国电信', '0728': '中国电信', '00728': '中国电信',
            
            #Bank
            '0939.HK': '建设银行', '0939': '建设银行', '00939': '建设银行',
            '1398.HK': '工商银行', '1398': '工商银行', '01398': '工商银行',
            '3988.HK': '中国银行', '3988': '中国银行', '03988': '中国银行',
            '0005.HK': '汇丰控股', '0005': '汇丰控股', '00005': '汇丰控股',
            
            #Insurance
            '1299.HK': '友邦保险', '1299': '友邦保险', '01299': '友邦保险',
            '2318.HK': '中国平安', '2318': '中国平安', '02318': '中国平安',
            '2628.HK': '中国人寿', '2628': '中国人寿', '02628': '中国人寿',
            
            #Petrochemicals
            '0857.HK': '中国石油', '0857': '中国石油', '00857': '中国石油',
            '0386.HK': '中国石化', '0386': '中国石化', '00386': '中国石化',
            
            #Property
            '1109.HK': '华润置地', '1109': '华润置地', '01109': '华润置地',
            '1997.HK': '九龙仓置业', '1997': '九龙仓置业', '01997': '九龙仓置业',
            
            #Technology
            '9988.HK': '阿里巴巴', '9988': '阿里巴巴', '09988': '阿里巴巴',
            '3690.HK': '美团', '3690': '美团', '03690': '美团',
            '1024.HK': '快手', '1024': '快手', '01024': '快手',
            '9618.HK': '京东集团', '9618': '京东集团', '09618': '京东集团',
            
            #Consumption
            '1876.HK': '百威亚太', '1876': '百威亚太', '01876': '百威亚太',
            '0291.HK': '华润啤酒', '0291': '华润啤酒', '00291': '华润啤酒',
            
            #Medicine
            '1093.HK': '石药集团', '1093': '石药集团', '01093': '石药集团',
            '0867.HK': '康师傅', '0867': '康师傅', '00867': '康师傅',
            
            #Car
            '2238.HK': '广汽集团', '2238': '广汽集团', '02238': '广汽集团',
            '1211.HK': '比亚迪', '1211': '比亚迪', '01211': '比亚迪',
            
            #Aviation
            '0753.HK': '中国国航', '0753': '中国国航', '00753': '中国国航',
            '0670.HK': '中国东航', '0670': '中国东航', '00670': '中国东航',
            
            #Steel
            '0347.HK': '鞍钢股份', '0347': '鞍钢股份', '00347': '鞍钢股份',
            
            #Electricity
            '0902.HK': '华能国际', '0902': '华能国际', '00902': '华能国际',
            '0991.HK': '大唐发电', '0991': '大唐发电', '00991': '大唐发电'
        }
        
        self._load_cache()
    
    def _load_cache(self):
        """Load Cache"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self.cache = json.load(f)
            else:
                self.cache = {}
        except Exception as e:
            logger.debug(f"[Port Cache] Loading cache failed:{e}")
            self.cache = {}
    
    def _save_cache(self):
        """Save Cache"""
        try:
            #Ensure directory exists
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.debug(f"[Hong Kong Stock Cache]{e}")
    
    def _is_cache_valid(self, key: str) -> bool:
        """Check if the cache is valid"""
        if key not in self.cache:
            return False

        cache_time = self.cache[key].get('timestamp', 0)
        return (time.time() - cache_time) < self.cache_ttl

    def _rate_limit(self):
        """Speed limit: ensure sufficient spacing between requests"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < self.rate_limit_wait:
            wait_time = self.rate_limit_wait - time_since_last_request
            logger.debug(f"Waiting{wait_time:.2f}sec")
            time.sleep(wait_time)

        self.last_request_time = time.time()

    def _normalize_hk_symbol(self, symbol: str) -> str:
        """Standardized port unit code"""
        #Remove. HK suffix
        clean_symbol = symbol.replace('.HK', '').replace('.hk', '')
        
        #Complete to 5 Digits
        if len(clean_symbol) == 4:
            clean_symbol = '0' + clean_symbol
        elif len(clean_symbol) == 3:
            clean_symbol = '00' + clean_symbol
        elif len(clean_symbol) == 2:
            clean_symbol = '000' + clean_symbol
        elif len(clean_symbol) == 1:
            clean_symbol = '0000' + clean_symbol
        
        return clean_symbol
    
    def get_company_name(self, symbol: str) -> str:
        """Obtaining the name of the Port Equity Company

Args:
Symbol: Port Unit Code

Returns:
str: Company name
"""
        try:
            #Check Cache
            cache_key = f"name_{symbol}"
            if self._is_cache_valid(cache_key):
                cached_name = self.cache[cache_key]['data']
                logger.debug(f"[Hong Kong Stock Cache]{symbol} -> {cached_name}")
                return cached_name
            
            #Option 1: Use built-in maps
            normalized_symbol = self._normalize_hk_symbol(symbol)
            
            #Try multiple formats
            for format_symbol in [symbol, normalized_symbol, f"{normalized_symbol}.HK"]:
                if format_symbol in self.hk_stock_names:
                    company_name = self.hk_stock_names[format_symbol]
                    
                    #Cache Result
                    self.cache[cache_key] = {
                        'data': company_name,
                        'timestamp': time.time(),
                        'source': 'builtin_mapping'
                    }
                    self._save_cache()
                    
                    logger.debug(f"[Hong Kong Stock Mapping]{symbol} -> {company_name}")
                    return company_name
            
            #Option 2: Prioritize the AKShare API acquisition (restricted protection)
            try:
                #Rate limit protection
                current_time = time.time()
                if current_time - self.last_request_time < self.rate_limit_wait:
                    wait_time = self.rate_limit_wait - (current_time - self.last_request_time)
                    logger.debug(f"[Hong Kong Unit API]{wait_time:.1f}sec")
                    time.sleep(wait_time)

                self.last_request_time = time.time()

                #Try AKShare first.
                try:
                    #Directly use the akshare library to access and avoid circular calls
                    logger.debug(f"[Hong Kong Unit API]{symbol}")

                    import akshare as ak
                    #Standardized code format (kshare requires 5-bit format)
                    normalized_symbol = self._normalize_hk_symbol(symbol)

                    #Attempt to obtain real-time information (includes name) on the Port Unit
                    try:
                        #Use of the New Wave financial interface (more stable)
                        df = ak.stock_hk_spot()
                        if df is not None and not df.empty:
                            #Find a matching stock
                            matched = df[df['代码'] == normalized_symbol]
                            if not matched.empty:
                                #The new wave interface returned a list of "Chinese names"
                                akshare_name = matched.iloc[0]['中文名称']
                                if akshare_name and not str(akshare_name).startswith('港股'):
                                    #Cache AKShare Results
                                    self.cache[cache_key] = {
                                        'data': akshare_name,
                                        'timestamp': time.time(),
                                        'source': 'akshare_sina'
                                    }
                                    self._save_cache()

                                    logger.debug(f"📊 [Hong Kong shares AKshare - New Wave]{symbol} -> {akshare_name}")
                                    return akshare_name
                    except Exception as e:
                        logger.debug(f"📊 [Hong Kong Unit Akshare-New Wave]{e}")

                except Exception as e:
                    logger.debug(f"[Hong Kong Unit AKShare] AKShare has failed to access:{e}")

                #Stand-by: attempt to obtain from a unified interface (including Yahoo Finance)
                from tradingagents.dataflows.interface import get_hk_stock_info_unified
                hk_info = get_hk_stock_info_unified(symbol)

                if hk_info and isinstance(hk_info, dict) and 'name' in hk_info:
                    api_name = hk_info['name']
                    if not api_name.startswith('港股'):
                        #Cache API Results
                        self.cache[cache_key] = {
                            'data': api_name,
                            'timestamp': time.time(),
                            'source': 'unified_api'
                        }
                        self._save_cache()

                        logger.debug(f"📊 [UAPI]{symbol} -> {api_name}")
                        return api_name

            except Exception as e:
                logger.debug(f"[Hong Kong Unit API]{e}")
            
            #Option 3: Generate friendly default names
            clean_symbol = self._normalize_hk_symbol(symbol)
            default_name = f"港股{clean_symbol}"
            
            #Cache default result (shorter TTL)
            self.cache[cache_key] = {
                'data': default_name,
                'timestamp': time.time() - self.cache_ttl + 3600,  #Expired in 1 hour
                'source': 'default'
            }
            self._save_cache()
            
            logger.debug(f"Use the default name:{symbol} -> {default_name}")
            return default_name
            
        except Exception as e:
            logger.error(f"[Hong Kong Unit]{e}")
            clean_symbol = self._normalize_hk_symbol(symbol)
            return f"港股{clean_symbol}"
    
    def get_financial_indicators(self, symbol: str) -> Dict[str, Any]:
        """Access to port unit financial indicators

Use the AKShare stop financial hk analysis indicator em interface
Access to key financial indicators including EPS, BPS, ROE, ROA, etc.

Args:
Symbol: Port Unit Code

Returns:
Dict: Financial indicators data
"""
        try:
            import akshare as ak

            #Standardized Code
            normalized_symbol = self._normalize_hk_symbol(symbol)

            #Check Cache
            cache_key = f"financial_{normalized_symbol}"
            if self._is_cache_valid(cache_key):
                logger.debug(f"[Port Unit Financial Indicators]{normalized_symbol}")
                return self.cache[cache_key]['data']

            #Rate limit
            self._rate_limit()

            logger.info(f"Access to financial indicators:{normalized_symbol}")

            #Call the AKShare interface
            df = ak.stock_financial_hk_analysis_indicator_em(symbol=normalized_symbol)

            if df is None or df.empty:
                logger.warning(f"No data were obtained:{normalized_symbol}")
                return {}

            #Get the latest data
            latest = df.iloc[0]

            #Extract key indicators
            indicators = {
                #Basic information
                'report_date': str(latest.get('REPORT_DATE', '')),
                'fiscal_year': str(latest.get('FISCAL_YEAR', '')),

                #Indicator per unit
                'eps_basic': float(latest.get('BASIC_EPS', 0)) if pd.notna(latest.get('BASIC_EPS')) else None,
                'eps_diluted': float(latest.get('DILUTED_EPS', 0)) if pd.notna(latest.get('DILUTED_EPS')) else None,
                'eps_ttm': float(latest.get('EPS_TTM', 0)) if pd.notna(latest.get('EPS_TTM')) else None,
                'bps': float(latest.get('BPS', 0)) if pd.notna(latest.get('BPS')) else None,
                'per_netcash_operate': float(latest.get('PER_NETCASH_OPERATE', 0)) if pd.notna(latest.get('PER_NETCASH_OPERATE')) else None,

                #Profitability indicators
                'roe_avg': float(latest.get('ROE_AVG', 0)) if pd.notna(latest.get('ROE_AVG')) else None,
                'roe_yearly': float(latest.get('ROE_YEARLY', 0)) if pd.notna(latest.get('ROE_YEARLY')) else None,
                'roa': float(latest.get('ROA', 0)) if pd.notna(latest.get('ROA')) else None,
                'roic_yearly': float(latest.get('ROIC_YEARLY', 0)) if pd.notna(latest.get('ROIC_YEARLY')) else None,
                'net_profit_ratio': float(latest.get('NET_PROFIT_RATIO', 0)) if pd.notna(latest.get('NET_PROFIT_RATIO')) else None,
                'gross_profit_ratio': float(latest.get('GROSS_PROFIT_RATIO', 0)) if pd.notna(latest.get('GROSS_PROFIT_RATIO')) else None,

                #Income indicators
                'operate_income': float(latest.get('OPERATE_INCOME', 0)) if pd.notna(latest.get('OPERATE_INCOME')) else None,
                'operate_income_yoy': float(latest.get('OPERATE_INCOME_YOY', 0)) if pd.notna(latest.get('OPERATE_INCOME_YOY')) else None,
                'operate_income_qoq': float(latest.get('OPERATE_INCOME_QOQ', 0)) if pd.notna(latest.get('OPERATE_INCOME_QOQ')) else None,
                'gross_profit': float(latest.get('GROSS_PROFIT', 0)) if pd.notna(latest.get('GROSS_PROFIT')) else None,
                'gross_profit_yoy': float(latest.get('GROSS_PROFIT_YOY', 0)) if pd.notna(latest.get('GROSS_PROFIT_YOY')) else None,
                'holder_profit': float(latest.get('HOLDER_PROFIT', 0)) if pd.notna(latest.get('HOLDER_PROFIT')) else None,
                'holder_profit_yoy': float(latest.get('HOLDER_PROFIT_YOY', 0)) if pd.notna(latest.get('HOLDER_PROFIT_YOY')) else None,

                #Debt sustainability indicators
                'debt_asset_ratio': float(latest.get('DEBT_ASSET_RATIO', 0)) if pd.notna(latest.get('DEBT_ASSET_RATIO')) else None,
                'current_ratio': float(latest.get('CURRENT_RATIO', 0)) if pd.notna(latest.get('CURRENT_RATIO')) else None,

                #Cash flow indicators
                'ocf_sales': float(latest.get('OCF_SALES', 0)) if pd.notna(latest.get('OCF_SALES')) else None,

                #Data Sources
                'source': 'akshare_eastmoney',
                'data_count': len(df)
            }

            #Cache Data
            self.cache[cache_key] = {
                'data': indicators,
                'timestamp': time.time()
            }
            self._save_cache()

            logger.info(f"✅ [Hong Kong Unit Financial Indicators]{normalized_symbol}reporting period:{indicators['report_date']}")
            return indicators

        except Exception as e:
            logger.error(f"❌ [Hong Kong Unit Financial Indicators]{symbol} - {e}")
            return {}

    def get_stock_info(self, symbol: str) -> Dict[str, Any]:
        """Access to basic information on port units

Args:
Symbol: Port Unit Code

Returns:
Dict: Port Unit Information
"""
        try:
            company_name = self.get_company_name(symbol)

            return {
                'symbol': symbol,
                'name': company_name,
                'currency': 'HKD',
                'exchange': 'HKG',
                'market': '港股',
                'source': 'improved_hk_provider'
            }
            
        except Exception as e:
            logger.error(f"[Hong Kong shares]{e}")
            clean_symbol = self._normalize_hk_symbol(symbol)
            return {
                'symbol': symbol,
                'name': f'港股{clean_symbol}',
                'currency': 'HKD',
                'exchange': 'HKG',
                'market': '港股',
                'source': 'error',
                'error': str(e)
            }


#Global Examples
_improved_hk_provider = None

def get_improved_hk_provider() -> ImprovedHKStockProvider:
    """Examples of access to improved port unit providers"""
    global _improved_hk_provider
    if _improved_hk_provider is None:
        _improved_hk_provider = ImprovedHKStockProvider()
    return _improved_hk_provider


def get_hk_company_name_improved(symbol: str) -> str:
    """Access to improved names of port equity companies

Args:
Symbol: Port Unit Code

Returns:
str: Company name
"""
    provider = get_improved_hk_provider()
    return provider.get_company_name(symbol)


def get_hk_stock_info_improved(symbol: str) -> Dict[str, Any]:
    """Improved access to information on port units

Args:
Symbol: Port Unit Code

Returns:
Dict: Port Unit Information
"""
    provider = get_improved_hk_provider()
    return provider.get_stock_info(symbol)


def get_hk_financial_indicators(symbol: str) -> Dict[str, Any]:
    """Access to port unit financial indicators

Args:
Symbol: Port Unit Code

Returns:
Dict: Financial indicators data, including:
- eps basic: basic per share
- eps ttm: scroll each share of proceeds
- bps: Net assets per share
- roe avg: Average net asset return
- Roa: Total asset return
- Operate income: operating income
- Operate income youy: Growth of operating income per year
-debt asset ratio: asset-liability ratio
Wait.
"""
    provider = get_improved_hk_provider()
    return provider.get_financial_indicators(symbol)


#Compatibility function: for old kshare utils import
def get_hk_stock_data_akshare(symbol: str, start_date: str = None, end_date: str = None):
    """Compatibility function: Access to historical Hong Kong stock data using the AKShare New Wave financial interface

Args:
Symbol: Port Unit Code
Start date: Start date
End date: End date

Returns:
Port Unit data (formatted string)
"""
    try:
        import akshare as ak
        from datetime import datetime, timedelta

        #Standardized Code
        provider = get_improved_hk_provider()
        normalized_symbol = provider._normalize_hk_symbol(symbol)

        #Set Default Date
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        logger.info(f"[AKShare-New Wave]{symbol} ({start_date} ~ {end_date})")

        #Access to historical data using the New Wave financial interface
        df = ak.stock_hk_daily(symbol=normalized_symbol, adjust="qfq")

        if df is None or df.empty:
            logger.warning(f"[AKShare-New Waves]{symbol}")
            return f"❌ 无法获取港股{symbol}的历史数据"

        #Filter Date Range
        df['date'] = pd.to_datetime(df['date'])
        mask = (df['date'] >= start_date) & (df['date'] <= end_date)
        df = df.loc[mask]

        if df.empty:
            logger.warning(f"No data available within date range:{symbol}")
            return f"❌ 港股{symbol}在指定日期范围内无数据"

        #🔥 Add pre close field (retributed from the previous day 's close)
        #AKShare does not return pre close field, manual calculation is required
        df['pre_close'] = df['close'].shift(1)

        #Counting up and down.
        df['change'] = df['close'] - df['pre_close']
        df['pct_change'] = (df['change'] / df['pre_close'] * 100).round(2)

        #🔥 Calculates functions using harmonized technical indicators
        from tradingagents.tools.analysis.indicators import add_all_indicators
        df = add_all_indicators(df, close_col='close', high_col='high', low_col='low')

        #Access to and calculation of financial indicators
        financial_indicators = provider.get_financial_indicators(symbol)

        #Formatting output (including price data and technical indicators)
        latest = df.iloc[-1]
        current_price = latest['close']

        #Calculate PE, PB
        pe_ratio = None
        pb_ratio = None
        financial_section = ""

        if financial_indicators:
            eps_ttm = financial_indicators.get('eps_ttm')
            bps = financial_indicators.get('bps')

            if eps_ttm and eps_ttm > 0:
                pe_ratio = current_price / eps_ttm

            if bps and bps > 0:
                pb_ratio = current_price / bps

            #Build the financial indicator segment (process the None value)
            def format_value(value, format_str=".2f", suffix="", default="N/A"):
                """Formatting Numeric Values to Process None Situation"""
                if value is None:
                    return default
                try:
                    return f"{value:{format_str}}{suffix}"
                except:
                    return default

            financial_section = f"""
### 财务指标（最新报告期：{financial_indicators.get('report_date', 'N/A')}）
**估值指标**:
- PE (市盈率): {f'{pe_ratio:.2f}' if pe_ratio else 'N/A'} (当前价 / EPS_TTM)
- PB (市净率): {f'{pb_ratio:.2f}' if pb_ratio else 'N/A'} (当前价 / BPS)

**每股指标**:
- 基本每股收益 (EPS): HK${format_value(financial_indicators.get('eps_basic'))}
- 滚动每股收益 (EPS_TTM): HK${format_value(financial_indicators.get('eps_ttm'))}
- 每股净资产 (BPS): HK${format_value(financial_indicators.get('bps'))}
- 每股经营现金流: HK${format_value(financial_indicators.get('per_netcash_operate'))}

**盈利能力**:
- 净资产收益率 (ROE): {format_value(financial_indicators.get('roe_avg'), suffix='%')}
- 总资产收益率 (ROA): {format_value(financial_indicators.get('roa'), suffix='%')}
- 净利率: {format_value(financial_indicators.get('net_profit_ratio'), suffix='%')}
- 毛利率: {format_value(financial_indicators.get('gross_profit_ratio'), suffix='%')}

**营收情况**:
- 营业收入: {format_value(financial_indicators.get('operate_income') / 1e8 if financial_indicators.get('operate_income') else None, suffix=' 亿港元')}
- 营收同比增长: {format_value(financial_indicators.get('operate_income_yoy'), suffix='%')}
- 归母净利润: {format_value(financial_indicators.get('holder_profit') / 1e8 if financial_indicators.get('holder_profit') else None, suffix=' 亿港元')}
- 净利润同比增长: {format_value(financial_indicators.get('holder_profit_yoy'), suffix='%')}

**偿债能力**:
- 资产负债率: {format_value(financial_indicators.get('debt_asset_ratio'), suffix='%')}
- 流动比率: {format_value(financial_indicators.get('current_ratio'))}
"""

        result = f"""## 港股历史数据 ({symbol})
**数据源**: AKShare (新浪财经)
**日期范围**: {start_date} ~ {end_date}
**数据条数**: {len(df)} 条

### 最新价格信息
- 最新价: HK${latest['close']:.2f}
- 昨收: HK${latest['pre_close']:.2f}
- 涨跌额: HK${latest['change']:.2f}
- 涨跌幅: {latest['pct_change']:.2f}%
- 最高: HK${latest['high']:.2f}
- 最低: HK${latest['low']:.2f}
- 成交量: {latest['volume']:,.0f}

### 技术指标（最新值）
**移动平均线**:
- MA5: HK${latest['ma5']:.2f}
- MA10: HK${latest['ma10']:.2f}
- MA20: HK${latest['ma20']:.2f}
- MA60: HK${latest['ma60']:.2f}

**MACD指标**:
- DIF: {latest['macd_dif']:.2f}
- DEA: {latest['macd_dea']:.2f}
- MACD: {latest['macd']:.2f}

**RSI指标**:
- RSI(14): {latest['rsi']:.2f}

**布林带**:
- 上轨: HK${latest['boll_upper']:.2f}
- 中轨: HK${latest['boll_mid']:.2f}
- 下轨: HK${latest['boll_lower']:.2f}
{financial_section}
### 最近10个交易日价格
{df[['date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_change', 'volume']].tail(10).to_string(index=False)}

### 数据统计
- 最高价: HK${df['high'].max():.2f}
- 最低价: HK${df['low'].min():.2f}
- 平均收盘价: HK${df['close'].mean():.2f}
- 总成交量: {df['volume'].sum():,.0f}
"""

        logger.info(f"[AKShare-New Wave]{symbol} ({len(df)}(Articles)")
        return result

    except Exception as e:
        logger.error(f"[Akshare-Since]{symbol} - {e}")
        return f"❌ 港股{symbol}历史数据获取失败: {str(e)}"


#Global Cache: Cache All Port Unit Data of AKShare
_akshare_hk_spot_cache = {
    'data': None,
    'timestamp': None,
    'ttl': 600  #Cache 10 Minutes (Reference U.S. Real Time Cache Time)
}

#Linelock: Prevent multiple threads from calling AKshare API
import threading
_akshare_hk_spot_lock = threading.Lock()


def get_hk_stock_info_akshare(symbol: str) -> Dict[str, Any]:
    """Compatibility function: directly use akshare to obtain information about the port stock (avoid recycling calls)
 Use global cache + thread lock to avoid repetition of calls for ak.stock hk spot()

Args:
Symbol: Port Unit Code

Returns:
Dict: Port Unit Information
"""
    try:
        import akshare as ak
        from datetime import datetime

        #Standardized Code
        provider = get_improved_hk_provider()
        normalized_symbol = provider._normalize_hk_symbol(symbol)

        #Try to get real-time lines from kshare
        try:
            #🔥 to protect the AKShare API call (prevents and leads to closure)
            #Policy:
            #1. Attempt to obtain locks (up to 60 seconds)
            #2. Check whether the cache has been updated by other threads after the lock has been retrieved
            #3. Direct if cache is valid; otherwise call API

            thread_id = threading.current_thread().name
            logger.info(f"[Akshare Locks]{thread_id}Try to get the lock...")

            #Try to get the lock and wait up to 60 seconds
            lock_acquired = _akshare_hk_spot_lock.acquire(timeout=60)

            if not lock_acquired:
                #Timeout, return error
                logger.error(f"[Akshare Locks]{thread_id}:: Obtain lock timeout (60 seconds), relinquish")
                raise Exception("AKShare API 调用超时（其他线程占用）")

            try:
                logger.info(f"[Akshare Locks]{thread_id} Retrieved lock")

                #Check if the cache has been updated by other threads after accessing the lock
                now = datetime.now()
                cache = _akshare_hk_spot_cache

                if cache['data'] is not None and cache['timestamp'] is not None:
                    elapsed = (now - cache['timestamp']).total_seconds()
                    if elapsed <= cache['ttl']:
                        #Cache is effective (possibly other threads have just been updated)
                        logger.info(f"[Akshare Cache]{thread_id}Use of cache data (){elapsed:.1f}2 seconds ago, possibly updated by another thread)")
                        df = cache['data']
                    else:
                        #Cache expired. Call API required
                        logger.info(f"[Akshare Cache]{thread_id}Cache expired (%){elapsed:.1f}Second) , Call API Refresh")
                        df = ak.stock_hk_spot()
                        cache['data'] = df
                        cache['timestamp'] = now
                        logger.info(f"[Akshare Cache]{thread_id}Cached{len(df)}Port-only data")
                else:
                    #Cache empty, first call
                    logger.info(f"[Akshare Cache]{thread_id}First-time acquisition of port unit data")
                    df = ak.stock_hk_spot()
                    cache['data'] = df
                    cache['timestamp'] = now
                    logger.info(f"[Akshare Cache]{thread_id}Cached{len(df)}Port-only data")

            finally:
                #Release the lock.
                _akshare_hk_spot_lock.release()
                logger.info(f"[Akshare Locks]{thread_id}Locks released")

            #Find target stocks from cache data
            if df is not None and not df.empty:
                matched = df[df['代码'] == normalized_symbol]
                if not matched.empty:
                    row = matched.iloc[0]

                    #Auxiliary function: safe conversion value
                    def safe_float(value):
                        try:
                            if value is None or value == '' or (isinstance(value, float) and value != value):  # NaN check
                                return None
                            return float(value)
                        except:
                            return None

                    def safe_int(value):
                        try:
                            if value is None or value == '' or (isinstance(value, float) and value != value):  # NaN check
                                return None
                            return int(value)
                        except:
                            return None

                    return {
                        'symbol': symbol,
                        'name': row['中文名称'],  #New Wave Interface Listing
                        'price': safe_float(row.get('最新价')),
                        'open': safe_float(row.get('今开')),
                        'high': safe_float(row.get('最高')),
                        'low': safe_float(row.get('最低')),
                        'volume': safe_int(row.get('成交量')),
                        'change_percent': safe_float(row.get('涨跌幅')),
                        'currency': 'HKD',
                        'exchange': 'HKG',
                        'market': '港股',
                        'source': 'akshare_sina'
                    }
        except Exception as e:
            logger.debug(f"📊 [Hong Kong Unit AKShare-New Wave]{e}")

        #If failed, return basic information
        return {
            'symbol': symbol,
            'name': f'港股{normalized_symbol}',
            'currency': 'HKD',
            'exchange': 'HKG',
            'market': '港股',
            'source': 'akshare_fallback'
        }

    except Exception as e:
        logger.error(f"❌ [Hong Kong Unit AKshare - New Wave]{e}")
        return {
            'symbol': symbol,
            'name': f'港股{symbol}',
            'currency': 'HKD',
            'exchange': 'HKG',
            'market': '港股',
            'source': 'error',
            'error': str(e)
        }
