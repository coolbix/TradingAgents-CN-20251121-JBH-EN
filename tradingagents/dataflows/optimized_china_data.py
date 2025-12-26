#!/usr/bin/env python3
"""Optimized data acquisition tool for Unit A
Integrated cache strategy and Tushare data interface to improve data acquisition efficiency
"""

import os
import time
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from typing import Optional, Dict, Any
from .cache import get_cache
from tradingagents.config.config_manager import config_manager

from tradingagents.config.runtime_settings import get_float, get_timezone_name
#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')

#Import MongoDB cache adapter
from .cache.mongodb_cache_adapter import get_mongodb_cache_adapter, get_stock_data_with_fallback, get_financial_data_with_fallback


class OptimizedChinaDataProvider:
    """Optimized A unit data provider - integrated cache and Tushare data interface"""

    def __init__(self):
        self.cache = get_cache()
        self.config = config_manager.load_settings()
        self.last_api_call = 0
        self.min_api_interval = get_float("TA_CHINA_MIN_API_INTERVAL_SECONDS", "ta_china_min_api_interval_seconds", 0.5)

        logger.info(f"Optimizing the initialization of the data provider for Unit A")

    def _wait_for_rate_limit(self):
        """Waiting for API Limit"""
        current_time = time.time()
        time_since_last_call = current_time - self.last_api_call

        if time_since_last_call < self.min_api_interval:
            wait_time = self.min_api_interval - time_since_last_call
            time.sleep(wait_time)

        self.last_api_call = time.time()

    def _format_financial_data_to_fundamentals(self, financial_data: Dict[str, Any], symbol: str) -> str:
        """Convert MongoDB financial data into basic face analysis format"""
        try:
            #Extracting key financial indicators
            revenue = financial_data.get('total_revenue', 'N/A')
            net_profit = financial_data.get('net_profit', 'N/A')
            total_assets = financial_data.get('total_assets', 'N/A')
            total_equity = financial_data.get('total_equity', 'N/A')
            report_period = financial_data.get('report_period', 'N/A')

            #Formatting value (add thousands if numbers, otherwise show original values)
            def format_number(value):
                if isinstance(value, (int, float)):
                    return f"{value:,.2f}"
                return str(value)

            revenue_str = format_number(revenue)
            net_profit_str = format_number(net_profit)
            total_assets_str = format_number(total_assets)
            total_equity_str = format_number(total_equity)

            #Calculation of financial ratios
            roe = 'N/A'
            if isinstance(net_profit, (int, float)) and isinstance(total_equity, (int, float)) and total_equity != 0:
                roe = f"{(net_profit / total_equity * 100):.2f}%"

            roa = 'N/A'
            if isinstance(net_profit, (int, float)) and isinstance(total_assets, (int, float)) and total_assets != 0:
                roa = f"{(net_profit / total_assets * 100):.2f}%"

            #Format Output
            fundamentals_report = f"""
# {symbol} 基本面数据分析

## 📊 财务概况
- **报告期**: {report_period}
- **营业收入**: {revenue_str} 元
- **净利润**: {net_profit_str} 元
- **总资产**: {total_assets_str} 元
- **股东权益**: {total_equity_str} 元

## 📈 财务比率
- **净资产收益率(ROE)**: {roe}
- **总资产收益率(ROA)**: {roa}

## 📝 数据说明
- 数据来源: MongoDB财务数据库
- 更新时间: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y-%m-%d %H:%M:%S')}
- 数据类型: 同步财务数据
"""
            return fundamentals_report.strip()

        except Exception as e:
            logger.warning(f"Financial data formatted failed:{e}")
            return f"# {symbol} 基本面数据\n\n❌ 数据格式化失败: {str(e)}"

    def get_stock_data(self, symbol: str, start_date: str, end_date: str,
                      force_refresh: bool = False) -> str:
        """Get A-unit data - Prioritize Cache

Args:
symbol: stock code (6-digit)
Start date: Start date (YYYYY-MM-DD)
End date: End Date (YYYYY-MM-DD)
source refresh: whether to forcibly refresh the cache

Returns:
Formatted stock data string
"""
        logger.info(f"For unit A data:{symbol} ({start_date}Present.{end_date})")

        #1. Preferably try to get it from MongoDB (if TA USE APP CACHE is enabled)
        if not force_refresh:
            adapter = get_mongodb_cache_adapter()
            if adapter.use_app_cache:
                df = adapter.get_historical_data(symbol, start_date, end_date)
                if df is not None and not df.empty:
                    logger.info(f"[Data source: MongoDB]{symbol} ({len(df)}(on file)")
                    return df.to_string()

        #2. Check file caches (unless mandatory updating)
        if not force_refresh:
            cache_key = self.cache.find_cached_stock_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                data_source="unified"  #Harmonization of data sources (Tushare/AKshare/BaoStock)
            )

            if cache_key:
                cached_data = self.cache.load_stock_data(cache_key)
                if cached_data:
                    logger.info(f"⚡ [Data Source: File Cache] Loads Unit A data from the cache:{symbol}")
                    return cached_data

        #Cache pending, retrieve from UDI
        logger.info(f"🌐 [Data source: API call]{symbol}")

        try:
            #API restricted processing
            self._wait_for_rate_limit()

            #Call the unified data source interface (default Tushare to support backup data sources)
            from .data_source_manager import get_china_stock_data_unified

            formatted_data = get_china_stock_data_unified(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )

            #Check for success
            if "❌" in formatted_data or "错误" in formatted_data:
                logger.error(f"Data source API call failed:{symbol}")
                #Try fetching data from old caches
                old_cache = self._try_get_old_cache(symbol, start_date, end_date)
                if old_cache:
                    logger.info(f"[Data source: expired cache]{symbol}")
                    return old_cache

                #Generate backup data
                logger.warning(f"⚠️ [data source: backup data] Generate secondary data:{symbol}")
                return self._generate_fallback_data(symbol, start_date, end_date, "数据源API调用失败")

            #Save to Cache
            self.cache.save_stock_data(
                symbol=symbol,
                data=formatted_data,
                start_date=start_date,
                end_date=end_date,
                data_source="unified"  #Use of harmonized data source identifiers
            )

            logger.info(f"✅ [Data source: API call successfully] Unit A data acquisition success:{symbol}")
            return formatted_data

        except Exception as e:
            error_msg = f"Tushare数据接口调用异常: {str(e)}"
            logger.error(f"❌ {error_msg}")

            #Try fetching data from old caches
            old_cache = self._try_get_old_cache(symbol, start_date, end_date)
            if old_cache:
                logger.info(f"Use of expired cache data:{symbol}")
                return old_cache

            #Generate backup data
            return self._generate_fallback_data(symbol, start_date, end_date, error_msg)

    def get_fundamentals_data(self, symbol: str, force_refresh: bool = False) -> str:
        """Get A Basic Data - Prioritize Cache

Args:
symbol: stock code
source refresh: whether to forcibly refresh the cache

Returns:
Formatting Basic Data Strings
"""
        logger.info(f"For basic data on Unit A:{symbol}")

        #1. Prioritize attempts to obtain financial data from MongoDB (if TA USE APP CACHE is enabled)
        if not force_refresh:
            adapter = get_mongodb_cache_adapter()
            if adapter.use_app_cache:
                financial_data = adapter.get_financial_data(symbol)
                if financial_data:
                    logger.info(f"Using MongoDB financial data:{symbol}")
                    #Conversion of financial data into basic face analysis format
                    return self._format_financial_data_to_fundamentals(financial_data, symbol)

        #2. Check file caches (unless mandatory updating)
        if not force_refresh:
            #Find Basic Data Cache
            for metadata_file in self.cache.metadata_dir.glob(f"*_meta.json"):
                try:
                    import json
                    with open(metadata_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)

                    if (metadata.get('symbol') == symbol and
                        metadata.get('data_type') == 'fundamentals' and
                        metadata.get('market_type') == 'china'):

                        cache_key = metadata_file.stem.replace('_meta', '')
                        if self.cache.is_cache_valid(cache_key, symbol=symbol, data_type='fundamentals'):
                            cached_data = self.cache.load_stock_data(cache_key)
                            if cached_data:
                                logger.info(f"⚡ [Data Source: File Cache] Loads Basic A Stock Data from Cache:{symbol}")
                                return cached_data
                except Exception:
                    continue

        #Cache uncut, generate basic face analysis
        logger.debug(f"🔍 [Data Source: Generating Analysis] Generating Basic Analysis of Unit A:{symbol}")

        try:
            #Basic analysis only requires basic information, not complete historical transaction data
            #Access to basic stock information (name of company, current price, etc.)
            stock_basic_info = self._get_stock_basic_info_only(symbol)

            #Generate basic analysis reports
            fundamentals_data = self._generate_fundamentals_report(symbol, stock_basic_info)

            #Save to Cache
            self.cache.save_fundamentals_data(
                symbol=symbol,
                fundamentals_data=fundamentals_data,
                data_source="unified_analysis"  #Harmonization of data source analysis
            )

            logger.info(f"✅ [Data Source: Generating Analysis Success] Unit A fundamental data generation success:{symbol}")
            return fundamentals_data

        except Exception as e:
            error_msg = f"基本面数据生成失败: {str(e)}"
            logger.error(f"❌{error_msg}")
            logger.warning(f"⚠️ [Data source: backup data] Generate secondary base data:{symbol}")
            return self._generate_fallback_fundamentals(symbol, error_msg)

    def _get_stock_basic_info_only(self, symbol: str) -> str:
        """Access to basic stock information (for basic face analysis only)
No historical transaction data obtained, only basic information such as company name, current price, etc.
"""
        logger.debug(f" [basic optimization]{symbol}Basic information (excluding historical data)")

        try:
            #Obtain basic stock information from a unified interface
            from .interface import get_china_stock_info_unified
            stock_info = get_china_stock_info_unified(symbol)

            #If successful, directly return basic information
            if stock_info and "股票名称:" in stock_info:
                logger.debug(f"📊 [BASIC PERFECT]{symbol}Basic information, without historical data")
                return stock_info

            #If access to basic information fails, try to obtain the most basic information from the cache
            try:
                from tradingagents.config.runtime_settings import use_app_cache_enabled
                if use_app_cache_enabled(False):
                    from .cache.app_adapter import get_market_quote_dataframe
                    df_q = get_market_quote_dataframe(symbol)
                    if df_q is not None and not df_q.empty:
                        row_q = df_q.iloc[-1]
                        current_price = str(row_q.get('close', 'N/A'))
                        change_pct = f"{float(row_q.get('pct_chg', 0)):+.2f}%" if row_q.get('pct_chg') is not None else 'N/A'
                        volume = str(row_q.get('volume', 'N/A'))

                        #Construct Basic Information Format
                        basic_info = f"""股票代码: {symbol}
股票名称: 未知公司
当前价格: {current_price}
涨跌幅: {change_pct}
成交量: {volume}"""
                        logger.debug(f"📊 [Basic Surface Optimization] from Cache Construction{symbol}Basic information")
                        return basic_info
            except Exception as e:
                logger.debug(f"📊 [basic optimization] Failed to access basic information from cache:{e}")

            #If you fail, return the most basic information.
            return f"股票代码: {symbol}\n股票名称: 未知公司\n当前价格: N/A\n涨跌幅: N/A\n成交量: N/A"

        except Exception as e:
            logger.warning(f"⚠️ [basic optimization]{symbol}Could not close temporary folder: %s{e}")
            return f"股票代码: {symbol}\n股票名称: 未知公司\n当前价格: N/A\n涨跌幅: N/A\n成交量: N/A"

    def _generate_fundamentals_report(self, symbol: str, stock_data: str, analysis_modules: str = "standard") -> str:
        """Generate real fundamental analysis based on equity data

Args:
symbol: stock code
Stock data: Stock data
Analysis modules: Analysis module level
"""

        #Add detailed stock code tracking log
        logger.debug(f"🔍 [Securities Code Tracking]  generate fundamentals report received stock codes: '{symbol}' (type:{type(symbol)})")
        logger.debug(f"[Equal code tracking]{len(str(symbol))}")
        logger.debug(f"[Equal code tracking]{list(str(symbol))}")
        logger.debug(f"[Equal code tracking]{stock_data[:200] if stock_data else 'None'}")

        #Extracting information from stock data
        company_name = "未知公司"
        current_price = "N/A"
        volume = "N/A"
        change_pct = "N/A"

        #First try to get basic stock information from a unified interface
        try:
            logger.debug(f"[Equal code tracking]{symbol}Basic information...")
            from .interface import get_china_stock_info_unified
            stock_info = get_china_stock_info_unified(symbol)
            logger.debug(f"[Equal code tracking]{stock_info}")

            if "股票名称:" in stock_info:
                lines = stock_info.split('\n')
                for line in lines:
                    if "股票名称:" in line:
                        company_name = line.split(':')[1].strip()
                        logger.debug(f"[Equal code tracking]{company_name}")
                        break
        except Exception as e:
            logger.warning(f"⚠️ failed to access basic stock information:{e}")

        #If the current price/fall/offset is still missing and the app cache is enabled, read the market quotes pocket
        try:
            if (current_price == "N/A" or change_pct == "N/A" or volume == "N/A"):
                from tradingagents.config.runtime_settings import use_app_cache_enabled  # type: ignore
                if use_app_cache_enabled(False):
                    from .cache.app_adapter import get_market_quote_dataframe
                    df_q = get_market_quote_dataframe(symbol)
                    if df_q is not None and not df_q.empty:
                        row_q = df_q.iloc[-1]
                        if current_price == "N/A" and row_q.get('close') is not None:
                            current_price = str(row_q.get('close'))
                            logger.debug(f"[Equal code tracking]{current_price}")
                        if change_pct == "N/A" and row_q.get('pct_chg') is not None:
                            try:
                                change_pct = f"{float(row_q.get('pct_chg')):+.2f}%"
                            except Exception:
                                change_pct = str(row_q.get('pct_chg'))
                            logger.debug(f"[Equal code tracking]{change_pct}")
                        if volume == "N/A" and row_q.get('volume') is not None:
                            volume = str(row_q.get('volume'))
                            logger.debug(f"[Share code tracking]{volume}")
        except Exception as _qe:
            logger.debug(f"🔍 [Securities Code Tracks] Reading market quotes failed (negative):{_qe}")

        #And then extract price information from stock data.
        if "股票名称:" in stock_data:
            lines = stock_data.split('\n')
            for line in lines:
                if "股票名称:" in line and company_name == "未知公司":
                    company_name = line.split(':')[1].strip()
                elif "当前价格:" in line:
                    current_price = line.split(':')[1].strip()
                elif "最新价格:" in line or "💰 最新价格:" in line:
                    #Compatible with another template output
                    try:
                        current_price = line.split(':', 1)[1].strip().lstrip('¥').strip()
                    except Exception:
                        current_price = line.split(':')[-1].strip()
                elif "涨跌幅:" in line:
                    change_pct = line.split(':')[1].strip()
                elif "成交量:" in line:
                    volume = line.split(':')[1].strip()

        #Try to extract up-to-date price information from stock data tables
        if current_price == "N/A" and stock_data:
            try:
                lines = stock_data.split('\n')
                for i, line in enumerate(lines):
                    if "最新数据:" in line and i + 1 < len(lines):
                        #Find Data Lines
                        for j in range(i + 1, min(i + 5, len(lines))):
                            data_line = lines[j].strip()
                            if data_line and not data_line.startswith('日期') and not data_line.startswith('-'):
                                #Try parsing data lines
                                parts = data_line.split()
                                if len(parts) >= 4:
                                    try:
                                        #Assumptions format: Date, stock code, opening, closing, highest, lowest exchange, turnover...
                                        current_price = parts[3]  #Discount price
                                        logger.debug(f"[Equal code tracking]{current_price}")
                                        break
                                    except (IndexError, ValueError):
                                        continue
                        break
            except Exception as e:
                logger.debug(f"[Equal code tracking]{e}")

        #Profession and basic information based on stock code
        logger.debug(f"[Securities code tracking]{symbol}'")
        industry_info = self._get_industry_info(symbol)
        logger.debug(f"Get industry info returns:{industry_info}")

        #Try to obtain financial indicators and return the simplified basic report if it fails
        logger.debug(f"[Securities Code Tracking]{symbol}'")
        try:
            financial_estimates = self._estimate_financial_metrics(symbol, current_price)
            logger.debug(f"[Stock code tracking]{financial_estimates}")
        except Exception as e:
            logger.warning(f"Financial indicators are not available:{e}")
            logger.info(f"📊 [basic analysis] returns the simplified basic report (no financial indicators)")

            #Returns simplified base reports (excluding financial indicators)
            simplified_report = f"""# 中国A股基本面分析报告 - {symbol} (简化版)

## 📊 基本信息
- **股票代码**: {symbol}
- **公司名称**: {company_name}
- **所属行业**: {industry_info.get('industry', '未知')}
- **当前价格**: {current_price}
- **涨跌幅**: {change_pct}
- **成交量**: {volume}

## 📈 行业分析
{industry_info.get('analysis', '暂无行业分析')}

## ⚠️ 数据说明
由于无法获取完整的财务数据，本报告仅包含基本价格信息和行业分析。
建议：
1. 查看公司最新财报获取详细财务数据
2. 关注行业整体走势
3. 结合技术分析进行综合判断

---
**生成时间**: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y-%m-%d %H:%M:%S')}
**数据来源**: 基础市场数据
"""
            return simplified_report.strip()

        logger.debug(f"[Equal code tracking]{symbol}'")

        #Check data sources and generate instructions
        data_source_note = ""
        data_source = financial_estimates.get('data_source', '')

        if any("（估算值）" in str(v) for v in financial_estimates.values() if isinstance(v, str)):
            data_source_note = "\n⚠️ **数据说明**: 部分财务指标为估算值，建议结合最新财报数据进行分析"
        elif data_source == "AKShare":
            data_source_note = "\n✅ **数据说明**: 财务指标基于AKShare真实财务数据计算"
        elif data_source == "Tushare":
            data_source_note = "\n✅ **数据说明**: 财务指标基于Tushare真实财务数据计算"
        else:
            data_source_note = "\n✅ **数据说明**: 财务指标基于真实财务数据计算"

        #Align the content of the report to the analytical module level
        logger.debug(f"Use of analytical module levels:{analysis_modules}")
        
        if analysis_modules == "basic":
            #Foundation model: core financial indicators only
            report = f"""# 中国A股基本面分析报告 - {symbol} (基础版)

## 📊 股票基本信息
- **股票代码**: {symbol}
- **股票名称**: {company_name}
- **当前股价**: {current_price}
- **涨跌幅**: {change_pct}
- **分析日期**: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y年%m月%d日')}{data_source_note}

## 💰 核心财务指标
- **总市值**: {financial_estimates.get('total_mv', 'N/A')}
- **市盈率(PE)**: {financial_estimates.get('pe', 'N/A')}
- **市盈率TTM(PE_TTM)**: {financial_estimates.get('pe_ttm', 'N/A')}
- **市净率(PB)**: {financial_estimates.get('pb', 'N/A')}
- **净资产收益率(ROE)**: {financial_estimates.get('roe', 'N/A')}
- **资产负债率**: {financial_estimates.get('debt_ratio', 'N/A')}

## 💡 基础评估
- **基本面评分**: {financial_estimates['fundamental_score']}/10
- **风险等级**: {financial_estimates['risk_level']}

---
**重要声明**: 本报告基于公开数据和模型估算生成，仅供参考，不构成投资建议。
**数据来源**: {data_source if data_source else "多源数据"}数据接口
**生成时间**: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y-%m-%d %H:%M:%S')}
"""
        elif analysis_modules in ["standard", "full"]:
            #Standard/complete model: including detailed analysis
            report = f"""# 中国A股基本面分析报告 - {symbol}

## 📊 股票基本信息
- **股票代码**: {symbol}
- **股票名称**: {company_name}
- **所属行业**: {industry_info['industry']}
- **市场板块**: {industry_info['market']}
- **当前股价**: {current_price}
- **涨跌幅**: {change_pct}
- **成交量**: {volume}
- **分析日期**: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y年%m月%d日')}{data_source_note}

## 💰 财务数据分析

### 估值指标
- **总市值**: {financial_estimates.get('total_mv', 'N/A')}
- **市盈率(PE)**: {financial_estimates.get('pe', 'N/A')}
- **市盈率TTM(PE_TTM)**: {financial_estimates.get('pe_ttm', 'N/A')}
- **市净率(PB)**: {financial_estimates.get('pb', 'N/A')}
- **市销率(PS)**: {financial_estimates.get('ps', 'N/A')}
- **股息收益率**: {financial_estimates.get('dividend_yield', 'N/A')}

### 盈利能力指标
- **净资产收益率(ROE)**: {financial_estimates['roe']}
- **总资产收益率(ROA)**: {financial_estimates['roa']}
- **毛利率**: {financial_estimates['gross_margin']}
- **净利率**: {financial_estimates['net_margin']}

### 财务健康度
- **资产负债率**: {financial_estimates['debt_ratio']}
- **流动比率**: {financial_estimates['current_ratio']}
- **速动比率**: {financial_estimates['quick_ratio']}
- **现金比率**: {financial_estimates['cash_ratio']}

## 📈 行业分析
{industry_info['analysis']}

## 🎯 投资价值评估
### 估值水平分析
{self._analyze_valuation(financial_estimates)}

### 成长性分析
{self._analyze_growth_potential(symbol, industry_info)}

## 💡 投资建议
- **基本面评分**: {financial_estimates['fundamental_score']}/10
- **估值吸引力**: {financial_estimates['valuation_score']}/10
- **成长潜力**: {financial_estimates['growth_score']}/10
- **风险等级**: {financial_estimates['risk_level']}

{self._generate_investment_advice(financial_estimates, industry_info)}

---
**重要声明**: 本报告基于公开数据和模型估算生成，仅供参考，不构成投资建议。
**数据来源**: {data_source if data_source else "多源数据"}数据接口
**生成时间**: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y-%m-%d %H:%M:%S')}
"""
        else:  # detailed, comprehensive
            #Detailed/comprehensive model: including the most complete analysis
            report = f"""# 中国A股基本面分析报告 - {symbol} (全面版)

## 📊 股票基本信息
- **股票代码**: {symbol}
- **股票名称**: {company_name}
- **所属行业**: {industry_info['industry']}
- **市场板块**: {industry_info['market']}
- **当前股价**: {current_price}
- **涨跌幅**: {change_pct}
- **成交量**: {volume}
- **分析日期**: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y年%m月%d日')}{data_source_note}

## 💰 财务数据分析

### 估值指标
- **总市值**: {financial_estimates.get('total_mv', 'N/A')}
- **市盈率(PE)**: {financial_estimates.get('pe', 'N/A')}
- **市盈率TTM(PE_TTM)**: {financial_estimates.get('pe_ttm', 'N/A')}
- **市净率(PB)**: {financial_estimates.get('pb', 'N/A')}
- **市销率(PS)**: {financial_estimates.get('ps', 'N/A')}
- **股息收益率**: {financial_estimates.get('dividend_yield', 'N/A')}

### 盈利能力指标
- **净资产收益率(ROE)**: {financial_estimates.get('roe', 'N/A')}
- **总资产收益率(ROA)**: {financial_estimates.get('roa', 'N/A')}
- **毛利率**: {financial_estimates.get('gross_margin', 'N/A')}
- **净利率**: {financial_estimates.get('net_margin', 'N/A')}

### 财务健康度
- **资产负债率**: {financial_estimates['debt_ratio']}
- **流动比率**: {financial_estimates['current_ratio']}
- **速动比率**: {financial_estimates['quick_ratio']}
- **现金比率**: {financial_estimates['cash_ratio']}

## 📈 行业分析

### 行业地位
{industry_info['analysis']}

### 竞争优势
- **市场份额**: {industry_info['market_share']}
- **品牌价值**: {industry_info['brand_value']}
- **技术优势**: {industry_info['tech_advantage']}

## 🎯 投资价值评估

### 估值水平分析
{self._analyze_valuation(financial_estimates)}

### 成长性分析
{self._analyze_growth_potential(symbol, industry_info)}

### 风险评估
{self._analyze_risks(symbol, financial_estimates, industry_info)}

## 💡 投资建议

### 综合评分
- **基本面评分**: {financial_estimates['fundamental_score']}/10
- **估值吸引力**: {financial_estimates['valuation_score']}/10
- **成长潜力**: {financial_estimates['growth_score']}/10
- **风险等级**: {financial_estimates['risk_level']}

### 操作建议
{self._generate_investment_advice(financial_estimates, industry_info)}

### 绝对估值
- **DCF估值**：基于现金流贴现的内在价值
- **资产价值**：净资产重估价值
- **分红收益率**：股息回报分析

## 风险分析
### 系统性风险
- **宏观经济风险**：经济周期对公司的影响
- **政策风险**：行业政策变化的影响
- **市场风险**：股市波动对估值的影响

### 非系统性风险
- **经营风险**：公司特有的经营风险
- **财务风险**：债务结构和偿债能力风险
- **管理风险**：管理层变动和决策风险

## 投资建议
### 综合评价
基于以上分析，该股票的投资价值评估：

**优势：**
- A股市场上市公司，监管相对完善
- 具备一定的市场地位和品牌价值
- 财务信息透明度较高

**风险：**
- 需要关注宏观经济环境变化
- 行业竞争加剧的影响
- 政策调整对业务的潜在影响

### 操作建议
- **投资策略**：建议采用价值投资策略，关注长期基本面
- **仓位建议**：根据风险承受能力合理配置仓位
- **关注指标**：重点关注ROE、PE、现金流等核心指标

---
**重要声明**: 本报告基于公开数据和模型估算生成，仅供参考，不构成投资建议。
实际投资决策请结合最新财报数据和专业分析师意见。

**数据来源**: {data_source if data_source else "多源数据"}数据接口 + 基本面分析模型
**生成时间**: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y-%m-%d %H:%M:%S')}
"""

        return report

    def _get_industry_info(self, symbol: str) -> dict:
        """Access to industry information according to the stock code (prioritize the use of real database data)"""

        #Add detailed stock code tracking log
        logger.debug(f"🔍 [Securities Code Tracking]  get indistry info received stock codes: '{symbol}' (type:{type(symbol)})")
        logger.debug(f"[Equal code tracking]{len(str(symbol))}")
        logger.debug(f"[Equal code tracking]{list(str(symbol))}")

        #First try to get real industry information from the database.
        try:
            from .cache.app_adapter import get_basics_from_cache
            doc = get_basics_from_cache(symbol)
            if doc:
                #Record key fields only and avoid printing complete documents
                logger.debug(f"[Equal code tracking]{doc.get('code')}, name={doc.get('name')}, industry={doc.get('industry')}")

                #Regulating the industry and the plate (avoiding miscalculation of the value of the board, such as the "Small/Starboard" sector)
                board_labels = {'主板', '中小板', '创业板', '科创板'}
                raw_industry = (doc.get('industry') or doc.get('industry_name') or '').strip()
                sec_or_cat = (doc.get('sec') or doc.get('category') or '').strip()
                market_val = (doc.get('market') or '').strip()
                industry_val = raw_industry or sec_or_cat or '未知'

                #If the industry field is a plate name, it is used as a market; industry is changed to a more detailed classification (sec/category)
                if raw_industry in board_labels:
                    if not market_val:
                        market_val = raw_industry
                    if sec_or_cat:
                        industry_val = sec_or_cat
                    logger.debug(f"🔧{raw_industry}♪ Industry ♪{industry_val}', market/board ='{market_val}'")

                #Build industry information
                info = {
                    "industry": industry_val or '未知',
                    "market": market_val or doc.get('market', '未知'),
                    "type": self._get_market_type_by_code(symbol)
                }

                logger.debug(f"[Equal code tracking]{info}")

                #Add detailed analysis of special shares
                if symbol in self._get_special_stocks():
                    info.update(self._get_special_stocks()[symbol])
                else:
                    info.update({
                        "analysis": f"该股票属于{info['industry']}行业，在{info['market']}上市交易。",
                        "market_share": "待分析",
                        "brand_value": "待评估",
                        "tech_advantage": "待分析"
                    })

                return info

        except Exception as e:
            logger.warning(f"Access to industry information from databases failed:{e}")

        #Alternative scenario: use of code prefix (but modified industry/market map)
        logger.debug(f"[Equal code tracking]")
        code_prefix = symbol[:3]
        logger.debug(f"[Equal code tracking]{code_prefix}'")

        #Revised Map: Distinguishing Industry from Market Blocks
        market_map = {
            "000": {"market": "主板", "exchange": "深圳证券交易所", "type": "综合"},
            "001": {"market": "主板", "exchange": "深圳证券交易所", "type": "综合"},
            "002": {"market": "主板", "exchange": "深圳证券交易所", "type": "成长型"},  #002 is the main panel now.
            "003": {"market": "创业板", "exchange": "深圳证券交易所", "type": "创新型"},
            "300": {"market": "创业板", "exchange": "深圳证券交易所", "type": "高科技"},
            "600": {"market": "主板", "exchange": "上海证券交易所", "type": "大盘蓝筹"},
            "601": {"market": "主板", "exchange": "上海证券交易所", "type": "大盘蓝筹"},
            "603": {"market": "主板", "exchange": "上海证券交易所", "type": "中小盘"},
            "688": {"market": "科创板", "exchange": "上海证券交易所", "type": "科技创新"},
        }

        market_info = market_map.get(code_prefix, {
            "market": "未知市场",
            "exchange": "未知交易所",
            "type": "综合"
        })

        info = {
            "industry": "未知",  #It's not possible to determine the exact industry from the prefix.
            "market": market_info["market"],
            "type": market_info["type"]
        }

        #Details on special stocks
        special_stocks = self._get_special_stocks()
        if symbol in special_stocks:
            info.update(special_stocks[symbol])
        else:
            info.update({
                "analysis": f"该股票在{info['market']}上市交易，具体行业信息需要进一步查询。",
                "market_share": "待分析",
                "brand_value": "待评估",
                "tech_advantage": "待分析"
            })

        return info

    def _get_market_type_by_code(self, symbol: str) -> str:
        """Market type by stock code"""
        code_prefix = symbol[:3]
        type_map = {
            "000": "综合", "001": "综合", "002": "成长型", "003": "创新型",
            "300": "高科技", "600": "大盘蓝筹", "601": "大盘蓝筹",
            "603": "中小盘", "688": "科技创新"
        }
        return type_map.get(code_prefix, "综合")

    def _get_special_stocks(self) -> dict:
        """Get details on special stocks"""
        return {
            "000001": {
                "industry": "银行业",
                "analysis": "平安银行是中国领先的股份制商业银行，在零售银行业务方面具有显著优势。",
                "market_share": "股份制银行前列",
                "brand_value": "知名金融品牌",
                "tech_advantage": "金融科技创新领先"
            },
            "600036": {
                "industry": "银行业",
                "analysis": "招商银行是中国优质的股份制银行，零售银行业务和财富管理业务领先。",
                "market_share": "股份制银行龙头",
                "brand_value": "优质银行品牌",
                "tech_advantage": "数字化银行先锋"
            },
            "000002": {
                "industry": "房地产",
                "analysis": "万科A是中国房地产行业龙头企业，在住宅开发领域具有领先地位。",
                "market_share": "房地产行业前三",
                "brand_value": "知名地产品牌",
                "tech_advantage": "绿色建筑技术"
            },
            "002475": {
                "industry": "元器件",
                "analysis": "立讯精密是全球领先的精密制造服务商，主要从事连接器、声学、无线充电等产品的研发制造。",
                "market_share": "消费电子连接器龙头",
                "brand_value": "精密制造知名品牌",
                "tech_advantage": "精密制造技术领先"
            }
        }

    def _estimate_financial_metrics(self, symbol: str, current_price: str) -> dict:
        """Obtaining real financial indicators (from MongoDB, AKshare, Tushare and failure to release anomalies)"""

        #Extract price value
        try:
            price_value = float(current_price.replace('¥', '').replace(',', ''))
        except:
            price_value = 10.0  #Default value

        #Trying to get real financial data
        real_metrics = self._get_real_financial_metrics(symbol, price_value)
        if real_metrics:
            logger.info(f"Using real financial data:{symbol}")
            return real_metrics

        #If you can't get real data, throw out the anomaly.
        error_msg = f"无法获取股票 {symbol} 的财务数据。已尝试所有数据源（MongoDB、AKShare、Tushare）均失败。"
        logger.error(f"❌ {error_msg}")
        raise ValueError(error_msg)

    def _get_real_financial_metrics(self, symbol: str, price_value: float) -> dict:
        """Getting real financial indicators - Prioritize database caches to use API"""
        try:
            #🔥 Prioritize real-time stock prices from market quotes to replace imported price value
            from tradingagents.config.database_manager import get_database_manager
            db_manager = get_database_manager()
            db_client = None

            if db_manager.is_mongodb_available():
                try:
                    db_client = db_manager.get_mongodb_client()
                    db = db_client['tradingagents']

                    #Standardised stock code is six.
                    code6 = symbol.replace('.SH', '').replace('.SZ', '').zfill(6)

                    #Get real-time share price from market quotes
                    quote = db.market_quotes.find_one({"code": code6})
                    if quote and quote.get("close"):
                        realtime_price = float(quote.get("close"))
                        logger.info(f"Get real-time stock prices from market quotes:{code6} = {realtime_price}(original price:{price_value}Dollars)")
                        price_value = realtime_price
                    else:
                        logger.info(f"Unfinished in market quotes{code6}real-time share price, using input price:{price_value}Dollar")
                except Exception as e:
                    logger.warning(f"@⚠️ > Failed to get real-time stock prices from market quotes:{e}, using imported prices:{price_value}Dollar")
            else:
                logger.info(f"MongoDB is not available, using input prices:{price_value}Dollar")

            #First priority: Obtain standardized financial data from the MongoDB stock financial data collection
            from tradingagents.config.runtime_settings import use_app_cache_enabled
            if use_app_cache_enabled(False):
                logger.info(f"Priority from MongoDB stock financial data{symbol}Financial data")

                #Obtain standardized financial data directly from MongoDB
                from tradingagents.dataflows.cache.mongodb_cache_adapter import get_mongodb_cache_adapter
                adapter = get_mongodb_cache_adapter()
                financial_data = adapter.get_financial_data(symbol)

                if financial_data:
                    logger.info(f"[Financial data]{symbol}Financial data")
                    #Parsing MongoDB standardized financial data
                    metrics = self._parse_mongodb_financial_data(financial_data, price_value)
                    if metrics:
                        logger.info(f"✅MongoDB Financial Data Analysis Success, Return Indicator")
                        return metrics
                    else:
                        logger.warning(f"MongoDB financial data analysis failed")
                else:
                    logger.info(f"MongoDB not found{symbol}Financial data, try to get from AKShare API")
            else:
                logger.info(f"The database cache 🔄 is not enabled and is obtained directly from AKShare API{symbol}Financial data")

            #Second priority: from Akshare API
            from .providers.china.akshare import get_akshare_provider
            import asyncio

            akshare_provider = get_akshare_provider()

            if akshare_provider.connected:
                #AKShare's Get financial data is an anisyncio.
                loop = asyncio.get_event_loop()
                financial_data = loop.run_until_complete(akshare_provider.get_financial_data(symbol))

                if financial_data and any(not v.empty if hasattr(v, 'empty') else bool(v) for v in financial_data.values()):
                    logger.info(f"AKShare's financial data were obtained successfully:{symbol}")
                    #Access to basic information on stocks (also a stifling method)
                    stock_info = loop.run_until_complete(akshare_provider.get_stock_basic_info(symbol))

                    #Parsing AKShare Financial Data
                    logger.debug(f"Call AKShare parsing function, share price:{price_value}")
                    metrics = self._parse_akshare_financial_data(financial_data, stock_info, price_value)
                    logger.debug(f"AKShare's analysis:{metrics}")
                    if metrics:
                        logger.info(f"AKShare's successfully deciphered and returned.")
                        #Cache raw financial data to the database (rather than decomposition indicators)
                        self._cache_raw_financial_data(symbol, financial_data, stock_info)
                        return metrics
                    else:
                        logger.warning(f"AKShare's resolution failed, returning to the net")
                else:
                    logger.warning(f"AKShare is not available.{symbol}Financial data, try Tushare")
            else:
                logger.warning(f"AKShare is not connected. Try Tushare")

            #Third priority: Use Tushare data source
            logger.info(f"🔄 with Tushare backup data source{symbol}Financial data")
            from .providers.china.tushare import get_tushare_provider
            import asyncio

            provider = get_tushare_provider()
            if not provider.connected:
                logger.debug(f"Tushare is not connected, not available{symbol}Real financial data")
                return None

            #Access to financial data (a different approach)
            loop = asyncio.get_event_loop()
            financial_data = loop.run_until_complete(provider.get_financial_data(symbol))
            if not financial_data:
                logger.debug(f"Not accessed{symbol}Financial data")
                return None

            #Access to basic information on equities (speech method)
            stock_info = loop.run_until_complete(provider.get_stock_basic_info(symbol))

            #Analysis of Tushare financial data
            metrics = self._parse_financial_data(financial_data, stock_info, price_value)
            if metrics:
                #Cache raw financial data to database
                self._cache_raw_financial_data(symbol, financial_data, stock_info)
                return metrics

        except Exception as e:
            logger.debug(f"Access{symbol}Real financial data failed:{e}")

        return None

    def _parse_mongodb_financial_data(self, financial_data: dict, price_value: float) -> dict:
        """Analysis of MongoDB standardized financial data as indicators"""
        try:
            logger.debug(f"📊 [financial data] Commence the analysis of MongoDB financial data, including fields:{list(financial_data.keys())}")

            metrics = {}

            #MongoDB's financial data is a flat structure that directly includes all financial indicators
            #No longer embedded   FT 0 structure

            #Draw indicator directly from financial data
            latest_indicators = financial_data

            #ROE - Rate of return on net assets (addition range validation)
            roe = latest_indicators.get('roe') or latest_indicators.get('roe_waa')
            if roe is not None and str(roe) != 'nan' and roe != '--':
                try:
                    roe_val = float(roe)
                    #ROE is usually between -100% and -100%, and extremes may exceed
                    if -200 <= roe_val <= 200:
                        metrics["roe"] = f"{roe_val:.1f}%"
                    else:
                        logger.warning(f"ROE data anomaly:{roe_val}, beyond reasonable range [200%, 200%], set to N/A")
                        metrics["roe"] = "N/A"
                except (ValueError, TypeError):
                    metrics["roe"] = "N/A"
            else:
                metrics["roe"] = "N/A"

            #ROA - Total Asset Rates of Return (addition range validation)
            roa = latest_indicators.get('roa') or latest_indicators.get('roa2')
            if roa is not None and str(roa) != 'nan' and roa != '--':
                try:
                    roa_val = float(roa)
                    #ROA is usually between -50 and 50%
                    if -100 <= roa_val <= 100:
                        metrics["roa"] = f"{roa_val:.1f}%"
                    else:
                        logger.warning(f"ROA data anomaly:{roa_val}, beyond reasonable range [-100%, 100%] set to N/A")
                        metrics["roa"] = "N/A"
                except (ValueError, TypeError):
                    metrics["roa"] = "N/A"
            else:
                metrics["roa"] = "N/A"

            #Māori Rate - Add Range Validation
            gross_margin = latest_indicators.get('gross_margin')
            if gross_margin is not None and str(gross_margin) != 'nan' and gross_margin != '--':
                try:
                    gross_margin_val = float(gross_margin)
                    #Validation range: Māori rates should range from -100% to -100%
                    #If out of scope, it could be a data error (e.g. stored in absolute amounts rather than percentages)
                    if -100 <= gross_margin_val <= 100:
                        metrics["gross_margin"] = f"{gross_margin_val:.1f}%"
                    else:
                        logger.warning(f"Māori ratio data anomaly:{gross_margin_val}, beyond reasonable range [-100%, 100%] set to N/A")
                        metrics["gross_margin"] = "N/A"
                except (ValueError, TypeError):
                    metrics["gross_margin"] = "N/A"
            else:
                metrics["gross_margin"] = "N/A"

            #Net interest rate - Add range authentication
            net_margin = latest_indicators.get('netprofit_margin')
            if net_margin is not None and str(net_margin) != 'nan' and net_margin != '--':
                try:
                    net_margin_val = float(net_margin)
                    #Validation range: Net interest rate should be between -100% and -100%
                    if -100 <= net_margin_val <= 100:
                        metrics["net_margin"] = f"{net_margin_val:.1f}%"
                    else:
                        logger.warning(f"Net interest rate data anomalies:{net_margin_val}, beyond reasonable range [-100%, 100%] set to N/A")
                        metrics["net_margin"] = "N/A"
                except (ValueError, TypeError):
                    metrics["net_margin"] = "N/A"
            else:
                metrics["net_margin"] = "N/A"

            #Calculate PE/PB - Prefer real-time calculations, downgrade to static data
            #Fetch both PE and PE TTM indicators
            pe_value = None
            pe_ttm_value = None
            pb_value = None
            is_loss_stock = False  #Whether or not the tag is a loss unit

            try:
                #Prioritize real-time calculations
                from tradingagents.dataflows.realtime_metrics import get_pe_pb_with_fallback
                from tradingagents.config.database_manager import get_database_manager

                db_manager = get_database_manager()
                if db_manager.is_mongodb_available():
                    client = db_manager.get_mongodb_client()
                    #Extract stock code from symbol
                    stock_code = latest_indicators.get('code') or latest_indicators.get('symbol', '').replace('.SZ', '').replace('.SH', '')

                    logger.info(f"[PE Calculating]{stock_code}PE/PB")

                    if stock_code:
                        logger.info(f"📊 [PE Calculator - 1st Floor]{stock_code})")

                        #Access real time PE/PB
                        realtime_metrics = get_pe_pb_with_fallback(stock_code, client)

                        if realtime_metrics:
                            #Obtain market value data (prioritize saving)
                            market_cap = realtime_metrics.get('market_cap')
                            if market_cap is not None and market_cap > 0:
                                is_realtime = realtime_metrics.get('is_realtime', False)
                                realtime_tag = " (实时)" if is_realtime else ""
                                metrics["total_mv"] = f"{market_cap:.2f}亿元{realtime_tag}"
                                logger.info(f"✅ [total market value obtained successfully]{market_cap:.2f}Billion dollars.{is_realtime}")

                            #Use real-time PE (dynamic gain-over)
                            pe_value = realtime_metrics.get('pe')
                            if pe_value is not None and pe_value > 0:
                                is_realtime = realtime_metrics.get('is_realtime', False)
                                realtime_tag = " (实时)" if is_realtime else ""
                                metrics["pe"] = f"{pe_value:.1f}倍{realtime_tag}"

                                #Detailed Log
                                price = realtime_metrics.get('price', 'N/A')
                                market_cap_log = realtime_metrics.get('market_cap', 'N/A')
                                source = realtime_metrics.get('source', 'unknown')
                                updated_at = realtime_metrics.get('updated_at', 'N/A')

                                logger.info(f"[PE Calculator - 1st Floor Success]{pe_value:.2f}Source:{source}= Real time={is_realtime}")
                                logger.info(f"└ Calculated: Share ={price}dollar, market value ={market_cap_log}Billion dollars, update time ={updated_at}")
                            elif pe_value is None:
                                #PE is None. Check if it's a loss.
                                pe_ttm_check = latest_indicators.get('pe_ttm')
                                #Pe ttm is None, < = 0, 'nan', '-' which is considered to be a loss.
                                if pe_ttm_check is None or pe_ttm_check <= 0 or str(pe_ttm_check) == 'nan' or pe_ttm_check == '--':
                                    is_loss_stock = True
                                    logger.info(f"⚠️ [PE Calculates - 1st Floor]{pe_ttm_check}, recognized as a loss unit")

                            #Use real time PE TTM
                            pe_ttm_value = realtime_metrics.get('pe_ttm')
                            if pe_ttm_value is not None and pe_ttm_value > 0:
                                is_realtime = realtime_metrics.get('is_realtime', False)
                                realtime_tag = " (实时)" if is_realtime else ""
                                metrics["pe_ttm"] = f"{pe_ttm_value:.1f}倍{realtime_tag}"
                                logger.info(f"[PE TTM Calculating - Level 1 Success]{pe_ttm_value:.2f}Source:{source}= Real time={is_realtime}")
                            elif pe_ttm_value is None and not is_loss_stock:
                                #PE TTM is None.
                                pe_ttm_check = latest_indicators.get('pe_ttm')
                                #Pe ttm is None, < = 0, 'nan', '-' which is considered to be a loss.
                                if pe_ttm_check is None or pe_ttm_check <= 0 or str(pe_ttm_check) == 'nan' or pe_ttm_check == '--':
                                    is_loss_stock = True
                                    logger.info(f"[PE TTM Calculating - 1st Floor]{pe_ttm_check}, recognized as a loss unit")

                            #Use Real Time PB
                            pb_value = realtime_metrics.get('pb')
                            if pb_value is not None and pb_value > 0:
                                is_realtime = realtime_metrics.get('is_realtime', False)
                                realtime_tag = " (实时)" if is_realtime else ""
                                metrics["pb"] = f"{pb_value:.2f}倍{realtime_tag}"
                                logger.info(f"[PB Calculator - 1st Floor Success]{pb_value:.2f}Source:{realtime_metrics.get('source')}= Real time={is_realtime}")
                        else:
                            #🔥Check if the loss has led to the return of None
                            #Get p tm from stock basic info to determine loss
                            pe_ttm_static = latest_indicators.get('pe_ttm')
                            #Pe ttm is None, < = 0, 'nan', '-' which is considered to be a loss.
                            if pe_ttm_static is None or pe_ttm_static <= 0 or str(pe_ttm_static) == 'nan' or pe_ttm_static == '--':
                                is_loss_stock = True
                                logger.info(f"[PE Calculator - Failed Level 1]{pe_ttm_static}) Skip downgrade calculations")
                            else:
                                logger.warning(f"⚠️ [PE Calculator-Failure 1st Layer]")

            except Exception as e:
                logger.warning(f"The real-time calculation failed:{e}will try to downgrade")

            #If real-time calculations fail, try to get total market value from late indicators
            if "total_mv" not in metrics:
                logger.info(f"📊 [total market value -- 2nd floor]")
                total_mv_static = latest_indicators.get('total_mv')
                if total_mv_static is not None and total_mv_static > 0:
                    metrics["total_mv"] = f"{total_mv_static:.2f}亿元"
                    logger.info(f"✅ [total market value -- 2nd floor success]{total_mv_static:.2f}Billion dollars (source: stock basic info)")
                else:
                    #Try to calculate from money cap
                    money_cap = latest_indicators.get('money_cap')
                    if money_cap is not None and money_cap > 0:
                        total_mv_yi = money_cap / 10000
                        metrics["total_mv"] = f"{total_mv_yi:.2f}亿元"
                        logger.info(f"✅ [total market value - 3rd floor success]{total_mv_yi:.2f}Billion dollars (converted from money cap)")
                    else:
                        metrics["total_mv"] = "N/A"
                        logger.warning(f"No data on total market value available")

            #If real-time calculations fail, try traditional calculations
            if pe_value is None:
                #If a loss is confirmed, set PE as N/A and no attempt to downgrade
                if is_loss_stock:
                    metrics["pe"] = "N/A"
                    logger.info(f"⚠️ [PE Calculating-Deficit Unit] recognized as a loss unit, set to N/A, skips the 2nd floor calculation")
                else:
                    logger.info(f"📊 [PE Calculates - 2nd Floor]")

                    net_profit = latest_indicators.get('net_profit')

                    #🔥 Critical repairs: check for positive net profits (losses do not account for PE)
                    if net_profit and net_profit > 0:
                        try:
                            #Calculation of PE using market value/net profit
                            money_cap = latest_indicators.get('money_cap')
                            if money_cap and money_cap > 0:
                                pe_calculated = money_cap / net_profit
                                metrics["pe"] = f"{pe_calculated:.1f}倍"
                                logger.info(f"[PE Calculator - 2nd Floor Success]{pe_calculated:.2f}Double")
                                logger.info(f"└ formula: market value{money_cap}Ten thousand dollars) / Net profit (%){net_profit}(In thousands of dollars)")
                            else:
                                logger.warning(f"Market value is invalid:{money_cap}Try the third floor.")

                                #Decline 3rd Layer: Directly use the pe field in the last indicators (in positive numbers only)
                                pe_static = latest_indicators.get('pe')
                                if pe_static is not None and str(pe_static) != 'nan' and pe_static != '--':
                                    try:
                                        pe_float = float(pe_static)
                                        #Only positive PEs
                                        if pe_float > 0:
                                            metrics["pe"] = f"{pe_float:.1f}倍"
                                            logger.info(f"✅ [PE Calculating - 3rd Floor Success]{metrics['pe']}")
                                            logger.info(f"Data source: block basic info.pe")
                                        else:
                                            metrics["pe"] = "N/A"
                                            logger.info(f"⚠️ [PE Calculates - 3rd Floor Skips] Static PE is negative or zero (losses):{pe_float}")
                                    except (ValueError, TypeError):
                                        metrics["pe"] = "N/A"
                                        logger.error(f"❌ [PE Calculator-Failure Level 3] Static PE format error:{pe_static}")
                                else:
                                    metrics["pe"] = "N/A"
                                    logger.error(f"No PE data available")
                        except (ValueError, TypeError, ZeroDivisionError) as e:
                            metrics["pe"] = "N/A"
                            logger.error(f"The calculation failed:{e}")
                    elif net_profit and net_profit < 0:
                        #Loss Unit: PE set to N/A
                        metrics["pe"] = "N/A"
                        logger.info(f"The net profit is negative.{net_profit}Ten thousand dollars)")
                    else:
                        logger.warning(f"[PE Calculating - 2nd Floor Skipping]{net_profit}Try the third floor.")

                        #Decline 3rd Layer: Directly use the pe field in the last indicators (in positive numbers only)
                        pe_static = latest_indicators.get('pe')
                        if pe_static is not None and str(pe_static) != 'nan' and pe_static != '--':
                            try:
                                pe_float = float(pe_static)
                                #Only positive PEs
                                if pe_float > 0:
                                    metrics["pe"] = f"{pe_float:.1f}倍"
                                    logger.info(f"✅ [PE Calculating - 3rd Floor Success]{metrics['pe']}")
                                    logger.info(f"Data source: block basic info.pe")
                                else:
                                    metrics["pe"] = "N/A"
                                    logger.info(f"⚠️ [PE Calculates - 3rd Floor Skips] Static PE is negative or zero (losses):{pe_float}")
                            except (ValueError, TypeError):
                                metrics["pe"] = "N/A"
                                logger.error(f"❌ [PE Calculator-Failure Level 3] Static PE format error:{pe_static}")
                        else:
                            metrics["pe"] = "N/A"
                            logger.error(f"No PE data available")

            #If PE TTM is not available, try to get from static data
            if pe_ttm_value is None:
                #If a loss is confirmed, set PE TTM as N/A
                if is_loss_stock:
                    metrics["pe_ttm"] = "N/A"
                    logger.info(f"⚠️ [PE TTM Calculated-Deficit Unit] recognized as a loss unit and PE TTM set to N/A")
                else:
                    logger.info(f"[PE TTM Calculating - Level 2]")
                    pe_ttm_static = latest_indicators.get('pe_ttm')
                    if pe_ttm_static is not None and str(pe_ttm_static) != 'nan' and pe_ttm_static != '--':
                        try:
                            pe_ttm_float = float(pe_ttm_static)
                            #Only positive PE TTM is accepted.
                            if pe_ttm_float > 0:
                                metrics["pe_ttm"] = f"{pe_ttm_float:.1f}倍"
                                logger.info(f"Use static PE TTM:{metrics['pe_ttm']}")
                                logger.info(f"└ - Data source: stock basic info.pe ttm")
                            else:
                                metrics["pe_ttm"] = "N/A"
                                logger.info(f"⚠️ [PE TTM Calculating - 2nd Floor Skipping] Static PE TTM is negative or zero (losses):{pe_ttm_float}")
                        except (ValueError, TypeError):
                            metrics["pe_ttm"] = "N/A"
                            logger.error(f"❌ [PE TTM Calculator - Failed Level 2] Static PE TTM format error:{pe_ttm_static}")
                    else:
                        metrics["pe_ttm"] = "N/A"
                        logger.warning(f"No PE TTM data available")

            if pb_value is None:
                total_equity = latest_indicators.get('total_hldr_eqy_exc_min_int')
                if total_equity and total_equity > 0:
                    try:
                        #Calculation of PB using market value/net assets
                        money_cap = latest_indicators.get('money_cap')
                        if money_cap and money_cap > 0:
                            #Note unit conversion: money cap is ten thousand dollars, total equity is one dollar
                            #PB = market value (millions of dollars) * 10000 / Net assets (dollars)
                            pb_calculated = (money_cap * 10000) / total_equity
                            metrics["pb"] = f"{pb_calculated:.2f}倍"
                            logger.info(f"[PB Calculator - 2nd Floor Success]{pb_calculated:.2f}Double")
                            logger.info(f"└ formula: Market value{money_cap}* 100 000 / Net assets{total_equity}Dollar ={metrics['pb']}")
                        else:
                            #Decline 3rd Layer: Directly use the pb field in last indicators
                            pb_static = latest_indicators.get('pb') or latest_indicators.get('pb_mrq')
                            if pb_static is not None and str(pb_static) != 'nan' and pb_static != '--':
                                try:
                                    metrics["pb"] = f"{float(pb_static):.2f}倍"
                                    logger.info(f"✅ [PB Calculator - 3rd Level Success]{metrics['pb']}")
                                    logger.info(f"└ - Data source: stock basic info.pb")
                                except (ValueError, TypeError):
                                    metrics["pb"] = "N/A"
                            else:
                                metrics["pb"] = "N/A"
                    except (ValueError, TypeError, ZeroDivisionError) as e:
                        logger.error(f"The calculation failed:{e}")
                        metrics["pb"] = "N/A"
                else:
                    #Decline 3rd Layer: Directly use the pb field in last indicators
                    pb_static = latest_indicators.get('pb') or latest_indicators.get('pb_mrq')
                    if pb_static is not None and str(pb_static) != 'nan' and pb_static != '--':
                        try:
                            metrics["pb"] = f"{float(pb_static):.2f}倍"
                            logger.info(f"✅ [PB Calculator - 3rd Level Success]{metrics['pb']}")
                            logger.info(f"└ - Data source: stock basic info.pb")
                        except (ValueError, TypeError):
                            metrics["pb"] = "N/A"
                    else:
                        metrics["pb"] = "N/A"

            #Assets and liabilities ratio
            debt_ratio = latest_indicators.get('debt_to_assets')
            if debt_ratio is not None and str(debt_ratio) != 'nan' and debt_ratio != '--':
                try:
                    metrics["debt_ratio"] = f"{float(debt_ratio):.1f}%"
                except (ValueError, TypeError):
                    metrics["debt_ratio"] = "N/A"
            else:
                metrics["debt_ratio"] = "N/A"

            #Calculation of PS - marketing rate (using TTM operating income)
            #Prioritize TTM operating income or, if not, single-stage operating income
            revenue_ttm = latest_indicators.get('revenue_ttm')
            revenue = latest_indicators.get('revenue')

            #Select which business income data to use
            revenue_for_ps = revenue_ttm if revenue_ttm and revenue_ttm > 0 else revenue
            revenue_type = "TTM" if revenue_ttm and revenue_ttm > 0 else "单期"

            if revenue_for_ps and revenue_for_ps > 0:
                try:
                    #Calculate PS using market value/business income
                    money_cap = latest_indicators.get('money_cap')
                    if money_cap and money_cap > 0:
                        ps_calculated = money_cap / revenue_for_ps
                        metrics["ps"] = f"{ps_calculated:.2f}倍"
                        logger.debug(f"Compute PS (✅){revenue_type}Market value{money_cap}Ten thousand dollars / operating income{revenue_for_ps}Ten thousand dollars ={metrics['ps']}")
                    else:
                        metrics["ps"] = "N/A"
                except (ValueError, TypeError, ZeroDivisionError):
                    metrics["ps"] = "N/A"
            else:
                metrics["ps"] = "N/A"

            #Dividend rate of return - provisional N/A, required dividends data
            metrics["dividend_yield"] = "N/A"
            metrics["current_ratio"] = latest_indicators.get('current_ratio', 'N/A')
            metrics["quick_ratio"] = latest_indicators.get('quick_ratio', 'N/A')
            metrics["cash_ratio"] = latest_indicators.get('cash_ratio', 'N/A')

            #Add scoring fields (using default values)
            metrics["fundamental_score"] = 7.0  #Default rating based on real data
            metrics["valuation_score"] = 6.5
            metrics["growth_score"] = 7.0
            metrics["risk_level"] = "中等"

            logger.info(f"MongoDB Financial Data Analysis Success: ROE={metrics.get('roe')}, ROA={metrics.get('roa')}, Māori rate ={metrics.get('gross_margin')}, net interest rate ={metrics.get('net_margin')}")
            return metrics

        except Exception as e:
            logger.error(f"The analysis of MongoDB's financial data failed:{e}", exc_info=True)
            return None

    def _parse_akshare_financial_data(self, financial_data: dict, stock_info: dict, price_value: float) -> dict:
        """Analysis of AKShare financial data as indicator"""
        try:
            #Access to up-to-date financial data
            balance_sheet = financial_data.get('balance_sheet', [])
            income_statement = financial_data.get('income_statement', [])
            cash_flow = financial_data.get('cash_flow', [])
            main_indicators = financial_data.get('main_indicators')

            #Main indicators may be the result of DataFrame or list (to dicts)
            if main_indicators is None:
                logger.warning("AKShare ' s main financial indicators are empty")
                return None

            #Check if empty
            if isinstance(main_indicators, list):
                if not main_indicators:
                    logger.warning("AKShare list of key financial indicators is empty")
                    return None
                #List format: [  FMT 0 ,...]
                #Convert to DataFrame for uniform processing
                import pandas as pd
                main_indicators = pd.DataFrame(main_indicators)
            elif hasattr(main_indicators, 'empty') and main_indicators.empty:
                logger.warning("DataFrame, the main financial indicator for AKShare, is empty.")
                return None

            #Main indicators is DataFrame, which needs to be converted to dictionary format for easy search
            #Get the latest data column (column 3, index 2)
            latest_col = main_indicators.columns[2] if len(main_indicators.columns) > 2 else None
            if not latest_col:
                logger.warning("Lack of data columns for AKShare key financial indicators")
                return None

            logger.info(f"While using the latest data from AKShare:{latest_col}")

            #Create map of indicator name to value
            indicators_dict = {}
            for _, row in main_indicators.iterrows():
                indicator_name = row['指标']
                value = row[latest_col]
                indicators_dict[indicator_name] = value

            logger.debug(f"Number of key financial indicators for AKshare:{len(indicators_dict)}")

            #Calculation of financial indicators
            metrics = {}

            #🔥 Preferably try to use real-time PE/PB calculations (in line with MongoDB resolution)
            pe_value = None
            pe_ttm_value = None
            pb_value = None

            try:
                #Get stock code
                stock_code = stock_info.get('code', '').replace('.SH', '').replace('.SZ', '').zfill(6)
                if stock_code:
                    logger.info(f"📊 [AKShare-PE Calculates - 1st Floor]{stock_code}")

                    from tradingagents.config.database_manager import get_database_manager
                    from tradingagents.dataflows.realtime_metrics import get_pe_pb_with_fallback

                    db_manager = get_database_manager()
                    if db_manager.is_mongodb_available():
                        client = db_manager.get_mongodb_client()

                        #Access real time PE/PB
                        realtime_metrics = get_pe_pb_with_fallback(stock_code, client)

                        if realtime_metrics:
                            #Acquisition of total market value
                            market_cap = realtime_metrics.get('market_cap')
                            if market_cap is not None and market_cap > 0:
                                is_realtime = realtime_metrics.get('is_realtime', False)
                                realtime_tag = " (实时)" if is_realtime else ""
                                metrics["total_mv"] = f"{market_cap:.2f}亿元{realtime_tag}"
                                logger.info(f"[AKShare - Total Market Value Successful]{market_cap:.2f}Billion dollars.{is_realtime}")

                            #Use Real Time PE
                            pe_value = realtime_metrics.get('pe')
                            if pe_value is not None and pe_value > 0:
                                is_realtime = realtime_metrics.get('is_realtime', False)
                                realtime_tag = " (实时)" if is_realtime else ""
                                metrics["pe"] = f"{pe_value:.1f}倍{realtime_tag}"
                                logger.info(f"[Akshare-PE Calculator - Success Level 1]{pe_value:.2f}Source:{realtime_metrics.get('source')}= Real time={is_realtime}")

                            #Use real time PE TTM
                            pe_ttm_value = realtime_metrics.get('pe_ttm')
                            if pe_ttm_value is not None and pe_ttm_value > 0:
                                is_realtime = realtime_metrics.get('is_realtime', False)
                                realtime_tag = " (实时)" if is_realtime else ""
                                metrics["pe_ttm"] = f"{pe_ttm_value:.1f}倍{realtime_tag}"
                                logger.info(f"PE TTM={pe_ttm_value:.2f}Double")

                            #Use Real Time PB
                            pb_value = realtime_metrics.get('pb')
                            if pb_value is not None and pb_value > 0:
                                is_realtime = realtime_metrics.get('is_realtime', False)
                                realtime_tag = " (实时)" if is_realtime else ""
                                metrics["pb"] = f"{pb_value:.2f}倍{realtime_tag}"
                                logger.info(f"[AKshare-PB Calculator-Face 1 Success]{pb_value:.2f}Double")
                        else:
                            logger.warning(f"⚠️ [AKShare-PE Calculator - Failed 1st Floor] Real-time calculations of returns to empty results will attempt to downgrade")
            except Exception as e:
                logger.warning(f"The real-time calculation failed:{e}will try to downgrade")

            #Get ROE - Get directly from the indicator
            roe_value = indicators_dict.get('净资产收益率(ROE)')
            if roe_value is not None and str(roe_value) != 'nan' and roe_value != '--':
                try:
                    roe_val = float(roe_value)
                    #ROE is usually in percentage form.
                    metrics["roe"] = f"{roe_val:.1f}%"
                    logger.debug(f"Get ROE:{metrics['roe']}")
                except (ValueError, TypeError):
                    metrics["roe"] = "N/A"
            else:
                metrics["roe"] = "N/A"

            #Try to obtain total market value from stock info if real-time calculations fail
            if "total_mv" not in metrics:
                logger.info(f"Try to get it from stock info")
                total_mv_static = stock_info.get('total_mv')
                if total_mv_static is not None and total_mv_static > 0:
                    metrics["total_mv"] = f"{total_mv_static:.2f}亿元"
                    logger.info(f"✅ [AKShare - Total Market Value - 2nd Floor Success]{total_mv_static:.2f}Billions.")
                else:
                    metrics["total_mv"] = "N/A"
                    logger.warning(f"⚠️ [AKShare - Total Market Value - All Failed] No data on total market value available")

            #If real-time calculations fail, downgrade to traditional calculations
            if pe_value is None:
                logger.info(f"📊 [AKShare-PE Calculates - 2nd Floor]")

                #Calculate PE - Prefer TTM data
                #Try to calculate TTM EPS frommain indicators DataFrame
                ttm_eps = None
                try:
                    #Main indicators is DataFrame with multiple periods of data
                    #Try to calculate TTM EPS
                    if '基本每股收益' in main_indicators['指标'].values:
                        #All period data extracted from basic per share of proceeds
                        eps_row = main_indicators[main_indicators['指标'] == '基本每股收益']
                        if not eps_row.empty:
                            #Get all value columns (exclusion 'indicator' columns)
                            value_cols = [col for col in eps_row.columns if col != '指标']

                            #Build DataFrame for TTM calculations
                            import pandas as pd
                            eps_data = []
                            for col in value_cols:
                                eps_val = eps_row[col].iloc[0]
                                if eps_val is not None and str(eps_val) != 'nan' and eps_val != '--':
                                    eps_data.append({'报告期': col, '基本每股收益': eps_val})

                            if len(eps_data) >= 2:
                                eps_df = pd.DataFrame(eps_data)
                                #Calculate function using TTM
                                from scripts.sync_financial_data import _calculate_ttm_metric
                                ttm_eps = _calculate_ttm_metric(eps_df, '基本每股收益')
                                if ttm_eps:
                                    logger.info(f"TM EPS:{ttm_eps:.4f}Dollar")
                except Exception as e:
                    logger.debug(f"Could not close temporary folder: %s{e}")

                #Calculate PE using TM EPS or single-stage EPS
                eps_for_pe = ttm_eps if ttm_eps else None
                pe_type = "TTM" if ttm_eps else "单期"

                if not eps_for_pe:
                    #Downgrade to single stage EPS
                    eps_value = indicators_dict.get('基本每股收益')
                    if eps_value is not None and str(eps_value) != 'nan' and eps_value != '--':
                        try:
                            eps_for_pe = float(eps_value)
                        except (ValueError, TypeError):
                            pass

                if eps_for_pe and eps_for_pe > 0:
                    pe_val = price_value / eps_for_pe
                    metrics["pe"] = f"{pe_val:.1f}倍"
                    logger.info(f"✅ [AKshare-PE Calculates - 2nd Floor Success] PE{pe_type}Share price{price_value} / EPS{eps_for_pe:.4f} = {metrics['pe']}")
                elif eps_for_pe and eps_for_pe <= 0:
                    metrics["pe"] = "N/A（亏损）"
                    logger.warning(f"[AKshare-PE Calculator - Failed 2nd Floor]{eps_for_pe}")
                else:
                    metrics["pe"] = "N/A"
                    logger.error(f"No EPS data available")

            #If real-time PB calculations fail, downgrade to the traditional mode of calculation
            if pb_value is None:
                logger.info(f"📊 [AKshare-PB Calculates - 2nd Floor]")

                #Acquisition of net assets per share - used to calculate PB
                bps_value = indicators_dict.get('每股净资产_最新股数')
                if bps_value is not None and str(bps_value) != 'nan' and bps_value != '--':
                    try:
                        bps_val = float(bps_value)
                        if bps_val > 0:
                            #Calculate PB = share price / net assets per share
                            pb_val = price_value / bps_val
                            metrics["pb"] = f"{pb_val:.2f}倍"
                            logger.info(f"PB: Stock price{price_value} / BPS{bps_val} = {metrics['pb']}")
                        else:
                            metrics["pb"] = "N/A"
                            logger.warning(f"BPS is invalid:{bps_val}")
                    except (ValueError, TypeError) as e:
                        metrics["pb"] = "N/A"
                        logger.error(f"[Akshare-PB Calculator - 2nd Level Aberrant]{e}")
                else:
                    metrics["pb"] = "N/A"
                    logger.error(f"No BPS data available")

            #Try to get other indicators
            #Total asset return (ROA)
            roa_value = indicators_dict.get('总资产报酬率')
            if roa_value is not None and str(roa_value) != 'nan' and roa_value != '--':
                try:
                    roa_val = float(roa_value)
                    metrics["roa"] = f"{roa_val:.1f}%"
                except (ValueError, TypeError):
                    metrics["roa"] = "N/A"
            else:
                metrics["roa"] = "N/A"

            #Māori rate
            gross_margin_value = indicators_dict.get('毛利率')
            if gross_margin_value is not None and str(gross_margin_value) != 'nan' and gross_margin_value != '--':
                try:
                    gross_margin_val = float(gross_margin_value)
                    metrics["gross_margin"] = f"{gross_margin_val:.1f}%"
                except (ValueError, TypeError):
                    metrics["gross_margin"] = "N/A"
            else:
                metrics["gross_margin"] = "N/A"

            #Net interest rate on sales
            net_margin_value = indicators_dict.get('销售净利率')
            if net_margin_value is not None and str(net_margin_value) != 'nan' and net_margin_value != '--':
                try:
                    net_margin_val = float(net_margin_value)
                    metrics["net_margin"] = f"{net_margin_val:.1f}%"
                except (ValueError, TypeError):
                    metrics["net_margin"] = "N/A"
            else:
                metrics["net_margin"] = "N/A"

            #Assets and liabilities ratio
            debt_ratio_value = indicators_dict.get('资产负债率')
            if debt_ratio_value is not None and str(debt_ratio_value) != 'nan' and debt_ratio_value != '--':
                try:
                    debt_ratio_val = float(debt_ratio_value)
                    metrics["debt_ratio"] = f"{debt_ratio_val:.1f}%"
                except (ValueError, TypeError):
                    metrics["debt_ratio"] = "N/A"
            else:
                metrics["debt_ratio"] = "N/A"

            #Mobility ratio
            current_ratio_value = indicators_dict.get('流动比率')
            if current_ratio_value is not None and str(current_ratio_value) != 'nan' and current_ratio_value != '--':
                try:
                    current_ratio_val = float(current_ratio_value)
                    metrics["current_ratio"] = f"{current_ratio_val:.2f}"
                except (ValueError, TypeError):
                    metrics["current_ratio"] = "N/A"
            else:
                metrics["current_ratio"] = "N/A"

            #Speed ratio
            quick_ratio_value = indicators_dict.get('速动比率')
            if quick_ratio_value is not None and str(quick_ratio_value) != 'nan' and quick_ratio_value != '--':
                try:
                    quick_ratio_val = float(quick_ratio_value)
                    metrics["quick_ratio"] = f"{quick_ratio_val:.2f}"
                except (ValueError, TypeError):
                    metrics["quick_ratio"] = "N/A"
            else:
                metrics["quick_ratio"] = "N/A"

            #Calculate PS - Marketing Rate (Priority TTM Business Income)
            #Try to calculate TTM operating income from plain indicators DataFrame
            ttm_revenue = None
            try:
                if '营业收入' in main_indicators['指标'].values:
                    revenue_row = main_indicators[main_indicators['指标'] == '营业收入']
                    if not revenue_row.empty:
                        value_cols = [col for col in revenue_row.columns if col != '指标']

                        import pandas as pd
                        revenue_data = []
                        for col in value_cols:
                            rev_val = revenue_row[col].iloc[0]
                            if rev_val is not None and str(rev_val) != 'nan' and rev_val != '--':
                                revenue_data.append({'报告期': col, '营业收入': rev_val})

                        if len(revenue_data) >= 2:
                            revenue_df = pd.DataFrame(revenue_data)
                            from scripts.sync_financial_data import _calculate_ttm_metric
                            ttm_revenue = _calculate_ttm_metric(revenue_df, '营业收入')
                            if ttm_revenue:
                                logger.info(f"✅ Calculates TTM operating income:{ttm_revenue:.2f}Ten thousand dollars.")
            except Exception as e:
                logger.debug(f"Could not close temporary folder: %s{e}")

            #Calculate PS
            revenue_for_ps = ttm_revenue if ttm_revenue else None
            ps_type = "TTM" if ttm_revenue else "单期"

            if not revenue_for_ps:
                #Downgrade to single-stage operating income
                revenue_value = indicators_dict.get('营业收入')
                if revenue_value is not None and str(revenue_value) != 'nan' and revenue_value != '--':
                    try:
                        revenue_for_ps = float(revenue_value)
                    except (ValueError, TypeError):
                        pass

            if revenue_for_ps and revenue_for_ps > 0:
                #Market value of gross equity acquisition
                total_share = stock_info.get('total_share') if stock_info else None
                if total_share and total_share > 0:
                    #Market value (thousands of United States dollars) = gross equity (millions of United States dollars)
                    market_cap = price_value * total_share
                    ps_val = market_cap / revenue_for_ps
                    metrics["ps"] = f"{ps_val:.2f}倍"
                    logger.info(f"Compute PS (✅){ps_type}Market value{market_cap:.2f}Ten thousand dollars / operating income{revenue_for_ps:.2f}Ten thousand dollars ={metrics['ps']}")
                else:
                    metrics["ps"] = "N/A（无总股本数据）"
                    logger.warning(f"Could not calculate PS: Lack of total equity data")
            else:
                metrics["ps"] = "N/A"

            #Default value to complement other indicators
            metrics.update({
                "dividend_yield": "待查询",
                "cash_ratio": "待分析"
            })

            #Rating (simplified rating based on AKShare data)
            fundamental_score = self._calculate_fundamental_score(metrics, stock_info)
            valuation_score = self._calculate_valuation_score(metrics)
            growth_score = self._calculate_growth_score(metrics, stock_info)
            risk_level = self._calculate_risk_level(metrics, stock_info)

            metrics.update({
                "fundamental_score": fundamental_score,
                "valuation_score": valuation_score,
                "growth_score": growth_score,
                "risk_level": risk_level,
                "data_source": "AKShare"
            })

            logger.info(f"AKshare's financial data analysis was successful:{metrics['pe']}, PB={metrics['pb']}, ROE={metrics['roe']}")
            return metrics

        except Exception as e:
            logger.error(f"AKShare's financial data analysis failed:{e}")
            return None

    def _parse_financial_data(self, financial_data: dict, stock_info: dict, price_value: float) -> dict:
        """Parsing financial data as indicators"""
        try:
            #Access to up-to-date financial data
            balance_sheet = financial_data.get('balance_sheet', [])
            income_statement = financial_data.get('income_statement', [])
            cash_flow = financial_data.get('cash_flow', [])

            if not (balance_sheet or income_statement):
                return None

            latest_balance = balance_sheet[0] if balance_sheet else {}
            latest_income = income_statement[0] if income_statement else {}
            latest_cash = cash_flow[0] if cash_flow else {}

            #Calculation of financial indicators
            metrics = {}

            #Basic data
            total_assets = latest_balance.get('total_assets', 0) or 0
            total_liab = latest_balance.get('total_liab', 0) or 0
            total_equity = latest_balance.get('total_hldr_eqy_exc_min_int', 0) or 0

            #Calculation of TTM operating income and net profits
            #Tushare income statement data are cumulative values (from the beginning of the year to the reporting period)
            #Calculate using TTM formulae
            ttm_revenue = None
            ttm_net_income = None

            try:
                if len(income_statement) >= 2:
                    #Preparing data for TTM calculations
                    import pandas as pd

                    #Build Business Income DataFrame
                    revenue_data = []
                    for stmt in income_statement:
                        end_date = stmt.get('end_date')
                        revenue = stmt.get('total_revenue')
                        if end_date and revenue is not None:
                            revenue_data.append({'报告期': str(end_date), '营业收入': float(revenue)})

                    if len(revenue_data) >= 2:
                        revenue_df = pd.DataFrame(revenue_data)
                        from scripts.sync_financial_data import _calculate_ttm_metric
                        ttm_revenue = _calculate_ttm_metric(revenue_df, '营业收入')
                        if ttm_revenue:
                            logger.info(f"Tushare calculates TTM operating income:{ttm_revenue:.2f}Ten thousand dollars.")

                    #Build net profit DataFrame
                    profit_data = []
                    for stmt in income_statement:
                        end_date = stmt.get('end_date')
                        profit = stmt.get('n_income')
                        if end_date and profit is not None:
                            profit_data.append({'报告期': str(end_date), '净利润': float(profit)})

                    if len(profit_data) >= 2:
                        profit_df = pd.DataFrame(profit_data)
                        ttm_net_income = _calculate_ttm_metric(profit_df, '净利润')
                        if ttm_net_income:
                            logger.info(f"Tushare calculates TTM net profit:{ttm_net_income:.2f}Ten thousand dollars.")
            except Exception as e:
                logger.warning(f"Tushare TTM calculation failed:{e}")

            #Downgrade to single-stage data
            total_revenue = ttm_revenue if ttm_revenue else (latest_income.get('total_revenue', 0) or 0)
            net_income = ttm_net_income if ttm_net_income else (latest_income.get('n_income', 0) or 0)
            operate_profit = latest_income.get('operate_profit', 0) or 0

            revenue_type = "TTM" if ttm_revenue else "单期"
            profit_type = "TTM" if ttm_net_income else "单期"

            #Market value for actual gross equity
            #Prefer from stock info, if not accurate valuation indicators cannot be calculated
            total_share = stock_info.get('total_share') if stock_info else None

            if total_share and total_share > 0:
                #Market value (dollars) = share price (dollars) x gross equity (millions) x 10000
                market_cap = price_value * total_share * 10000
                market_cap_yi = market_cap / 100000000  #Convert to Billion Dollars
                metrics["total_mv"] = f"{market_cap_yi:.2f}亿元"
                logger.info(f"[Tushare - total market value calculated successfully]{market_cap_yi:.2f}Billions{price_value}Total equity{total_share}1 000 shares)")
            else:
                logger.error(f"❌ {stock_info.get('code', 'Unknown')}Total equity is not available and accurate valuation indicators cannot be calculated")
                market_cap = None
                metrics["total_mv"] = "N/A"

            #Calculated indicators (only when an accurate market value exists)
            if market_cap:
                #PE ratio (priority for TTM net profit)
                if net_income > 0:
                    pe_ratio = market_cap / (net_income * 10000)  #Convert Unit
                    metrics["pe"] = f"{pe_ratio:.1f}倍"
                    logger.info(f"Tushare Calculating PE{profit_type}Market value{market_cap/100000000:.2f}Billions dollars / net profit{net_income:.2f}Ten thousand dollars ={pe_ratio:.1f}Double")
                else:
                    metrics["pe"] = "N/A（亏损）"

                #PB ratio (net assets using latest available data, relative accuracy)
                if total_equity > 0:
                    pb_ratio = market_cap / (total_equity * 10000)
                    metrics["pb"] = f"{pb_ratio:.2f}倍"
                else:
                    metrics["pb"] = "N/A"

                #PS ratio (priority TTM operating income)
                if total_revenue > 0:
                    ps_ratio = market_cap / (total_revenue * 10000)
                    metrics["ps"] = f"{ps_ratio:.1f}倍"
                    logger.info(f"Tushare Calculating PS(){revenue_type}Market value{market_cap/100000000:.2f}Billion dollars / Business income{total_revenue:.2f}Ten thousand dollars ={ps_ratio:.1f}Double")
                else:
                    metrics["ps"] = "N/A"
            else:
                #Total equity not available, valuation indicator not possible
                metrics["pe"] = "N/A（无总股本数据）"
                metrics["pb"] = "N/A（无总股本数据）"
                metrics["ps"] = "N/A（无总股本数据）"

            # ROE
            if total_equity > 0 and net_income > 0:
                roe = (net_income / total_equity) * 100
                metrics["roe"] = f"{roe:.1f}%"
            else:
                metrics["roe"] = "N/A"

            # ROA
            if total_assets > 0 and net_income > 0:
                roa = (net_income / total_assets) * 100
                metrics["roa"] = f"{roa:.1f}%"
            else:
                metrics["roa"] = "N/A"

            #Net interest rate
            if total_revenue > 0 and net_income > 0:
                net_margin = (net_income / total_revenue) * 100
                metrics["net_margin"] = f"{net_margin:.1f}%"
            else:
                metrics["net_margin"] = "N/A"

            #Assets and liabilities ratio
            if total_assets > 0:
                debt_ratio = (total_liab / total_assets) * 100
                metrics["debt_ratio"] = f"{debt_ratio:.1f}%"
            else:
                metrics["debt_ratio"] = "N/A"

            #Set other indicators as default values
            metrics.update({
                "dividend_yield": "待查询",
                "gross_margin": "待计算",
                "current_ratio": "待计算",
                "quick_ratio": "待计算",
                "cash_ratio": "待分析"
            })

            #Rating (simplified rating based on real data)
            fundamental_score = self._calculate_fundamental_score(metrics, stock_info)
            valuation_score = self._calculate_valuation_score(metrics)
            growth_score = self._calculate_growth_score(metrics, stock_info)
            risk_level = self._calculate_risk_level(metrics, stock_info)

            metrics.update({
                "fundamental_score": fundamental_score,
                "valuation_score": valuation_score,
                "growth_score": growth_score,
                "risk_level": risk_level
            })

            return metrics

        except Exception as e:
            logger.error(f"Can not open message{e}")
            return None

    def _calculate_fundamental_score(self, metrics: dict, stock_info: dict) -> float:
        """Calculate basic profile score"""
        score = 5.0  #Base Score

        #ROE Rating
        roe_str = metrics.get("roe", "N/A")
        if roe_str != "N/A":
            try:
                roe = float(roe_str.replace("%", ""))
                if roe > 15:
                    score += 1.5
                elif roe > 10:
                    score += 1.0
                elif roe > 5:
                    score += 0.5
            except:
                pass

        #Net rate rating
        net_margin_str = metrics.get("net_margin", "N/A")
        if net_margin_str != "N/A":
            try:
                net_margin = float(net_margin_str.replace("%", ""))
                if net_margin > 20:
                    score += 1.0
                elif net_margin > 10:
                    score += 0.5
            except:
                pass

        return min(score, 10.0)

    def _calculate_valuation_score(self, metrics: dict) -> float:
        """Calculation of valuation ratings"""
        score = 5.0  #Base Score

        #PE rating
        pe_str = metrics.get("pe", "N/A")
        if pe_str != "N/A" and "亏损" not in pe_str:
            try:
                pe = float(pe_str.replace("倍", ""))
                if pe < 15:
                    score += 2.0
                elif pe < 25:
                    score += 1.0
                elif pe > 50:
                    score -= 1.0
            except:
                pass

        #PB rating
        pb_str = metrics.get("pb", "N/A")
        if pb_str != "N/A":
            try:
                pb = float(pb_str.replace("倍", ""))
                if pb < 1.5:
                    score += 1.0
                elif pb < 3:
                    score += 0.5
                elif pb > 5:
                    score -= 0.5
            except:
                pass

        return min(max(score, 1.0), 10.0)

    def _calculate_growth_score(self, metrics: dict, stock_info: dict) -> float:
        """Calculate growth scores"""
        score = 6.0  #Base Score

        #Adjustment by industry
        industry = stock_info.get('industry', '')
        if '科技' in industry or '软件' in industry or '互联网' in industry:
            score += 1.0
        elif '银行' in industry or '保险' in industry:
            score -= 0.5

        return min(max(score, 1.0), 10.0)

    def _calculate_risk_level(self, metrics: dict, stock_info: dict) -> str:
        """Calculate risk level"""
        #Assets and liabilities ratio
        debt_ratio_str = metrics.get("debt_ratio", "N/A")
        if debt_ratio_str != "N/A":
            try:
                debt_ratio = float(debt_ratio_str.replace("%", ""))
                if debt_ratio > 70:
                    return "较高"
                elif debt_ratio > 50:
                    return "中等"
                else:
                    return "较低"
            except:
                pass

        #By industry
        industry = stock_info.get('industry', '')
        if '银行' in industry:
            return "中等"
        elif '科技' in industry or '创业板' in industry:
            return "较高"

        return "中等"



    def _analyze_valuation(self, financial_estimates: dict) -> str:
        """Analysis of valuation levels"""
        valuation_score = financial_estimates['valuation_score']

        if valuation_score >= 8:
            return "当前估值水平较为合理，具有一定的投资价值。市盈率和市净率相对较低，安全边际较高。"
        elif valuation_score >= 6:
            return "估值水平适中，需要结合基本面和成长性综合判断投资价值。"
        else:
            return "当前估值偏高，投资需谨慎。建议等待更好的买入时机。"

    def _analyze_growth_potential(self, symbol: str, industry_info: dict) -> str:
        """Analysis of growth potential"""
        if symbol.startswith(('000001', '600036')):
            return "银行业整体增长稳定，受益于经济发展和金融深化。数字化转型和财富管理业务是主要增长点。"
        elif symbol.startswith('300'):
            return "创业板公司通常具有较高的成长潜力，但也伴随着较高的风险。需要关注技术创新和市场拓展能力。"
        else:
            return "成长潜力需要结合具体行业和公司基本面分析。建议关注行业发展趋势和公司竞争优势。"

    def _analyze_risks(self, symbol: str, financial_estimates: dict, industry_info: dict) -> str:
        """Investment risk analysis"""
        risk_level = financial_estimates['risk_level']

        risk_analysis = f"**风险等级**: {risk_level}\n\n"

        if symbol.startswith(('000001', '600036')):
            risk_analysis += """**主要风险**:
- 利率环境变化对净息差的影响
- 信贷资产质量风险
- 监管政策变化风险
- 宏观经济下行对银行业的影响"""
        elif symbol.startswith('300'):
            risk_analysis += """**主要风险**:
- 技术更新换代风险
- 市场竞争加剧风险
- 估值波动较大
- 业绩不确定性较高"""
        else:
            risk_analysis += """**主要风险**:
- 行业周期性风险
- 宏观经济环境变化
- 市场竞争风险
- 政策调整风险"""

        return risk_analysis

    def _generate_investment_advice(self, financial_estimates: dict, industry_info: dict) -> str:
        """Generate investment recommendations"""
        fundamental_score = financial_estimates['fundamental_score']
        valuation_score = financial_estimates['valuation_score']
        growth_score = financial_estimates['growth_score']

        total_score = (fundamental_score + valuation_score + growth_score) / 3

        if total_score >= 7.5:
            return """**投资建议**: 🟢 **买入**
- 基本面良好，估值合理，具有较好的投资价值
- 建议分批建仓，长期持有
- 适合价值投资者和稳健型投资者"""
        elif total_score >= 6.0:
            return """**投资建议**: 🟡 **观望**
- 基本面一般，需要进一步观察
- 可以小仓位试探，等待更好时机
- 适合有经验的投资者"""
        else:
            return """**投资建议**: 🔴 **回避**
- 当前风险较高，不建议投资
- 建议等待基本面改善或估值回落
- 风险承受能力较低的投资者应避免"""

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
                        metadata.get('market_type') == 'china'):

                        cache_key = metadata_file.stem.replace('_meta', '')
                        cached_data = self.cache.load_stock_data(cache_key)
                        if cached_data:
                            return cached_data + "\n\n⚠️ 注意: 使用的是过期缓存数据"
                except Exception:
                    continue
        except Exception:
            pass

        return None

    def _generate_fallback_data(self, symbol: str, start_date: str, end_date: str, error_msg: str) -> str:
        """Generate backup data"""
        return f"""# {symbol} A股数据获取失败

## ❌ 错误信息
{error_msg}

## 📊 模拟数据（仅供演示）
- 股票代码: {symbol}
- 股票名称: 模拟公司
- 数据期间: {start_date} 至 {end_date}
- 模拟价格: ¥{random.uniform(10, 50):.2f}
- 模拟涨跌: {random.uniform(-5, 5):+.2f}%

## ⚠️ 重要提示
由于数据接口限制或网络问题，无法获取实时数据。
建议稍后重试或检查网络连接。

生成时间: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y-%m-%d %H:%M:%S')}
"""

    def _generate_fallback_fundamentals(self, symbol: str, error_msg: str) -> str:
        """Generate backup base surface data"""
        return f"""# {symbol} A股基本面分析失败

## ❌ 错误信息
{error_msg}

## 📊 基本信息
- 股票代码: {symbol}
- 分析状态: 数据获取失败
- 建议: 稍后重试或检查网络连接

生成时间: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y-%m-%d %H:%M:%S')}
"""


#Global Examples
_china_data_provider = None

def get_optimized_china_data_provider() -> OptimizedChinaDataProvider:
    """Example of access to global unit A data provider"""
    global _china_data_provider
    if _china_data_provider is None:
        _china_data_provider = OptimizedChinaDataProvider()
    return _china_data_provider


def get_china_stock_data_cached(symbol: str, start_date: str, end_date: str,
                               force_refresh: bool = False) -> str:
    """An easy function to access A share data

Args:
symbol: stock code (6-digit)
Start date: Start date (YYYYY-MM-DD)
End date: End Date (YYYYY-MM-DD)
source refresh: whether to forcibly refresh the cache

Returns:
Formatted stock data string
"""
    provider = get_optimized_china_data_provider()
    return provider.get_stock_data(symbol, start_date, end_date, force_refresh)


def get_china_fundamentals_cached(symbol: str, force_refresh: bool = False) -> str:
    """An easy function to access fundamental A data

Args:
symbol: stock code (6-digit)
source refresh: whether to forcibly refresh the cache

Returns:
Formatting Basic Data Strings
"""
    provider = get_optimized_china_data_provider()
    return provider.get_fundamentals_data(symbol, force_refresh)


#Add Cache Method to Optimized ChinaDataProvider Category
def _add_financial_cache_methods():
    """Add Financial Data Cache Method to Optimize ChinaDataProvider"""

    def _get_cached_raw_financial_data(self, symbol: str) -> dict:
        """Obtain raw financial data from database cache"""
        try:
            from .cache.app_adapter import get_mongodb_client
            client = get_mongodb_client()
            if not client:
                logger.debug(f"[financial cache] MongoDB client not available")
                return None

            db = client.get_database('tradingagents')

            #First priority: read from stock financial data collection (termination data for scheduled task sync)
            stock_financial_collection = db.stock_financial_data

            #Try symbol or code field query (compatible with different sync services)
            financial_doc = stock_financial_collection.find_one({
                '$or': [
                    {'symbol': symbol},
                    {'code': symbol}
                ]
            }, sort=[('updated_at', -1)])

            if financial_doc:
                logger.info(f"[Financial data]{symbol}Financial data")
                #Convert database documents into financial data formats
                financial_data = {}

                #Extracting various financial data
                #First Priority: Check the Raw data field (structure used by the Tushare Sync Service)
                if 'raw_data' in financial_doc and isinstance(financial_doc['raw_data'], dict):
                    raw_data = financial_doc['raw_data']
                    #Map field name: Raw data uses Cashflow statement, we need Cash Flow
                    if 'balance_sheet' in raw_data and raw_data['balance_sheet']:
                        financial_data['balance_sheet'] = raw_data['balance_sheet']
                    if 'income_statement' in raw_data and raw_data['income_statement']:
                        financial_data['income_statement'] = raw_data['income_statement']
                    if 'cashflow_statement' in raw_data and raw_data['cashflow_statement']:
                        financial_data['cash_flow'] = raw_data['cashflow_statement']  #Note field name map
                    if 'financial_indicators' in raw_data and raw_data['financial_indicators']:
                        financial_data['main_indicators'] = raw_data['financial_indicators']  #Note field name map
                    if 'main_business' in raw_data and raw_data['main_business']:
                        financial_data['main_business'] = raw_data['main_business']

                #Priority 2: Check financial data embedded fields
                elif 'financial_data' in financial_doc and isinstance(financial_doc['financial_data'], dict):
                    nested_data = financial_doc['financial_data']
                    if 'balance_sheet' in nested_data:
                        financial_data['balance_sheet'] = nested_data['balance_sheet']
                    if 'income_statement' in nested_data:
                        financial_data['income_statement'] = nested_data['income_statement']
                    if 'cash_flow' in nested_data:
                        financial_data['cash_flow'] = nested_data['cash_flow']
                    if 'main_indicators' in nested_data:
                        financial_data['main_indicators'] = nested_data['main_indicators']

                #Priority 3: Read directly from the root level of the document
                else:
                    if 'balance_sheet' in financial_doc and financial_doc['balance_sheet']:
                        financial_data['balance_sheet'] = financial_doc['balance_sheet']
                    if 'income_statement' in financial_doc and financial_doc['income_statement']:
                        financial_data['income_statement'] = financial_doc['income_statement']
                    if 'cash_flow' in financial_doc and financial_doc['cash_flow']:
                        financial_data['cash_flow'] = financial_doc['cash_flow']
                    if 'main_indicators' in financial_doc and financial_doc['main_indicators']:
                        financial_data['main_indicators'] = financial_doc['main_indicators']

                if financial_data:
                    logger.info(f"📊 [Financial data] Successful extraction{symbol}, containing fields:{list(financial_data.keys())}")
                    return financial_data
                else:
                    logger.warning(f"[Financial data]{symbol}Stock financial data records exist but no valid financial data fields")
            else:
                logger.debug(f"[Financial data] Stock financial data collection not found{symbol}Records")

            #Second Priority: Read from Financial data cache (temporary cache)
            collection = db.financial_data_cache

            #Search for cached raw financial data
            cache_doc = collection.find_one({
                'symbol': symbol,
                'cache_type': 'raw_financial_data'
            }, sort=[('updated_at', -1)])

            if cache_doc:
                #Check if the cache is expired (24 hours)
                from datetime import datetime, timedelta
                cache_time = cache_doc.get('updated_at')
                if cache_time and datetime.now() - cache_time < timedelta(hours=24):
                    financial_data = cache_doc.get('financial_data', {})
                    if financial_data:
                        logger.info(f"[Financial cache]{symbol}Original financial data")
                        return financial_data
                else:
                    logger.debug(f"[financial cache]{symbol}Original financial data cache expired")
            else:
                logger.debug(f"[financial cache] Not found{symbol}Original financial data cache")

        except Exception as e:
            logger.debug(f"[financial cache]{symbol}Original financial data cache failed:{e}")

        return None

    def _get_cached_stock_info(self, symbol: str) -> dict:
        """Get basic stock information from the database cache"""
        try:
            from .cache.app_adapter import get_mongodb_client
            client = get_mongodb_client()
            if not client:
                return {}

            db = client.get_database('tradingagents')
            collection = db.stock_basic_info

            #Search for Basic Stock Information
            doc = collection.find_one({'code': symbol})
            if doc:
                return {
                    'symbol': symbol,
                    'name': doc.get('name', ''),
                    'industry': doc.get('industry', ''),
                    'market': doc.get('market', ''),
                    'source': 'database_cache'
                }
        except Exception as e:
            logger.debug(f"Access{symbol}Basic information cache failed:{e}")

        return {}

    def _restore_financial_data_format(self, cached_data: dict) -> dict:
        """Restore cached financial data to DataFrame format"""
        try:
            import pandas as pd
            restored_data = {}

            for key, value in cached_data.items():
                if isinstance(value, list) and value:  #If it's in list format,
                    #Convert back to DataFrame
                    restored_data[key] = pd.DataFrame(value)
                else:
                    restored_data[key] = value

            return restored_data
        except Exception as e:
            logger.debug(f"The restoration of the financial data format failed:{e}")
            return cached_data

    def _cache_raw_financial_data(self, symbol: str, financial_data: dict, stock_info: dict):
        """Cache raw financial data to database"""
        try:
            from tradingagents.config.runtime_settings import use_app_cache_enabled
            if not use_app_cache_enabled(False):
                logger.debug(f"📊 [Financial Cache] Apply cache not enabled, skip cache save")
                return

            from .cache.app_adapter import get_mongodb_client
            client = get_mongodb_client()
            if not client:
                logger.debug(f"[financial cache] MongoDB client not available")
                return

            db = client.get_database('tradingagents')
            collection = db.financial_data_cache

            from datetime import datetime

            #Convert DataFrame into a sequenced format
            serializable_data = {}
            for key, value in financial_data.items():
                if hasattr(value, 'to_dict'):  # pandas DataFrame
                    serializable_data[key] = value.to_dict('records')
                else:
                    serializable_data[key] = value

            cache_doc = {
                'symbol': symbol,
                'cache_type': 'raw_financial_data',
                'financial_data': serializable_data,
                'stock_info': stock_info,
                'updated_at': datetime.now()
            }

            #Update or insert withupsert
            collection.replace_one(
                {'symbol': symbol, 'cache_type': 'raw_financial_data'},
                cache_doc,
                upsert=True
            )

            logger.info(f"[financial cache]{symbol}Original financial data cached to data Library")

        except Exception as e:
            logger.debug(f"[financial cache]{symbol}Original financial data failed:{e}")

    #Add method to class
    OptimizedChinaDataProvider._get_cached_raw_financial_data = _get_cached_raw_financial_data
    OptimizedChinaDataProvider._get_cached_stock_info = _get_cached_stock_info
    OptimizedChinaDataProvider._restore_financial_data_format = _restore_financial_data_format
    OptimizedChinaDataProvider._cache_raw_financial_data = _cache_raw_financial_data

#Other Organiser
_add_financial_cache_methods()
