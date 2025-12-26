#!/usr/bin/env python3
"""Stock data pre-acquisition and validation module
To validate the existence of stocks prior to the start of the analysis process and to pre-empt and cache the necessary data
"""

import re
from typing import Dict, Tuple, Optional
from datetime import datetime, timedelta

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('stock_validator')


class StockDataPreparationResult:
    """Equities Data Pre-Access Category"""

    def __init__(self, is_valid: bool, stock_code: str, market_type: str = "",
                 stock_name: str = "", error_message: str = "", suggestion: str = "",
                 has_historical_data: bool = False, has_basic_info: bool = False,
                 data_period_days: int = 0, cache_status: str = ""):
        self.is_valid = is_valid
        self.stock_code = stock_code
        self.market_type = market_type
        self.stock_name = stock_name
        self.error_message = error_message
        self.suggestion = suggestion
        self.has_historical_data = has_historical_data
        self.has_basic_info = has_basic_info
        self.data_period_days = data_period_days
        self.cache_status = cache_status

    def to_dict(self) -> Dict:
        """Convert to Dictionary Format"""
        return {
            'is_valid': self.is_valid,
            'stock_code': self.stock_code,
            'market_type': self.market_type,
            'stock_name': self.stock_name,
            'error_message': self.error_message,
            'suggestion': self.suggestion,
            'has_historical_data': self.has_historical_data,
            'has_basic_info': self.has_basic_info,
            'data_period_days': self.data_period_days,
            'cache_status': self.cache_status
        }


#Maintain backward compatibility
StockValidationResult = StockDataPreparationResult


class StockDataPreparer:
    """Pre-acquirers and certifiers for stock data"""

    def __init__(self, default_period_days: int = 30):
        self.timeout_seconds = 15  #Data acquisition timeout
        self.default_period_days = default_period_days  #Default length of historical data (days)
    
    def prepare_stock_data(self, stock_code: str, market_type: str = "auto",
                          period_days: int = None, analysis_date: str = None) -> StockDataPreparationResult:
        """Pre-acquisition and validation of stock data

Args:
Stock code: Stock code
Market type: Market type ("A" equity, "Hong Kong equity", "Auto")
period days: length of historical data (days), value when defaulting on class initialization
Analysis date: date analysed, default today

Returns:
StockDataPreparationResult: Data Preparation Results
"""
        if period_days is None:
            period_days = self.default_period_days

        if analysis_date is None:
            analysis_date = datetime.now().strftime('%Y-%m-%d')

        logger.info(f"[Data Preparation]{stock_code}(Market:{market_type}, duration:{period_days}Oh, my God.")

        #1. Basic format validation
        format_result = self._validate_format(stock_code, market_type)
        if not format_result.is_valid:
            return format_result

        #2. Automatic detection of market types
        if market_type == "auto":
            market_type = self._detect_market_type(stock_code)
            logger.debug(f"📊 [Data Preparation] Automatic detection of market types:{market_type}")

        #3. Advance data acquisition and validation
        return self._prepare_data_by_market(stock_code, market_type, period_days, analysis_date)
    
    def _validate_format(self, stock_code: str, market_type: str) -> StockDataPreparationResult:
        """Validate stock code format"""
        stock_code = stock_code.strip()
        
        if not stock_code:
            return StockDataPreparationResult(
                is_valid=False,
                stock_code=stock_code,
                error_message="股票代码不能为空",
                suggestion="请输入有效的股票代码"
            )

        if len(stock_code) > 10:
            return StockDataPreparationResult(
                is_valid=False,
                stock_code=stock_code,
                error_message="股票代码长度不能超过10个字符",
                suggestion="请检查股票代码格式"
            )
        
        #Certification format by market type
        if market_type == "A股":
            if not re.match(r'^\d{6}$', stock_code):
                return StockDataPreparationResult(
                    is_valid=False,
                    stock_code=stock_code,
                    market_type="A股",
                    error_message="A股代码格式错误，应为6位数字",
                    suggestion="请输入6位数字的A股代码，如：000001、600519"
                )
        elif market_type == "港股":
            stock_code_upper = stock_code.upper()
            hk_format = re.match(r'^\d{4,5}\.HK$', stock_code_upper)
            digit_format = re.match(r'^\d{4,5}$', stock_code)

            if not (hk_format or digit_format):
                return StockDataPreparationResult(
                    is_valid=False,
                    stock_code=stock_code,
                    market_type="港股",
                    error_message="港股代码格式错误",
                    suggestion="请输入4-5位数字.HK格式（如：0700.HK）或4-5位数字（如：0700）"
                )
        elif market_type == "美股":
            if not re.match(r'^[A-Z]{1,5}$', stock_code.upper()):
                return StockDataPreparationResult(
                    is_valid=False,
                    stock_code=stock_code,
                    market_type="美股",
                    error_message="美股代码格式错误，应为1-5位字母",
                    suggestion="请输入1-5位字母的美股代码，如：AAPL、TSLA"
                )
        
        return StockDataPreparationResult(
            is_valid=True,
            stock_code=stock_code,
            market_type=market_type
        )
    
    def _detect_market_type(self, stock_code: str) -> str:
        """Automatically detect market types"""
        stock_code = stock_code.strip().upper()
        
        #Unit A: 6 figures
        if re.match(r'^\d{6}$', stock_code):
            return "A股"
        
        #Port Unit: 4-5 figures. HK or 4-5 figures
        if re.match(r'^\d{4,5}\.HK$', stock_code) or re.match(r'^\d{4,5}$', stock_code):
            return "港股"
        
        #United States share: 1-5 letters
        if re.match(r'^[A-Z]{1,5}$', stock_code):
            return "美股"
        
        return "未知"

    def _get_hk_network_limitation_suggestion(self) -> str:
        """Detailed recommendations on access to port unit network restrictions"""
        suggestions = [
            "🌐 港股数据获取受到网络API限制，这是常见的临时问题",
            "",
            "💡 解决方案：",
            "1. 等待5-10分钟后重试（API限制通常会自动解除）",
            "2. 检查网络连接是否稳定",
            "3. 如果是知名港股（如腾讯0700.HK、阿里9988.HK），代码格式通常正确",
            "4. 可以尝试使用其他时间段进行分析",
            "",
            "📋 常见港股代码格式：",
            "• 腾讯控股：0700.HK",
            "• 阿里巴巴：9988.HK",
            "• 美团：3690.HK",
            "• 小米集团：1810.HK",
            "",
            "⏰ 建议稍后重试，或联系技术支持获取帮助"
        ]
        return "\n".join(suggestions)

    def _extract_hk_stock_name(self, stock_info, stock_code: str) -> str:
        """Extracting stock names from port information to support multiple formats"""
        if not stock_info:
            return "未知"

        #Process different types of return values
        if isinstance(stock_info, dict):
            #If Dictionary, try to extract names from common fields
            name_fields = ['name', 'longName', 'shortName', 'companyName', '公司名称', '股票名称']
            for field in name_fields:
                if field in stock_info and stock_info[field]:
                    name = str(stock_info[field]).strip()
                    if name and name != "未知":
                        return name

            #Use stock code if the dictionary contains valid information without name fields
            if len(stock_info) > 0:
                return stock_code
            return "未知"

        #Convert to String Processing
        stock_info_str = str(stock_info)

        #Method 1: Standard format.
        if "公司名称:" in stock_info_str:
            lines = stock_info_str.split('\n')
            for line in lines:
                if "公司名称:" in line:
                    name = line.split(':')[1].strip()
                    if name and name != "未知":
                        return name

        #Method 2: Yahoo Finance Format Testing
        #Log shows: "✅ Yahoo Finance successfully accessed information on the Port Unit: 0700.HK->TENCENT"
        if "Yahoo Finance成功获取港股信息" in stock_info_str:
            #Extract name from log
            if " -> " in stock_info_str:
                parts = stock_info_str.split(" -> ")
                if len(parts) > 1:
                    name = parts[-1].strip()
                    if name and name != "未知":
                        return name

        #Method 3: Checking for common company names is critical Word
        company_indicators = [
            "Limited", "Ltd", "Corporation", "Corp", "Inc", "Group",
            "Holdings", "Company", "Co", "集团", "控股", "有限公司"
        ]

        lines = stock_info_str.split('\n')
        for line in lines:
            line = line.strip()
            if any(indicator in line for indicator in company_indicators):
                #Try extracting company names
                if ":" in line:
                    potential_name = line.split(':')[-1].strip()
                    if potential_name and len(potential_name) > 2:
                        return potential_name
                elif len(line) > 2 and len(line) < 100:  #Reasonable length of company name
                    return line

        #Method 4: Use the stock code if the information appears valid but cannot be deciphered
        if len(stock_info_str) > 50 and "❌" not in stock_info_str:
            #The information appears to be valid, but cannot be deciphered, using code as name
            return stock_code

        return "未知"

    def _prepare_data_by_market(self, stock_code: str, market_type: str,
                               period_days: int, analysis_date: str) -> StockDataPreparationResult:
        """Advance data acquisition by market type"""
        logger.debug(f"[Data Preparation]{market_type}Equities{stock_code}Prepare Data")

        try:
            if market_type == "A股":
                return self._prepare_china_stock_data(stock_code, period_days, analysis_date)
            elif market_type == "港股":
                return self._prepare_hk_stock_data(stock_code, period_days, analysis_date)
            elif market_type == "美股":
                return self._prepare_us_stock_data(stock_code, period_days, analysis_date)
            else:
                return StockDataPreparationResult(
                    is_valid=False,
                    stock_code=stock_code,
                    market_type=market_type,
                    error_message=f"不支持的市场类型: {market_type}",
                    suggestion="请选择支持的市场类型：A股、港股、美股"
                )
        except Exception as e:
            logger.error(f"Data preparation anomaly:{e}")
            return StockDataPreparationResult(
                is_valid=False,
                stock_code=stock_code,
                market_type=market_type,
                error_message=f"数据准备过程中发生错误: {str(e)}",
                suggestion="请检查网络连接或稍后重试"
            )

    async def _prepare_data_by_market_async(self, stock_code: str, market_type: str,
                                           period_days: int, analysis_date: str) -> StockDataPreparationResult:
        """Pre-acquire data according to market type (speech version)"""
        logger.debug(f"[Data Preparation - Step ]{market_type}Equities{stock_code}Prepare Data")

        try:
            if market_type == "A股":
                return await self._prepare_china_stock_data_async(stock_code, period_days, analysis_date)
            elif market_type == "港股":
                return self._prepare_hk_stock_data(stock_code, period_days, analysis_date)
            elif market_type == "美股":
                return self._prepare_us_stock_data(stock_code, period_days, analysis_date)
            else:
                return StockDataPreparationResult(
                    is_valid=False,
                    stock_code=stock_code,
                    market_type=market_type,
                    error_message=f"不支持的市场类型: {market_type}",
                    suggestion="请选择支持的市场类型：A股、港股、美股"
                )
        except Exception as e:
            logger.error(f"Data readiness anomaly:{e}")
            return StockDataPreparationResult(
                is_valid=False,
                stock_code=stock_code,
                market_type=market_type,
                error_message=f"数据准备过程中发生错误: {str(e)}",
                suggestion="请检查网络连接或稍后重试"
            )

    def _prepare_china_stock_data(self, stock_code: str, period_days: int,
                                 analysis_date: str) -> StockDataPreparationResult:
        """Advance acquisition of Unit A data, including database checks and automatic synchronization"""
        logger.info(f"[A unit data]{stock_code}Data (time:{period_days}Oh, my God.")

        #Calculated date range (using extended date range, consistent with Get china stock data unified)
        end_date = datetime.strptime(analysis_date, '%Y-%m-%d')

        #Fetching configuration backtrace days (consistent with Get china stock data unified)
        from app.core.config import settings
        lookback_days = getattr(settings, 'MARKET_ANALYST_LOOKBACK_DAYS', 365)

        #Use extended date range for data checking and synchronization
        extended_start_date = end_date - timedelta(days=lookback_days)
        extended_start_date_str = extended_start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        logger.info(f"Actual data range:{extended_start_date_str}Present.{end_date_str} ({lookback_days}Oh, my God.")

        has_historical_data = False
        has_basic_info = False
        stock_name = "未知"
        cache_status = ""
        data_synced = False

        try:
            #1. Check the availability and updating of data in the database
            logger.debug(f"Check the database.{stock_code}Data...")
            db_check_result = self._check_database_data(stock_code, extended_start_date_str, end_date_str)

            #2. Automatically trigger sync if data are non-existent or not up to date
            if not db_check_result["has_data"] or not db_check_result["is_latest"]:
                logger.warning(f"The database data are incomplete:{db_check_result['message']}")
                logger.info(f"[Unit A data]{stock_code}")

                #Sync with extended date range
                sync_result = self._trigger_data_sync_sync(stock_code, extended_start_date_str, end_date_str)
                if sync_result["success"]:
                    logger.info(f"Data sync successfully:{sync_result['message']}")
                    data_synced = True
                    cache_status += "数据已同步; "
                else:
                    logger.warning(f"Data synchronisation failed:{sync_result['message']}")
                    #Keep trying to get data from API
            else:
                logger.info(f"The database data check has been approved:{db_check_result['message']}")
                cache_status += "数据库数据最新; "

            #3. Access to basic information
            logger.debug(f"[Unit A data]{stock_code}Basic information...")
            from tradingagents.dataflows.interface import get_china_stock_info_unified

            stock_info = get_china_stock_info_unified(stock_code)

            if stock_info and "❌" not in stock_info and "未能获取" not in stock_info:
                #Parsing stock name
                if "股票名称:" in stock_info:
                    lines = stock_info.split('\n')
                    for line in lines:
                        if "股票名称:" in line:
                            stock_name = line.split(':')[1].strip()
                            break

                #Check for valid stock names
                if stock_name != "未知" and not stock_name.startswith(f"股票{stock_code}"):
                    has_basic_info = True
                    logger.info(f"[Unit A data]{stock_code} - {stock_name}")
                    cache_status += "基本信息已缓存; "
                else:
                    logger.warning(f"Basic information is invalid:{stock_code}")
                    return StockDataPreparationResult(
                        is_valid=False,
                        stock_code=stock_code,
                        market_type="A股",
                        error_message=f"股票代码 {stock_code} 不存在或信息无效",
                        suggestion="请检查股票代码是否正确，或确认该股票是否已上市"
                    )
            else:
                logger.warning(f"No basic information is available:{stock_code}")
                return StockDataPreparationResult(
                    is_valid=False,
                    stock_code=stock_code,
                    market_type="A股",
                    error_message=f"无法获取股票 {stock_code} 的基本信息",
                    suggestion="请检查股票代码是否正确，或确认该股票是否已上市"
                )

            #4. Access to historical data (use extended date range)
            logger.debug(f"[Unit A data]{stock_code}Historical Data ({extended_start_date_str}Present.{end_date_str})...")
            from tradingagents.dataflows.interface import get_china_stock_data_unified

            historical_data = get_china_stock_data_unified(stock_code, extended_start_date_str, end_date_str)

            if historical_data and "❌" not in historical_data and "获取失败" not in historical_data:
                #More liberal data validity checks
                data_indicators = [
                    "开盘价", "收盘价", "最高价", "最低价", "成交量",
                    "open", "close", "high", "low", "volume",
                    "日期", "date", "时间", "time"
                ]

                has_valid_data = (
                    len(historical_data) > 50 and  #Lower length requirement
                    any(indicator in historical_data for indicator in data_indicators)
                )

                if has_valid_data:
                    has_historical_data = True
                    logger.info(f"[Unit A data]{stock_code} ({lookback_days}Oh, my God.")
                    cache_status += f"历史数据已缓存({lookback_days}天); "
                else:
                    logger.warning(f"[A unit data]{stock_code}")
                    logger.debug(f"Data content preview:{historical_data[:200]}...")
                    return StockDataPreparationResult(
                        is_valid=False,
                        stock_code=stock_code,
                        market_type="A股",
                        stock_name=stock_name,
                        has_basic_info=has_basic_info,
                        error_message=f"股票 {stock_code} 的历史数据无效或不足",
                        suggestion="该股票可能为新上市股票或数据源暂时不可用，请稍后重试"
                    )
            else:
                logger.warning(f"No historical data are available:{stock_code}")
                return StockDataPreparationResult(
                    is_valid=False,
                    stock_code=stock_code,
                    market_type="A股",
                    stock_name=stock_name,
                    has_basic_info=has_basic_info,
                    error_message=f"无法获取股票 {stock_code} 的历史数据",
                    suggestion="请检查网络连接或数据源配置，或稍后重试"
                )

            #5. Data preparation success
            logger.info(f"Data ready:{stock_code} - {stock_name}")
            return StockDataPreparationResult(
                is_valid=True,
                stock_code=stock_code,
                market_type="A股",
                stock_name=stock_name,
                has_historical_data=has_historical_data,
                has_basic_info=has_basic_info,
                data_period_days=lookback_days,  #Number of days to use actual data
                cache_status=cache_status.rstrip('; ')
            )

        except Exception as e:
            logger.error(f"Data preparation failed:{e}")
            import traceback
            logger.debug(f"Detailed error:{traceback.format_exc()}")
            return StockDataPreparationResult(
                is_valid=False,
                stock_code=stock_code,
                market_type="A股",
                stock_name=stock_name,
                has_basic_info=has_basic_info,
                has_historical_data=has_historical_data,
                error_message=f"数据准备失败: {str(e)}",
                suggestion="请检查网络连接或数据源配置"
            )

    async def _prepare_china_stock_data_async(self, stock_code: str, period_days: int,
                                             analysis_date: str) -> StockDataPreparationResult:
        """Advance acquisition of Unit A data (speech version), including database checks and automatic synchronization"""
        logger.info(f"Let's get ready.{stock_code}Data (time:{period_days}Oh, my God.")

        #Calculate Date Range
        end_date = datetime.strptime(analysis_date, '%Y-%m-%d')
        from app.core.config import settings
        lookback_days = getattr(settings, 'MARKET_ANALYST_LOOKBACK_DAYS', 365)
        extended_start_date = end_date - timedelta(days=lookback_days)
        extended_start_date_str = extended_start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        logger.info(f"Actual data ranges:{extended_start_date_str}Present.{end_date_str} ({lookback_days}Oh, my God.")

        has_historical_data = False
        has_basic_info = False
        stock_name = "未知"
        cache_status = ""

        try:
            #1. Check the availability and updating of data in the database
            logger.debug(f"Check the database. Medium{stock_code}Data...")
            db_check_result = self._check_database_data(stock_code, extended_start_date_str, end_date_str)

            #2. Automatically trigger synchronization (using a walk method) if the data do not exist or are not up to date
            if not db_check_result["has_data"] or not db_check_result["is_latest"]:
                logger.warning(f"The database is incomplete:{db_check_result['message']}")
                logger.info(f"🔄 [A Unit Data-Instant] Automatically triggers data synchronization:{stock_code}")

                #🔥Sync data using a different way
                sync_result = await self._trigger_data_sync_async(stock_code, extended_start_date_str, end_date_str)
                if sync_result["success"]:
                    logger.info(f"Data sync successfully:{sync_result['message']}")
                    cache_status += "数据已同步; "
                else:
                    logger.warning(f"⚠️ [A Unit Data-Instant] Data sync failed:{sync_result['message']}")
            else:
                logger.info(f"The database data check has been approved:{db_check_result['message']}")
                cache_status += "数据库数据最新; "

            #3. Access to basic information (synchronous operations)
            logger.debug(f"📊 [A Unit Data - Step ]{stock_code}Basic information...")
            from tradingagents.dataflows.interface import get_china_stock_info_unified
            stock_info = get_china_stock_info_unified(stock_code)

            if stock_info and "❌" not in stock_info and "未能获取" not in stock_info:
                if "股票名称:" in stock_info:
                    lines = stock_info.split('\n')
                    for line in lines:
                        if "股票名称:" in line:
                            stock_name = line.split(':')[1].strip()
                            break

                if stock_name != "未知" and not stock_name.startswith(f"股票{stock_code}"):
                    has_basic_info = True
                    logger.info(f"Basic information acquisition success:{stock_code} - {stock_name}")
                    cache_status += "基本信息已缓存; "

            #4. Access to historical data (synchronous operations)
            logger.debug(f"📊 [A Unit Data - Step ]{stock_code}Historical Data...")
            from tradingagents.dataflows.interface import get_china_stock_data_unified
            historical_data = get_china_stock_data_unified(stock_code, extended_start_date_str, end_date_str)

            if historical_data and "❌" not in historical_data and "获取失败" not in historical_data:
                data_indicators = ["开盘价", "收盘价", "最高价", "最低价", "成交量"]
                has_valid_data = (
                    len(historical_data) > 50 and
                    any(indicator in historical_data for indicator in data_indicators)
                )

                if has_valid_data:
                    has_historical_data = True
                    logger.info(f"✅ [A Unit Data-Instant] Historical data acquisition success:{stock_code}")
                    cache_status += f"历史数据已缓存({lookback_days}天); "
                else:
                    return StockDataPreparationResult(
                        is_valid=False,
                        stock_code=stock_code,
                        market_type="A股",
                        stock_name=stock_name,
                        has_basic_info=has_basic_info,
                        error_message=f"股票 {stock_code} 的历史数据无效或不足",
                        suggestion="该股票可能为新上市股票或数据源暂时不可用，请稍后重试"
                    )
            else:
                return StockDataPreparationResult(
                    is_valid=False,
                    stock_code=stock_code,
                    market_type="A股",
                    stock_name=stock_name,
                    has_basic_info=has_basic_info,
                    error_message=f"无法获取股票 {stock_code} 的历史数据",
                    suggestion="请检查网络连接或数据源配置，或稍后重试"
                )

            #5. Data preparation success
            logger.info(f"Data ready:{stock_code} - {stock_name}")
            return StockDataPreparationResult(
                is_valid=True,
                stock_code=stock_code,
                market_type="A股",
                stock_name=stock_name,
                has_historical_data=has_historical_data,
                has_basic_info=has_basic_info,
                data_period_days=lookback_days,
                cache_status=cache_status.rstrip('; ')
            )

        except Exception as e:
            logger.error(f"Data preparation failed:{e}")
            import traceback
            logger.debug(f"Detailed error:{traceback.format_exc()}")
            return StockDataPreparationResult(
                is_valid=False,
                stock_code=stock_code,
                market_type="A股",
                stock_name=stock_name,
                has_basic_info=has_basic_info,
                has_historical_data=has_historical_data,
                error_message=f"数据准备失败: {str(e)}",
                suggestion="请检查网络连接或数据源配置"
            )

    def _check_database_data(self, stock_code: str, start_date: str, end_date: str) -> Dict:
        """Check the existence and updating of data in the database

Returns:
Dict:   FMT 0 
"""
        try:
            from tradingagents.dataflows.cache.mongodb_cache_adapter import get_mongodb_cache_adapter

            adapter = get_mongodb_cache_adapter()
            if not adapter.use_app_cache or adapter.db is None:
                return {
                    "has_data": False,
                    "is_latest": False,
                    "record_count": 0,
                    "latest_date": None,
                    "message": "MongoDB缓存未启用"
                }

            #Query historical data in database
            df = adapter.get_historical_data(stock_code, start_date, end_date)

            if df is None or df.empty:
                return {
                    "has_data": False,
                    "is_latest": False,
                    "record_count": 0,
                    "latest_date": None,
                    "message": "数据库中没有数据"
                }

            #Check data volume
            record_count = len(df)

            #Date of acquisition of latest data
            if 'trade_date' in df.columns:
                latest_date = df['trade_date'].max()
            elif 'date' in df.columns:
                latest_date = df['date'].max()
            else:
                latest_date = None

            #Check to include the latest transaction date
            from datetime import datetime, timedelta
            today = datetime.now()

            #Get the latest trading day (consider weekends)
            recent_trade_date = today
            for i in range(5):  #Five days at most.
                check_date = today - timedelta(days=i)
                if check_date.weekday() < 5:  #Monday to Friday.
                    recent_trade_date = check_date
                    break

            recent_trade_date_str = recent_trade_date.strftime('%Y-%m-%d')

            #Determination of whether the data are up to date (a 1-day delay allowed)
            is_latest = False
            if latest_date:
                latest_date_str = str(latest_date)[:10]  #YYY-MM-DD
                latest_dt = datetime.strptime(latest_date_str, '%Y-%m-%d')
                days_diff = (recent_trade_date - latest_dt).days
                is_latest = days_diff <= 1  #1 day delay allowed

            message = f"找到{record_count}条记录，最新日期: {latest_date}"
            if not is_latest:
                message += f"（需要更新到{recent_trade_date_str}）"

            return {
                "has_data": True,
                "is_latest": is_latest,
                "record_count": record_count,
                "latest_date": str(latest_date) if latest_date else None,
                "message": message
            }

        except Exception as e:
            logger.error(f"[Data Check] Checking database data failed:{e}")
            return {
                "has_data": False,
                "is_latest": False,
                "record_count": 0,
                "latest_date": None,
                "message": f"检查失败: {str(e)}"
            }

    def _trigger_data_sync_sync(self, stock_code: str, start_date: str, end_date: str) -> Dict:
        """Trigger Data Synchronization (Sync Packer)
Call the step synchronisation method in sync context

compatible with asyncio.to thread() calling:
- Create a new cycle of events if running in a line created by asyncio.to thread()
"attached to a different loop" error
"""
        import asyncio

        try:
            #Check if there is a running cycle of events
            #If yes, this indicates that we need to create a new cycle of events in the line created by Asyncio.to thread()
            try:
                running_loop = asyncio.get_running_loop()
                #There is a running cycle that indicates that run until complete cannot be used in the aniso context
                #Create a new event cycle to run in a new thread
                logger.info(f"🔍 [DataSync] Detecting running event cycles, creating new event cycles")
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    return loop.run_until_complete(
                        self._trigger_data_sync_async(stock_code, start_date, end_date)
                    )
                finally:
                    loop.close()
                    asyncio.set_event_loop(None)
            except RuntimeError:
                #There is no running cycle, you can securely access or create event cycle
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_closed():
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                #Call the heap method
                return loop.run_until_complete(
                    self._trigger_data_sync_async(stock_code, start_date, end_date)
                )
        except Exception as e:
            logger.error(f"[Data syncs]{e}", exc_info=True)
            return {
                "success": False,
                "message": f"同步失败: {str(e)}",
                "synced_records": 0,
                "data_source": None
            }

    async def _trigger_data_sync_async(self, stock_code: str, start_date: str, end_date: str) -> Dict:
        """Trigger data synchronisation (show, according to data source priorities configured by the database)
Synchronization includes: historical, financial, real-time

Returns:
Dict:   FMT 0 
"""
        try:
            logger.info(f"[Data syncs]{stock_code}Data (History + Finance + Real Time)...")

            #Data source priorities from databases
            priority_order = self._get_data_source_priority_for_sync(stock_code)
            logger.info(f"Data source priorities:{priority_order}")

            #2. Attempt to synchronize according to priority
            last_error = None
            for data_source in priority_order:
                try:
                    logger.info(f"[Data Synchronization]{data_source}")

                    #BaoStock does not support single stock synchronization, skip
                    if data_source == "baostock":
                        logger.warning(f"BaoStock does not support single stock synchronization, skipping")
                        last_error = f"{data_source}: 不支持单个股票同步"
                        continue

                    #Get the corresponding synchronized services from data sources
                    if data_source == "tushare":
                        from app.worker.tushare_sync_service import get_tushare_sync_service
                        service = await get_tushare_sync_service()
                    elif data_source == "akshare":
                        from app.worker.akshare_sync_service import get_akshare_sync_service
                        service = await get_akshare_sync_service()
                    else:
                        logger.warning(f"Data sources not supported:{data_source}")
                        continue

                    #Initialization Results Statistics
                    historical_records = 0
                    financial_synced = False
                    realtime_synced = False

                    #2.1 Synchronization of historical data
                    logger.info(f"Synchronize historical data...")
                    hist_result = await service.sync_historical_data(
                        symbols=[stock_code],
                        start_date=start_date,
                        end_date=end_date,
                        incremental=False  #Full Sync
                    )

                    if hist_result.get("success_count", 0) > 0:
                        historical_records = hist_result.get("total_records", 0)
                        logger.info(f"✅ [DataSync] Historical data sync successfully:{historical_records}Article")
                    else:
                        errors = hist_result.get("errors", [])
                        error_msg = errors[0].get("error", "未知错误") if errors else "同步失败"
                        logger.warning(f"[Data syncs]{error_msg}")

                    #2.2 Synchronization of financial data
                    logger.info(f"Synchronization of financial data...")
                    try:
                        fin_result = await service.sync_financial_data(
                            symbols=[stock_code],
                            limit=20  #Access to the latest 20 issues (approximately 5 years)
                        )

                        if fin_result.get("success_count", 0) > 0:
                            financial_synced = True
                            logger.info(f"[Data sync]")
                        else:
                            logger.warning(f"[Data sync]")
                    except Exception as e:
                        logger.warning(f"[Data Synchronization]{e}")

                    #2.3 Synchronization of real-time patterns
                    logger.info(f"[Data Synchronization]")
                    try:
                        #AKShare is better suited for real-time business for a single stock
                        if data_source == "tushare":
                            #Tushare's real-time line interface is limited, moving to AKShare
                            from app.worker.akshare_sync_service import get_akshare_sync_service
                            realtime_service = await get_akshare_sync_service()
                        else:
                            realtime_service = service

                        rt_result = await realtime_service.sync_realtime_quotes(
                            symbols=[stock_code],
                            force=True  #Enforcement, skip transaction time check
                        )

                        if rt_result.get("success_count", 0) > 0:
                            realtime_synced = True
                            logger.info(f"[Data Synchronization]")
                        else:
                            logger.warning(f"[Data Sync] Real-time line sync failed")
                    except Exception as e:
                        logger.warning(f"[Data syncs] Real-time line sync anomalies:{e}")

                    #Check sync results (at least historical data are successful)
                    if historical_records > 0:
                        message = f"使用{data_source}同步成功: 历史{historical_records}条"
                        if financial_synced:
                            message += ", 财务数据✓"
                        if realtime_synced:
                            message += ", 实时行情✓"

                        logger.info(f"[Data Syncs]{message}")
                        return {
                            "success": True,
                            "message": message,
                            "synced_records": historical_records,
                            "data_source": data_source,
                            "historical_records": historical_records,
                            "financial_synced": financial_synced,
                            "realtime_synced": realtime_synced
                        }
                    else:
                        last_error = f"{data_source}: 历史数据同步失败"
                        logger.warning(f"[Data Syncs]{data_source}Synchronising failed: History data empty")
                        #Continue to try the next data source

                except Exception as e:
                    last_error = f"{data_source}: {str(e)}"
                    logger.warning(f"[Data Syncs]{data_source}Synchronization anomaly:{e}")
                    import traceback
                    logger.debug(f"Detailed error:{traceback.format_exc()}")
                    #Continue to try the next data source
                    continue

            #All data sources failed
            message = f"所有数据源同步失败，最后错误: {last_error}"
            logger.error(f"[Data Syncs]{message}")
            return {
                "success": False,
                "message": message,
                "synced_records": 0,
                "data_source": None,
                "historical_records": 0,
                "financial_synced": False,
                "realtime_synced": False
            }

        except Exception as e:
            logger.error(f"Synchronising data failed:{e}")
            import traceback
            logger.debug(f"Detailed error:{traceback.format_exc()}")
            return {
                "success": False,
                "message": f"同步失败: {str(e)}",
                "synced_records": 0,
                "data_source": None,
                "historical_records": 0,
                "financial_synced": False,
                "realtime_synced": False
            }

    def _get_data_source_priority_for_sync(self, stock_code: str) -> list:
        """Acquisition of data source priorities (for synchronization)

Returns:
list: list of data sources, in order of priority ['tushare', 'akshare', 'baostock']
"""
        try:
            from tradingagents.dataflows.cache.mongodb_cache_adapter import get_mongodb_cache_adapter

            adapter = get_mongodb_cache_adapter()
            if adapter.use_app_cache and adapter.db is not None:
                #Get priority with MongoDB adapter
                priority_order = adapter._get_data_source_priority(stock_code)
                logger.info(f"[Data source priority]{priority_order}")
                return priority_order
            else:
                logger.warning(f"MongoDB is not enabled, using default order")
                return ['tushare', 'akshare', 'baostock']

        except Exception as e:
            logger.error(f"[Data source priority]{e}")
            #Returns the default order
            return ['tushare', 'akshare', 'baostock']

    def _prepare_hk_stock_data(self, stock_code: str, period_days: int,
                              analysis_date: str) -> StockDataPreparationResult:
        """Advance access to port unit data"""
        logger.info(f"[Hong Kong Unit Data]{stock_code}Data (time:{period_days}Oh, my God.")

        #Standardized port unit code format
        if not stock_code.upper().endswith('.HK'):
            #Remove pilot 0 and complete it to four.
            clean_code = stock_code.lstrip('0') or '0'  #If it's all zeros, keep one zero.
            formatted_code = f"{clean_code.zfill(4)}.HK"
            logger.debug(f"[Hong Kong Unit Data]{stock_code} → {formatted_code}")
        else:
            formatted_code = stock_code.upper()

        #Calculate Date Range
        end_date = datetime.strptime(analysis_date, '%Y-%m-%d')
        start_date = end_date - timedelta(days=period_days)
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        logger.debug(f"[Port Unit data] Date range:{start_date_str} → {end_date_str}")

        has_historical_data = False
        has_basic_info = False
        stock_name = "未知"
        cache_status = ""

        try:
            #1. Access to basic information
            logger.debug(f"[Hong Kong Unit Data]{formatted_code}Basic information...")
            from tradingagents.dataflows.interface import get_hk_stock_info_unified

            stock_info = get_hk_stock_info_unified(formatted_code)

            if stock_info and "❌" not in stock_info and "未找到" not in stock_info:
                #Parsing stock names - Supporting multiple formats
                stock_name = self._extract_hk_stock_name(stock_info, formatted_code)

                if stock_name and stock_name != "未知":
                    has_basic_info = True
                    logger.info(f"[Hong Kong Unit Data]{formatted_code} - {stock_name}")
                    cache_status += "基本信息已缓存; "
                else:
                    logger.warning(f"Basic information is invalid:{formatted_code}")
                    logger.debug(f"Information content:{stock_info[:200]}...")
                    return StockDataPreparationResult(
                        is_valid=False,
                        stock_code=formatted_code,
                        market_type="港股",
                        error_message=f"港股代码 {formatted_code} 不存在或信息无效",
                        suggestion="请检查港股代码是否正确，格式如：0700.HK"
                    )
            else:
                #Check for network restrictions
                network_error_indicators = [
                    "Too Many Requests", "Rate limited", "Connection aborted",
                    "Remote end closed connection", "网络连接", "超时", "限制"
                ]

                is_network_issue = any(indicator in str(stock_info) for indicator in network_error_indicators)

                if is_network_issue:
                    logger.warning(f"The impact of network restrictions:{formatted_code}")
                    return StockDataPreparationResult(
                        is_valid=False,
                        stock_code=formatted_code,
                        market_type="港股",
                        error_message=f"港股数据获取受到网络限制影响",
                        suggestion=self._get_hk_network_limitation_suggestion()
                    )
                else:
                    logger.warning(f"Basic information is not available:{formatted_code}")
                    return StockDataPreparationResult(
                        is_valid=False,
                        stock_code=formatted_code,
                        market_type="港股",
                        error_message=f"港股代码 {formatted_code} 可能不存在或数据源暂时不可用",
                        suggestion="请检查港股代码是否正确，格式如：0700.HK，或稍后重试"
                    )

            #2. Access to historical data
            logger.debug(f"[Hong Kong Unit Data]{formatted_code}Historical Data ({start_date_str}Present.{end_date_str})...")
            from tradingagents.dataflows.interface import get_hk_stock_data_unified

            historical_data = get_hk_stock_data_unified(formatted_code, start_date_str, end_date_str)

            if historical_data and "❌" not in historical_data and "获取失败" not in historical_data:
                #More liberal data validity checks
                data_indicators = [
                    "开盘价", "收盘价", "最高价", "最低价", "成交量",
                    "open", "close", "high", "low", "volume",
                    "日期", "date", "时间", "time"
                ]

                has_valid_data = (
                    len(historical_data) > 50 and  #Lower length requirement
                    any(indicator in historical_data for indicator in data_indicators)
                )

                if has_valid_data:
                    has_historical_data = True
                    logger.info(f"[Hong Kong Unit Data]{formatted_code} ({period_days}Oh, my God.")
                    cache_status += f"历史数据已缓存({period_days}天); "
                else:
                    logger.warning(f"[Hong Kong Unit Data]{formatted_code}")
                    logger.debug(f"Data content preview:{historical_data[:200]}...")
                    return StockDataPreparationResult(
                        is_valid=False,
                        stock_code=formatted_code,
                        market_type="港股",
                        stock_name=stock_name,
                        has_basic_info=has_basic_info,
                        error_message=f"港股 {formatted_code} 的历史数据无效或不足",
                        suggestion="该股票可能为新上市股票或数据源暂时不可用，请稍后重试"
                    )
            else:
                #Check for network restrictions
                network_error_indicators = [
                    "Too Many Requests", "Rate limited", "Connection aborted",
                    "Remote end closed connection", "网络连接", "超时", "限制"
                ]

                is_network_issue = any(indicator in str(historical_data) for indicator in network_error_indicators)

                if is_network_issue:
                    logger.warning(f"Access to historical data is restricted by the Internet:{formatted_code}")
                    return StockDataPreparationResult(
                        is_valid=False,
                        stock_code=formatted_code,
                        market_type="港股",
                        stock_name=stock_name,
                        has_basic_info=has_basic_info,
                        error_message=f"港股历史数据获取受到网络限制影响",
                        suggestion=self._get_hk_network_limitation_suggestion()
                    )
                else:
                    logger.warning(f"[Hong Kong Unit Data]{formatted_code}")
                    return StockDataPreparationResult(
                        is_valid=False,
                        stock_code=formatted_code,
                        market_type="港股",
                        stock_name=stock_name,
                        has_basic_info=has_basic_info,
                        error_message=f"无法获取港股 {formatted_code} 的历史数据",
                        suggestion="数据源可能暂时不可用，请稍后重试或联系技术支持"
                    )

            #3. Successful data preparation
            logger.info(f"The data are ready:{formatted_code} - {stock_name}")
            return StockDataPreparationResult(
                is_valid=True,
                stock_code=formatted_code,
                market_type="港股",
                stock_name=stock_name,
                has_historical_data=has_historical_data,
                has_basic_info=has_basic_info,
                data_period_days=period_days,
                cache_status=cache_status.rstrip('; ')
            )

        except Exception as e:
            logger.error(f"Data preparation failed:{e}")
            return StockDataPreparationResult(
                is_valid=False,
                stock_code=formatted_code,
                market_type="港股",
                stock_name=stock_name,
                has_basic_info=has_basic_info,
                has_historical_data=has_historical_data,
                error_message=f"数据准备失败: {str(e)}",
                suggestion="请检查网络连接或数据源配置"
            )

    def _prepare_us_stock_data(self, stock_code: str, period_days: int,
                              analysis_date: str) -> StockDataPreparationResult:
        """Advance access to US stock data"""
        logger.info(f"Let's get ready.{stock_code}Data (time:{period_days}Oh, my God.")

        #Standardized USE code format
        formatted_code = stock_code.upper()

        #Calculate Date Range
        end_date = datetime.strptime(analysis_date, '%Y-%m-%d')
        start_date = end_date - timedelta(days=period_days)
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        logger.debug(f"Date range:{start_date_str} → {end_date_str}")

        has_historical_data = False
        has_basic_info = False
        stock_name = formatted_code  #The U.S. stock usually uses code as its name.
        cache_status = ""

        try:
            #1. Access to historical data (United States shares are usually directly validated through historical data)
            logger.debug(f"[United States data]{formatted_code}Historical Data ({start_date_str}Present.{end_date_str})...")

            #Import U.S. stock data provider (support for old and new paths)
            try:
                from tradingagents.dataflows.providers.us import OptimizedUSDataProvider
                provider = OptimizedUSDataProvider()
                historical_data = provider.get_stock_data(
                    formatted_code,
                    start_date_str,
                    end_date_str
                )
            except ImportError:
                from tradingagents.dataflows.providers.us.optimized import get_us_stock_data_cached
                historical_data = get_us_stock_data_cached(
                    formatted_code,
                    start_date_str,
                    end_date_str
                )

            if historical_data and "❌" not in historical_data and "错误" not in historical_data and "无法获取" not in historical_data:
                #More liberal data validity checks
                data_indicators = [
                    "开盘价", "收盘价", "最高价", "最低价", "成交量",
                    "Open", "Close", "High", "Low", "Volume",
                    "日期", "Date", "时间", "Time"
                ]

                has_valid_data = (
                    len(historical_data) > 50 and  #Lower length requirement
                    any(indicator in historical_data for indicator in data_indicators)
                )

                if has_valid_data:
                    has_historical_data = True
                    has_basic_info = True  #The U.S. stock usually doesn't get basic information alone.
                    logger.info(f"[United States stock data]{formatted_code} ({period_days}Oh, my God.")
                    cache_status = f"历史数据已缓存({period_days}天)"

                    #Data ready.
                    logger.info(f"Data ready:{formatted_code}")
                    return StockDataPreparationResult(
                        is_valid=True,
                        stock_code=formatted_code,
                        market_type="美股",
                        stock_name=stock_name,
                        has_historical_data=has_historical_data,
                        has_basic_info=has_basic_info,
                        data_period_days=period_days,
                        cache_status=cache_status
                    )
                else:
                    logger.warning(f"[United States equity data]{formatted_code}")
                    logger.debug(f"Data content preview:{historical_data[:200]}...")
                    return StockDataPreparationResult(
                        is_valid=False,
                        stock_code=formatted_code,
                        market_type="美股",
                        error_message=f"美股 {formatted_code} 的历史数据无效或不足",
                        suggestion="该股票可能为新上市股票或数据源暂时不可用，请稍后重试"
                    )
            else:
                logger.warning(f"No historical data can be obtained:{formatted_code}")
                return StockDataPreparationResult(
                    is_valid=False,
                    stock_code=formatted_code,
                    market_type="美股",
                    error_message=f"美股代码 {formatted_code} 不存在或无法获取数据",
                    suggestion="请检查美股代码是否正确，如：AAPL、TSLA、MSFT"
                )

        except Exception as e:
            logger.error(f"Data preparation failed:{e}")
            return StockDataPreparationResult(
                is_valid=False,
                stock_code=formatted_code,
                market_type="美股",
                error_message=f"数据准备失败: {str(e)}",
                suggestion="请检查网络连接或数据源配置"
            )




#Examples of global data preparation
_stock_preparer = None

def get_stock_preparer(default_period_days: int = 30) -> StockDataPreparer:
    """Examples of stock acquisition data preparation (single mode)"""
    global _stock_preparer
    if _stock_preparer is None:
        _stock_preparer = StockDataPreparer(default_period_days)
    return _stock_preparer


def prepare_stock_data(stock_code: str, market_type: str = "auto",
                      period_days: int = None, analysis_date: str = None) -> StockDataPreparationResult:
    """Easy function: Pre-acquisition and validation of stock data

Args:
Stock code: Stock code
Market type: Market type ("A" equity, "Hong Kong equity", "Auto")
period days: length of historical data (days), default 30 days
Analysis date: date analysed, default today

Returns:
StockDataPreparationResult: Data Preparation Results
"""
    preparer = get_stock_preparer()
    return preparer.prepare_stock_data(stock_code, market_type, period_days, analysis_date)


def is_stock_data_ready(stock_code: str, market_type: str = "auto",
                       period_days: int = None, analysis_date: str = None) -> bool:
    """Easy function: Check for stock data readiness

Args:
Stock code: Stock code
Market type: Market type ("A" equity, "Hong Kong equity", "Auto")
period days: length of historical data (days), default 30 days
Analysis date: date analysed, default today

Returns:
Bool: Data ready
"""
    result = prepare_stock_data(stock_code, market_type, period_days, analysis_date)
    return result.is_valid


def get_stock_preparation_message(stock_code: str, market_type: str = "auto",
                                 period_days: int = None, analysis_date: str = None) -> str:
    """Easy function: Get stock data ready messages

Args:
Stock code: Stock code
Market type: Market type ("A" equity, "Hong Kong equity", "Auto")
period days: length of historical data (days), default 30 days
Analysis date: date analysed, default today

Returns:
str: Data Preparation Message
"""
    result = prepare_stock_data(stock_code, market_type, period_days, analysis_date)

    if result.is_valid:
        return f"✅ 数据准备成功: {result.stock_code} ({result.market_type}) - {result.stock_name}\n📊 {result.cache_status}"
    else:
        return f"❌ 数据准备失败: {result.error_message}\n💡 建议: {result.suggestion}"


async def prepare_stock_data_async(stock_code: str, market_type: str = "auto",
                                   period_days: int = None, analysis_date: str = None) -> StockDataPreparationResult:
    """Offset: pre-acquisition and validation of stock data

 is dedicated to the FastAPI rectangular context to avoid a cycle of incident conflict

Args:
Stock code: Stock code
Market type: Market type ("A" equity, "Hong Kong equity", "Auto")
period days: length of historical data (days), default 30 days
Analysis date: date analysed, default today

Returns:
StockDataPreparationResult: Data Preparation Results
"""
    preparer = get_stock_preparer()

    #Use an in-house method using a different version
    if period_days is None:
        period_days = preparer.default_period_days

    if analysis_date is None:
        from datetime import datetime
        analysis_date = datetime.now().strftime('%Y-%m-%d')

    logger.info(f"[Data Preparation-Step ] Start preparing stock data:{stock_code}(Market:{market_type}, duration:{period_days}Oh, my God.")

    #1. Basic format validation (synchronous operations)
    format_result = preparer._validate_format(stock_code, market_type)
    if not format_result.is_valid:
        return format_result

    #2. Automatic detection of market types
    if market_type == "auto":
        market_type = preparer._detect_market_type(stock_code)
        logger.debug(f"📊 [Data Preparation - Step ] Automatic detection of market types:{market_type}")

    #3. Pre-acquire data and validate them (using a walker version)
    return await preparer._prepare_data_by_market_async(stock_code, market_type, period_days, analysis_date)


#Keep a backward compatible alias
StockValidator = StockDataPreparer
get_stock_validator = get_stock_preparer
validate_stock_exists = prepare_stock_data
is_stock_valid = is_stock_data_ready
get_stock_validation_message = get_stock_preparation_message
