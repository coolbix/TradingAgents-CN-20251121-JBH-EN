#!/usr/bin/env python3
"""Data Source Manager
Integrated management of selection and switching of Chinese stock data sources in support of Tushare, Akshare, BaoStock, etc.
"""

import os
import time
from typing import Dict, List, Optional, Any
from enum import Enum
import warnings
import pandas as pd
import numpy as np

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')
warnings.filterwarnings('ignore')

#Import Unified Log System
from tradingagents.utils.logging_init import setup_dataflow_logging
logger = setup_dataflow_logging()

#Import Unified Data Source Encoding
from tradingagents.constants import DataSourceCode


class ChinaDataSource(Enum):
    """Chinese Stock Data Sources

    Note: This count is synchronized with trafficas.constants. DataSurceCode
    Value using uniform data source code
    """
    MONGODB = DataSourceCode.MONGODB  #MongoDB database cache (highest priority)
    TUSHARE = DataSourceCode.TUSHARE
    AKSHARE = DataSourceCode.AKSHARE
    BAOSTOCK = DataSourceCode.BAOSTOCK


class USDataSource(Enum):
    """United States stock data source count

    Note: This count is synchronized with trafficas.constants. DataSurceCode
    Value using uniform data source code
    """
    MONGODB = DataSourceCode.MONGODB  #MongoDB database cache (highest priority)
    YFINANCE = DataSourceCode.YFINANCE  #Yahoo Finance (free, stock prices and technical indicators)
    ALPHA_VANTAGE = DataSourceCode.ALPHA_VANTAGE  #Alpha Vantage (basic and news)
    FINNHUB = DataSourceCode.FINNHUB  #Finnhub (back-up data source)





class DataSourceManager:
    """
    Data Source Manager
    NOTE: there is another DataSourceManager in app/services/data_sources/manager.py
    NOTE: consider unifying them in the future
    NOTE: DataSourceManager   is for China stock data sources only.
    NOTE: USDataSourceManager is for US stock data sources only.
    NOTE: consider unifying them in the future
    """

    def __init__(self):
        """Initialize data source manager"""
        #Check to enable the MongoDB cache
        self.use_mongodb_cache = self._check_if_use_mongodb_enabled()

        self.default_source = self._get_default_source()
        self.available_china_sources = self._check_available_china_data_sources()
        self.current_source = self.default_source

        #Initialise Unified Cache Manager
        self.cache_manager = None
        self.cache_enabled = False
        try:
            from .cache import get_cache
            self.cache_manager = get_cache()
            self.cache_enabled = True
            logger.info(f"Unified cache manager enabled")
        except Exception as e:
            logger.warning(f"Initialization of the Unified Cache Manager failed:{e}")

        logger.info(f"Initialization of data source manager completed")
        logger.info(f"MongoDB cache:{'Enabled' if self.use_mongodb_cache else 'It\'s not working.'}")
        logger.info(f"Unified Cache:{'Enabled' if self.cache_enabled else 'It\'s not working.'}")
        logger.info(f"Default data source:{self.default_source.value}")
        logger.info(f"Available data sources:{[s.value for s in self.available_china_sources]}")

    def _check_if_use_mongodb_enabled(self) -> bool:
        """Check if using MongoDB cache is enabled from runtime settings"""
        from tradingagents.config.runtime_settings import is_use_app_cache_enabled
        return is_use_app_cache_enabled()

    def _get_data_source_priority_order(self, symbol: Optional[str] = None) -> List[ChinaDataSource]:
        """Data source prioritization from database (for downgrading)
        Args:
            Symbol: Equities code to identify market types (A/US/Hong Kong)

        Returns:
            List of data sources in order of priority (not including MongoDB because MongoDB is the highest priority)
        """
        #Identification of market types
        market_category = self._identify_market_category(symbol)

        try:
            #🔥 Read the data source configuration from the database (using sync client)
            from app.core.database import get_mongo_db_synchronous
            db = get_mongo_db_synchronous()
            config_collection = db.system_configs

            #Get the latest active configuration
            config_data = config_collection.find_one(
                {"is_active": True},
                sort=[("version", -1)]
            )

            if config_data and config_data.get('data_source_configs'):
                data_source_configs = config_data.get('data_source_configs', [])

                #🔥 Filters enabled data sources and filters by market category
                enabled_sources = []
                for ds in data_source_configs:
                    if not ds.get('enabled', True):
                        continue

                    #Check if data sources are current market classifications
                    market_categories = ds.get('market_categories', [])
                    if market_categories and market_category:
                        #If data sources are configured for market classifications, select only matching data sources
                        if market_category not in market_categories:
                            continue

                    enabled_sources.append(ds)

                #Sort by priority (the larger the number, the higher the priority)
                enabled_sources.sort(key=lambda x: x.get('priority', 0), reverse=True)

                #Convert to ChinaDataSource enumerator (using uniform code)
                source_mapping = {
                    DataSourceCode.TUSHARE: ChinaDataSource.TUSHARE,
                    DataSourceCode.AKSHARE: ChinaDataSource.AKSHARE,
                    DataSourceCode.BAOSTOCK: ChinaDataSource.BAOSTOCK,
                }

                result = []
                for ds in enabled_sources:
                    ds_type = ds.get('type', '').lower()
                    if ds_type in source_mapping:
                        source = source_mapping[ds_type]
                        #Excludes MongoDB (MongoDB is the highest priority and does not participate in downgrading)
                        if source != ChinaDataSource.MONGODB and source in self.available_china_sources:
                            result.append(source)

                if result:
                    logger.info(f"[Data Source Priority] Market ={market_category or 'All'}, read from the database:{[s.value for s in result]}")
                    return result
                else:
                    logger.warning(f"[Data Source Priority] Market ={market_category or 'All'}, there are no available data sources in the database configuration, use default order")
            else:
                logger.warning("⚠️ [Data Source Priority] There is no data source configuration in the database, using default order")
        except Exception as e:
            logger.warning(f"⚠️ [Data Source Priority] Reading from database failed:{e}, using default order")

        #Back to default order (compatibility)
        #Default order: AKshare > Tushare > BaoStock
        default_order = [
            ChinaDataSource.AKSHARE,
            ChinaDataSource.TUSHARE,
            ChinaDataSource.BAOSTOCK,
        ]
        #Return only available data sources
        return [s for s in default_order if s in self.available_china_sources]

    def _identify_market_category(self, symbol: Optional[str]) -> Optional[str]:
        """Identification of market classifications to which stock codes belong
        Args:
            symbol: stock code

        Returns:
            Market classification ID (a shares/us stocks/hk stocks) returns Noone if it cannot be identified
        """
        if not symbol:
            return None

        try:
            from tradingagents.utils.stock_utils import StockUtils, StockMarket

            market = StockUtils.identify_stock_market(symbol)

            #Map to market classification ID
            market_mapping = {
                StockMarket.CHINA_A: 'a_shares',
                StockMarket.US: 'us_stocks',
                StockMarket.HONG_KONG: 'hk_stocks',
            }

            category = market_mapping.get(market)
            if category:
                logger.debug(f"[Market Identification]{symbol} → {category}")
            return category
        except Exception as e:
            logger.warning(f"[Market Identification]{e}")
            return None

    def _get_default_source(self) -> ChinaDataSource:
        """Get Default Data Sources"""
        #MongoDB as the highest priority data source if the MongoDB cache is enabled
        if self.use_mongodb_cache:
            return ChinaDataSource.MONGODB

        #Obtain from environmental variables, use AKShare as the first priority data source by default
        env_source = os.getenv('DEFAULT_CHINA_DATA_SOURCE', DataSourceCode.AKSHARE).lower()

        #Map to Enumeration (Use Harmonized Encoding)
        source_mapping = {
            DataSourceCode.TUSHARE: ChinaDataSource.TUSHARE,
            DataSourceCode.AKSHARE: ChinaDataSource.AKSHARE,
            DataSourceCode.BAOSTOCK: ChinaDataSource.BAOSTOCK,
        }

        return source_mapping.get(env_source, ChinaDataSource.AKSHARE)

    #== sync, corrected by elderman == @elder man

    def get_china_stock_data_tushare(self, symbol: str, start_date: str, end_date: str) -> str:
        """Using Tushare to access Chinese stock A historical data
        Args:
            symbol: stock code
            Start date: Start date
            End date: End date

        Returns:
            str: Formatted Stock Data Reports
        """
        #Switch to Tushare Data Source
        original_source = self.current_source
        self.current_source = ChinaDataSource.TUSHARE

        try:
            result = self._get_tushare_data(symbol, start_date, end_date)
            return result
        finally:
            #Restore raw data source
            self.current_source = original_source

    def get_fundamentals_data(self, symbol: str) -> str:
        """Obtain basic face data to support multiple data sources and automatic downgrade
        Priority: MongoDB →Tushare →AKShare → Generate Analysis

        Args:
            symbol: stock code

        Returns:
            str: Basic analysis reports
        """
        logger.info(f"[Data source:{self.current_source.value}Start access to basic data:{symbol}",
                   extra={
                       'symbol': symbol,
                       'data_source': self.current_source.value,
                       'event_type': 'fundamentals_fetch_start'
                   })

        start_time = time.time()

        try:
            #Call for appropriate acquisition methods based on data sources
            if self.current_source == ChinaDataSource.MONGODB:
                result = self._get_mongodb_fundamentals(symbol)
            elif self.current_source == ChinaDataSource.TUSHARE:
                result = self._get_tushare_fundamentals(symbol)
            elif self.current_source == ChinaDataSource.AKSHARE:
                result = self._get_akshare_fundamentals(symbol)
            else:
                #Other data sources do not support fundamental data to generate basic analysis
                result = self._generate_fundamentals_analysis(symbol)

            #Check results
            duration = time.time() - start_time
            result_length = len(result) if result else 0

            if result and "❌" not in result:
                logger.info(f"[Data source:{self.current_source.value}:: Successful access to basic data:{symbol} ({result_length}Character, time-consuming{duration:.2f}sec)",
                           extra={
                               'symbol': symbol,
                               'data_source': self.current_source.value,
                               'duration': duration,
                               'result_length': result_length,
                               'event_type': 'fundamentals_fetch_success'
                           })
                return result
            else:
                logger.warning(f"[Data source:{self.current_source.value}Basic data quality abnormal, trying to downgrade:{symbol}",
                              extra={
                                  'symbol': symbol,
                                  'data_source': self.current_source.value,
                                  'event_type': 'fundamentals_fetch_fallback'
                              })
                return self._try_fallback_fundamentals(symbol)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[Data source:{self.current_source.value}Could not close temporary folder: %s{symbol} - {e}",
                        extra={
                            'symbol': symbol,
                            'data_source': self.current_source.value,
                            'duration': duration,
                            'error': str(e),
                            'event_type': 'fundamentals_fetch_exception'
                        }, exc_info=True)
            return self._try_fallback_fundamentals(symbol)

    def get_china_stock_fundamentals_tushare(self, symbol: str) -> str:
        """Access to Chinese stock fundamentals using Tushare (old interface compatible)
        Args:
            symbol: stock code

        Returns:
            str: Basic analysis reports
        """
        #Redirect to Unified Interface
        return self._get_tushare_fundamentals(symbol)

    def get_news_data(self, symbol: str = None, hours_back: int = 24, limit: int = 20) -> List[Dict[str, Any]]:
        """A unified interface for access to news data to support multiple data sources and automatic downgrading
        Priority: MongoDB → Tushare → AKShare

        Args:
            Symbol: stock code, market news for empty
            Hours back: backtrace hours
            Limited number of returns

        Returns:
            List [Dict]: News Data List
        """
        logger.info(f"[Data source:{self.current_source.value}Start access to news data:{symbol or 'Market News'}Backtracking{hours_back}Hours",
                   extra={
                       'symbol': symbol,
                       'hours_back': hours_back,
                       'limit': limit,
                       'data_source': self.current_source.value,
                       'event_type': 'news_fetch_start'
                   })

        start_time = time.time()

        try:
            #Call for appropriate acquisition methods based on data sources
            if self.current_source == ChinaDataSource.MONGODB:
                result = self._get_mongodb_news(symbol, hours_back, limit)
            elif self.current_source == ChinaDataSource.TUSHARE:
                result = self._get_tushare_news(symbol, hours_back, limit)
            elif self.current_source == ChinaDataSource.AKSHARE:
                result = self._get_akshare_news(symbol, hours_back, limit)
            else:
                #Other data sources are not supporting news data for the time being
                logger.warning(f"Data source ⚠️{self.current_source.value}Information data not supported")
                result = []

            #Check results
            duration = time.time() - start_time
            result_count = len(result) if result else 0

            if result and result_count > 0:
                logger.info(f"[Data source:{self.current_source.value}Successful access to news data:{symbol or 'Market News'} ({result_count}Article, time-consuming{duration:.2f}sec)",
                           extra={
                               'symbol': symbol,
                               'data_source': self.current_source.value,
                               'news_count': result_count,
                               'duration': duration,
                               'event_type': 'news_fetch_success'
                           })
                return result
            else:
                logger.warning(f"[Data source:{self.current_source.value}No news data available:{symbol or 'Market News'}Try to downgrade",
                              extra={
                                  'symbol': symbol,
                                  'data_source': self.current_source.value,
                                  'duration': duration,
                                  'event_type': 'news_fetch_fallback'
                              })
                return self._try_fallback_news(symbol, hours_back, limit)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[Data source:{self.current_source.value}Can not get folder: %s: %s{symbol or 'Market News'} - {e}",
                        extra={
                            'symbol': symbol,
                            'data_source': self.current_source.value,
                            'duration': duration,
                            'error': str(e),
                            'event_type': 'news_fetch_exception'
                        }, exc_info=True)
            return self._try_fallback_news(symbol, hours_back, limit)

    def _check_available_china_data_sources(self) -> List[ChinaDataSource]:
        """Check available data sources
        Check logic:
        1. Inspection of the installation of dependent packages (technical availability)
        2. Check whether the database configuration is enabled (business availability)

        Returns:
            List of available and enabled data sources
        """
        available = []

        #🔥 Read the data source configuration from the database, get access status
        enabled_sources_in_db = set()
        try:
            from app.core.database import get_mongo_db_synchronous
            db = get_mongo_db_synchronous()
            config_collection = db.system_configs

            #Get the latest active configuration
            config_data = config_collection.find_one(
                {"is_active": True},
                sort=[("version", -1)]
            )

            if config_data and config_data.get('data_source_configs'):
                data_source_configs = config_data.get('data_source_configs', [])
                #Extracting enabled data source type
                for ds in data_source_configs:
                    if ds.get('enabled', True):
                        ds_type = ds.get('type', '').lower()
                        enabled_sources_in_db.add(ds_type)
                logger.info(f"✅ [Data Source Configuration] Read enabled data sources list from database:{enabled_sources_in_db}")
            else:
                logger.warning("⚠️ [Data source configuration] There is no data source configuration in the database and all installed data sources will be checked")
                #Default all data sources enabled if database is not configured
                enabled_sources_in_db = {'mongodb', 'tushare', 'akshare', 'baostock'}
        except Exception as e:
            logger.warning(f"[Data source configuration] Failed to read from database:{e}, will check all installed data sources")
            #Default all data sources enabled if reading failed
            enabled_sources_in_db = {'mongodb', 'tushare', 'akshare', 'baostock'}

        #Check MongoDB (highest priority)
        if self.use_mongodb_cache and 'mongodb' in enabled_sources_in_db:
            try:
                from tradingagents.dataflows.cache.mongodb_cache_adapter import get_mongodb_cache_adapter
                adapter = get_mongodb_cache_adapter()
                if adapter.use_app_cache and adapter.db is not None:
                    available.append(ChinaDataSource.MONGODB)
                    logger.info("✅MongoDB data sources are available and enabled (highest priority)")
                else:
                    logger.warning("MongoDB data source not available: database not connected")
            except Exception as e:
                logger.warning(f"MongoDB data sources are not available:{e}")
        elif self.use_mongodb_cache and 'mongodb' not in enabled_sources_in_db:
            logger.info("ℹ️ MongoDB data source is NOT enabled in database")

        #Read data source configuration from the database
        datasource_configs = self._get_datasource_configs_from_db()
        """
        {'akshare':       {'api_key': '', 'api_secret': '', 'config_params': {}},
         'tushare':       {'api_key': '', 'api_secret': '', 'config_params': {}},
         'finnhub':       {'api_key': '', 'api_secret': '', 'config_params': {}},
         'baostock':      {'api_key': '', 'api_secret': '', 'config_params': {}},
         'alpha_vantage': {'api_key': '', 'api_secret': '', 'config_params': {}},
         'yahoo_finance': {'api_key': '', 'api_secret': '', 'config_params': {}}}
        """

        #Check Tushare.
        if 'tushare' in enabled_sources_in_db:
            try:
                import tushare as ts
                #Prefer API Key to database configuration, followed by environment variables
                token = datasource_configs.get('tushare', {}).get('api_key') or os.getenv('TUSHARE_TOKEN')
                if token and not token.startswith('your_'):
                    available.append(ChinaDataSource.TUSHARE)
                    source = "数据库配置" if datasource_configs.get('tushare', {}).get('api_key') else "环境变量"
                    logger.info(f"Tushare data sources are available and enabled (API Key source:{source})")
                else:
                    logger.warning("⚠️ Tushare data source not available: API Key is not configured (no database and environmental variables are found)")
            except ImportError:
                logger.warning("Tushare data source not available: Library not installed")
        else:
            logger.info("ℹ️ Tushare data source disabled in database")

        #Check AK Share.
        if 'akshare' in enabled_sources_in_db:
            try:
                import akshare as ak
                available.append(ChinaDataSource.AKSHARE)
                logger.info("AKShare data source is available and enabled")
            except ImportError:
                logger.warning("AKShare data source not available: Library not installed")
        else:
            logger.info("AKShare data source has been disabled in the database")

        #Check BaoStock.
        if 'baostock' in enabled_sources_in_db:
            try:
                import baostock as bs
                available.append(ChinaDataSource.BAOSTOCK)
                logger.info(f"BaoStock data sources are available and enabled")
            except ImportError:
                logger.warning(f"BaoStock data source not available: Library not installed")
        else:
            logger.info("BaoStock data source has been disabled in the database")

        #TDX (Together) removed
        #Do not check and support TDX data sources

        return available

    def _get_datasource_configs_from_db(self) -> dict:
        """Read data source configuration from database (including API Key)"""
        try:
            from app.core.database import get_mongo_db_synchronous
            db = get_mongo_db_synchronous()

            #Read activated configurations from system configs
            config = db.system_configs.find_one({"is_active": True})
            if not config:
                return {}

            #Extracting data source configuration
            datasource_configs = config.get('data_source_configs', [])

            #Build Configuration Dictionary
            result = {}
            for ds_config in datasource_configs:
                name = ds_config.get('name', '').lower()
                result[name] = {
                    'api_key': ds_config.get('api_key', ''),
                    'api_secret': ds_config.get('api_secret', ''),
                    'config_params': ds_config.get('config_params', {})
                }

            return result
        except Exception as e:
            logger.warning(f"Access to data source configuration from database failed:{e}")
            return {}

    def get_current_source(self) -> ChinaDataSource:
        """Get Current Data Source"""
        return self.current_source

    def set_current_source(self, source: ChinaDataSource) -> bool:
        """Set Current Data Source"""
        if source in self.available_china_sources:
            self.current_source = source
            logger.info(f"The data source has been converted to:{source.value}")
            return True
        else:
            logger.error(f"Data sources are not available:{source.value}")
            return False

    def get_data_adapter(self):
        """Adapter to capture current data source"""
        if self.current_source == ChinaDataSource.MONGODB:
            return self._get_mongodb_adapter()
        elif self.current_source == ChinaDataSource.TUSHARE:
            return self._get_tushare_adapter()
        elif self.current_source == ChinaDataSource.AKSHARE:
            return self._get_akshare_adapter()
        elif self.current_source == ChinaDataSource.BAOSTOCK:
            return self._get_baostock_adapter()
        #TDX removed
        else:
            raise ValueError(f"不支持的数据源: {self.current_source}")

    def _get_mongodb_adapter(self):
        """Get MongoDB adapter"""
        try:
            from tradingagents.dataflows.cache.mongodb_cache_adapter import get_mongodb_cache_adapter
            return get_mongodb_cache_adapter()
        except ImportError as e:
            logger.error(f"The MongoDB adapter failed to import:{e}")
            return None

    def _get_tushare_adapter(self):
        """Access to Tushare provider (formerly obsolete, now directly using provider)"""
        try:
            from .providers.china.tushare import get_tushare_provider
            return get_tushare_provider()
        except ImportError as e:
            logger.error(f"The import of Tushare provider failed:{e}")
            return None

    def _get_akshare_adapter(self):
        """Get AKShare adapter"""
        try:
            from .providers.china.akshare import get_akshare_provider
            return get_akshare_provider()
        except ImportError as e:
            logger.error(f"The import of the AKShare adaptor failed:{e}")
            return None

    def _get_baostock_adapter(self):
        """Get the BaoStock adapter"""
        try:
            from .providers.china.baostock import get_baostock_provider
            return get_baostock_provider()
        except ImportError as e:
            logger.error(f"The import of BaoStock adapter failed:{e}")
            return None

    #TDX adapter removed
    # def _get_tdx_adapter(self):
    #""Getting TX adapter."
    #Logger.error (f "❌ TDX data source no longer supported")
    #     return None

    def _get_cached_data(self, symbol: str, start_date: str = None, end_date: str = None, max_age_hours: int = 24) -> Optional[pd.DataFrame]:
        """Fetch data from cache
        Args:
            symbol: stock code
            Start date: Start date
            End date: End date
            max age hours: maximum cache time (hours)

        Returns:
            DataFrame: Cache data, if not returnedNone
        """
        if not self.cache_enabled or not self.cache_manager:
            return None

        try:
            cache_key = self.cache_manager.find_cached_stock_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                max_age_hours=max_age_hours
            )

            if cache_key:
                cached_data = self.cache_manager.load_stock_data(cache_key)
                if cached_data is not None and hasattr(cached_data, 'empty') and not cached_data.empty:
                    logger.debug(f"Get it from the cache.{symbol}Data:{len(cached_data)}Article")
                    return cached_data
        except Exception as e:
            logger.warning(f"Reading data from the cache failed:{e}")

        return None

    def _save_to_cache(self, symbol: str, data: pd.DataFrame, start_date: str = None, end_date: str = None):
        """Save Data to Cache
        Args:
            symbol: stock code
            Data:
            Start date: Start date
            End date: End date
        """
        if not self.cache_enabled or not self.cache_manager:
            return

        try:
            if data is not None and hasattr(data, 'empty') and not data.empty:
                self.cache_manager.save_stock_data(symbol, data, start_date, end_date)
                logger.debug(f"Save{symbol}Data to cache:{len(data)}Article")
        except Exception as e:
            logger.warning(f"Could not close temporary folder: %s{e}")

    def _get_volume_safely(self, data: pd.DataFrame) -> float:
        """Secure access to traffic data
        Args:
            Data: Stock data DataFrame

        Returns:
            float: barter, return 0 if access failed
        """
        try:
            if 'volume' in data.columns:
                return data['volume'].iloc[-1]
            elif 'vol' in data.columns:
                return data['vol'].iloc[-1]
            else:
                return 0
        except Exception:
            return 0

    def _format_stock_data_response(self, data: pd.DataFrame, symbol: str, stock_name: str,
                                    start_date: str, end_date: str) -> str:
        """Formatting of stock data responses (including technical indicators)
        Args:
            Data: Stock data DataFrame
            symbol: stock code
            Stock name: Stock name
            Start date: Start date
            End date: End date

        Returns:
            str: Formatted data reports (including technical indicators)
        """
        try:
            original_data_count = len(data)
            logger.info(f"[Technical indicators]{original_data_count}Article")

            #🔧 Calculating Technical Indicators (using complete data)
            #Ensure that data are sorted by date
            if 'date' in data.columns:
                data = data.sort_values('date')

            #Calculate moving average lines
            data['ma5'] = data['close'].rolling(window=5, min_periods=1).mean()
            data['ma10'] = data['close'].rolling(window=10, min_periods=1).mean()
            data['ma20'] = data['close'].rolling(window=20, min_periods=1).mean()
            data['ma60'] = data['close'].rolling(window=60, min_periods=1).mean()

            #Calculating RSI (relative strength and weakness indicator) - Same flower style: using Chinese-style SMA (EMA with adjust=True)
            #Reference: https://blog.csdn.net/u011218867/articles/117427927
            #RSIs with the same flower/toucher use the SMA function at the same value as the newm of pandas (com=N-1, adjust=True)
            delta = data['close'].diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)

            #RSI6 - Use Chinese SMA
            avg_gain6 = gain.ewm(com=5, adjust=True).mean()  # com = N - 1
            avg_loss6 = loss.ewm(com=5, adjust=True).mean()
            rs6 = avg_gain6 / avg_loss6.replace(0, np.nan)
            data['rsi6'] = 100 - (100 / (1 + rs6))

            #RSI12 - Use Chinese SMA
            avg_gain12 = gain.ewm(com=11, adjust=True).mean()
            avg_loss12 = loss.ewm(com=11, adjust=True).mean()
            rs12 = avg_gain12 / avg_loss12.replace(0, np.nan)
            data['rsi12'] = 100 - (100 / (1 + rs12))

            #RSI24 - Use Chinese SMA
            avg_gain24 = gain.ewm(com=23, adjust=True).mean()
            avg_loss24 = loss.ewm(com=23, adjust=True).mean()
            rs24 = avg_gain24 / avg_loss24.replace(0, np.nan)
            data['rsi24'] = 100 - (100 / (1 + rs24))

            #Retain RSI14 as reference for international standards (use simple moving average)
            gain14 = gain.rolling(window=14, min_periods=1).mean()
            loss14 = loss.rolling(window=14, min_periods=1).mean()
            rs14 = gain14 / loss14.replace(0, np.nan)
            data['rsi14'] = 100 - (100 / (1 + rs14))

            #Compute MCD
            ema12 = data['close'].ewm(span=12, adjust=False).mean()
            ema26 = data['close'].ewm(span=26, adjust=False).mean()
            data['macd_dif'] = ema12 - ema26
            data['macd_dea'] = data['macd_dif'].ewm(span=9, adjust=False).mean()
            data['macd'] = (data['macd_dif'] - data['macd_dea']) * 2

            #Calculating Brink Belts
            data['boll_mid'] = data['close'].rolling(window=20, min_periods=1).mean()
            std = data['close'].rolling(window=20, min_periods=1).std()
            data['boll_upper'] = data['boll_mid'] + 2 * std
            data['boll_lower'] = data['boll_mid'] - 2 * std

            logger.info(f"✅ [Technical indicators]")

            #🔧 Only the last 3-5 days of data are retained for display (reduce token consumption)
            display_rows = min(5, len(data))
            display_data = data.tail(display_rows)
            latest_data = data.iloc[-1]

            #🔍 [Debug log] Prints raw data and technical indicators for the last five days
            logger.info(f"🔍 [Details on technical indicators] = = = most recent ={display_rows}Number of transactions")
            for i, (idx, row) in enumerate(display_data.iterrows(), 1):
                logger.info(f"🔍 [Details on technical indicators]{i}Day{row.get('date', 'N/A')}):")
                logger.info(f"Price: Open ={row.get('open', 0):.2f}High{row.get('high', 0):.2f}Low ={row.get('low', 0):.2f}, received ={row.get('close', 0):.2f}")
                logger.info(f"   MA: MA5={row.get('ma5', 0):.2f}, MA10={row.get('ma10', 0):.2f}, MA20={row.get('ma20', 0):.2f}, MA60={row.get('ma60', 0):.2f}")
                logger.info(f"   MACD: DIF={row.get('macd_dif', 0):.4f}, DEA={row.get('macd_dea', 0):.4f}, MACD={row.get('macd', 0):.4f}")
                logger.info(f"   RSI: RSI6={row.get('rsi6', 0):.2f}, RSI12={row.get('rsi12', 0):.2f}, RSI24={row.get('rsi24', 0):.2f}♪ Same flower style ♪")
                logger.info(f"   RSI14: {row.get('rsi14', 0):.2f}(International standards)")
                logger.info(f"BOLL: Up{row.get('boll_upper', 0):.2f}, Medium ={row.get('boll_mid', 0):.2f}, below{row.get('boll_lower', 0):.2f}")

            logger.info(f"🔍 [Technology indicator details] = = = data details end = = = = = =")

            #Calculating the latest prices and declines
            latest_price = latest_data.get('close', 0)
            prev_close = data.iloc[-2].get('close', latest_price) if len(data) > 1 else latest_price
            change = latest_price - prev_close
            change_pct = (change / prev_close * 100) if prev_close != 0 else 0

            #Formatting data reports
            result = f"📊 {stock_name}({symbol}) - 技术分析数据\n"
            result += f"数据期间: {start_date} 至 {end_date}\n"
            result += f"数据条数: {original_data_count}条 (展示最近{display_rows}个交易日)\n\n"

            result += f"💰 最新价格: ¥{latest_price:.2f}\n"
            result += f"📈 涨跌额: {change:+.2f} ({change_pct:+.2f}%)\n\n"

            #Add Technical Indicators
            result += f"📊 移动平均线 (MA):\n"
            result += f"   MA5:  ¥{latest_data['ma5']:.2f}"
            if latest_price > latest_data['ma5']:
                result += " (价格在MA5上方 ↑)\n"
            else:
                result += " (价格在MA5下方 ↓)\n"

            result += f"   MA10: ¥{latest_data['ma10']:.2f}"
            if latest_price > latest_data['ma10']:
                result += " (价格在MA10上方 ↑)\n"
            else:
                result += " (价格在MA10下方 ↓)\n"

            result += f"   MA20: ¥{latest_data['ma20']:.2f}"
            if latest_price > latest_data['ma20']:
                result += " (价格在MA20上方 ↑)\n"
            else:
                result += " (价格在MA20下方 ↓)\n"

            result += f"   MA60: ¥{latest_data['ma60']:.2f}"
            if latest_price > latest_data['ma60']:
                result += " (价格在MA60上方 ↑)\n\n"
            else:
                result += " (价格在MA60下方 ↓)\n\n"

            #MACD indicators
            result += f"📈 MACD指标:\n"
            result += f"   DIF:  {latest_data['macd_dif']:.3f}\n"
            result += f"   DEA:  {latest_data['macd_dea']:.3f}\n"
            result += f"   MACD: {latest_data['macd']:.3f}"
            if latest_data['macd'] > 0:
                result += " (多头 ↑)\n"
            else:
                result += " (空头 ↓)\n"

            #Fork of gold/death
            if len(data) > 1:
                prev_dif = data.iloc[-2]['macd_dif']
                prev_dea = data.iloc[-2]['macd_dea']
                curr_dif = latest_data['macd_dif']
                curr_dea = latest_data['macd_dea']

                if prev_dif <= prev_dea and curr_dif > curr_dea:
                    result += "   ⚠️ MACD金叉信号（DIF上穿DEA）\n\n"
                elif prev_dif >= prev_dea and curr_dif < curr_dea:
                    result += "   ⚠️ MACD死叉信号（DIF下穿DEA）\n\n"
                else:
                    result += "\n"
            else:
                result += "\n"

            #RSI indicator - same smooth style (6, 12, 24)
            rsi6 = latest_data['rsi6']
            rsi12 = latest_data['rsi12']
            rsi24 = latest_data['rsi24']
            result += f"📉 RSI指标 (同花顺风格):\n"
            result += f"   RSI6:  {rsi6:.2f}"
            if rsi6 >= 80:
                result += " (超买 ⚠️)\n"
            elif rsi6 <= 20:
                result += " (超卖 ⚠️)\n"
            else:
                result += "\n"

            result += f"   RSI12: {rsi12:.2f}"
            if rsi12 >= 80:
                result += " (超买 ⚠️)\n"
            elif rsi12 <= 20:
                result += " (超卖 ⚠️)\n"
            else:
                result += "\n"

            result += f"   RSI24: {rsi24:.2f}"
            if rsi24 >= 80:
                result += " (超买 ⚠️)\n"
            elif rsi24 <= 20:
                result += " (超卖 ⚠️)\n"
            else:
                result += "\n"

            #Figure RSI Trends
            if rsi6 > rsi12 > rsi24:
                result += "   趋势: 多头排列 ↑\n\n"
            elif rsi6 < rsi12 < rsi24:
                result += "   趋势: 空头排列 ↓\n\n"
            else:
                result += "   趋势: 震荡整理 ↔\n\n"

            #Blinks.
            result += f"📊 布林带 (BOLL):\n"
            result += f"   上轨: ¥{latest_data['boll_upper']:.2f}\n"
            result += f"   中轨: ¥{latest_data['boll_mid']:.2f}\n"
            result += f"   下轨: ¥{latest_data['boll_lower']:.2f}\n"

            #To determine where the price is in the Brink Belt.
            boll_position = (latest_price - latest_data['boll_lower']) / (latest_data['boll_upper'] - latest_data['boll_lower']) * 100
            result += f"   价格位置: {boll_position:.1f}%"
            if boll_position >= 80:
                result += " (接近上轨，可能超买 ⚠️)\n\n"
            elif boll_position <= 20:
                result += " (接近下轨，可能超卖 ⚠️)\n\n"
            else:
                result += " (中性区域)\n\n"

            #Price statistics
            result += f"📊 价格统计 (最近{display_rows}个交易日):\n"
            result += f"   最高价: ¥{display_data['high'].max():.2f}\n"
            result += f"   最低价: ¥{display_data['low'].min():.2f}\n"
            result += f"   平均价: ¥{display_data['close'].mean():.2f}\n"

            #Defensive access to traffic data
            volume_value = self._get_volume_safely(display_data)
            result += f"   平均成交量: {volume_value:,.0f}股\n"

            return result

        except Exception as e:
            logger.error(f"Formatting data response failed:{e}", exc_info=True)
            return f"❌ 格式化{symbol}数据失败: {e}"

    def get_stock_dataframe(self, symbol: str, start_date: str = None, end_date: str = None, period: str = "daily") -> pd.DataFrame:
        """DataFrame interface to capture stock data, support multiple data sources and automatically downgrade
        Args:
            symbol: stock code
            Start date: Start date
            End date: End date
            period: data cycle (daily/weekly/monthly), default is Daily

        Returns:
            DataFrame: DataFrame, column: open, high, low, close, vol, amount, date
        """
        logger.info(f"[DataFrame interface]{symbol} ({start_date}Present.{end_date})")

        try:
            #Try Current Data Source
            df = None
            if self.current_source == ChinaDataSource.MONGODB:
                from tradingagents.dataflows.cache.mongodb_cache_adapter import get_mongodb_cache_adapter
                adapter = get_mongodb_cache_adapter()
                df = adapter.get_historical_data(symbol, start_date, end_date, period=period)
            elif self.current_source == ChinaDataSource.TUSHARE:
                from .providers.china.tushare import get_tushare_provider
                provider = get_tushare_provider()
                df = provider.get_daily_data(symbol, start_date, end_date)
            elif self.current_source == ChinaDataSource.AKSHARE:
                from .providers.china.akshare import get_akshare_provider
                provider = get_akshare_provider()
                df = provider.get_stock_data(symbol, start_date, end_date)
            elif self.current_source == ChinaDataSource.BAOSTOCK:
                from .providers.china.baostock import get_baostock_provider
                provider = get_baostock_provider()
                df = provider.get_stock_data(symbol, start_date, end_date)

            if df is not None and not df.empty:
                logger.info(f"[DataFrame Interface] From{self.current_source.value}Success:{len(df)}Article")
                return self._standardize_dataframe(df)

            #Downgrade to other data sources
            logger.warning(f"[DataFrame Interface]{self.current_source.value}Failed. Try demotion.")
            for source in self.available_china_sources:
                if source == self.current_source:
                    continue
                try:
                    if source == ChinaDataSource.MONGODB:
                        from tradingagents.dataflows.cache.mongodb_cache_adapter import get_mongodb_cache_adapter
                        adapter = get_mongodb_cache_adapter()
                        df = adapter.get_historical_data(symbol, start_date, end_date, period=period)
                    elif source == ChinaDataSource.TUSHARE:
                        from .providers.china.tushare import get_tushare_provider
                        provider = get_tushare_provider()
                        df = provider.get_daily_data(symbol, start_date, end_date)
                    elif source == ChinaDataSource.AKSHARE:
                        from .providers.china.akshare import get_akshare_provider
                        provider = get_akshare_provider()
                        df = provider.get_stock_data(symbol, start_date, end_date)
                    elif source == ChinaDataSource.BAOSTOCK:
                        from .providers.china.baostock import get_baostock_provider
                        provider = get_baostock_provider()
                        df = provider.get_stock_data(symbol, start_date, end_date)

                    if df is not None and not df.empty:
                        logger.info(f"[DataFrame Interface]{source.value}Success:{len(df)}Article")
                        return self._standardize_dataframe(df)
                except Exception as e:
                    logger.warning(f"[DataFrame Interface]{source.value}Failed:{e}")
                    continue

            logger.error(f"All data sources failed:{symbol}")
            return pd.DataFrame()

        except Exception as e:
            logger.error(f"[DataFrame interface]{e}", exc_info=True)
            return pd.DataFrame()

    def _standardize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize DataFrame listing and format

        Args:
            df: Original DataFrame

        Returns:
            DataFrame: Standardized DataFrame
        """
        if df is None or df.empty:
            return pd.DataFrame()

        out = df.copy()

        #List Map
        colmap = {
            # English
            'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close',
            'Volume': 'vol', 'Amount': 'amount', 'symbol': 'code', 'Symbol': 'code',
            # Already lower
            'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close',
            'vol': 'vol', 'volume': 'vol', 'amount': 'amount', 'code': 'code',
            'date': 'date', 'trade_date': 'date',
            # Chinese (AKShare common)
            '日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close',
            '成交量': 'vol', '成交额': 'amount', '涨跌幅': 'pct_change', '涨跌额': 'change',
        }
        out = out.rename(columns={c: colmap.get(c, c) for c in out.columns})

        #Ensure date sorting
        if 'date' in out.columns:
            try:
                out['date'] = pd.to_datetime(out['date'])
                out = out.sort_values('date')
            except Exception:
                pass

        #Calculates increase or drop (if missing)
        if 'pct_change' not in out.columns and 'close' in out.columns:
            out['pct_change'] = out['close'].pct_change() * 100.0

        return out

    def get_stock_data(self, symbol: str, start_date: str = None, end_date: str = None, period: str = "daily") -> str:
        """Integrated interface to capture stock data to support multi-cycle data
        Args:
            symbol: stock code
            Start date: Start date
            End date: End date
            period: data cycle (daily/weekly/monthly), default is Daily

        Returns:
            str: Formatted Stock Data
        """
        #Record detailed input parameters
        logger.info(f"[Data source:{self.current_source.value}Start acquisition{period}Data:{symbol}",
                   extra={
                       'symbol': symbol,
                       'start_date': start_date,
                       'end_date': end_date,
                       'period': period,
                       'data_source': self.current_source.value,
                       'event_type': 'data_fetch_start'
                   })

        #Add detailed stock code tracking log
        logger.info(f"DataSurceManager.get stock data received stock code: '{symbol}' (type:{type(symbol)})")
        logger.info(f"[Equal code tracking]{len(str(symbol))}")
        logger.info(f"[Equal code tracking]{list(str(symbol))}")
        logger.info(f"Current data source:{self.current_source.value}")

        start_time = time.time()

        try:
            #Call for appropriate acquisition methods based on data sources
            actual_source = None  #Data sources actually used

            if self.current_source == ChinaDataSource.MONGODB:
                result, actual_source = self._get_mongodb_data(symbol, start_date, end_date, period)
            elif self.current_source == ChinaDataSource.TUSHARE:
                logger.info(f"[Equal code tracking]{symbol}', period='{period}'")
                result = self._get_tushare_data(symbol, start_date, end_date, period)
                actual_source = "tushare"
            elif self.current_source == ChinaDataSource.AKSHARE:
                result = self._get_akshare_data(symbol, start_date, end_date, period)
                actual_source = "akshare"
            elif self.current_source == ChinaDataSource.BAOSTOCK:
                result = self._get_baostock_data(symbol, start_date, end_date, period)
                actual_source = "baostock"
            #TDX removed
            else:
                result = f"❌ 不支持的数据源: {self.current_source.value}"
                actual_source = None

            #Record detailed output results
            duration = time.time() - start_time
            result_length = len(result) if result else 0
            is_success = result and "❌" not in result and "错误" not in result

            #Use the actual data source name or, if not, the current source
            display_source = actual_source or self.current_source.value

            if is_success:
                logger.info(f"[Data source:{display_source}Successful access to stock data:{symbol} ({result_length}Character, time-consuming{duration:.2f}sec)",
                           extra={
                               'symbol': symbol,
                               'start_date': start_date,
                               'end_date': end_date,
                               'data_source': display_source,
                               'actual_source': actual_source,
                               'requested_source': self.current_source.value,
                               'duration': duration,
                               'result_length': result_length,
                               'result_preview': result[:200] + '...' if result_length > 200 else result,
                               'event_type': 'data_fetch_success'
                           })
                return result
            else:
                logger.warning(f"[Data source:{self.current_source.value}Failed] Data quality abnormal, trying to downgrade to other data sources:{symbol}",
                              extra={
                                  'symbol': symbol,
                                  'start_date': start_date,
                                  'end_date': end_date,
                                  'data_source': self.current_source.value,
                                  'duration': duration,
                                  'result_length': result_length,
                                  'result_preview': result[:200] + '...' if result_length > 200 else result,
                                  'event_type': 'data_fetch_warning'
                              })

                #When data quality is abnormal, attempts are made to downgrade to other data sources.
                fallback_result = self._try_fallback_sources(symbol, start_date, end_date)
                if fallback_result and "❌" not in fallback_result and "错误" not in fallback_result:
                    logger.info(f"✅ [data source: secondary data source]{symbol}")
                    return fallback_result
                else:
                    logger.error(f"All data sources are unable to obtain valid data:{symbol}")
                    return result  #Returns original result (includes error information)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[data acquisition]{e}",
                        extra={
                            'symbol': symbol,
                            'start_date': start_date,
                            'end_date': end_date,
                            'data_source': self.current_source.value,
                            'duration': duration,
                            'error': str(e),
                            'event_type': 'data_fetch_exception'
                        }, exc_info=True)
            return self._try_fallback_sources(symbol, start_date, end_date)

    def _get_mongodb_data(self, symbol: str, start_date: str, end_date: str, period: str = "daily") -> tuple[str, str | None]:
        """Obtain multi-cycle data from MongoDB - with calculation of technical indicators
        Returns:
            tuple[str, str|None]: (result string, actual data source name)
        """
        logger.debug(f"[MongoDB] Call parameters: symbol={symbol}, start_date={start_date}, end_date={end_date}, period={period}")

        try:
            from tradingagents.dataflows.cache.mongodb_cache_adapter import get_mongodb_cache_adapter
            adapter = get_mongodb_cache_adapter()

            #Get historical data from MongoDB for the specified cycle
            df = adapter.get_historical_data(symbol, start_date, end_date, period=period)

            if df is not None and not df.empty:
                logger.info(f"✅ [Data source: MongoDB cache]{period}Data:{symbol} ({len(df)}(on file)")

                #🔧 fixation: using a uniform format, including technical indicators
                #Obtain stock names (draw or use default values from DataFrame)
                stock_name = f'股票{symbol}'
                if 'name' in df.columns and not df['name'].empty:
                    stock_name = df['name'].iloc[0]

                #Call for harmonized formatting (including technical indicator calculations)
                result = self._format_stock_data_response(df, symbol, stock_name, start_date, end_date)

                logger.info(f"[MongoDB] Calculated technical indicators: MA5/10/20/60, MACD, RSI, BOLL")
                return result, "mongodb"
            else:
                #MongoDB does not have data (detailed data sources are recorded internally) and downgrades to other data sources
                logger.info(f"[MongoDB]{period}Data:{symbol}Start trying the backup data source.")
                return self._try_fallback_sources(symbol, start_date, end_date, period)

        except Exception as e:
            logger.error(f"❌{period}Data failed:{symbol}, Error:{e}")
            #MongoDB anomaly down to other data sources
            return self._try_fallback_sources(symbol, start_date, end_date, period)

    def _get_tushare_data(self, symbol: str, start_date: str, end_date: str, period: str = "daily") -> str:
        """Get multi-cycle data using Tushare - use programr+ unified cache"""
        logger.debug(f"[Tushare] Call parameter: symbol={symbol}, start_date={start_date}, end_date={end_date}, period={period}")

        #Add detailed stock code tracking log
        logger.info(f"🔍 [Securities Code Tracking]  get tushare data received stock codes: '{symbol}' (type:{type(symbol)})")
        logger.info(f"[Equal code tracking]{len(str(symbol))}")
        logger.info(f"[Equal code tracking]{list(str(symbol))}")
        logger.info(f"get tushare data")
        logger.info(f"Current data source:{self.current_source.value}")

        start_time = time.time()
        try:
            #First try to get from the cache
            cached_data = self._get_cached_data(symbol, start_date, end_date, max_age_hours=24)
            if cached_data is not None and not cached_data.empty:
                logger.info(f"Get it from the cache.{symbol}Data")
                #Access to basic stock information
                provider = self._get_tushare_adapter()
                if provider:
                    import asyncio
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_closed():
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                    except RuntimeError:
                        #There is no cycle of events in the online pool. Create new
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)

                    stock_info = loop.run_until_complete(provider.get_stock_basic_info(symbol))
                    stock_name = stock_info.get('name', f'股票{symbol}') if stock_info else f'股票{symbol}'
                else:
                    stock_name = f'股票{symbol}'

                #Format Return
                return self._format_stock_data_response(cached_data, symbol, stock_name, start_date, end_date)

            #Cache outstanding, obtained from provider
            logger.info(f"[Share code tracking]{symbol}'")
            logger.info(f"[DatasourceManager Detailed Log]")

            provider = self._get_tushare_adapter()
            if not provider:
                return f"❌ Tushare提供器不可用"

            #Access to historical data using a walk method
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
            except RuntimeError:
                #There is no cycle of events in the online pool. Create new
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            data = loop.run_until_complete(provider.get_historical_data(symbol, start_date, end_date))

            if data is not None and not data.empty:
                #Save to Cache
                self._save_to_cache(symbol, data, start_date, end_date)

                #Access to basic stock information (shows)
                stock_info = loop.run_until_complete(provider.get_stock_basic_info(symbol))
                stock_name = stock_info.get('name', f'股票{symbol}') if stock_info else f'股票{symbol}'

                #Format Return
                result = self._format_stock_data_response(data, symbol, stock_name, start_date, end_date)

                duration = time.time() - start_time
                logger.info(f"[DataSourceManager Detailed Log]{duration:.3f}sec")
                logger.info(f"[Equal code tracking]{result[:200] if result else 'None'}")
                logger.debug(f"[Tushare]{duration:.2f}s, result length={len(result) if result else 0}")

                return result
            else:
                result = f"❌ 未获取到{symbol}的有效数据"
                duration = time.time() - start_time
                logger.warning(f"[Tushare] Data not available, time-consuming={duration:.2f}s")
                return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[Tushare] Call failed:{e}time ={duration:.2f}s", exc_info=True)
            logger.error(f"[DataSourceManager Detailed Log]{type(e).__name__}")
            logger.error(f"[DataSourceManager Detailed Log]{str(e)}")
            import traceback
            logger.error(f"[DataSourceManager Detailed Log]{traceback.format_exc()}")
            raise

    def _get_akshare_data(self, symbol: str, start_date: str, end_date: str, period: str = "daily") -> str:
        """Using AKShare to access multi-cycle data - including technical indicators"""
        logger.debug(f"[AKShare] Call parameter: symbol={symbol}, start_date={start_date}, end_date={end_date}, period={period}")

        start_time = time.time()
        try:
            #Use an AKShare unified interface
            from .providers.china.akshare import get_akshare_provider
            provider = get_akshare_provider()

            #Access to historical data using a walk method
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
            except RuntimeError:
                #There is no cycle of events in the online pool. Create new
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            data = loop.run_until_complete(provider.get_historical_data(symbol, start_date, end_date, period))

            duration = time.time() - start_time

            if data is not None and not data.empty:
                #🔧 fixation: using a uniform format, including technical indicators
                #Access to basic stock information
                stock_info = loop.run_until_complete(provider.get_stock_basic_info(symbol))
                stock_name = stock_info.get('name', f'股票{symbol}') if stock_info else f'股票{symbol}'

                #Call for harmonized formatting (including technical indicator calculations)
                result = self._format_stock_data_response(data, symbol, stock_name, start_date, end_date)

                logger.debug(f"[AKShare] Call success: Time-consuming ={duration:.2f}s, data bar ={len(data)}, result length ={len(result)}")
                logger.info(f"✅ [AKShare] Calculated technical indicators: MA5/10/20/60, MACD, RSI, BOLL")
                return result
            else:
                result = f"❌ 未能获取{symbol}的股票数据"
                logger.warning(f"[AKShare] Data is empty: time-consuming={duration:.2f}s")
                return result

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[AKShare] Call failed:{e}time ={duration:.2f}s", exc_info=True)
            return f"❌ AKShare获取{symbol}数据失败: {e}"

    def _get_baostock_data(self, symbol: str, start_date: str, end_date: str, period: str = "daily") -> str:
        """Obtain multi-cycle data using BaoStock - with technical indicators"""
        #Use the BaoStock unified interface
        from .providers.china.baostock import get_baostock_provider
        provider = get_baostock_provider()

        #Access to historical data using a walk method
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
        except RuntimeError:
            #There is no cycle of events in the online pool. Create new
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        data = loop.run_until_complete(provider.get_historical_data(symbol, start_date, end_date, period))

        if data is not None and not data.empty:
            #🔧 fixation: using a uniform format, including technical indicators
            #Access to basic stock information
            stock_info = loop.run_until_complete(provider.get_stock_basic_info(symbol))
            stock_name = stock_info.get('name', f'股票{symbol}') if stock_info else f'股票{symbol}'

            #Call for harmonized formatting (including technical indicator calculations)
            result = self._format_stock_data_response(data, symbol, stock_name, start_date, end_date)

            logger.info(f"✅ [BaoStock] Calculated technical indicators: MA5/10/20/60, MACD, RSI, BOLL")
            return result
        else:
            return f"❌ 未能获取{symbol}的股票数据"

    #TDX data acquisition method removed
    # def _get_tdx_data(self, symbol: str, start_date: str, end_date: str, period: str = "daily") -> str:
    #""Use TDX to get multi-cycle data."
    #Logger.error (f "❌ TDX data source no longer supported")
    #Turn f "The TDX data source is no longer supported"

    def _get_volume_safely(self, data) -> float:
        """Secure access to traffic data to support multiple listings"""
        try:
            #Support for multiple possible trade listings
            volume_columns = ['volume', 'vol', 'turnover', 'trade_volume']

            for col in volume_columns:
                if col in data.columns:
                    logger.info(f"We've found the trader:{col}")
                    return data[col].sum()

            #If you don't find it, record a warning and return zero.
            logger.warning(f"⚠️ has not been found and can be found in:{list(data.columns)}")
            return 0

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return 0

    def _try_fallback_sources(self, symbol: str, start_date: str, end_date: str, period: str = "daily") -> tuple[str, str | None]:
        """Try backup data source - avoid recursive callback

        Returns:
            tuple[str, str|None]: (result string, actual data source name)
        """
        logger.info(f"🔄 [{self.current_source.value}Failure to attempt secondary data source acquisition{period}Data:{symbol}")

        # Data source priority (identify market according to stock code)
        #Note: MongoDB is not included because MongoDB is the highest priority and does not try if it fails
        fallback_order = self._get_data_source_priority_order(symbol)

        for source in fallback_order:
            if source != self.current_source and source in self.available_china_sources:
                try:
                    logger.info(f"[Reserve data source]{source.value}Access{period}Data:{symbol}")

                    #Directly calls specific data source methods to avoid regression
                    if source == ChinaDataSource.TUSHARE:
                        result = self._get_tushare_data(symbol, start_date, end_date, period)
                    elif source == ChinaDataSource.AKSHARE:
                        result = self._get_akshare_data(symbol, start_date, end_date, period)
                    elif source == ChinaDataSource.BAOSTOCK:
                        result = self._get_baostock_data(symbol, start_date, end_date, period)
                    #TDX removed
                    else:
                        logger.warning(f"Unknown data source:{source.value}")
                        continue

                    if "❌" not in result:
                        logger.info(f"[Reserve data source--]{source.value}Successful access{period}Data:{symbol}")
                        return result, source.value  #Returns results and actual data sources used
                    else:
                        logger.warning(f"[Reserve data source--]{source.value}Return error result:{symbol}")

                except Exception as e:
                    logger.error(f"[Reserve data source--]{source.value}Acquisition failure:{symbol}, Error:{e}")
                    continue

        logger.error(f"[All data sources fail]{period}Data:{symbol}")
        return f"❌ 所有数据源都无法获取{symbol}的{period}数据", None

    def get_stock_info(self, symbol: str) -> Dict:
        """Access to stock basic information to support multiple data sources and automatic downgrading
        Priority: MongoDB → Tushare → Akshare → BaoStock
        """
        logger.info(f"[Data source:{self.current_source.value}Start access to stock information:{symbol}")

        #Prefer App Mongo cache (when ta use app cache=True)
        try:
            from tradingagents.config.runtime_settings import is_use_app_cache_enabled  # type: ignore
            use_cache = is_use_app_cache_enabled(False)
            logger.info(f"🔧 [configuration check] use app cache enabled() returns value:{use_cache}")
        except Exception as e:
            logger.error(f"❌ [configuration check] use app cache enabled() call failed:{e}", exc_info=True)
            use_cache = False

        logger.info(f"[Assigning]{use_cache}, current_source={self.current_source.value}")

        if use_cache:

            try:
                from .cache.app_adapter import get_basics_from_cache, get_market_quote_dataframe
                doc = get_basics_from_cache(symbol)
                if doc:
                    name = doc.get('name') or doc.get('stock_name') or ''
                    #Regulating the industry and the plate (avoiding miscalculation of the value of the board, such as the "Small/Starboard" sector)
                    board_labels = {'主板', '中小板', '创业板', '科创板'}
                    raw_industry = (doc.get('industry') or doc.get('industry_name') or '').strip()
                    sec_or_cat = (doc.get('sec') or doc.get('category') or '').strip()
                    market_val = (doc.get('market') or '').strip()
                    industry_val = raw_industry or sec_or_cat or '未知'
                    changed = False
                    if raw_industry in board_labels:
                        #If industry is the name of the plate, it is used as a market; industry is changed to a more detailed classification (sec/category)
                        if not market_val:
                            market_val = raw_industry
                            changed = True
                        if sec_or_cat:
                            industry_val = sec_or_cat
                            changed = True
                    if changed:
                        try:
                            logger.debug(f"🔧{raw_industry}♪ Industry ♪{industry_val}', market/board ='{market_val or doc.get('market', 'Unknown')}'")
                        except Exception:
                            pass

                    result = {
                        'symbol': symbol,
                        'name': name or f'股票{symbol}',
                        'area': doc.get('area', '未知'),
                        'industry': industry_val or '未知',
                        'market': market_val or doc.get('market', '未知'),
                        'list_date': doc.get('list_date', '未知'),
                        'source': 'app_cache'
                    }
                    #Additional fast-tracking (if available)
                    try:
                        df = get_market_quote_dataframe(symbol)
                        if df is not None and not df.empty:
                            row = df.iloc[-1]
                            result['current_price'] = row.get('close')
                            result['change_pct'] = row.get('pct_chg')
                            result['volume'] = row.get('volume')
                            result['quote_date'] = row.get('date')
                            result['quote_source'] = 'market_quotes'
                            logger.info(f"[Equal Information]{result['current_price']} pct={result['change_pct']} vol={result['volume']} code={symbol}")
                    except Exception as _e:
                        logger.debug(f"Additional line failed (neglect):{_e}")

                    if name:
                        logger.info(f"[Data source: MongoDB-stock basic info]{symbol}")
                        return result
                    else:
                        logger.warning(f"[Data source: MongoDB] No valid name found:{symbol}down to other data sources")
            except Exception as e:
                logger.error(f"❌ [Data source: MongoDB anomaly] Failed to access stock information:{e}", exc_info=True)


        #First try the current data source
        try:
            if self.current_source == ChinaDataSource.TUSHARE:
                from .interface import get_china_stock_info_tushare
                info_str = get_china_stock_info_tushare(symbol)
                result = self._parse_stock_info_string(info_str, symbol)

                #Check for access to valid information
                if result.get('name') and result['name'] != f'股票{symbol}':
                    logger.info(f"✅ [data source: Tushare-Equities Information]{symbol}")
                    return result
                else:
                    logger.warning(f"Could not close temporary folder: %s{symbol}")
                    return self._try_fallback_stock_info(symbol)
            else:
                adapter = self.get_data_adapter()
                if adapter and hasattr(adapter, 'get_stock_info'):
                    result = adapter.get_stock_info(symbol)
                    if result.get('name') and result['name'] != f'股票{symbol}':
                        logger.info(f"[Data source:{self.current_source.value}Other Organiser{symbol}")
                        return result
                    else:
                        logger.warning(f"[Data source:{self.current_source.value}Could not close temporary folder: %s{symbol}")
                        return self._try_fallback_stock_info(symbol)
                else:
                    logger.warning(f"[Data source:{self.current_source.value}:: Not supporting access to stock information, trying to downgrade:{symbol}")
                    return self._try_fallback_stock_info(symbol)

        except Exception as e:
            logger.error(f"[Data source:{self.current_source.value}Could not close temporary folder: %s{e}", exc_info=True)
            return self._try_fallback_stock_info(symbol)

    def get_stock_basic_info(self, stock_code: str = None) -> Optional[Dict[str, Any]]:
        """Access to stock base information (compatible stock data service interface)
        Args:
            Stock code: Stock code, return all stock lists if None

        Returns:
            Dict: Stock Dictionary, or error words containing error fields General
        """
        if stock_code is None:
            #Returns all stock lists
            logger.info("Get all the stock lists.")
            try:
                #Try to get from MongoDB
                from tradingagents.config.database_manager import get_database_manager
                db_manager = get_database_manager()
                if db_manager and db_manager.is_mongodb_available():
                    collection = db_manager.mongodb_db['stock_basic_info']
                    stocks = list(collection.find({}, {'_id': 0}))
                    if stocks:
                        logger.info(f"All shares obtained from MongoDB:{len(stocks)}Article")
                        return stocks
            except Exception as e:
                logger.warning(f"All shares from MongoDB failed:{e}")

            #Downgrade: return empty list
            return []

        #Can not open message
        try:
            result = self.get_stock_info(stock_code)
            if result and result.get('name'):
                return result
            else:
                return {'error': f'未找到股票 {stock_code} 的信息'}
        except Exception as e:
            logger.error(f"This post is part of our special coverage Egypt Protests 2011.{e}")
            return {'error': str(e)}

    def get_stock_data_with_fallback(self, stock_code: str, start_date: str, end_date: str) -> str:
        """Get Stock Data (compatible stock data service interface)
        Args:
            Stock code: Stock code
            Start date: Start date
            End date: End date

        Returns:
            str: Formatted Stock Data Reports
        """
        logger.info(f"Access to stock data:{stock_code} ({start_date}Present.{end_date})")

        try:
            #Use of unified data access interfaces
            return self.get_stock_data(stock_code, start_date, end_date)
        except Exception as e:
            logger.error(f"@❌> Failed to access stock data:{e}")
            return f"❌ 获取股票数据失败: {str(e)}\n\n💡 建议：\n1. 检查网络连接\n2. 确认股票代码格式正确\n3. 检查数据源配置"

    def _try_fallback_stock_info(self, symbol: str) -> Dict:
        """Try using a backup data source to obtain basic stock information"""
        logger.error(f"🔄 {self.current_source.value}Failed to attempt backup data source for stock information...")

        #Get all available data sources
        available_sources = self.available_china_sources.copy()

        #Remove the current data source
        if self.current_source.value in available_sources:
            available_sources.remove(self.current_source.value)

        #Try all the backup data sources
        for source_name in available_sources:
            try:
                source = ChinaDataSource(source_name)
                logger.info(f"🔄 trying to get stock information from a secondary data source:{source_name}")

                #Obtain share information by data source type
                if source == ChinaDataSource.TUSHARE:
                    #Direct call to Tushare adapter to avoid recycling
                    result = self._get_tushare_stock_info(symbol)
                elif source == ChinaDataSource.AKSHARE:
                    result = self._get_akshare_stock_info(symbol)
                elif source == ChinaDataSource.BAOSTOCK:
                    result = self._get_baostock_stock_info(symbol)
                else:
                    #Try Universal Adapter
                    original_source = self.current_source
                    self.current_source = source
                    adapter = self.get_data_adapter()
                    self.current_source = original_source

                    if adapter and hasattr(adapter, 'get_stock_info'):
                        result = adapter.get_stock_info(symbol)
                    else:
                        logger.warning(f"[Equal Information]{source_name}Cannot initialise Evolution's mail component.")
                        continue

                #Check for access to valid information
                if result.get('name') and result['name'] != f'股票{symbol}':
                    logger.info(f"✅ [data source: backup data source] Successfully obtained stock information:{source_name}")
                    return result
                else:
                    logger.warning(f"[Data source:{source_name}Return invalid information")

            except Exception as e:
                logger.error(f"Alternative data source ❌{source_name}Failed:{e}")
                continue

        #All data sources failed to return the default
        logger.error(f"All data sources are unavailable.{symbol}Share information")
        return {'symbol': symbol, 'name': f'股票{symbol}', 'source': 'unknown'}

    def _get_akshare_stock_info(self, symbol: str) -> Dict:
        """Use AKShare for basic stock information
        Important: AKshare needs to distinguish between stocks and indices
        - For 000001, it is recognized as Shenzhen.
        - For equities, full code is required (e.g. sz00001 or ssh60000)
        """
        try:
            import akshare as ak

            #Share code converted to AKShare
            #AKShare's stock indigenous info em requires "sz00001" or "sh60000" formats
            if symbol.startswith('6'):
                #Shanghai Stock: 600,000 - > ssh600,000
                akshare_symbol = f"sh{symbol}"
            elif symbol.startswith(('0', '3', '2')):
                #Shenzhen stocks: 000001 - > sz00001
                akshare_symbol = f"sz{symbol}"
            elif symbol.startswith(('8', '4')):
                #Beijing stocks: 830,000 - > bj830,000
                akshare_symbol = f"bj{symbol}"
            else:
                #In other cases, use the original code directly
                akshare_symbol = symbol

            logger.debug(f"Original code:{symbol}, AKShare format:{akshare_symbol}")

            #Try to get a stock information
            stock_info = ak.stock_individual_info_em(symbol=akshare_symbol)

            if stock_info is not None and not stock_info.empty:
                #Convert to Dictionary Format
                info = {'symbol': symbol, 'source': 'akshare'}

                #Extract stock name
                name_row = stock_info[stock_info['item'] == '股票简称']
                if not name_row.empty:
                    stock_name = name_row['value'].iloc[0]
                    info['name'] = stock_name
                    logger.info(f"[Akshare Stock Info]{symbol} -> {stock_name}")
                else:
                    info['name'] = f'股票{symbol}'
                    logger.warning(f"[Akshare Stock Information] No short names of shares found:{symbol}")

                #Can not open message
                info['area'] = '未知'  #AKShare has no area information.
                info['industry'] = '未知'  #Available through other APIs
                info['market'] = '未知'  #It can be inferred from stock code.
                info['list_date'] = '未知'  #Available through other APIs

                return info
            else:
                logger.warning(f"[Akshare Stock Information]{symbol}")
                return {'symbol': symbol, 'name': f'股票{symbol}', 'source': 'akshare'}

        except Exception as e:
            logger.error(f"[Equal Information] AKShare has failed to access:{symbol}, Error:{e}")
            return {'symbol': symbol, 'name': f'股票{symbol}', 'source': 'akshare', 'error': str(e)}

    def _get_baostock_stock_info(self, symbol: str) -> Dict:
        """Access to basic stock information using BaoStock"""
        try:
            import baostock as bs

            #Convert stock code format
            if symbol.startswith('6'):
                bs_code = f"sh.{symbol}"
            else:
                bs_code = f"sz.{symbol}"

            #Login BaoStock
            lg = bs.login()
            if lg.error_code != '0':
                logger.error(f"BaoStock login failed:{lg.error_msg}")
                return {'symbol': symbol, 'name': f'股票{symbol}', 'source': 'baostock'}

            #Search for stock base information
            rs = bs.query_stock_basic(code=bs_code)
            if rs.error_code != '0':
                bs.logout()
                logger.error(f"BaoStock failed:{rs.error_msg}")
                return {'symbol': symbol, 'name': f'股票{symbol}', 'source': 'baostock'}

            #Parsing Results
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())

            #Logout
            bs.logout()

            if data_list:
                #BaoStock returns format: [code, code name, ipoDate, outDate, type, status]
                info = {'symbol': symbol, 'source': 'baostock'}
                info['name'] = data_list[0][1]  # code_name
                info['area'] = '未知'  #BaoStock has no area information.
                info['industry'] = '未知'  #BaoStock has no industry information.
                info['market'] = '未知'  #It can be inferred from stock code.
                info['list_date'] = data_list[0][2]  # ipoDate

                return info
            else:
                return {'symbol': symbol, 'name': f'股票{symbol}', 'source': 'baostock'}

        except Exception as e:
            logger.error(f"BaoStock failed:{e}")
            return {'symbol': symbol, 'name': f'股票{symbol}', 'source': 'baostock', 'error': str(e)}

    def _parse_stock_info_string(self, info_str: str, symbol: str) -> Dict:
        """Parsing stock information string as dictionary"""
        try:
            info = {'symbol': symbol, 'source': self.current_source.value}
            lines = info_str.split('\n')

            for line in lines:
                if ':' in line:
                    key, value = line.split(':', 1)
                    key = key.strip()
                    value = value.strip()

                    if '股票名称' in key:
                        info['name'] = value
                    elif '所属行业' in key:
                        info['industry'] = value
                    elif '所属地区' in key:
                        info['area'] = value
                    elif '上市市场' in key:
                        info['market'] = value
                    elif '上市日期' in key:
                        info['list_date'] = value

            return info

        except Exception as e:
            logger.error(f"This post is part of our special coverage Egypt Protests 2011.{e}")
            return {'symbol': symbol, 'name': f'股票{symbol}', 'source': self.current_source.value}

    #== sync, corrected by elderman == @elder man

    def _get_mongodb_fundamentals(self, symbol: str) -> str:
        """Fetch financial data from MongoDB"""
        logger.debug(f"[MongoDB] Call parameters: symbol={symbol}")

        try:
            from tradingagents.dataflows.cache.mongodb_cache_adapter import get_mongodb_cache_adapter
            import pandas as pd
            adapter = get_mongodb_cache_adapter()

            #Fetch financial data from MongoDB
            financial_data = adapter.get_financial_data(symbol)

            #Check data type and content
            if financial_data is not None:
                #If DataFrame, convert to dictionary list
                if isinstance(financial_data, pd.DataFrame):
                    if not financial_data.empty:
                        logger.info(f"✅ [data source: MongoDB-financial data]{symbol} ({len(financial_data)}(on file)")
                        #Convert to dictionary list
                        financial_dict_list = financial_data.to_dict('records')
                        #Format financial data for reporting
                        return self._format_financial_data(symbol, financial_dict_list)
                    else:
                        logger.warning(f"[Source: MongoDB] Financial data are empty:{symbol}down to other data sources")
                        return self._try_fallback_fundamentals(symbol)
                #If List
                elif isinstance(financial_data, list) and len(financial_data) > 0:
                    logger.info(f"✅ [data source: MongoDB-financial data]{symbol} ({len(financial_data)}(on file)")
                    return self._format_financial_data(symbol, financial_data)
                #If a single dictionary (this is the format actually returned by MongoDB)
                elif isinstance(financial_data, dict):
                    logger.info(f"✅ [data source: MongoDB-financial data]{symbol}(single record)")
                    #Pack a single dictionary into a list
                    financial_dict_list = [financial_data]
                    return self._format_financial_data(symbol, financial_dict_list)
                else:
                    logger.warning(f"[Data source: MongoDB] No financial data found:{symbol}down to other data sources")
                    return self._try_fallback_fundamentals(symbol)
            else:
                logger.warning(f"[Data source: MongoDB] No financial data found:{symbol}down to other data sources")
                #MongoDB No data, downgrade to other data sources
                return self._try_fallback_fundamentals(symbol)

        except Exception as e:
            logger.error(f"[Data source: MongoDB anomaly]{e}", exc_info=True)
            #MongoDB anomaly down to other data sources
            return self._try_fallback_fundamentals(symbol)

    def _get_tushare_fundamentals(self, symbol: str) -> str:
        """Fetch basic face data from Tushare - not available for the time being, need to be achieved"""
        logger.warning(f"Tushare Basic Data is temporarily unavailable")
        return f"⚠️ Tushare基本面数据功能暂时不可用，请使用其他数据源"

    def _get_akshare_fundamentals(self, symbol: str) -> str:
        """Generate basic face analysis from AKShare"""
        logger.debug(f"[AKShare] Call parameter: symbol={symbol}")

        try:
            #AKShare does not have a direct fundamental data interface, using generation analysis
            logger.info(f"📊 [Data source: AKShare- Generating Analysis] Generate basic analysis:{symbol}")
            return self._generate_fundamentals_analysis(symbol)

        except Exception as e:
            logger.error(f"❌ [Data source: AKShare anomaly] Generating fundamental analysis failed:{e}")
            return f"❌ 生成{symbol}基本面分析失败: {e}"

    def _get_valuation_indicators(self, symbol: str) -> Dict:
        """Acquisition of valuation indicators from stock basic info pool"""
        try:
            db_manager = get_database_manager()
            if not db_manager.is_mongodb_available():
                return {}
                
            client = db_manager.get_mongodb_client()
            db = client[db_manager.config.mongodb_config.database_name]
            
            #Acquisition of valuation indicators from stock basic info pool
            collection = db['stock_basic_info']
            result = collection.find_one({'ts_code': symbol})
            
            if result:
                return {
                    'pe': result.get('pe'),
                    'pb': result.get('pb'),
                    'pe_ttm': result.get('pe_ttm'),
                    'total_mv': result.get('total_mv'),
                    'circ_mv': result.get('circ_mv')
                }
            return {}
            
        except Exception as e:
            logger.error(f"Access{symbol}Valuation indicator failed:{e}")
            return {}

    def _format_financial_data(self, symbol: str, financial_data: List[Dict]) -> str:
        """Format financial data for reporting"""
        try:
            if not financial_data or len(financial_data) == 0:
                return f"❌ 未找到{symbol}的财务数据"

            #Access to up-to-date financial data
            latest = financial_data[0]

            #Build Report
            report = f"📊 {symbol} 基本面数据（来自MongoDB）\n\n"

            #Basic information
            report += f"📅 报告期: {latest.get('report_period', latest.get('end_date', '未知'))}\n"
            report += f"📈 数据来源: MongoDB财务数据库\n\n"

            #Financial indicators
            report += "💰 财务指标:\n"
            revenue = latest.get('revenue') or latest.get('total_revenue')
            if revenue is not None:
                report += f"   营业总收入: {revenue:,.2f}\n"
            
            net_profit = latest.get('net_profit') or latest.get('net_income')
            if net_profit is not None:
                report += f"   净利润: {net_profit:,.2f}\n"
                
            total_assets = latest.get('total_assets')
            if total_assets is not None:
                report += f"   总资产: {total_assets:,.2f}\n"
                
            total_liab = latest.get('total_liab')
            if total_liab is not None:
                report += f"   总负债: {total_liab:,.2f}\n"
                
            total_equity = latest.get('total_equity')
            if total_equity is not None:
                report += f"   股东权益: {total_equity:,.2f}\n"

            #Valuation indicator - obtained from stock basic info pool
            report += "\n📊 估值指标:\n"
            valuation_data = self._get_valuation_indicators(symbol)
            if valuation_data:
                pe = valuation_data.get('pe')
                if pe is not None:
                    report += f"   市盈率(PE): {pe:.2f}\n"
                    
                pb = valuation_data.get('pb')
                if pb is not None:
                    report += f"   市净率(PB): {pb:.2f}\n"
                    
                pe_ttm = valuation_data.get('pe_ttm')
                if pe_ttm is not None:
                    report += f"   市盈率TTM(PE_TTM): {pe_ttm:.2f}\n"
                    
                total_mv = valuation_data.get('total_mv')
                if total_mv is not None:
                    report += f"   总市值: {total_mv:.2f}亿元\n"
                    
                circ_mv = valuation_data.get('circ_mv')
                if circ_mv is not None:
                    report += f"   流通市值: {circ_mv:.2f}亿元\n"
            else:
                #Try to calculate from financial data if it is not available from stock basic info
                pe = latest.get('pe')
                if pe is not None:
                    report += f"   市盈率(PE): {pe:.2f}\n"
                    
                pb = latest.get('pb')
                if pb is not None:
                    report += f"   市净率(PB): {pb:.2f}\n"
                    
                ps = latest.get('ps')
                if ps is not None:
                    report += f"   市销率(PS): {ps:.2f}\n"

            #Profitability
            report += "\n💹 盈利能力:\n"
            roe = latest.get('roe')
            if roe is not None:
                report += f"   净资产收益率(ROE): {roe:.2f}%\n"
                
            roa = latest.get('roa')
            if roa is not None:
                report += f"   总资产收益率(ROA): {roa:.2f}%\n"
                
            gross_margin = latest.get('gross_margin')
            if gross_margin is not None:
                report += f"   毛利率: {gross_margin:.2f}%\n"
                
            netprofit_margin = latest.get('netprofit_margin') or latest.get('net_margin')
            if netprofit_margin is not None:
                report += f"   净利率: {netprofit_margin:.2f}%\n"

            #Cash flows
            n_cashflow_act = latest.get('n_cashflow_act')
            if n_cashflow_act is not None:
                report += "\n💰 现金流:\n"
                report += f"   经营活动现金流: {n_cashflow_act:,.2f}\n"
                
                n_cashflow_inv_act = latest.get('n_cashflow_inv_act')
                if n_cashflow_inv_act is not None:
                    report += f"   投资活动现金流: {n_cashflow_inv_act:,.2f}\n"
                    
                c_cash_equ_end_period = latest.get('c_cash_equ_end_period')
                if c_cash_equ_end_period is not None:
                    report += f"   期末现金及等价物: {c_cash_equ_end_period:,.2f}\n"

            report += f"\n📝 共有 {len(financial_data)} 期财务数据\n"

            return report

        except Exception as e:
            logger.error(f"Financial data formatted failed:{e}")
            return f"❌ 格式化{symbol}财务数据失败: {e}"

    def _generate_fundamentals_analysis(self, symbol: str) -> str:
        """Generate basic face analysis"""
        try:
            #Access to basic stock information
            stock_info = self.get_stock_info(symbol)

            report = f"📊 {symbol} 基本面分析（生成）\n\n"
            report += f"📈 股票名称: {stock_info.get('name', '未知')}\n"
            report += f"🏢 所属行业: {stock_info.get('industry', '未知')}\n"
            report += f"📍 所属地区: {stock_info.get('area', '未知')}\n"
            report += f"📅 上市日期: {stock_info.get('list_date', '未知')}\n"
            report += f"🏛️ 交易所: {stock_info.get('exchange', '未知')}\n\n"

            report += "⚠️ 注意: 详细财务数据需要从数据源获取\n"
            report += "💡 建议: 启用MongoDB缓存以获取完整的财务数据\n"

            return report

        except Exception as e:
            logger.error(f"❌ Generating fundamental face analysis failed:{e}")
            return f"❌ 生成{symbol}基本面分析失败: {e}"

    def _try_fallback_fundamentals(self, symbol: str) -> str:
        """Declining Basic Data"""
        logger.error(f"🔄 {self.current_source.value}Failed to try secondary data source access base...")

        #🔥 Data source priority (identify market according to stock code)
        fallback_order = self._get_data_source_priority_order(symbol)

        for source in fallback_order:
            if source != self.current_source and source in self.available_china_sources:
                try:
                    logger.info(f"🔄 Try secondary data source access fundamentals:{source.value}")

                    #Directly calls specific data source methods to avoid regression
                    if source == ChinaDataSource.TUSHARE:
                        result = self._get_tushare_fundamentals(symbol)
                    elif source == ChinaDataSource.AKSHARE:
                        result = self._get_akshare_fundamentals(symbol)
                    else:
                        continue

                    if result and "❌" not in result:
                        logger.info(f"✅ [data source: backup data source] Successfully access basics:{source.value}")
                        return result
                    else:
                        logger.warning(f"Alternative data source ⚠️{source.value}Returns error result")

                except Exception as e:
                    logger.error(f"Alternative data source ❌{source.value}Unusual:{e}")
                    continue

        #All data sources failed to generate basic analysis
        logger.warning(f"⚠️ [Data source: Generating analysis] All data sources failed to generate basic analysis:{symbol}")
        return self._generate_fundamentals_analysis(symbol)

    def _get_mongodb_news(self, symbol: str, hours_back: int, limit: int) -> List[Dict[str, Any]]:
        """Get news data from MongoDB"""
        try:
            from tradingagents.dataflows.cache.mongodb_cache_adapter import get_mongodb_cache_adapter
            adapter = get_mongodb_cache_adapter()

            #Get news data from MongoDB
            news_data = adapter.get_news_data(symbol, hours_back=hours_back, limit=limit)

            if news_data and len(news_data) > 0:
                logger.info(f"✅ [Data source: MongoDB-News]{symbol or 'Market News'} ({len(news_data)}(Articles)")
                return news_data
            else:
                logger.warning(f"No news:{symbol or 'Market News'}down to other data sources")
                return self._try_fallback_news(symbol, hours_back, limit)

        except Exception as e:
            logger.error(f"❌ [data source: MongoDB]{e}")
            return self._try_fallback_news(symbol, hours_back, limit)

    def _get_tushare_news(self, symbol: str, hours_back: int, limit: int) -> List[Dict[str, Any]]:
        """Get news data from Tushare"""
        try:
            #Tushare News is temporarily unavailable, return empty list
            logger.warning(f"⚠️ [data source: Tushare] Tushare News is temporarily unavailable")
            return []

        except Exception as e:
            logger.error(f"[Data source: Tushare]{e}")
            return []

    def _get_akshare_news(self, symbol: str, hours_back: int, limit: int) -> List[Dict[str, Any]]:
        """Get news data from Akshare."""
        try:
            #AKShare News is temporarily unavailable, return empty list
            logger.warning(f"⚠️ [Data Source: AKShare] AKShare News is temporarily unavailable")
            return []

        except Exception as e:
            logger.error(f"[Data source: AKShare]{e}")
            return []

    def _try_fallback_news(self, symbol: str, hours_back: int, limit: int) -> List[Dict[str, Any]]:
        """Degraded news data processing"""
        logger.error(f"🔄 {self.current_source.value}Failed to try backup data source for news...")

        #🔥 Data source priority (identify market according to stock code)
        fallback_order = self._get_data_source_priority_order(symbol)

        for source in fallback_order:
            if source != self.current_source and source in self.available_china_sources:
                try:
                    logger.info(f"🔄 trying to get news from secondary data sources:{source.value}")

                    #Directly calls specific data source methods to avoid regression
                    if source == ChinaDataSource.TUSHARE:
                        result = self._get_tushare_news(symbol, hours_back, limit)
                    elif source == ChinaDataSource.AKSHARE:
                        result = self._get_akshare_news(symbol, hours_back, limit)
                    else:
                        continue

                    if result and len(result) > 0:
                        logger.info(f"✅ [data source: backup data source] Successfully accessed news:{source.value}")
                        return result
                    else:
                        logger.warning(f"Alternative data source ⚠️{source.value}No news returned")

                except Exception as e:
                    logger.error(f"Alternative data source ❌{source.value}Unusual:{e}")
                    continue

        #All data sources failed
        logger.warning(f"⚠️ [data source: all data sources failed]{symbol or 'Market News'}")
        return []


#Examples of global data source manager
_data_source_manager = None

def get_data_source_manager() -> DataSourceManager:
    """Access global data source manager instance"""
    global _data_source_manager
    if _data_source_manager is None:
        _data_source_manager = DataSourceManager()
    return _data_source_manager


def get_china_stock_data_unified(symbol: str, start_date: str, end_date: str) -> str:
    """Unified Chinese stock access interface
    Automatically use configured data sources to support backup data Source

    Args:
        symbol: stock code
        Start date: Start date
        End date: End date

    Returns:
        str: Formatted Stock Data
    """
    from tradingagents.utils.logging_init import get_logger


    #Add detailed stock code tracking log
    logger.info(f"[Equal code tracking]{symbol}' (type:{type(symbol)})")
    logger.info(f"[Equal code tracking]{len(str(symbol))}")
    logger.info(f"[Equal code tracking]{list(str(symbol))}")

    manager = get_data_source_manager()
    logger.info(f"Call manager.get stock data, input parameter: symbol='{symbol}', start_date='{start_date}', end_date='{end_date}'")
    result = manager.get_stock_data(symbol, start_date, end_date)
    #Detailed information for analysis of return results
    if result:
        lines = result.split('\n')
        data_lines = [line for line in lines if '2025-' in line and symbol in line]
        logger.info(f"🔍{len(lines)}, Number of Data Lines ={len(data_lines)}, result length ={len(result)}Character")
        logger.info(f"[Equal code tracking]{result[:500]}")
        if len(data_lines) > 0:
            logger.info(f"[Equal code tracking]{data_lines[0][:100]}', last line ={data_lines[-1][:100]}'")
    else:
        logger.info(f"[Equal code tracking]")
    return result


def get_china_stock_info_unified(symbol: str) -> Dict:
    """Unified Chinese stock access interface
    Args:
        symbol: stock code

    Returns:
        Dict: Stock Basic Information
    """
    manager = get_data_source_manager()
    return manager.get_stock_info(symbol)


#Examples of global data source manager
_data_source_manager = None

def get_data_source_manager() -> DataSourceManager:
    """Access global data source manager instance"""
    global _data_source_manager
    if _data_source_manager is None:
        _data_source_manager = DataSourceManager()
    return _data_source_manager

#== sync, corrected by elderman == @elder man
#To fit stock data service, provide the same interface

def get_stock_data_service() -> DataSourceManager:
    """Examples of access to stock data services (compatible stock data service interface)
    This function is compatible interface and actually returns DataManager instance
    Recommended direct use()
    """
    return get_data_source_manager()


#== sync, corrected by elderman == @elder man

class USDataSourceManager:
    """American Stock Data Source Manager
    Supported data sources:
    - yfinance: Stock prices and technical indicators (free of charge)
    - alpha vantage: Basics and news data (needs API Key)
    - Finnhub: Back-up data source (needs API Key)
    - Mongodb: Cache data source (highest priority)
    """

    def __init__(self):
        """Initialise aesthetic data source manager"""
        #Check to enable MongoDB cache
        self.use_mongodb_cache = self._check_if_use_mongodb_enabled()

        #Check available data sources
        self.available_sources = self._check_available_sources()

        #Set Default Data Source
        self.default_source = self._get_default_source()
        self.current_source = self.default_source

        logger.info(f"Initialization of U.S. stock data source manager completed")
        logger.info(f"MongoDB cache:{'Enabled' if self.use_mongodb_cache else 'It\'s not working.'}")
        logger.info(f"Default data source:{self.default_source.value}")
        logger.info(f"Available data sources:{[s.value for s in self.available_sources]}")

    def _check_if_use_mongodb_enabled(self) -> bool:
        """Check to enable the MongoDB cache"""
        from tradingagents.config.runtime_settings import is_use_app_cache_enabled
        return is_use_app_cache_enabled()

    def _get_data_source_priority_order(self, symbol: Optional[str] = None) -> List[USDataSource]:
        """Acquiring US stock data source priorities from the database (for downgrading)
        Args:
            symbol: stock code

        Returns:
            List of data sources in order of priority (excluding MongoDB)
        """
        try:
            #Read data source configuration from the database
            from app.core.database import get_mongo_db_synchronous
            db = get_mongo_db_synchronous()

            #Method 1: Read from Data groupings (recommended)
            groupings_collection = db.datasource_groupings
            groupings = list(groupings_collection.find({
                "market_category_id": "us_stocks",
                "enabled": True
            }).sort("priority", -1))  #Sort down, first priority high

            if groupings:
                #Convert to USDatasource
                #Data source name map (database name USDatasource)
                source_mapping = {
                    'yfinance': USDataSource.YFINANCE,
                    'yahoo_finance': USDataSource.YFINANCE,  #Alias
                    'alpha_vantage': USDataSource.ALPHA_VANTAGE,
                    'finnhub': USDataSource.FINNHUB,
                }

                result = []
                for grouping in groupings:
                    ds_name = grouping.get('data_source_name', '').lower()
                    if ds_name in source_mapping:
                        source = source_mapping[ds_name]
                        #Excludes MongoDB (MongoDB is the highest priority and does not participate in downgrading)
                        if source != USDataSource.MONGODB and source in self.available_sources:
                            result.append(source)

                if result:
                    logger.info(f"Read from the database:{[s.value for s in result]}")
                    return result

            logger.warning("⚠️ [U.S. Data Source Priority] database is not configured, using default order")
        except Exception as e:
            logger.warning(f"⚠️ [USE Data Source Priority] Failed to read from the database:{e}, using default order")

        #Back to Default Order
        #Default order: yfinance > Alpha Vantage > Finnhub
        default_order = [
            USDataSource.YFINANCE,
            USDataSource.ALPHA_VANTAGE,
            USDataSource.FINNHUB,
        ]
        #Return only available data sources
        return [s for s in default_order if s in self.available_sources]

    def _get_default_source(self) -> USDataSource:
        """Get Default Data Sources"""
        #MongoDB as the highest priority data source if the MongoDB cache is enabled
        if self.use_mongodb_cache:
            return USDataSource.MONGODB

        #Get from the environment variable, use yfinance by default
        env_source = os.getenv('DEFAULT_US_DATA_SOURCE', DataSourceCode.YFINANCE).lower()

        #Map to Enumeration
        source_mapping = {
            DataSourceCode.YFINANCE: USDataSource.YFINANCE,
            DataSourceCode.ALPHA_VANTAGE: USDataSource.ALPHA_VANTAGE,
            DataSourceCode.FINNHUB: USDataSource.FINNHUB,
        }

        return source_mapping.get(env_source, USDataSource.YFINANCE)

    def _check_available_sources(self) -> List[USDataSource]:
        """Check available data sources
        Read enabled status from the database and check if dependency is satisfied
        """
        available = []

        #MongoDB Cache
        if self.use_mongodb_cache:
            available.append(USDataSource.MONGODB)
            logger.info("MongoDB cache data sources are available")

        #Read enabled data source list and configuration from the database
        enabled_sources_in_db = self._get_enabled_sources_from_db()
        datasource_configs = self._get_datasource_configs_from_db()

        #Check yfinance
        if 'yfinance' in enabled_sources_in_db:
            try:
                import yfinance
                available.append(USDataSource.YFINANCE)
                logger.info("✅ yfinance data sources are available and enabled")
            except ImportError:
                logger.warning("⚠️ yfinance data source not available: yfinance library not installed")
        else:
            logger.info("ℹ️ yfinance source disabled in database")

        #Check AlphaVantage
        if 'alpha_vantage' in enabled_sources_in_db:
            try:
                #Prefer API Key to database configuration, followed by environment variables
                api_key = datasource_configs.get('alpha_vantage', {}).get('api_key') or os.getenv("ALPHA_VANTAGE_API_KEY")
                if api_key:
                    available.append(USDataSource.ALPHA_VANTAGE)
                    source = "数据库配置" if datasource_configs.get('alpha_vantage', {}).get('api_key') else "环境变量"
                    logger.info(f"Alpha Vantage data sources are available and enabled (API Key source:{source})")
                else:
                    logger.warning("Alpha Vantage data source not available: API Key is not configured (no database and environmental variables are found)")
            except Exception as e:
                logger.warning(f"Alpha Vantage data source check failed:{e}")
        else:
            logger.info("ℹ️Alpha Vantage data source disabled in the database")

        #Check Finnhub
        if 'finnhub' in enabled_sources_in_db:
            try:
                #Prefer API Key to database configuration, followed by environment variables
                api_key = datasource_configs.get('finnhub', {}).get('api_key') or os.getenv("FINNHUB_API_KEY")
                if api_key:
                    available.append(USDataSource.FINNHUB)
                    source = "数据库配置" if datasource_configs.get('finnhub', {}).get('api_key') else "环境变量"
                    logger.info(f"✅Finnhub data source is available and enabled (API Key source:{source})")
                else:
                    logger.warning("⚠️ Finnhub data source not available: API Key is not configured (no database and environmental variables are found)")
            except Exception as e:
                logger.warning(f"Check of Finnhub data source failed:{e}")
        else:
            logger.info("Finnhub data source is disabled in the database")

        return available

    def _get_enabled_sources_from_db(self) -> List[str]:
        """Read enabled list of data sources from the database"""
        try:
            from app.core.database import get_mongo_db_synchronous
            db = get_mongo_db_synchronous()

            #Read from Data groupings
            groupings = list(db.datasource_groupings.find({
                "market_category_id": "us_stocks",
                "enabled": True
            }))

            #🔥 Data Source Name Map (name used in database name → code)
            name_mapping = {
                'alpha vantage': 'alpha_vantage',
                'yahoo finance': 'yfinance',
                'finnhub': 'finnhub',
            }

            result = []
            for g in groupings:
                db_name = g.get('data_source_name', '').lower()
                #Convert Name with Map
                code_name = name_mapping.get(db_name, db_name)
                result.append(code_name)
                logger.debug(f"Data source name map: '{db_name}' → '{code_name}'")

            return result
        except Exception as e:
            logger.warning(f"Access to enabled data sources from databases failed:{e}")
            #Default Enable All
            return ['yfinance', 'alpha_vantage', 'finnhub']

    def _get_datasource_configs_from_db(self) -> dict:
        """Read data source configuration from database (including API Key)"""
        try:
            from app.core.database import get_mongo_db_synchronous
            db = get_mongo_db_synchronous()

            #Read activated configurations from system configs
            config = db.system_configs.find_one({"is_active": True})
            if not config:
                return {}

            #Extracting data source configuration
            datasource_configs = config.get('data_source_configs', [])

            #Build Configuration Dictionary
            result = {}
            for ds_config in datasource_configs:
                name = ds_config.get('name', '').lower()
                result[name] = {
                    'api_key': ds_config.get('api_key', ''),
                    'api_secret': ds_config.get('api_secret', ''),
                    'config_params': ds_config.get('config_params', {})
                }

            return result
        except Exception as e:
            logger.warning(f"Access to data source configuration from database failed:{e}")
            return {}

    def get_current_source(self) -> USDataSource:
        """Get Current Data Source"""
        return self.current_source

    def set_current_source(self, source: USDataSource) -> bool:
        """Set Current Data Source"""
        if source in self.available_sources:
            self.current_source = source
            logger.info(f"The United States share data source has been converted to:{source.value}")
            return True
        else:
            logger.error(f"United States share data sources are not available:{source.value}")
            return False


#Examples of global US stock data source manager
_us_data_source_manager = None

def get_us_data_source_manager() -> USDataSourceManager:
    """Example of acquiring global US stock data source manager"""
    global _us_data_source_manager
    if _us_data_source_manager is None:
        _us_data_source_manager = USDataSourceManager()
    return _us_data_source_manager
