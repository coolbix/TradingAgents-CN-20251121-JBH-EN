"""Unified Tushare data provider
Merge all advantages of the app layer and the TradingAGents layer
"""
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, date, timedelta
import pandas as pd
import asyncio
import logging

from ..base_provider import BaseStockDataProvider
from tradingagents.config.providers_config import get_provider_config

#Try importing tushare
try:
    import tushare as ts
    TUSHARE_AVAILABLE = True
except ImportError:
    TUSHARE_AVAILABLE = False
    ts = None

logger = logging.getLogger(__name__)


class TushareProvider(BaseStockDataProvider):
    """Unified Tushare data provider
    Merge all advantages of the app layer and the TradingAGents layer
    """
    
    def __init__(self):
        super().__init__("Tushare")
        self.api = None
        self.config = get_provider_config("tushare")
        """
        {'enabled':         False,
         'token':           'your_tushare_token_here',
         'timeout':         30,
         'rate_limit':      3.3333333333333335,
         'max_retries':     3,
         'cache_enabled':   True,
         'cache_ttl':       3600}
        """
        self.token_source = None  #Record Token Source: 'database' or 'env'

        if not TUSHARE_AVAILABLE:
            self.logger.error("❌ Tushare library not installed, please run: pip initial Tushare")

    def _get_token_from_database(self) -> Optional[str]:
        """Read Tushare Token (API Key) from database
        Priority: Database Configuration > Environmental Variable
        This will take effect immediately after the user changes configuration in the Web backstage
        """
        try:
            self.logger.info("[DB query] Start reading Token...")
            from app.core.database import get_mongo_db_synchronous
            #JBH: why not use DatabaseManager @ tradingagents/config/database_manager.py ?
            #     app.core.database is outside tradingagents package
            db = get_mongo_db_synchronous()
            config_collection = db.system_configs

            #Get the latest active configuration
            self.logger.info("[DB Query] is active=True configuration...")
            config_data = config_collection.find_one(
                {"is_active": True},
                sort=[("version", -1)]
            )

            if config_data:
                self.logger.info(f"[DB Query] version: {config_data.get('version')}")
                if config_data.get('data_source_configs'):
                    self.logger.info(f"[DB query] {len(config_data['data_source_configs'])} Data sources")
                    for ds_config in config_data['data_source_configs']:
                        ds_type = ds_config.get('type')
                        self.logger.info(f"Checking data sources: {ds_type}")
                        if ds_type == 'tushare':
                            api_key = ds_config.get('api_key')
                            self.logger.info(f"[DB query] Found Tushare configuration, api key length:{len(api_key) if api_key else 0}")
                            if api_key and not api_key.startswith("your_"):
                                self.logger.info(f"Token is valid (long:{len(api_key)})")
                                return api_key
                            else:
                                self.logger.warning(f"[DB query] Token is invalid or occupied Arguments")
                else:
                    self.logger.warning("Cannot initialise Evolution's mail component.")
            else:
                self.logger.warning("[DB query] No active configuration found")

            self.logger.info("No valid Tushare Token found in database ⚠️")
        except Exception as e:
            self.logger.error(f"Could not close temporary folder: %s{e}")
            import traceback
            self.logger.error(f"[DB query] Stack tracking:{traceback.format_exc()}")

        return None

    def connect_sync(self) -> bool:
        """Synchronous connection to Tushare"""
        #NOTE: async version is connect()
        if not TUSHARE_AVAILABLE:
            self.logger.error("The Tushare library is not available")
            return False

        #Test connection timeout (sec) -- just test connectivity, not long.
        test_timeout = 10

        try:
            #Token
            self.logger.info("[Step 1] Start reading Tushare Token from database...")
            db_token = self._get_token_from_database()
            if db_token:
                self.logger.info(f"Token (Long: {len(db_token)})")
            else:
                self.logger.info("[Step 1] Token not found in database")

            self.logger.info("[Step 2] Get Token in os.environ (.env)...")
            env_token = self.config.get('token')
            if env_token:
                self.logger.info(f"Token (Long: {len(env_token)})")
            else:
                self.logger.info("Token not found in [step 2].env")

            #Try Database Token
            if db_token:
                try:
                    self.logger.info(f"[Step 3] {test_timeout} Seconds...")
                    ts.set_token(db_token)
                    self.api = ts.pro_api()

                    #Test Connection - Directly Call Synchronization Method (no asyncio.run)
                    try:
                        self.logger.info("[Step 3.1] Call stop basic API test connection...")
                        test_data = self.api.stock_basic(list_status='L', limit=1)
                        self.logger.info(f"[Step 3.1] API calls successfully, returns data: {len(test_data) if test_data is not None else 0} Article")
                    except Exception as e:
                        self.logger.warning(f"[Step 3.1] Database Token test failed:{e}.env configuration...")
                        test_data = None

                    if test_data is not None and not test_data.empty:
                        self.connected = True
                        self.token_source = 'database'
                        self.logger.info(f"✅ [step 3.2] Tushare connection successfully (Token source: database)")
                        return True
                    else:
                        self.logger.warning("[Step 3.2] Database Token test failed, trying to downgrade to .env configuration...")
                except Exception as e:
                    self.logger.warning(f"[Step 3] Database Token connection failed:{e}.env configuration...")

            #Down to Environment Variable Token
            if env_token:
                try:
                    self.logger.info(f"[Step 4] Try to use Tushare Token in .env (over {test_timeout} Seconds...")
                    ts.set_token(env_token)
                    self.api = ts.pro_api()

                    #Test Connection - Directly Call Synchronization Method (no asyncio.run)
                    try:
                        self.logger.info("[Step 4.1] Call stop basic API test connection...")
                        test_data = self.api.stock_basic(list_status='L', limit=1)
                        self.logger.info(f"[Step 4.1] API calls successfully, returns data: {len(test_data) if test_data is not None else 0} Article")
                    except Exception as e:
                        self.logger.error(f".env Token test failed:{e}")
                        return False

                    if test_data is not None and not test_data.empty:
                        self.connected = True
                        self.token_source = 'env'
                        self.logger.info(f"✅ [step 4.2] Tushare connection successfully (Token source: .env environmental variable)")
                        return True
                    else:
                        self.logger.error(".env Token test failed")
                        return False
                except Exception as e:
                    self.logger.error(f".env Token connection failed:{e}")
                    return False

            #Neither.
            self.logger.error("❌ [Step 5] Tushare token is not configured, please configure TUSHARE TOKEN in the Web backstage or .env file")
            return False

        except Exception as e:
            self.logger.error(f"Tushare connection failed:{e}")
            return False

    async def connect(self) -> bool:
        """Asynchronous connection to Tushare"""
        if not TUSHARE_AVAILABLE:
            self.logger.error("The Tushare library is not available")
            return False

        #Test connection timeout (sec) -- just test connectivity, not long.
        test_timeout = 10

        try:
            #Token
            db_token = self._get_token_from_database()
            env_token = self.config.get('token')

            #Try Database Token
            if db_token:
                try:
                    self.logger.info(f"Try to use Tushare Token in the database.{test_timeout}Seconds...")
                    ts.set_token(db_token)
                    self.api = ts.pro_api()

                    #Test Connection (Step) - Use Timeout
                    try:
                        test_data = await asyncio.wait_for(
                            asyncio.to_thread(
                                self.api.stock_basic,
                                list_status='L',
                                limit=1
                            ),
                            timeout=test_timeout
                        )
                    except asyncio.TimeoutError:
                        self.logger.warning(f"Token test timed out (⚠️){test_timeout}seconds) try to downgrade to .env configuration...")
                        test_data = None

                    if test_data is not None and not test_data.empty:
                        self.connected = True
                        self.logger.info(f"Tushare was successfully connected (Token Source: Database)")
                        return True
                    else:
                        self.logger.warning("Token test failed, trying to downgrade to .env configuration...")
                except Exception as e:
                    self.logger.warning(f"Token connection failed:{e}.env configuration...")

            #Down to Environment Variable Token
            if env_token:
                try:
                    self.logger.info(f"Try Tushare Token in .env{test_timeout}Seconds...")
                    ts.set_token(env_token)
                    self.api = ts.pro_api()

                    #Test Connection (Step) - Use Timeout
                    try:
                        test_data = await asyncio.wait_for(
                            asyncio.to_thread(
                                self.api.stock_basic,
                                list_status='L',
                                limit=1
                            ),
                            timeout=test_timeout
                        )
                    except asyncio.TimeoutError:
                        self.logger.error(f".env Token test timed out.{test_timeout}sec)")
                        return False

                    if test_data is not None and not test_data.empty:
                        self.connected = True
                        self.logger.info(f"Tushare was successfully connected.")
                        return True
                    else:
                        self.logger.error("Test failed. env Token")
                        return False
                except Exception as e:
                    self.logger.error(f".env Token connection failed:{e}")
                    return False

            #Neither.
            self.logger.error("Tushare token is not configured, please configure TUSHARE TOKEN in the Web background or .env file")
            return False

        except Exception as e:
            self.logger.error(f"Tushare connection failed:{e}")
            return False
    
    def is_available(self) -> bool:
        """Check that Tushare is available."""
        return TUSHARE_AVAILABLE and self.connected and self.api is not None
    
    #== sync, corrected by elderman == @elder man
    
    def get_stock_list_sync(self, market: str = None) -> Optional[pd.DataFrame]:
        """Retrieving list of shares (Sync version)"""
        if not self.is_available():
            return None

        try:
            df = self.api.stock_basic(
                list_status='L',
                fields='ts_code,symbol,name,area,industry,market,exchange,list_date,is_hs'
            )
            if df is not None and not df.empty:
                self.logger.info(f"Successfully accessed{len(df)}Stock data")
                return df
            else:
                self.logger.warning("Tushare API returns empty data")
                return None
        except Exception as e:
            self.logger.error(f"Can not get folder: %s: %s{e}")
            return None

    async def get_stock_list(self, market: str = None) -> Optional[List[Dict[str, Any]]]:
        """Retrieving the list of shares (speech version)"""
        if not self.is_available():
            return None

        try:
            #Build query parameters
            params = {
                'list_status': 'L',  #Acquisition of listed shares only
                'fields': 'ts_code,symbol,name,area,industry,market,exchange,list_date,is_hs'
            }
            
            if market:
                #It's on the market.
                if market == "CN":
                    params['exchange'] = 'SSE,SZSE'  #Deep Exchange
                elif market == "HK":
                    return None  #The Tushare Port Unit needs to be handled separately.
                elif market == "US":
                    return None  #Tushare doesn't support the U.S.
            
            #Getting data
            df = await asyncio.to_thread(self.api.stock_basic, **params)
            
            if df is None or df.empty:
                return None
            
            #Convert to Standard Formatting
            stock_list = []
            for _, row in df.iterrows():
                stock_info = self.standardize_basic_info(row.to_dict())
                stock_list.append(stock_info)
            
            self.logger.info(f"✅ for the list of shares:{len(stock_list)}Only")
            return stock_list
            
        except Exception as e:
            self.logger.error(f"Can not get folder: %s: %s{e}")
            return None
    
    async def get_stock_basic_info(self, symbol: str = None) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """Access to basic stock information"""
        if not self.is_available():
            return None
        
        try:
            if symbol:
                #Can not open message
                ts_code = self._normalize_ts_code(symbol)
                df = await asyncio.to_thread(
                    self.api.stock_basic,
                    ts_code=ts_code,
                    fields='ts_code,symbol,name,area,industry,market,exchange,list_date,is_hs,act_name,act_ent_type'
                )
                
                if df is None or df.empty:
                    return None
                
                return self.standardize_basic_info(df.iloc[0].to_dict())
            else:
                #Get all stock information
                return await self.get_stock_list()
                
        except Exception as e:
            self.logger.error(f"Failed to get basic stock information{symbol}: {e}")
            return None
    
    async def get_stock_quotes(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get a single stock in real time

        policy: use the Daily interface to obtain data for the latest day (without rt k batch interface)
        -rt k interface is a volume interface, a single stock calls a waste quota
        -Daily interface to get up-to-date dayline data on a single stock with more indicators

        Note: This method is suitable for small stock acquisition and a large number of stocks are recommended for use ()
        """
        if not self.is_available():
            return None

        try:
            ts_code = self._normalize_ts_code(symbol)

            #🔥 Use the Daily interface to get data for the latest day (with more savings)
            from datetime import datetime, timedelta

            #Access to data for the last 3 days (consider weekends and holidays)
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d')

            df = await asyncio.to_thread(
                self.api.daily,
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )

            if df is not None and not df.empty:
                #Taking data from the latest day
                row = df.iloc[0].to_dict()

                #Standardized Fields
                quote_data = {
                    'ts_code': row.get('ts_code'),
                    'symbol': symbol,
                    'trade_date': row.get('trade_date'),
                    'open': row.get('open'),
                    'high': row.get('high'),
                    'low': row.get('low'),
                    'close': row.get('close'),  #Discount price
                    'pre_close': row.get('pre_close'),
                    'change': row.get('change'),  #The drop.
                    'pct_chg': row.get('pct_chg'),  #♪ Up and down ♪
                    'volume': row.get('vol'),  #Exchange (hands)
                    'amount': row.get('amount'),  #(thousands of dollars)
                }

                return self.standardize_quotes(quote_data)

            return None

        except Exception as e:
            #Check for limit error
            if self._is_rate_limit_error(str(e)):
                self.logger.error(f"❌ For real-time line failure symbol={symbol}: {e}")
                raise  #Drop limit error, let the top handle

            self.logger.error(f"❌ Getting real time line failed symbol={symbol}: {e}")
            return None

    async def get_realtime_quotes_batch(self) -> Optional[Dict[str, Dict[str, Any]]]:
        """Batch access to market-wide real-time businesses
        Use the wildcard function of the rt k interface to get all A units in real time at once

        Returns:
            Dict [str, Dict]:
            For example:   FT 1  
        """
        if not self.is_available():
            return None

        try:
            #Use wildcards to get a market-wide line at once
            #3*.SZ: Entrepreneurship Board 6*.SH: Turning in 0*.SZ: Main Board 9*.BJ: North
            df = await asyncio.to_thread(
                self.api.rt_k,
                ts_code='3*.SZ,6*.SH,0*.SZ,9*.BJ'
            )

            if df is None or df.empty:
                self.logger.warning("⚠️rt k interface returns empty data")
                return None

            self.logger.info(f"Other Organiser{len(df)}Real-time equity only.")

            #Get the current date (UTC+8)
            from datetime import datetime, timezone, timedelta
            cn_tz = timezone(timedelta(hours=8))
            now_cn = datetime.now(cn_tz)
            trade_date = now_cn.strftime("%Y%m%d")  #Format: 20251114 (in Tushare format)

            #Convert to Dictionary Format
            result = {}
            for _, row in df.iterrows():
                ts_code = row.get('ts_code')
                if not ts_code or '.' not in ts_code:
                    continue

                #Extract 6-bit code.
                symbol = ts_code.split('.')[0]

                #Build Line Data
                quote_data = {
                    'ts_code': ts_code,
                    'symbol': symbol,
                    'name': row.get('name'),
                    'open': row.get('open'),
                    'high': row.get('high'),
                    'low': row.get('low'),
                    'close': row.get('close'),  #Current price
                    'pre_close': row.get('pre_close'),
                    'volume': row.get('vol'),  #Trade (units)
                    'amount': row.get('amount'),  #Volume ($)
                    'num': row.get('num'),  #Number of deals
                    'trade_date': trade_date,  #Add transaction date field 🔥
                }

                #Calculating Increases and Declines
                if quote_data.get('close') and quote_data.get('pre_close'):
                    try:
                        close = float(quote_data['close'])
                        pre_close = float(quote_data['pre_close'])
                        if pre_close > 0:
                            pct_chg = ((close - pre_close) / pre_close) * 100
                            quote_data['pct_chg'] = round(pct_chg, 2)
                            quote_data['change'] = round(close - pre_close, 2)
                    except (ValueError, TypeError):
                        pass

                result[symbol] = quote_data

            return result

        except Exception as e:
            #Check for limit error
            if self._is_rate_limit_error(str(e)):
                self.logger.error(f"Batch access to real-time lines failed (restricted flow):{e}")
                raise  #Drop limit error, let the top handle

            self.logger.error(f"Batch access to real-time lines failed:{e}")
            return None

    def _is_rate_limit_error(self, error_msg: str) -> bool:
        """Test for API limit error"""
        rate_limit_keywords = [
            "每分钟最多访问",
            "每分钟最多",
            "rate limit",
            "too many requests",
            "访问频率",
            "请求过于频繁"
        ]
        error_msg_lower = error_msg.lower()
        return any(keyword in error_msg_lower for keyword in rate_limit_keywords)
    
    async def get_historical_data(
        self,
        symbol: str,
        start_date: Union[str, date],
        end_date: Union[str, date] = None,
        period: str = "daily"
    ) -> Optional[pd.DataFrame]:
        """Access to historical data

        Args:
            symbol: stock code
            Start date: Start date
            End date: End date
            period: data cycle (daily/weekly/montly)
        """
        if not self.is_available():
            return None

        try:
            ts_code = self._normalize_ts_code(symbol)

            #Formatting Date
            start_str = self._format_date(start_date)
            end_str = self._format_date(end_date) if end_date else datetime.now().strftime('%Y%m%d')

            #🔧 Use pro bar interface to obtain pre-recognition data (consistent with the flower)
            #Note: Daly/weekly/montly interface for Tushare does not support rights
            #Must use the ts.pro bar() function and specify the adj='qfq' argument

            #Periodic Map
            freq_map = {
                "daily": "D",
                "weekly": "W",
                "monthly": "M"
            }
            freq = freq_map.get(period, "D")

            #Use the ts.pro bar() function to get pre-recognised data
            #Note: pro bar is a Tushare module function, not api object method
            df = await asyncio.to_thread(
                ts.pro_bar,
                ts_code=ts_code,
                api=self.api,  #Import api objects
                start_date=start_str,
                end_date=end_str,
                freq=freq,
                adj='qfq'  #Ex-referral rights (consistent with cash)
            )

            if df is None or df.empty:
                self.logger.warning(
                    f"Tushare API returns empty data: symbol={symbol}, ts_code={ts_code}, "
                    f"period={period}, start={start_str}, end={end_str}"
                )
                self.logger.warning(
                    f"Possible causes:"
                    f"1) No trade data on the stock during this period"
                    f"2) Incorrect date range"
                    f"3) Stock code format error"
                    f"4) Tushare API restrictions or insufficient points"
                )
                return None

            #Data standardization
            df = self._standardize_historical_data(df)

            self.logger.info(f"Access{period}Historical data:{symbol} {len(df)}Record (former right qfq)")
            return df
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            self.logger.error(
                f"Could not close temporary folder: %s{symbol}, period={period}\n"
                f"Parameters: ts code={ts_code if 'ts_code' in locals() else 'N/A'}, "
                f"start={start_str if 'start_str' in locals() else 'N/A'}, "
                f"end={end_str if 'end_str' in locals() else 'N/A'}\n"
                f"Error type:{type(e).__name__}\n"
                f"Cannot initialise Evolution's mail component.{str(e)}\n"
                f"Stack tracking: \n{error_details}"
            )
            return None
    
    #== sync, corrected by elderman == @elder man
    
    async def get_daily_basic(self, trade_date: str) -> Optional[pd.DataFrame]:
        """Access to daily basic financial data"""
        if not self.is_available():
            return None
        
        try:
            date_str = trade_date.replace('-', '')
            df = await asyncio.to_thread(
                self.api.daily_basic,
                trade_date=date_str,
                fields='ts_code,total_mv,circ_mv,pe,pb,turnover_rate,volume_ratio,pe_ttm,pb_mrq'
            )
            
            if df is not None and not df.empty:
                self.logger.info(f"Access to daily basic data:{trade_date} {len(df)}Notes")
                return df
            
            return None
            
        except Exception as e:
            self.logger.error(f"Could not close temporary folder: %s{trade_date}: {e}")
            return None
    
    async def find_latest_trade_date(self) -> Optional[str]:
        """Find Recent Transaction Date"""
        if not self.is_available():
            return None
        
        try:
            today = datetime.now()
            for delta in range(0, 10):  #Ten days at most.
                check_date = (today - timedelta(days=delta)).strftime('%Y%m%d')
                
                try:
                    df = await asyncio.to_thread(
                        self.api.daily_basic,
                        trade_date=check_date,
                        fields='ts_code',
                        limit=1
                    )
                    
                    if df is not None and not df.empty:
                        formatted_date = f"{check_date[:4]}-{check_date[4:6]}-{check_date[6:8]}"
                        self.logger.info(f"Can you get the latest date of transaction:{formatted_date}")
                        return formatted_date
                        
                except Exception:
                    continue
            
            return None
            
        except Exception as e:
            self.logger.error(f"Could not close temporary folder: %s{e}")
            return None
    
    async def get_financial_data(self, symbol: str, report_type: str = "quarterly",
                                period: str = None, limit: int = 4) -> Optional[Dict[str, Any]]:
        """Access to financial data

        Args:
            symbol: stock code
            Report type: Report type (quarterly/annual)
            period: For the specified reporting period (YYYYMMDD format), obtain the latest data for empty
            Limited: number of access records, default 4 (last 4 quarters)

        Returns:
            Financial data dictionary with profit statement, balance sheet, cash flow statement and financial indicators
        """
        if not self.is_available():
            return None

        try:
            ts_code = self._normalize_ts_code(symbol)
            self.logger.debug(f"📊 for Tushare financial data:{ts_code}type:{report_type}")

            #Build query parameters
            query_params = {
                'ts_code': ts_code,
                'limit': limit
            }

            #If a reporting period is specified, add parameters for the period
            if period:
                query_params['period'] = period

            financial_data = {}

            #1. Obtaining profit statement data (income status)
            try:
                income_df = await asyncio.to_thread(
                    self.api.income,
                    **query_params
                )
                if income_df is not None and not income_df.empty:
                    financial_data['income_statement'] = income_df.to_dict('records')
                    self.logger.debug(f"✅ {ts_code}Profit statement data obtained successfully:{len(income_df)}Notes")
                else:
                    self.logger.debug(f"⚠️ {ts_code}Profit statement data is empty")
            except Exception as e:
                self.logger.warning(f"Access{ts_code}Profit statement data failed:{e}")

            #2. Obtain balance sheet data (balance view)
            try:
                balance_df = await asyncio.to_thread(
                    self.api.balancesheet,
                    **query_params
                )
                if balance_df is not None and not balance_df.empty:
                    financial_data['balance_sheet'] = balance_df.to_dict('records')
                    self.logger.debug(f"✅ {ts_code}Balance sheet data acquisition success:{len(balance_df)}Notes")
                else:
                    self.logger.debug(f"⚠️ {ts_code}Balance sheet data is empty")
            except Exception as e:
                self.logger.warning(f"Access{ts_code}Balance sheet data failed:{e}")

            #3. Access to cash flow statement data (cash flow status)
            try:
                cashflow_df = await asyncio.to_thread(
                    self.api.cashflow,
                    **query_params
                )
                if cashflow_df is not None and not cashflow_df.empty:
                    financial_data['cashflow_statement'] = cashflow_df.to_dict('records')
                    self.logger.debug(f"✅ {ts_code}Cash flow statement data obtained successfully:{len(cashflow_df)}Notes")
                else:
                    self.logger.debug(f"⚠️ {ts_code}Cash flow statement data are empty")
            except Exception as e:
                self.logger.warning(f"Access{ts_code}Cash flow statement data failed:{e}")

            #4. Access to financial indicators
            try:
                indicator_df = await asyncio.to_thread(
                    self.api.fina_indicator,
                    **query_params
                )
                if indicator_df is not None and not indicator_df.empty:
                    financial_data['financial_indicators'] = indicator_df.to_dict('records')
                    self.logger.debug(f"✅ {ts_code}Successful data acquisition on financial indicators:{len(indicator_df)}Notes")
                else:
                    self.logger.debug(f"⚠️ {ts_code}Financial indicators data are empty")
            except Exception as e:
                self.logger.warning(f"Access{ts_code}Financial indicator data fail:{e}")

            #5. Access to data on the main business composition (optional)
            try:
                mainbz_df = await asyncio.to_thread(
                    self.api.fina_mainbz,
                    **query_params
                )
                if mainbz_df is not None and not mainbz_df.empty:
                    financial_data['main_business'] = mainbz_df.to_dict('records')
                    self.logger.debug(f"✅ {ts_code}Successful data acquisition for the main business component:{len(mainbz_df)}Notes")
                else:
                    self.logger.debug(f"⚠️ {ts_code}Main business composition data is empty")
            except Exception as e:
                self.logger.debug(f"Access{ts_code}Main business composition failed:{e}")  #Main operating data is not necessary, maintain the debug level

            if financial_data:
                #Standardized financial data
                standardized_data = self._standardize_tushare_financial_data(financial_data, ts_code)
                self.logger.info(f"✅ {ts_code}Tushare financial data acquisition completed:{len(financial_data)}Data sets")
                return standardized_data
            else:
                self.logger.warning(f"⚠️ {ts_code}No Tushare financial data obtained")
                return None

        except Exception as e:
            self.logger.error(f"Can not get folder: %s: %s{symbol}: {e}")
            return None

    async def get_stock_news(self, symbol: str = None, limit: int = 10,
                           hours_back: int = 24, src: str = None) -> Optional[List[Dict[str, Any]]]:
        """Access to stock news (needing Tushare access)

        Args:
            Symbol: Stock code, market news for None
            Limited number of returns
            Hours back: Backtrace hours, default 24 hours
            src: news source, default automatic selection

        Returns:
            NewsList
        """
        if not self.is_available():
            return None

        try:
            from datetime import datetime, timedelta

            #Calculate the time frame
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours_back)

            start_date = start_time.strftime('%Y-%m-%d %H:%M:%S')
            end_date = end_time.strftime('%Y-%m-%d %H:%M:%S')

            self.logger.debug(f"== sync, corrected by elderman =={symbol}time frame ={start_date}Present.{end_date}")

            #List of supported news sources (in order of priority)
            news_sources = [
                'sina',        #The New Wave.
                'eastmoney',   #East wealth.
                '10jqka',      #Same flower.
                'wallstreetcn', #See you on Wall Street.
                'cls',         #Associated Press
                'yicai',       #First fortune.
                'jinrongjie',  #Financial community
                'yuncaijing',  #Cloudy.
                'fenghuang'    #Phoenix News.
            ]

            #If data sources are specified, give priority to use
            if src and src in news_sources:
                sources_to_try = [src]
            else:
                sources_to_try = news_sources[:3]  #Default try first three sources

            all_news = []

            for source in sources_to_try:
                try:
                    self.logger.debug(f"Try from{source}Get the news...")

                    #Access to news data
                    news_df = await asyncio.to_thread(
                        self.api.news,
                        src=source,
                        start_date=start_date,
                        end_date=end_date
                    )

                    if news_df is not None and not news_df.empty:
                        source_news = self._process_tushare_news(news_df, source, symbol, limit)
                        all_news.extend(source_news)

                        self.logger.info(f"From{source}Fetched{len(source_news)}News")

                        #If you get enough news, stop trying other sources.
                        if len(all_news) >= limit:
                            break
                    else:
                        self.logger.debug(f"⚠️ {source}No news data returned")

                except Exception as e:
                    self.logger.debug(f"From{source}Access to news failed:{e}")
                    continue

                #API limit flow
                await asyncio.sleep(0.2)

            #To reorder and sort
            if all_news:
                #Sort and weight by time
                unique_news = self._deduplicate_news(all_news)
                sorted_news = sorted(unique_news, key=lambda x: x.get('publish_time', datetime.min), reverse=True)

                #Limit number of returns
                final_news = sorted_news[:limit]

                self.logger.info(f"Tushare News Access Success:{len(final_news)}Article")
                return final_news
            else:
                self.logger.warning("No Tushare news data obtained")
                return []

        except Exception as e:
            #If it's a question of authority, give a clear hint.
            if any(keyword in str(e).lower() for keyword in ['权限', 'permission', 'unauthorized', 'access denied']):
                self.logger.warning(f"⚠️Tushare news interface requires separate access (paying function):{e}")
            elif "积分" in str(e) or "point" in str(e).lower():
                self.logger.warning(f"The Tushare score is insufficient to access news data:{e}")
            else:
                self.logger.error(f"The news of Tushare failed:{e}")
            return None

    def _process_tushare_news(self, news_df: pd.DataFrame, source: str,
                            symbol: str = None, limit: int = 10) -> List[Dict[str, Any]]:
        """Processing of Tushare news data"""
        news_list = []

        #Limiting the number of processes
        df_limited = news_df.head(limit * 2)  #Get more for filtering

        for _, row in df_limited.iterrows():
            news_item = {
                "title": str(row.get('title', '') or row.get('content', '')[:50] + '...'),
                "content": str(row.get('content', '')),
                "summary": self._generate_summary(row.get('content', '')),
                "url": "",  #Tushare news interface does not provide URLs
                "source": self._get_source_name(source),
                "author": "",
                "publish_time": self._parse_tushare_news_time(row.get('datetime', '')),
                "category": self._classify_tushare_news(row.get('channels', ''), row.get('content', '')),
                "sentiment": self._analyze_news_sentiment(row.get('content', ''), row.get('title', '')),
                "importance": self._assess_news_importance(row.get('content', ''), row.get('title', '')),
                "keywords": self._extract_keywords(row.get('content', ''), row.get('title', '')),
                "data_source": "tushare",
                "original_source": source
            }

            #If you assign the stock code, filter the news.
            if symbol:
                if self._is_news_relevant_to_symbol(news_item, symbol):
                    news_list.append(news_item)
            else:
                news_list.append(news_item)

        return news_list

    def _get_source_name(self, source_code: str) -> str:
        """Can not open message"""
        source_names = {
            'sina': '新浪财经',
            'eastmoney': '东方财富',
            '10jqka': '同花顺',
            'wallstreetcn': '华尔街见闻',
            'cls': '财联社',
            'yicai': '第一财经',
            'jinrongjie': '金融界',
            'yuncaijing': '云财经',
            'fenghuang': '凤凰新闻'
        }
        return source_names.get(source_code, source_code)

    def _generate_summary(self, content: str) -> str:
        """Generate press summaries"""
        if not content:
            return ""

        content_str = str(content)
        if len(content_str) <= 200:
            return content_str

        #Simple summary generation: pre-200 words Arguments
        return content_str[:200] + "..."

    def _is_news_relevant_to_symbol(self, news_item: Dict[str, Any], symbol: str) -> bool:
        """To determine whether the news is stock-related."""
        content = news_item.get("content", "").lower()
        title = news_item.get("title", "").lower()

        #Standardised stock code
        symbol_clean = symbol.replace('.SH', '').replace('.SZ', '').zfill(6)

        #Keyword Match
        return any([
            symbol_clean in content,
            symbol_clean in title,
            symbol in content,
            symbol in title
        ])

    def _deduplicate_news(self, news_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """The news is heavy."""
        seen_titles = set()
        unique_news = []

        for news in news_list:
            title = news.get('title', '')
            if title and title not in seen_titles:
                seen_titles.add(title)
                unique_news.append(news)

        return unique_news

    def _analyze_news_sentiment(self, content: str, title: str) -> str:
        """Analysis of news moods"""
        text = f"{title} {content}".lower()

        positive_keywords = ['利好', '上涨', '增长', '盈利', '突破', '创新高', '买入', '推荐']
        negative_keywords = ['利空', '下跌', '亏损', '风险', '暴跌', '卖出', '警告', '下调']

        positive_count = sum(1 for keyword in positive_keywords if keyword in text)
        negative_count = sum(1 for keyword in negative_keywords if keyword in text)

        if positive_count > negative_count:
            return 'positive'
        elif negative_count > positive_count:
            return 'negative'
        else:
            return 'neutral'

    def _assess_news_importance(self, content: str, title: str) -> str:
        """Assessing the importance of information"""
        text = f"{title} {content}".lower()

        high_importance_keywords = ['业绩', '财报', '重大', '公告', '监管', '政策', '并购', '重组']
        medium_importance_keywords = ['分析', '预测', '观点', '建议', '行业', '市场']

        if any(keyword in text for keyword in high_importance_keywords):
            return 'high'
        elif any(keyword in text for keyword in medium_importance_keywords):
            return 'medium'
        else:
            return 'low'

    def _extract_keywords(self, content: str, title: str) -> List[str]:
        """Extract Keywords"""
        text = f"{title} {content}"

        #Simple keyword extraction
        keywords = []
        common_keywords = ['股票', '公司', '市场', '投资', '业绩', '财报', '政策', '行业', '分析', '预测']

        for keyword in common_keywords:
            if keyword in text:
                keywords.append(keyword)

        return keywords[:5]  #returns up to 5 keywords

    def _parse_tushare_news_time(self, time_str: str) -> Optional[datetime]:
        """Parsing Tushare newstime"""
        if not time_str:
            return datetime.utcnow()

        try:
            #Tushare time format: 2018-11-21 09:30:00
            return datetime.strptime(str(time_str), '%Y-%m-%d %H:%M:%S')
        except Exception as e:
            self.logger.debug(f"This post is part of our special coverage Tunisia Protests 2011.{e}")
            return datetime.utcnow()

    def _classify_tushare_news(self, channels: str, content: str) -> str:
        """Tushare News"""
        channels = str(channels).lower()
        content = str(content).lower()

        #Classify by channel and content keyword
        if any(keyword in channels or keyword in content for keyword in ['公告', '业绩', '财报']):
            return 'company_announcement'
        elif any(keyword in channels or keyword in content for keyword in ['政策', '监管', '央行']):
            return 'policy_news'
        elif any(keyword in channels or keyword in content for keyword in ['行业', '板块']):
            return 'industry_news'
        elif any(keyword in channels or keyword in content for keyword in ['市场', '指数', '大盘']):
            return 'market_news'
        else:
            return 'other'

    async def get_financial_data_by_period(self, symbol: str, start_period: str = None,
                                         end_period: str = None, report_type: str = "quarterly") -> Optional[List[Dict[str, Any]]]:
        """Time frame for obtaining financial data

        Args:
            symbol: stock code
            Start period: Initial reporting period (YYYYMMDD)
            End period: end reporting period (YYYYMMDD)
            Report type: Report type (quarterly/annual)

        Returns:
            List of financial data, in descending order of reporting period
        """
        if not self.is_available():
            return None

        try:
            ts_code = self._normalize_ts_code(symbol)
            self.logger.debug(f"📊Requiring financial data on Tushare by period:{ts_code}, {start_period} - {end_period}")

            #Build query parameters
            query_params = {'ts_code': ts_code}

            if start_period:
                query_params['start_date'] = start_period
            if end_period:
                query_params['end_date'] = end_period

            #Access to profit statement data as the main data source
            income_df = await asyncio.to_thread(
                self.api.income,
                **query_params
            )

            if income_df is None or income_df.empty:
                self.logger.warning(f"⚠️ {ts_code}No financial data available for a specified period")
                return None

            #Complete financial data by reporting period
            financial_data_list = []

            for _, income_row in income_df.iterrows():
                period = income_row['end_date']

                #Obtaining complete financial data for the period
                period_data = await self.get_financial_data(
                    symbol=symbol,
                    period=period,
                    limit=1
                )

                if period_data:
                    financial_data_list.append(period_data)

                #API limit flow
                await asyncio.sleep(0.1)

            self.logger.info(f"✅ {ts_code}Access to financial data by period is complete:{len(financial_data_list)}Reporting period")
            return financial_data_list

        except Exception as e:
            self.logger.error(f"❌ Failed to obtain Tushare financial data by period{symbol}: {e}")
            return None

    async def get_financial_indicators_only(self, symbol: str, limit: int = 4) -> Optional[Dict[str, Any]]:
        """Access to financial indicator data only (light interface)

        Args:
            symbol: stock code
            number of records obtained

        Returns:
            Data on financial indicators
        """
        if not self.is_available():
            return None

        try:
            ts_code = self._normalize_ts_code(symbol)

            #Obtain financial indicators only
            indicator_df = await asyncio.to_thread(
                self.api.fina_indicator,
                ts_code=ts_code,
                limit=limit
            )

            if indicator_df is not None and not indicator_df.empty:
                indicators = indicator_df.to_dict('records')

                return {
                    "symbol": symbol,
                    "ts_code": ts_code,
                    "financial_indicators": indicators,
                    "data_source": "tushare",
                    "updated_at": datetime.utcnow()
                }

            return None

        except Exception as e:
            self.logger.error(f"Could not close temporary folder: %s{symbol}: {e}")
            return None

    #== sync, corrected by elderman == @elder man

    def standardize_basic_info(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Standardized stock base information"""
        ts_code = raw_data.get('ts_code', '')
        symbol = raw_data.get('symbol', ts_code.split('.')[0] if '.' in ts_code else ts_code)

        return {
            #Base Fields
            "code": symbol,
            "name": raw_data.get('name', ''),
            "symbol": symbol,
            "full_symbol": ts_code,

            #Market information
            "market_info": self._determine_market_info_from_ts_code(ts_code),

            #Operational information
            "area": self._safe_str(raw_data.get('area')),
            "industry": self._safe_str(raw_data.get('industry')),
            "market": raw_data.get('market'),  #Main Board/Enterprise Board/Screen Board
            "list_date": self._format_date_output(raw_data.get('list_date')),

            #Port Unit information
            "is_hs": raw_data.get('is_hs'),

            #Control information
            "act_name": raw_data.get('act_name'),
            "act_ent_type": raw_data.get('act_ent_type'),

            #Metadata
            "data_source": "tushare",
            "data_version": 1,
            "updated_at": datetime.utcnow()
        }

    def standardize_quotes(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Standardized real-time behaviour data"""
        ts_code = raw_data.get('ts_code', '')
        symbol = ts_code.split('.')[0] if '.' in ts_code else ts_code

        return {
            #Base Fields
            "code": symbol,
            "symbol": symbol,
            "full_symbol": ts_code,
            "market": self._determine_market(ts_code),

            #Price data
            "close": self._convert_to_float(raw_data.get('close')),
            "current_price": self._convert_to_float(raw_data.get('close')),
            "open": self._convert_to_float(raw_data.get('open')),
            "high": self._convert_to_float(raw_data.get('high')),
            "low": self._convert_to_float(raw_data.get('low')),
            "pre_close": self._convert_to_float(raw_data.get('pre_close')),

            #Change data
            "change": self._convert_to_float(raw_data.get('change')),
            "pct_chg": self._convert_to_float(raw_data.get('pct_chg')),

            #Sold data
            #🔥 Conversion unit: Tushare returns hand and needs to be converted to unit
            "volume": self._convert_to_float(raw_data.get('vol')) * 100 if raw_data.get('vol') else None,
            #🔥 Trade-off unit conversion: Tushare interface returns thousands of dollars, which need to be converted to a dollar
            "amount": self._convert_to_float(raw_data.get('amount')) * 1000 if raw_data.get('amount') else None,

            #Financial indicators
            "total_mv": self._convert_to_float(raw_data.get('total_mv')),
            "circ_mv": self._convert_to_float(raw_data.get('circ_mv')),
            "pe": self._convert_to_float(raw_data.get('pe')),
            "pb": self._convert_to_float(raw_data.get('pb')),
            "turnover_rate": self._convert_to_float(raw_data.get('turnover_rate')),

            #Time data
            "trade_date": self._format_date_output(raw_data.get('trade_date')),
            "timestamp": datetime.utcnow(),

            #Metadata
            "data_source": "tushare",
            "data_version": 1,
            "updated_at": datetime.utcnow()
        }

    #== sync, corrected by elderman == @elder man

    def _normalize_ts_code(self, symbol: str) -> str:
        """Ts code format standardized as Tushare"""
        if '.' in symbol:
            return symbol  #Already in ts code format

        #6-digit code, need to add suffix
        if symbol.isdigit() and len(symbol) == 6:
            if symbol.startswith(('60', '68', '90')):
                return f"{symbol}.SH"  #Turn it in.
            else:
                return f"{symbol}.SZ"  #In-depth

        return symbol

    def _determine_market_info_from_ts_code(self, ts_code: str) -> Dict[str, Any]:
        """Identify market information according to ts code"""
        if '.SH' in ts_code:
            return {
                "market": "CN",
                "exchange": "SSE",
                "exchange_name": "上海证券交易所",
                "currency": "CNY",
                "timezone": "Asia/Shanghai"
            }
        elif '.SZ' in ts_code:
            return {
                "market": "CN",
                "exchange": "SZSE",
                "exchange_name": "深圳证券交易所",
                "currency": "CNY",
                "timezone": "Asia/Shanghai"
            }
        elif '.BJ' in ts_code:
            return {
                "market": "CN",
                "exchange": "BSE",
                "exchange_name": "北京证券交易所",
                "currency": "CNY",
                "timezone": "Asia/Shanghai"
            }
        else:
            return {
                "market": "CN",
                "exchange": "UNKNOWN",
                "exchange_name": "未知交易所",
                "currency": "CNY",
                "timezone": "Asia/Shanghai"
            }

    def _determine_market(self, ts_code: str) -> str:
        """Identification of market codes"""
        market_info = self._determine_market_info_from_ts_code(ts_code)
        return market_info.get("market", "CN")

    def _format_date(self, date_value: Union[str, date]) -> str:
        """Format date in Tushare format (YYYYMMDD)"""
        if isinstance(date_value, str):
            return date_value.replace('-', '')
        elif isinstance(date_value, date):
            return date_value.strftime('%Y%m%d')
        else:
            return str(date_value).replace('-', '')

    def _standardize_historical_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardized historical data"""
        #Rename Column
        column_mapping = {
            'trade_date': 'date',
            'vol': 'volume'
        }
        df = df.rename(columns=column_mapping)

        #Formatting Date
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
            df.set_index('date', inplace=True)

        #Sort by Date
        df = df.sort_index()

        return df

    def _standardize_tushare_financial_data(self, financial_data: Dict[str, Any], ts_code: str) -> Dict[str, Any]:
        """Standardized Tushare financial data

        Args:
            Financial data: Original Financial Data Dictionary
            ts code: Tushare stock code

        Returns:
            Standardized financial data
        """
        try:
            #Access to up-to-date data records (the first record is usually the latest)
            latest_income = financial_data.get('income_statement', [{}])[0] if financial_data.get('income_statement') else {}
            latest_balance = financial_data.get('balance_sheet', [{}])[0] if financial_data.get('balance_sheet') else {}
            latest_cashflow = financial_data.get('cashflow_statement', [{}])[0] if financial_data.get('cashflow_statement') else {}
            latest_indicator = financial_data.get('financial_indicators', [{}])[0] if financial_data.get('financial_indicators') else {}

            #Extract Basic Information
            symbol = ts_code.split('.')[0] if '.' in ts_code else ts_code
            report_period = latest_income.get('end_date') or latest_balance.get('end_date') or latest_cashflow.get('end_date')
            ann_date = latest_income.get('ann_date') or latest_balance.get('ann_date') or latest_cashflow.get('ann_date')

            #Compute TTM data
            income_statements = financial_data.get('income_statement', [])
            revenue_ttm = self._calculate_ttm_from_tushare(income_statements, 'revenue')
            net_profit_ttm = self._calculate_ttm_from_tushare(income_statements, 'n_income_attr_p')

            standardized_data = {
                #Basic information
                "symbol": symbol,
                "ts_code": ts_code,
                "report_period": report_period,
                "ann_date": ann_date,
                "report_type": self._determine_report_type(report_period),

                #Core profit statement indicators
                "revenue": self._safe_float(latest_income.get('revenue')),  #Operating income (single period)
                "revenue_ttm": revenue_ttm,  #Business income (TTM)
                "oper_rev": self._safe_float(latest_income.get('oper_rev')),  #Business income
                "net_income": self._safe_float(latest_income.get('n_income')),  #Net profit (single period)
                "net_profit": self._safe_float(latest_income.get('n_income_attr_p')),  #Net profits attributable to parent company (one instalment)
                "net_profit_ttm": net_profit_ttm,  #Net profit attributable to parent company (TTM)
                "oper_profit": self._safe_float(latest_income.get('oper_profit')),  #Business profits
                "total_profit": self._safe_float(latest_income.get('total_profit')),  #Total profits
                "oper_cost": self._safe_float(latest_income.get('oper_cost')),  #Operating costs
                "oper_exp": self._safe_float(latest_income.get('oper_exp')),  #Operating costs
                "admin_exp": self._safe_float(latest_income.get('admin_exp')),  #Administrative costs
                "fin_exp": self._safe_float(latest_income.get('fin_exp')),  #Financial costs
                "rd_exp": self._safe_float(latest_income.get('rd_exp')),  #R & D costs

                #Core balance sheet indicators
                "total_assets": self._safe_float(latest_balance.get('total_assets')),  #Total assets
                "total_liab": self._safe_float(latest_balance.get('total_liab')),  #Total liabilities
                "total_equity": self._safe_float(latest_balance.get('total_hldr_eqy_exc_min_int')),  #Shareholder interests
                "total_cur_assets": self._safe_float(latest_balance.get('total_cur_assets')),  #Current assets
                "total_nca": self._safe_float(latest_balance.get('total_nca')),  #Non-current assets
                "total_cur_liab": self._safe_float(latest_balance.get('total_cur_liab')),  #Current liabilities
                "total_ncl": self._safe_float(latest_balance.get('total_ncl')),  #Non-current liabilities
                "money_cap": self._safe_float(latest_balance.get('money_cap')),  #Monetary funds
                "accounts_receiv": self._safe_float(latest_balance.get('accounts_receiv')),  #Accounts receivable
                "inventories": self._safe_float(latest_balance.get('inventories')),  #Inventory
                "fix_assets": self._safe_float(latest_balance.get('fix_assets')),  #Fixed assets

                #Core indicators of the statement of cash flows
                "n_cashflow_act": self._safe_float(latest_cashflow.get('n_cashflow_act')),  #Cash flows from operating activities
                "n_cashflow_inv_act": self._safe_float(latest_cashflow.get('n_cashflow_inv_act')),  #Cash flows from investing activities
                "n_cashflow_fin_act": self._safe_float(latest_cashflow.get('n_cashflow_fin_act')),  #Cash flows from fund-raising activities
                "c_cash_equ_end_period": self._safe_float(latest_cashflow.get('c_cash_equ_end_period')),  #Cash at end of period
                "c_cash_equ_beg_period": self._safe_float(latest_cashflow.get('c_cash_equ_beg_period')),  #Cash at beginning of period

                #Financial indicators
                "roe": self._safe_float(latest_indicator.get('roe')),  #Net asset rate of return
                "roa": self._safe_float(latest_indicator.get('roa')),  #Total asset return
                "roe_waa": self._safe_float(latest_indicator.get('roe_waa')),  #Weighted average net asset return
                "roe_dt": self._safe_float(latest_indicator.get('roe_dt')),  #Net asset rate of return (net of non-recurrent gains and losses)
                "roa2": self._safe_float(latest_indicator.get('roa2')),  #Total asset return (net of non-recurrent gains and losses)
                "gross_margin": self._safe_float(latest_indicator.get('grossprofit_margin')),  #🔥 fixation: use grosprofit margin instead of gross margin (Maori absolute)
                "netprofit_margin": self._safe_float(latest_indicator.get('netprofit_margin')),  #Net interest rate on sales
                "cogs_of_sales": self._safe_float(latest_indicator.get('cogs_of_sales')),  #Sales cost rate
                "expense_of_sales": self._safe_float(latest_indicator.get('expense_of_sales')),  #Cost rate during sale
                "profit_to_gr": self._safe_float(latest_indicator.get('profit_to_gr')),  #Net profit/total operating income
                "saleexp_to_gr": self._safe_float(latest_indicator.get('saleexp_to_gr')),  #Sales costs/total operating income
                "adminexp_of_gr": self._safe_float(latest_indicator.get('adminexp_of_gr')),  #Management costs/total operating income
                "finaexp_of_gr": self._safe_float(latest_indicator.get('finaexp_of_gr')),  #Financial costs/total operating income
                "debt_to_assets": self._safe_float(latest_indicator.get('debt_to_assets')),  #Assets and liabilities ratio
                "assets_to_eqt": self._safe_float(latest_indicator.get('assets_to_eqt')),  #Entitlement multiplier
                "dp_assets_to_eqt": self._safe_float(latest_indicator.get('dp_assets_to_eqt')),  #Entitlement multiplier (Dubon analysis)
                "ca_to_assets": self._safe_float(latest_indicator.get('ca_to_assets')),  #Current/total assets
                "nca_to_assets": self._safe_float(latest_indicator.get('nca_to_assets')),  #Non-current/total assets
                "current_ratio": self._safe_float(latest_indicator.get('current_ratio')),  #Mobility ratio
                "quick_ratio": self._safe_float(latest_indicator.get('quick_ratio')),  #Speed ratio
                "cash_ratio": self._safe_float(latest_indicator.get('cash_ratio')),  #Cash ratio

                #Original data retention (for detailed analysis)
                "raw_data": {
                    "income_statement": financial_data.get('income_statement', []),
                    "balance_sheet": financial_data.get('balance_sheet', []),
                    "cashflow_statement": financial_data.get('cashflow_statement', []),
                    "financial_indicators": financial_data.get('financial_indicators', []),
                    "main_business": financial_data.get('main_business', [])
                },

                #Metadata
                "data_source": "tushare",
                "updated_at": datetime.utcnow()
            }

            return standardized_data

        except Exception as e:
            self.logger.error(f"The standard Tushare financial data failed:{e}")
            return {
                "symbol": ts_code.split('.')[0] if '.' in ts_code else ts_code,
                "data_source": "tushare",
                "updated_at": datetime.utcnow(),
                "error": str(e)
            }

    def _calculate_ttm_from_tushare(self, income_statements: list, field: str) -> Optional[float]:
        """Calculate TTM from Tushare profit statement data (most recent 12 months)

        Tushare profit statement data are cumulative values (cumulative from the beginning of the year to the reporting period):
        - 2025Q1 (20250331): Cumulative January-March 2025
        - 2025Q2 (20250630): Cumulative January-June 2025
        - 2025Q3 (20250930): Cumulative January-September 2025
        - 2025Q4 (202512131): Cumulative January-December 2025 (annual report)

        TTM formula:
        TTM = latest annual report after the same period last year + (cumulative for the current period - cumulative for the same period last year)

        For example: 2025Q2 TTM = 2024 + (2025Q2 - 2024Q2)
        = January-December 2024 + (January-June 2025-January-2024)
        = July-December 2024 + January-June 2025
        = Last 12 months

        Args:
            Income statements: list of profit sheet data (in descending order of reporting period)
            Field: Field name ('revenue' or 'n income attr p')

        Returns:
            TTM value, return None if uncalculated
        """
        if not income_statements or len(income_statements) < 1:
            return None

        try:
            latest = income_statements[0]
            latest_period = latest.get('end_date')
            latest_value = self._safe_float(latest.get(field))

            if not latest_period or latest_value is None:
                return None

            #Type of updated period
            month_day = latest_period[4:8]

            #If the latest report is annual (1231), directly used
            if month_day == '1231':
                self.logger.debug(f"✅ TTM calculations: use annualized data{latest_period} = {latest_value:.2f}")
                return latest_value

            #TM = base period + (cumulative for the current period - cumulative for the same period last year)

            #1. Finding the same period last year
            latest_year = latest_period[:4]
            last_year = str(int(latest_year) - 1)
            last_year_same_period = last_year + latest_period[4:]

            last_year_same = None
            for stmt in income_statements:
                if stmt.get('end_date') == last_year_same_period:
                    last_year_same = stmt
                    break

            if not last_year_same:
                #Lack of data for the same period last year to accurately calculate TTM
                self.logger.warning(f"⚠️ TTM calculation failed: missing data for the same period last year (need:{last_year_same_period}, latest:{latest_period}）")
                return None

            last_year_value = self._safe_float(last_year_same.get(field))
            if last_year_value is None:
                self.logger.warning(f"⚠️ TTM calculation failed: data values were empty for the same period last year{last_year_same_period}）")
                return None

            #2. Search for "the latest annual report after the same period last year" as a benchmark Period
            #For example, if the latest period is 2025Q2 and the same period last year is 2024Q2, find the 2024 newspaper (20241231)
            base_period = None
            for stmt in income_statements:
                period = stmt.get('end_date')
                #Requirement: After the same period last year and annual (1231)
                if period and period > last_year_same_period and period[4:8] == '1231':
                    base_period = stmt
                    break

            if not base_period:
                #I can't calculate without finding a proper annual report.
                #This usually happens: the latest period is 2025Q1, but the 2024 report is not published.
                self.logger.warning(f"⚠️ TTM calculation failed: lack of base year reporting (need to){last_year_same_period}Subsequent annual reports, latest:{latest_period}）")
                return None

            base_value = self._safe_float(base_period.get(field))
            if base_value is None:
                self.logger.warning(f"⚠️ TTM calculation failed: base year reported values are empty ({base_period.get('end_date')}）")
                return None

            #3. Calculation of TTM = base year + (cumulative for the current period - cumulative for the same period last year)
            ttm_value = base_value + (latest_value - last_year_value)

            self.logger.debug(
                f"TTM calculates:{base_period.get('end_date')}({base_value:.2f}) + "
                f"({latest_period}({latest_value:.2f}) - {last_year_same_period}({last_year_value:.2f})) = {ttm_value:.2f}"
            )

            return ttm_value

        except Exception as e:
            self.logger.warning(f"TTM calculation anomaly:{e}")
            return None

    def _determine_report_type(self, report_period: str) -> str:
        """Identification of types of reports by reporting period"""
        if not report_period:
            return "quarterly"

        try:
            #Format for reporting period: YYYYMMDD
            month_day = report_period[4:8]
            if month_day == "1231":
                return "annual"  #Annual report
            else:
                return "quarterly"  #Quarterly
        except:
            return "quarterly"

    def _safe_float(self, value) -> Optional[float]:
        """Safely converted to floating point numbers to handle anomalies"""
        if value is None:
            return None

        try:
            #Process String Type
            if isinstance(value, str):
                value = value.strip()
                if not value or value.lower() in ['nan', 'null', 'none', '--', '']:
                    return None
                #Remove possible unit symbol
                value = value.replace(',', '').replace('万', '').replace('亿', '')

            #Type of value processed
            if isinstance(value, (int, float)):
                #Check for NaN
                if isinstance(value, float) and (value != value):  #NAN check.
                    return None
                return float(value)

            #Try conversion
            return float(value)

        except (ValueError, TypeError, AttributeError):
            return None

    def _calculate_gross_profit(self, revenue, oper_cost) -> Optional[float]:
        """Safe calculation of gross profit"""
        revenue_float = self._safe_float(revenue)
        oper_cost_float = self._safe_float(oper_cost)

        if revenue_float is not None and oper_cost_float is not None:
            return revenue_float - oper_cost_float
        return None

    def _safe_str(self, value) -> Optional[str]:
        """Securely convert to string, handle NAN values"""
        if value is None:
            return None
        if isinstance(value, float) and (value != value):  #Check NAN.
            return None
        return str(value) if value else None


#Examples of global providers
_tushare_provider = None
_tushare_provider_initialized = False

def get_tushare_provider() -> TushareProvider:
    """Get global Tushare provider examples"""
    global _tushare_provider, _tushare_provider_initialized
    if _tushare_provider is None:
        _tushare_provider = TushareProvider()
        #Use synchronous connection to avoid the problem of a different context
        if not _tushare_provider_initialized:
            try:
                #connect using synchronous method
                _tushare_provider.connect_sync()
                #NOTE: there is asynchronous version of connect --> connect()
                #      Other providers use asynchronous connection by default.
                _tushare_provider_initialized = True
            except Exception as e:
                logger.warning(f"Tushare has failed:{e}")
    return _tushare_provider
