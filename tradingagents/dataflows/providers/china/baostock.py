#!/usr/bin/env python3
"""BaoStock Unified Data Provider
Implementation of BaseStockDataProvider interface to provide standardized BaoStock data access
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Union
import pandas as pd

from ..base_provider import BaseStockDataProvider

logger = logging.getLogger(__name__)


class BaoStockProvider(BaseStockDataProvider):
    """BaoStock Unified Data Provider"""
    
    def __init__(self):
        """Initializing BaoStock Provider"""
        super().__init__("baostock")
        self.bs = None
        self.connected = False
        self._init_baostock()
    
    def _init_baostock(self):
        """Initialize BaoStock Connection"""
        try:
            import baostock as bs
            self.bs = bs
            logger.info("BaoStock module loaded successfully")
            self.connected = True
        except ImportError as e:
            logger.error(f"The BaoStock module is not installed:{e}")
            self.connected = False
        except Exception as e:
            logger.error(f"The initialization of BaoStock failed:{e}")
            self.connected = False
    
    async def connect(self) -> bool:
        """Connect to BaoStock Data Source"""
        return await self.test_connection()

    async def test_connection(self) -> bool:
        """Test BaoStock Connection"""
        if not self.connected or not self.bs:
            return False
        
        try:
            #Step Test Login
            def test_login():
                lg = self.bs.login()
                if lg.error_code != '0':
                    raise Exception(f"登录失败: {lg.error_msg}")
                self.bs.logout()
                return True
            
            await asyncio.to_thread(test_login)
            logger.info("BaoStock connection test successfully.")
            return True
        except Exception as e:
            logger.error(f"BaoStock connection test failed:{e}")
            return False
    
    def get_stock_list_sync(self) -> Optional[pd.DataFrame]:
        """Retrieving list of shares (Sync version)"""
        if not self.connected:
            return None

        try:
            logger.info("Get the BaoStock List (Sync)...")

            lg = self.bs.login()
            if lg.error_code != '0':
                logger.error(f"BaoStock login failed:{lg.error_msg}")
                return None

            try:
                rs = self.bs.query_stock_basic()
                if rs.error_code != '0':
                    logger.error(f"BaoStock query failed:{rs.error_msg}")
                    return None

                data_list = []
                while (rs.error_code == '0') & rs.next():
                    data_list.append(rs.get_row_data())

                if not data_list:
                    logger.warning("BaoStock list is empty")
                    return None

                #Convert to DataFrame
                import pandas as pd
                df = pd.DataFrame(data_list, columns=rs.fields)

                #Stock type reserved only (type=1)
                df = df[df['type'] == '1']

                logger.info(f"BaoStock List was successful:{len(df)}Only stocks")
                return df

            finally:
                self.bs.logout()

        except Exception as e:
            logger.error(f"BaoStock failed to access the list of shares:{e}")
            return None

    async def get_stock_list(self) -> List[Dict[str, Any]]:
        """Get Stock List

        Returns:
            List of stocks, including codes and names
        """
        if not self.connected:
            return []
        
        try:
            logger.info("Get the BaoStock list...")
            
            def fetch_stock_list():
                lg = self.bs.login()
                if lg.error_code != '0':
                    raise Exception(f"登录失败: {lg.error_msg}")
                
                try:
                    rs = self.bs.query_stock_basic()
                    if rs.error_code != '0':
                        raise Exception(f"查询失败: {rs.error_msg}")
                    
                    data_list = []
                    while (rs.error_code == '0') & rs.next():
                        data_list.append(rs.get_row_data())
                    
                    return data_list, rs.fields
                finally:
                    self.bs.logout()
            
            data_list, fields = await asyncio.to_thread(fetch_stock_list)
            
            if not data_list:
                logger.warning("BaoStock list is empty")
                return []
            
            #Convert to Standard Formatting
            stock_list = []
            for row in data_list:
                if len(row) >= 6:
                    code = row[0]  # code
                    name = row[1]  # code_name
                    stock_type = row[4] if len(row) > 4 else '0'  # type
                    status = row[5] if len(row) > 5 else '0'  # status
                    
                    #A stock only (type=1, status=1)
                    if stock_type == '1' and status == '1':
                        #Convert code format sh.6000 - > 600000
                        clean_code = code.replace('sh.', '').replace('sz.', '')
                        stock_list.append({
                            "code": clean_code,
                            "name": str(name),
                            "source": "baostock"
                        })
            
            logger.info(f"BaoStock List was successful:{len(stock_list)}Only stocks")
            return stock_list
            
        except Exception as e:
            logger.error(f"BaoStock failed to access the list of shares:{e}")
            return []
    
    async def get_stock_basic_info(self, code: str) -> Dict[str, Any]:
        """Access to basic stock information

        Args:
            code: stock code

        Returns:
            Standardized stock base information
        """
        if not self.connected:
            return {}

        try:
            #Get details
            basic_info = await self._get_stock_info_detail(code)

            #Standardized data
            return {
                "code": code,
                "name": basic_info.get("name", f"股票{code}"),
                "industry": basic_info.get("industry", "未知"),
                "area": basic_info.get("area", "未知"),
                "list_date": basic_info.get("list_date", ""),
                "full_symbol": self._get_full_symbol(code),
                "market_info": self._get_market_info(code),
                "data_source": "baostock",
                "last_sync": datetime.now(timezone.utc),
                "sync_status": "success"
            }

        except Exception as e:
            logger.error(f"BaoStock{code}Could not close temporary folder: %s{e}")
            return {}

    async def get_valuation_data(self, code: str, trade_date: Optional[str] = None) -> Dict[str, Any]:
        """Acquisition of stock valuation data (PE, PB, PS, PCF, etc.)

        Args:
            code: stock code
            trade date: transaction date (YYYYY-MM-DD), default as last transaction date

        Returns:
            Valuation data dictionary containing pe ttm, pb mrq, ps tm, pcf ttm, close, total shares, etc.
        """
        if not self.connected:
            return {}

        try:
            #If no date is specified, use the latest 5 days (ensure that the latest transaction date data are available)
            if not trade_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
                start_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
            else:
                start_date = trade_date
                end_date = trade_date

            logger.debug(f"Access{code}Valuation data:{start_date}Present.{end_date}")

            def fetch_valuation_data():
                bs_code = self._to_baostock_code(code)
                lg = self.bs.login()
                if lg.error_code != '0':
                    raise Exception(f"登录失败: {lg.error_msg}")

                try:
                    #Access to valuation indicators: pETTM, pbMRQ, psTTM, pcfNcfTTM
                    rs = self.bs.query_history_k_data_plus(
                        code=bs_code,
                        fields="date,code,close,peTTM,pbMRQ,psTTM,pcfNcfTTM",
                        start_date=start_date,
                        end_date=end_date,
                        frequency="d",
                        adjustflag="3"  #Right of no restoration
                    )

                    if rs.error_code != '0':
                        raise Exception(f"查询失败: {rs.error_msg}")

                    data_list = []
                    while (rs.error_code == '0') & rs.next():
                        data_list.append(rs.get_row_data())

                    return data_list, rs.fields
                finally:
                    self.bs.logout()

            data_list, fields = await asyncio.to_thread(fetch_valuation_data)

            if not data_list:
                logger.warning(f"⚠️ {code}Valuation data are empty")
                return {}

            #Get the latest data.
            latest_row = data_list[-1]

            #Parsing data (fields: date, code, close, petTM, pbMRQ, pstTM, pcfNcfTTM)
            valuation_data = {
                "date": latest_row[0] if len(latest_row) > 0 else None,
                "code": code,
                "close": self._safe_float(latest_row[2]) if len(latest_row) > 2 else None,
                "pe_ttm": self._safe_float(latest_row[3]) if len(latest_row) > 3 else None,
                "pb_mrq": self._safe_float(latest_row[4]) if len(latest_row) > 4 else None,
                "ps_ttm": self._safe_float(latest_row[5]) if len(latest_row) > 5 else None,
                "pcf_ttm": self._safe_float(latest_row[6]) if len(latest_row) > 6 else None,
            }

            logger.debug(f"✅ {code}Successful acquisition of valuation data: PE={valuation_data['pe_ttm']}, PB={valuation_data['pb_mrq']}")
            return valuation_data

        except Exception as e:
            logger.error(f"BaoStock{code}Valuation data failed:{e}")
            return {}
    
    async def _get_stock_info_detail(self, code: str) -> Dict[str, Any]:
        """Get stock details"""
        try:
            def fetch_stock_info():
                bs_code = self._to_baostock_code(code)
                lg = self.bs.login()
                if lg.error_code != '0':
                    raise Exception(f"登录失败: {lg.error_msg}")
                
                try:
                    rs = self.bs.query_stock_basic(code=bs_code)
                    if rs.error_code != '0':
                        return {"code": code, "name": f"股票{code}"}
                    
                    data_list = []
                    while (rs.error_code == '0') & rs.next():
                        data_list.append(rs.get_row_data())
                    
                    if not data_list:
                        return {"code": code, "name": f"股票{code}"}
                    
                    row = data_list[0]
                    return {
                        "code": code,
                        "name": str(row[1]) if len(row) > 1 else f"股票{code}",  # code_name
                        "list_date": str(row[2]) if len(row) > 2 else "",  # ipoDate
                        "industry": "未知",  #BaoStock Basic Information does not cover industry
                        "area": "未知"  #BaoStock Basic Information Does Not Contain Areas
                    }
                finally:
                    self.bs.logout()
            
            return await asyncio.to_thread(fetch_stock_info)
            
        except Exception as e:
            logger.debug(f"Access{code}Could not close temporary folder: %s{e}")
            return {"code": code, "name": f"股票{code}", "industry": "未知", "area": "未知"}
    
    async def get_stock_quotes(self, code: str) -> Dict[str, Any]:
        """Getting stock in real time

        Args:
            code: stock code

        Returns:
            Standardized practice data
        """
        if not self.connected:
            return {}
        
        try:
            #BaoStock has no real-time line interface, using the latest day K-line data
            quotes_data = await self._get_latest_kline_data(code)
            
            if not quotes_data:
                return {}
            
            #Standardized data
            return {
                "code": code,
                "name": quotes_data.get("name", f"股票{code}"),
                "price": quotes_data.get("close", 0),
                "change": quotes_data.get("change", 0),
                "change_percent": quotes_data.get("change_percent", 0),
                "volume": quotes_data.get("volume", 0),
                "amount": quotes_data.get("amount", 0),
                "open": quotes_data.get("open", 0),
                "high": quotes_data.get("high", 0),
                "low": quotes_data.get("low", 0),
                "pre_close": quotes_data.get("preclose", 0),
                "full_symbol": self._get_full_symbol(code),
                "market_info": self._get_market_info(code),
                "data_source": "baostock",
                "last_sync": datetime.now(timezone.utc),
                "sync_status": "success"
            }
            
        except Exception as e:
            logger.error(f"BaoStock{code}Project failure:{e}")
            return {}
    
    async def _get_latest_kline_data(self, code: str) -> Dict[str, Any]:
        """Get the latest K-line data as a line"""
        try:
            def fetch_latest_kline():
                bs_code = self._to_baostock_code(code)
                lg = self.bs.login()
                if lg.error_code != '0':
                    raise Exception(f"登录失败: {lg.error_msg}")
                
                try:
                    #Access to data for the last 5 days
                    end_date = datetime.now().strftime('%Y-%m-%d')
                    start_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
                    
                    rs = self.bs.query_history_k_data_plus(
                        code=bs_code,
                        fields="date,code,open,high,low,close,preclose,volume,amount,pctChg",
                        start_date=start_date,
                        end_date=end_date,
                        frequency="d",
                        adjustflag="3"
                    )
                    
                    if rs.error_code != '0':
                        return {}
                    
                    data_list = []
                    while (rs.error_code == '0') & rs.next():
                        data_list.append(rs.get_row_data())
                    
                    if not data_list:
                        return {}
                    
                    #Get the latest data.
                    latest_row = data_list[-1]
                    return {
                        "name": f"股票{code}",
                        "open": self._safe_float(latest_row[2]),
                        "high": self._safe_float(latest_row[3]),
                        "low": self._safe_float(latest_row[4]),
                        "close": self._safe_float(latest_row[5]),
                        "preclose": self._safe_float(latest_row[6]),
                        "volume": self._safe_int(latest_row[7]),
                        "amount": self._safe_float(latest_row[8]),
                        "change_percent": self._safe_float(latest_row[9]),
                        "change": self._safe_float(latest_row[5]) - self._safe_float(latest_row[6])
                    }
                finally:
                    self.bs.logout()
            
            return await asyncio.to_thread(fetch_latest_kline)
            
        except Exception as e:
            logger.debug(f"Access{code}Recent K-line data failed:{e}")
            return {}
    
    def _to_baostock_code(self, symbol: str) -> str:
        """Convert to BaoStock Code Format"""
        s = str(symbol).strip().upper()
        #Processing 600519. SH/000001.SZ/600519/000001
        if s.endswith('.SH') or s.endswith('.SZ'):
            code, exch = s.split('.')
            prefix = 'sh' if exch == 'SH' else 'sz'
            return f"{prefix}.{code}"
        #6 Initial surrender, otherwise in-depth (simplified rule)
        if len(s) >= 6 and s[0] == '6':
            return f"sh.{s[:6]}"
        return f"sz.{s[:6]}"
    
    def _determine_market(self, code: str) -> str:
        """Identification of stock markets"""
        if code.startswith('6'):
            return "上海证券交易所"
        elif code.startswith('0') or code.startswith('3'):
            return "深圳证券交易所"
        elif code.startswith('8'):
            return "北京证券交易所"
        else:
            return "未知市场"
    
    def _get_full_symbol(self, code: str) -> str:
        """Get the full stock code

        Args:
            code: 6-bit stock code

        Returns:
            Full standardized code, return original code if unidentifiable (ensure not to be empty)
        """
        #Make sure the code isn't empty.
        if not code:
            return ""

        #Standardise as String
        code = str(code).strip()

        #By prefixing the exchange
        if code.startswith(('6', '9')):  #Shanghai Stock Exchange (addition of initial 9 B shares)
            return f"{code}.SS"
        elif code.startswith(('0', '3', '2')):  #Shenzhen Stock Exchange (add 2 initial B shares)
            return f"{code}.SZ"
        elif code.startswith(('8', '4')):  #Beijing Stock Exchange (add 4 new board)
            return f"{code}.BJ"
        else:
            #Unidentifiable code, return original code (ensure not to be empty)
            return code if code else ""
    
    def _get_market_info(self, code: str) -> Dict[str, Any]:
        """Access to market information"""
        if code.startswith('6'):
            return {
                "market_type": "CN",
                "exchange": "SSE",
                "exchange_name": "上海证券交易所",
                "currency": "CNY",
                "timezone": "Asia/Shanghai"
            }
        elif code.startswith('0') or code.startswith('3'):
            return {
                "market_type": "CN",
                "exchange": "SZSE", 
                "exchange_name": "深圳证券交易所",
                "currency": "CNY",
                "timezone": "Asia/Shanghai"
            }
        elif code.startswith('8'):
            return {
                "market_type": "CN",
                "exchange": "BSE",
                "exchange_name": "北京证券交易所", 
                "currency": "CNY",
                "timezone": "Asia/Shanghai"
            }
        else:
            return {
                "market_type": "CN",
                "exchange": "UNKNOWN",
                "exchange_name": "未知交易所",
                "currency": "CNY",
                "timezone": "Asia/Shanghai"
            }
    
    def _safe_float(self, value: Any) -> float:
        """Convert safe to floating point"""
        try:
            if value is None or value == '' or value == 'None':
                return 0.0
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def _safe_int(self, value: Any) -> int:
        """Convert safe to integer"""
        try:
            if value is None or value == '' or value == 'None':
                return 0
            return int(float(value))
        except (ValueError, TypeError):
            return 0
    
    def _safe_str(self, value: Any) -> str:
        """Securely convert to string"""
        try:
            if value is None:
                return ""
            return str(value)
        except:
            return ""

    async def get_historical_data(self, code: str, start_date: str, end_date: str,
                                period: str = "daily") -> Optional[pd.DataFrame]:
        """Access to historical data

        Args:
            code: stock code
            Start date: Start date (YYYYY-MM-DD)
            End date: End Date (YYYYY-MM-DD)
            period: data cycle

        Returns:
            DataFrame
        """
        if not self.connected:
            return None

        try:
            logger.info(f"To access BaoStock's historical data:{code} ({start_date}Present.{end_date})")

            #Conversion cycle parameters
            frequency_map = {
                "daily": "d",
                "weekly": "w",
                "monthly": "m"
            }
            bs_frequency = frequency_map.get(period, "d")

            def fetch_historical_data():
                bs_code = self._to_baostock_code(code)
                lg = self.bs.login()
                if lg.error_code != '0':
                    raise Exception(f"登录失败: {lg.error_msg}")

                try:
                    #Select different fields based on frequency (less supported by weeklines and moon lines)
                    if bs_frequency == "d":
                        fields_str = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST"
                    else:
                        #The weekly and moon lines only support base fields
                        fields_str = "date,code,open,high,low,close,volume,amount,pctChg"

                    rs = self.bs.query_history_k_data_plus(
                        code=bs_code,
                        fields=fields_str,
                        start_date=start_date,
                        end_date=end_date,
                        frequency=bs_frequency,
                        adjustflag="2"  #Former right of reinstatement
                    )

                    if rs.error_code != '0':
                        raise Exception(f"查询失败: {rs.error_msg}")

                    data_list = []
                    while (rs.error_code == '0') & rs.next():
                        data_list.append(rs.get_row_data())

                    return data_list, rs.fields
                finally:
                    self.bs.logout()

            data_list, fields = await asyncio.to_thread(fetch_historical_data)

            if not data_list:
                logger.warning(f"The historical data of BaoStock is empty:{code}")
                return None

            #Convert to DataFrame
            df = pd.DataFrame(data_list, columns=fields)

            #Data type conversion
            numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'pctChg', 'turn']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            #If no preclose field, use the previous day ' s closing price estimate Fine.
            if 'preclose' not in df.columns and len(df) > 0:
                df['preclose'] = df['close'].shift(1)
                df.loc[0, 'preclose'] = df.loc[0, 'close']  #First line uses the current closing price

            #Standardized listing
            df = df.rename(columns={
                'pctChg': 'change_percent'
            })

            #Add standardized fields
            df['股票代码'] = code
            df['full_symbol'] = self._get_full_symbol(code)

            logger.info(f"BaoStock history data acquisition success:{code}, {len(df)}Notes")
            return df

        except Exception as e:
            logger.error(f"BaoStock{code}Historical data failed:{e}")
            return None

    async def get_financial_data(self, code: str, year: Optional[int] = None,
                               quarter: Optional[int] = None) -> Dict[str, Any]:
        """Access to financial data

        Args:
            code: stock code
            year: year
            Qarter: Quarterly

        Returns:
            Financial data dictionary
        """
        if not self.connected:
            return {}

        try:
            logger.info(f"To obtain BaoStock financial data:{code}")

            #If no year and quarter are specified, use the latest quarter of the current year
            if year is None:
                year = datetime.now().year
            if quarter is None:
                current_month = datetime.now().month
                quarter = (current_month - 1) // 3 + 1

            financial_data = {}

            #1. Access to profitability data
            try:
                profit_data = await self._get_profit_data(code, year, quarter)
                if profit_data:
                    financial_data['profit_data'] = profit_data
                    logger.debug(f"✅ {code}Profitability data acquisition success")
            except Exception as e:
                logger.debug(f"Access{code}Loss of profitability data:{e}")

            #2. Access to operational capacity data
            try:
                operation_data = await self._get_operation_data(code, year, quarter)
                if operation_data:
                    financial_data['operation_data'] = operation_data
                    logger.debug(f"✅ {code}Operational capability data acquisition success")
            except Exception as e:
                logger.debug(f"Access{code}Operational capability data failure:{e}")

            #3. Access to growth data
            try:
                growth_data = await self._get_growth_data(code, year, quarter)
                if growth_data:
                    financial_data['growth_data'] = growth_data
                    logger.debug(f"✅ {code}Growth data acquisition success")
            except Exception as e:
                logger.debug(f"Access{code}Growth data fail:{e}")

            #4. Access to solvency data
            try:
                balance_data = await self._get_balance_data(code, year, quarter)
                if balance_data:
                    financial_data['balance_data'] = balance_data
                    logger.debug(f"✅ {code}Debt service data acquisition success")
            except Exception as e:
                logger.debug(f"Access{code}Debt service data fail:{e}")

            #5. Access to cash flow data
            try:
                cash_flow_data = await self._get_cash_flow_data(code, year, quarter)
                if cash_flow_data:
                    financial_data['cash_flow_data'] = cash_flow_data
                    logger.debug(f"✅ {code}Successful access to cash flow data")
            except Exception as e:
                logger.debug(f"Access{code}Loss of cash flow data:{e}")

            if financial_data:
                logger.info(f"BaoStock's financial data were obtained successfully:{code}, {len(financial_data)}Data sets")
            else:
                logger.warning(f"BaoStock financial data is empty:{code}")

            return financial_data

        except Exception as e:
            logger.error(f"BaoStock{code}Financial data failed:{e}")
            return {}

    async def _get_profit_data(self, code: str, year: int, quarter: int) -> Optional[Dict[str, Any]]:
        """Access to data on profitability"""
        try:
            def fetch_profit_data():
                bs_code = self._to_baostock_code(code)
                lg = self.bs.login()
                if lg.error_code != '0':
                    raise Exception(f"登录失败: {lg.error_msg}")

                try:
                    rs = self.bs.query_profit_data(code=bs_code, year=year, quarter=quarter)
                    if rs.error_code != '0':
                        return None

                    data_list = []
                    while (rs.error_code == '0') & rs.next():
                        data_list.append(rs.get_row_data())

                    return data_list, rs.fields
                finally:
                    self.bs.logout()

            result = await asyncio.to_thread(fetch_profit_data)
            if not result or not result[0]:
                return None

            data_list, fields = result
            df = pd.DataFrame(data_list, columns=fields)
            return df.to_dict('records')[0] if not df.empty else None

        except Exception as e:
            logger.debug(f"Access{code}Loss of profitability data:{e}")
            return None

    async def _get_operation_data(self, code: str, year: int, quarter: int) -> Optional[Dict[str, Any]]:
        """Access to operational capacity data"""
        try:
            def fetch_operation_data():
                bs_code = self._to_baostock_code(code)
                lg = self.bs.login()
                if lg.error_code != '0':
                    raise Exception(f"登录失败: {lg.error_msg}")

                try:
                    rs = self.bs.query_operation_data(code=bs_code, year=year, quarter=quarter)
                    if rs.error_code != '0':
                        return None

                    data_list = []
                    while (rs.error_code == '0') & rs.next():
                        data_list.append(rs.get_row_data())

                    return data_list, rs.fields
                finally:
                    self.bs.logout()

            result = await asyncio.to_thread(fetch_operation_data)
            if not result or not result[0]:
                return None

            data_list, fields = result
            df = pd.DataFrame(data_list, columns=fields)
            return df.to_dict('records')[0] if not df.empty else None

        except Exception as e:
            logger.debug(f"Access{code}Operational capability data failure:{e}")
            return None

    async def _get_growth_data(self, code: str, year: int, quarter: int) -> Optional[Dict[str, Any]]:
        """Access to growth data"""
        try:
            def fetch_growth_data():
                bs_code = self._to_baostock_code(code)
                lg = self.bs.login()
                if lg.error_code != '0':
                    raise Exception(f"登录失败: {lg.error_msg}")

                try:
                    rs = self.bs.query_growth_data(code=bs_code, year=year, quarter=quarter)
                    if rs.error_code != '0':
                        return None

                    data_list = []
                    while (rs.error_code == '0') & rs.next():
                        data_list.append(rs.get_row_data())

                    return data_list, rs.fields
                finally:
                    self.bs.logout()

            result = await asyncio.to_thread(fetch_growth_data)
            if not result or not result[0]:
                return None

            data_list, fields = result
            df = pd.DataFrame(data_list, columns=fields)
            return df.to_dict('records')[0] if not df.empty else None

        except Exception as e:
            logger.debug(f"Access{code}Growth data fail:{e}")
            return None

    async def _get_balance_data(self, code: str, year: int, quarter: int) -> Optional[Dict[str, Any]]:
        """Access to solvency data"""
        try:
            def fetch_balance_data():
                bs_code = self._to_baostock_code(code)
                lg = self.bs.login()
                if lg.error_code != '0':
                    raise Exception(f"登录失败: {lg.error_msg}")

                try:
                    rs = self.bs.query_balance_data(code=bs_code, year=year, quarter=quarter)
                    if rs.error_code != '0':
                        return None

                    data_list = []
                    while (rs.error_code == '0') & rs.next():
                        data_list.append(rs.get_row_data())

                    return data_list, rs.fields
                finally:
                    self.bs.logout()

            result = await asyncio.to_thread(fetch_balance_data)
            if not result or not result[0]:
                return None

            data_list, fields = result
            df = pd.DataFrame(data_list, columns=fields)
            return df.to_dict('records')[0] if not df.empty else None

        except Exception as e:
            logger.debug(f"Access{code}Debt service data fail:{e}")
            return None

    async def _get_cash_flow_data(self, code: str, year: int, quarter: int) -> Optional[Dict[str, Any]]:
        """Access to cash flow data"""
        try:
            def fetch_cash_flow_data():
                bs_code = self._to_baostock_code(code)
                lg = self.bs.login()
                if lg.error_code != '0':
                    raise Exception(f"登录失败: {lg.error_msg}")

                try:
                    rs = self.bs.query_cash_flow_data(code=bs_code, year=year, quarter=quarter)
                    if rs.error_code != '0':
                        return None

                    data_list = []
                    while (rs.error_code == '0') & rs.next():
                        data_list.append(rs.get_row_data())

                    return data_list, rs.fields
                finally:
                    self.bs.logout()

            result = await asyncio.to_thread(fetch_cash_flow_data)
            if not result or not result[0]:
                return None

            data_list, fields = result
            df = pd.DataFrame(data_list, columns=fields)
            return df.to_dict('records')[0] if not df.empty else None

        except Exception as e:
            logger.debug(f"Access{code}Loss of cash flow data:{e}")
            return None


#Examples of global providers
_baostock_provider = None


def get_baostock_provider() -> BaoStockProvider:
    """Get a global BaoStock provider example"""
    global _baostock_provider
    if _baostock_provider is None:
        _baostock_provider = BaoStockProvider()
    return _baostock_provider
