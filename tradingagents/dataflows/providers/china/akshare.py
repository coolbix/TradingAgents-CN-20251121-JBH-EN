"""AKShare Unified Data Provider
Harmonized Data Synchronization Program based on AKShare SDK, providing standardized data interfaces
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Union
import pandas as pd

from ..base_provider import BaseStockDataProvider

logger = logging.getLogger(__name__)


class AKShareProvider(BaseStockDataProvider):
    """AKShare Unified Data Provider

Provide standardized stock data interfaces to support:
- Access to basic stock information
- Historical data.
- Timeline data
- Financial data
- Port Unit data support
"""
    
    def __init__(self):
        super().__init__("AKShare")
        self.ak = None
        self.connected = False
        self._stock_list_cache = None  #Cache list of shares to avoid duplication
        self._cache_time = None  #Cache Time
        self._initialize_akshare()
    
    def _initialize_akshare(self):
        """Initialize AKShare Connection"""
        try:
            import akshare as ak
            import requests
            import time

            #Try importing curl cffi and use it, if available, to bypass anti-crawlers
            try:
                from curl_cffi import requests as curl_requests
                use_curl_cffi = True
                logger.info("Curl cffi was detected and will be used to simulate real browser TLS fingerprints")
            except ImportError:
                use_curl_cffi = False
                logger.warning("⚠️curl cffi, not installed, will use standard requets (possible anti-pastoral intercept)")
                logger.warning("Suggested installation: pip initial Curl-cffi")

            #Fixing AKShare's bug: setting default headers for requests and adding request delay
            #The AKShare Stock news em() function does not set the necessary headers, resulting in API returning to empty sound Response
            if not hasattr(requests, '_akshare_headers_patched'):
                original_get = requests.get
                last_request_time = {'time': 0}  #Use dictionary to modify in closed package

                def patched_get(url, **kwargs):
                    """Packaging lists.get method, automatically adding necessary headers and request delay
Fixing the AKShare stock news em() function missing headers problem
Simulate real browser TLS fingerprints with curl cffi, if available
"""
                    #Add request delay to avoid anti-crawling Seal Ban
                    #Add delay only to requests for Eastern Wealth Network
                    if 'eastmoney.com' in url:
                        current_time = time.time()
                        time_since_last_request = current_time - last_request_time['time']
                        if time_since_last_request < 0.5:  #At least 0.5 seconds apart.
                            time.sleep(0.5 - time_since_last_request)
                        last_request_time['time'] = time.time()

                    #If it's a request from the Eastern Wealth Network, and Curl cffi is available, use it to bypass the anti-pastoral.
                    if use_curl_cffi and 'eastmoney.com' in url:
                        try:
                            #Use curl cffi to simulate the TLS fingerprints of Chrome 120
                            #Note: Do not send custom headers when using impersonate, let curl cffi automatically set
                            curl_kwargs = {
                                'timeout': kwargs.get('timeout', 10),
                                'impersonate': "chrome120"  #Simulation Chrome 120
                            }

                            #Pass only non-headers arguments
                            if 'params' in kwargs:
                                curl_kwargs['params'] = kwargs['params']
                            #Do not pass headers, allow automatic setting
                            if 'data' in kwargs:
                                curl_kwargs['data'] = kwargs['data']
                            if 'json' in kwargs:
                                curl_kwargs['json'] = kwargs['json']

                            response = curl_requests.get(url, **curl_kwargs)
                            #curl cffi Response
                            return response
                        except Exception as e:
                            #Curl cffi failed, back to standard
                            error_msg = str(e)
                            #Ignore TLS library error and 400 error details log (this is a known problem for Docker environment)
                            if 'invalid library' not in error_msg and '400' not in error_msg:
                                logger.warning(f"The request failed.{e}")

                    #Standard requests (non-Oriental Wealth Network, or Curl cffi not available/failed)
                    #Set browser request header
                    if 'headers' not in kwargs or kwargs['headers'] is None:
                        kwargs['headers'] = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Referer': 'https://www.eastmoney.com/',
                            'Connection': 'keep-alive',
                        }
                    elif isinstance(kwargs['headers'], dict):
                        #Ensure that necessary fields are included if there are headers
                        if 'User-Agent' not in kwargs['headers']:
                            kwargs['headers']['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                        if 'Referer' not in kwargs['headers']:
                            kwargs['headers']['Referer'] = 'https://www.eastmoney.com/'
                        if 'Accept' not in kwargs['headers']:
                            kwargs['headers']['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
                        if 'Accept-Language' not in kwargs['headers']:
                            kwargs['headers']['Accept-Language'] = 'zh-CN,zh;q=0.9,en;q=0.8'

                    #Add a retry mechanism (up to 3)
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            return original_get(url, **kwargs)
                        except Exception as e:
                            #Check for SSL error
                            error_str = str(e)
                            is_ssl_error = ('SSL' in error_str or 'ssl' in error_str or
                                          'UNEXPECTED_EOF_WHILE_READING' in error_str)

                            if is_ssl_error and attempt < max_retries - 1:
                                #SSL error, wait and try again
                                wait_time = 0.5 * (attempt + 1)  #Incremental waiting time
                                time.sleep(wait_time)
                                continue
                            else:
                                #Non-SSL error or maximum number of retrys reached, directly thrown
                                raise

                #Apply Patch
                requests.get = patched_get
                requests._akshare_headers_patched = True

                if use_curl_cffi:
                    logger.info("🔧 Rehabilitated AKShare's headers problem using curl cffi Simulation Real Browser (Chrome 120)")
                else:
                    logger.info("🔧 Rehabilitated AKShare's headers problem and added request delay (0.5 seconds)")

            self.ak = ak
            self.connected = True

            #Configure Timeout and Retry
            self._configure_timeout()

            logger.info("The AKShare connection was successful.")
        except ImportError as e:
            logger.error(f"AKShare is not installed:{e}")
            self.connected = False
        except Exception as e:
            logger.error(f"The initialization of AKshare failed:{e}")
            self.connected = False

    def _get_stock_news_direct(self, symbol: str, limit: int = 10) -> Optional[pd.DataFrame]:
        """Direct to East Wealth Network News, API.
Simulate real browser using curl cffi for Docker environment

Args:
symbol: stock code
Limited number of returns

Returns:
News DataFrame or None
"""
        try:
            from curl_cffi import requests as curl_requests
            import json
            import time
            import os

            #Standardised stock code
            symbol_6 = symbol.zfill(6)

            #Build Request Parameters
            url = "https://search-api-web.eastmoney.com/search/jsonp"
            param = {
                "uid": "",
                "keyword": symbol_6,
                "type": ["cmsArticleWebOld"],
                "client": "web",
                "clientType": "web",
                "clientVersion": "curr",
                "param": {
                    "cmsArticleWebOld": {
                        "searchScope": "default",
                        "sort": "default",
                        "pageIndex": 1,
                        "pageSize": limit,
                        "preTag": "<em>",
                        "postTag": "</em>"
                    }
                }
            }

            params = {
                "cb": f"jQuery{int(time.time() * 1000)}",
                "param": json.dumps(param),
                "_": str(int(time.time() * 1000))
            }

            #Send request with curl cffi
            response = curl_requests.get(
                url,
                params=params,
                timeout=10,
                impersonate="chrome120"
            )

            if response.status_code != 200:
                self.logger.error(f"❌ {symbol}Eastern Wealth Network API returned error:{response.status_code}")
                return None

            #Parsing JSONP Response
            text = response.text
            if text.startswith("jQuery"):
                text = text[text.find("(")+1:text.rfind(")")]

            data = json.loads(text)

            #Check Back Data
            if "result" not in data or "cmsArticleWebOld" not in data["result"]:
                self.logger.error(f"❌ {symbol}East Wealth Network API returns data structure abnormal")
                return None

            articles = data["result"]["cmsArticleWebOld"]

            if not articles:
                self.logger.warning(f"⚠️ {symbol}No news obtained")
                return None

            #Convert to DataFrame (compatible with AKShare format)
            news_data = []
            for article in articles:
                news_data.append({
                    "新闻标题": article.get("title", ""),
                    "新闻内容": article.get("content", ""),
                    "发布时间": article.get("date", ""),
                    "新闻链接": article.get("url", ""),
                    "关键词": article.get("keywords", ""),
                    "新闻来源": article.get("source", "东方财富网"),
                    "新闻类型": article.get("type", "")
                })

            df = pd.DataFrame(news_data)
            self.logger.info(f"✅ {symbol}Directly calling API for news success:{len(df)}Article")
            return df

        except Exception as e:
            self.logger.error(f"❌ {symbol}Could not close temporary folder: %s{e}")
            return None

    def _configure_timeout(self):
        """Configure timeout settings for AKShare"""
        try:
            import socket
            socket.setdefaulttimeout(60)  #60 seconds past time.
            logger.info("AKShare's timeout is complete: 60 seconds.")
        except Exception as e:
            logger.warning(f"AKShare's overtime configuration failed:{e}")
    
    async def connect(self) -> bool:
        """Connect to AKShare Data Source"""
        return await self.test_connection()

    async def test_connection(self) -> bool:
        """Test AKShare Connection"""
        if not self.connected:
            return False

        #AKShare is based on web reptiles.
        #As long as the library has been imported, it is considered available.
        #Actual network requests are made when called and each has its own error processing
        logger.info("✅The AKShare connection test was successfully tested (the library is loaded)")
        return True
    
    def get_stock_list_sync(self) -> Optional[pd.DataFrame]:
        """Retrieving list of shares (Sync version)"""
        if not self.connected:
            return None

        try:
            logger.info("Get AKShare List (Sync)...")
            stock_df = self.ak.stock_info_a_code_name()

            if stock_df is None or stock_df.empty:
                logger.warning("The list of Akshare shares is empty.")
                return None

            logger.info(f"The AKShare list was successful:{len(stock_df)}Only stocks")
            return stock_df

        except Exception as e:
            logger.error(f"AKShare failed to access the list of shares:{e}")
            return None

    async def get_stock_list(self) -> List[Dict[str, Any]]:
        """Get Stock List

Returns:
List of stocks, including codes and names
"""
        if not self.connected:
            return []

        try:
            logger.info("Get the AKshare list...")

            #Use a linear pool walk to get a list of shares and add timeout protection
            def fetch_stock_list():
                return self.ak.stock_info_a_code_name()

            stock_df = await asyncio.to_thread(fetch_stock_list)

            if stock_df is None or stock_df.empty:
                logger.warning("The list of Akshare shares is empty.")
                return []

            #Convert to Standard Formatting
            stock_list = []
            for _, row in stock_df.iterrows():
                stock_list.append({
                    "code": str(row.get("code", "")),
                    "name": str(row.get("name", "")),
                    "source": "akshare"
                })

            logger.info(f"The AKShare list was successful:{len(stock_list)}Only stocks")
            return stock_list

        except Exception as e:
            logger.error(f"AKShare failed to access the list of shares:{e}")
            return []
    
    async def get_stock_basic_info(self, code: str) -> Optional[Dict[str, Any]]:
        """Access to basic stock information

Args:
code: stock code

Returns:
Standardized stock base information
"""
        if not self.connected:
            return None
        
        try:
            logger.debug(f"Access{code}Basic information...")
            
            #Access to basic stock information
            stock_info = await self._get_stock_info_detail(code)
            
            if not stock_info:
                logger.warning(f"Not found{code}Basic information")
                return None
            
            #Convert to a standardized dictionary
            basic_info = {
                "code": code,
                "name": stock_info.get("name", f"股票{code}"),
                "area": stock_info.get("area", "未知"),
                "industry": stock_info.get("industry", "未知"),
                "market": self._determine_market(code),
                "list_date": stock_info.get("list_date", ""),
                #Expand Fields
                "full_symbol": self._get_full_symbol(code),
                "market_info": self._get_market_info(code),
                "data_source": "akshare",
                "last_sync": datetime.now(timezone.utc),
                "sync_status": "success"
            }
            
            logger.debug(f"✅ {code}Access to basic information was successful")
            return basic_info
            
        except Exception as e:
            logger.error(f"Access{code}Could not close temporary folder: %s{e}")
            return None
    
    async def _get_stock_list_cached(self):
        """Retrieving list of cached shares (duplicate acquisitions)"""
        from datetime import datetime, timedelta

        #If cache exists and does not expire (1 hour), return directly
        if self._stock_list_cache is not None and self._cache_time is not None:
            if datetime.now() - self._cache_time < timedelta(hours=1):
                return self._stock_list_cache

        #Otherwise retake
        def fetch_stock_list():
            return self.ak.stock_info_a_code_name()

        try:
            stock_list = await asyncio.to_thread(fetch_stock_list)
            if stock_list is not None and not stock_list.empty:
                self._stock_list_cache = stock_list
                self._cache_time = datetime.now()
                logger.info(f"List of stock cache updates:{len(stock_list)}Only stocks")
                return stock_list
        except Exception as e:
            logger.error(f"Can not get folder: %s: %s{e}")

        return None

    async def _get_stock_info_detail(self, code: str) -> Dict[str, Any]:
        """Get stock details"""
        try:
            #Method 1: Attempt to obtain details of the unit (including industry, region, etc.)
            def fetch_individual_info():
                return self.ak.stock_individual_info_em(symbol=code)

            try:
                stock_info = await asyncio.to_thread(fetch_individual_info)

                if stock_info is not None and not stock_info.empty:
                    #Can not open message
                    info = {"code": code}

                    #Extract stock name
                    name_row = stock_info[stock_info['item'] == '股票简称']
                    if not name_row.empty:
                        info['name'] = str(name_row['value'].iloc[0])

                    #Ripping industry information
                    industry_row = stock_info[stock_info['item'] == '所属行业']
                    if not industry_row.empty:
                        info['industry'] = str(industry_row['value'].iloc[0])

                    #Can not open message
                    area_row = stock_info[stock_info['item'] == '所属地区']
                    if not area_row.empty:
                        info['area'] = str(area_row['value'].iloc[0])

                    #Extract listing date
                    list_date_row = stock_info[stock_info['item'] == '上市时间']
                    if not list_date_row.empty:
                        info['list_date'] = str(list_date_row['value'].iloc[0])

                    return info
            except Exception as e:
                logger.debug(f"Access{code}The details of the units failed:{e}")

            #Method 2: Obtain basic information (codes and names only) from the list of cached shares
            try:
                stock_list = await self._get_stock_list_cached()
                if stock_list is not None and not stock_list.empty:
                    stock_row = stock_list[stock_list['code'] == code]
                    if not stock_row.empty:
                        return {
                            "code": code,
                            "name": str(stock_row['name'].iloc[0]),
                            "industry": "未知",
                            "area": "未知"
                        }
            except Exception as e:
                logger.debug(f"Get from the stock list{code}Can not open message{e}")

            #If both fail, return basic information
            return {"code": code, "name": f"股票{code}", "industry": "未知", "area": "未知"}

        except Exception as e:
            logger.debug(f"Access{code}Could not close temporary folder: %s{e}")
            return {"code": code, "name": f"股票{code}", "industry": "未知", "area": "未知"}
    
    def _determine_market(self, code: str) -> str:
        """The market is judged by stock code."""
        if code.startswith(('60', '68')):
            return "上海证券交易所"
        elif code.startswith(('00', '30')):
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
        if code.startswith(('60', '68', '90')):  #Shanghai Stock Exchange (addition of B stock starting with 90)
            return f"{code}.SS"
        elif code.startswith(('00', '30', '20')):  #Shenzhen Stock Exchange (addition of B stock starting 20)
            return f"{code}.SZ"
        elif code.startswith(('8', '4')):  #Beijing Stock Exchange (add 4 new board)
            return f"{code}.BJ"
        else:
            #Unidentifiable code, return original code (ensure not to be empty)
            return code if code else ""
    
    def _get_market_info(self, code: str) -> Dict[str, Any]:
        """Access to market information"""
        if code.startswith(('60', '68')):
            return {
                "market_type": "CN",
                "exchange": "SSE",
                "exchange_name": "上海证券交易所",
                "currency": "CNY",
                "timezone": "Asia/Shanghai"
            }
        elif code.startswith(('00', '30')):
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
    
    async def get_batch_stock_quotes(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """Batch acquisition of real-time equity performance (optimized version: a market-wide snapshot)

Give priority to the New Wave financial interface (more stable) and back to the East wealth interface when it fails

Args:
codes: list of stock codes

Returns:
Stock code to line data map dictionary
"""
        if not self.connected:
            return {}

        #Retry Logic
        max_retries = 2
        retry_delay = 1  #sec

        for attempt in range(max_retries):
            try:
                logger.debug(f"Bulk acquisition{len(codes)}The real-time business of stocks only...{attempt + 1}/{max_retries})")

                #Prioritize the use of the New Wave financial interface (more stable, not easily sealed)
                def fetch_spot_data_sina():
                    import time
                    time.sleep(0.3)  #Add Delay Avoid Frequency Limit
                    return self.ak.stock_zh_a_spot()

                try:
                    spot_df = await asyncio.to_thread(fetch_spot_data_sina)
                    data_source = "sina"
                    logger.debug("✅ for data acquisition using the New Wave financial interface")
                except Exception as e:
                    logger.warning(f"The New Wave interface failed:{e}Try the Eastern wealth interface...")
                    #Back to the East Wealth Interface.
                    def fetch_spot_data_em():
                        import time
                        time.sleep(0.5)
                        return self.ak.stock_zh_a_spot_em()
                    spot_df = await asyncio.to_thread(fetch_spot_data_em)
                    data_source = "eastmoney"
                    logger.debug("✅ Using the Eastern Wealth Interface for Data")

                if spot_df is None or spot_df.empty:
                    logger.warning("All-market snapshots are empty.")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay)
                        continue
                    return {}

                #Build a map of code to line
                quotes_map = {}
                codes_set = set(codes)

                #Build code map (support for prefix matching)
                #For example: sh60,000 - > 600 000, sz00001 - > 000001
                code_mapping = {}
                for code in codes:
                    code_mapping[code] = code  #Original code
                    #Add possible prefix variant
                    for prefix in ['sh', 'sz', 'bj']:
                        code_mapping[f"{prefix}{code}"] = code

                for _, row in spot_df.iterrows():
                    raw_code = str(row.get("代码", ""))

                    #Try matching codes (support prefixes and without prefixes)
                    matched_code = None
                    if raw_code in code_mapping:
                        matched_code = code_mapping[raw_code]
                    elif raw_code in codes_set:
                        matched_code = raw_code

                    if matched_code:
                        quotes_data = {
                            "name": str(row.get("名称", f"股票{matched_code}")),
                            "price": self._safe_float(row.get("最新价", 0)),
                            "change": self._safe_float(row.get("涨跌额", 0)),
                            "change_percent": self._safe_float(row.get("涨跌幅", 0)),
                            "volume": self._safe_int(row.get("成交量", 0)),
                            "amount": self._safe_float(row.get("成交额", 0)),
                            "open": self._safe_float(row.get("今开", 0)),
                            "high": self._safe_float(row.get("最高", 0)),
                            "low": self._safe_float(row.get("最低", 0)),
                            "pre_close": self._safe_float(row.get("昨收", 0)),
                            #New: Financial indicators field
                            "turnover_rate": self._safe_float(row.get("换手率", None)),  #Exchange rate (%)
                            "volume_ratio": self._safe_float(row.get("量比", None)),  #Scale
                            "pe": self._safe_float(row.get("市盈率-动态", None)),  #Dynamic surplus
                            "pb": self._safe_float(row.get("市净率", None)),  #Net market rate
                            "total_mv": self._safe_float(row.get("总市值", None)),  #Total market value ($)
                            "circ_mv": self._safe_float(row.get("流通市值", None)),  #Market value in circulation ($)
                        }

                        #Convert to a standardized dictionary (using a matching code)
                        quotes_map[matched_code] = {
                            "code": matched_code,
                            "symbol": matched_code,
                            "name": quotes_data.get("name", f"股票{matched_code}"),
                            "price": float(quotes_data.get("price", 0)),
                            "change": float(quotes_data.get("change", 0)),
                            "change_percent": float(quotes_data.get("change_percent", 0)),
                            "volume": int(quotes_data.get("volume", 0)),
                            "amount": float(quotes_data.get("amount", 0)),
                            "open_price": float(quotes_data.get("open", 0)),
                            "high_price": float(quotes_data.get("high", 0)),
                            "low_price": float(quotes_data.get("low", 0)),
                            "pre_close": float(quotes_data.get("pre_close", 0)),
                            #New: Financial indicators field
                            "turnover_rate": quotes_data.get("turnover_rate"),  #Exchange rate (%)
                            "volume_ratio": quotes_data.get("volume_ratio"),  #Scale
                            "pe": quotes_data.get("pe"),  #Dynamic surplus
                            "pe_ttm": quotes_data.get("pe"),  #TTM gain (same as dynamic gain)
                            "pb": quotes_data.get("pb"),  #Net market rate
                            "total_mv": quotes_data.get("total_mv") / 1e8 if quotes_data.get("total_mv") else None,  #Total market value (converted to billions)
                            "circ_mv": quotes_data.get("circ_mv") / 1e8 if quotes_data.get("circ_mv") else None,  #Market value in circulation (conversion to billions of yuan)
                            #Expand Fields
                            "full_symbol": self._get_full_symbol(matched_code),
                            "market_info": self._get_market_info(matched_code),
                            "data_source": "akshare",
                            "last_sync": datetime.now(timezone.utc),
                            "sync_status": "success"
                        }

                found_count = len(quotes_map)
                missing_count = len(codes) - found_count
                logger.debug(f"Batch acquisition complete: found{found_count}Only, Not Found{missing_count}Only")

                #Record undiscovered shares
                if missing_count > 0:
                    missing_codes = codes_set - set(quotes_map.keys())
                    if missing_count <= 10:
                        logger.debug(f"(b) Unfinished stocks:{list(missing_codes)}")
                    else:
                        logger.debug(f"(b) Unfinished stocks:{list(missing_codes)[:10]}... (total){missing_count}Only)")

                return quotes_map

            except Exception as e:
                logger.warning(f"Batch acquisition of real-time lines failed (attempted){attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"❌ Batch acquisition of real-time lines failed to maximize the number of hard attempts:{e}")
                    return {}

    async def get_stock_quotes(self, code: str) -> Optional[Dict[str, Any]]:
        """Get a single stock real time line

 policy: use stock bid ask em interface to obtain real-time intelligence prices for individual stocks
- Advantages: capture single stock data only, fast, no waste of resources
- Application scenario: manual synchronization of individual stocks

Args:
code: stock code

Returns:
Standardized practice data
"""
        if not self.connected:
            return None

        try:
            logger.info(f"📈 With stock bid ask em interface{code}Real time...")

            #🔥 Use stock bid ask em interface to access real-time information on individual stocks
            def fetch_bid_ask():
                return self.ak.stock_bid_ask_em(symbol=code)

            bid_ask_df = await asyncio.to_thread(fetch_bid_ask)

            #Print original return data
            logger.info(f"Return data type:{type(bid_ask_df)}")
            if bid_ask_df is not None:
                logger.info(f"📊 DataFrame shape: {bid_ask_df.shape}")
                logger.info(f"📊 DataFrame columns: {list(bid_ask_df.columns)}")
                logger.info(f"DataFrame complete data:{bid_ask_df.to_string()}")

            if bid_ask_df is None or bid_ask_df.empty:
                logger.warning(f"Not found{code}Other Organiser")
                return None

            #Convert DataFrame into a dictionary
            data_dict = dict(zip(bid_ask_df['item'], bid_ask_df['value']))
            logger.info(f"Converted dictionary:{data_dict}")

            #Convert to a standardized dictionary
            #Note: Field names must match the query fields in app/routers/stocks.py
            #The front-end query is high/low/open, not high price/low price/open price

            #Get the current date (UTC+8)
            from datetime import datetime, timezone, timedelta
            cn_tz = timezone(timedelta(hours=8))
            now_cn = datetime.now(cn_tz)
            trade_date = now_cn.strftime("%Y-%m-%d")  #Format: 2025-11-05

            #🔥 Conversion in unit of exchange: hand unit (one hand = 100 units)
            volume_in_lots = int(data_dict.get("总手", 0))  #Unit: hand
            volume_in_shares = volume_in_lots * 100  #Unit: Unit

            quotes = {
                "code": code,
                "symbol": code,
                "name": f"股票{code}",  #Stock bid ask em do not return stock name
                "price": float(data_dict.get("最新", 0)),
                "close": float(data_dict.get("最新", 0)),  #🔥 close field (same as price)
                "current_price": float(data_dict.get("最新", 0)),  #🔥current price field (compatible with old data)
                "change": float(data_dict.get("涨跌", 0)),
                "change_percent": float(data_dict.get("涨幅", 0)),
                "pct_chg": float(data_dict.get("涨幅", 0)),  #🔥 pct chg field (compatible with old data)
                "volume": volume_in_shares,  #🔥 Unit: Unit (converted)
                "amount": float(data_dict.get("金额", 0)),  #Unit: dollars
                "open": float(data_dict.get("今开", 0)),  #Use open instead of open price
                "high": float(data_dict.get("最高", 0)),  #Use high instead of high price
                "low": float(data_dict.get("最低", 0)),  #Use low instead of low price
                "pre_close": float(data_dict.get("昨收", 0)),
                #New: Financial indicators field
                "turnover_rate": float(data_dict.get("换手", 0)),  #Exchange rate (%)
                "volume_ratio": float(data_dict.get("量比", 0)),  #Scale
                "pe": None,  #Stock bid ask em not returning surplus
                "pe_ttm": None,
                "pb": None,  #Stock bid ask em not returning net market
                "total_mv": None,  #Stock bid ask em do not return total market value
                "circ_mv": None,  #Stock bid ask em not return market value in circulation
                #Add: date of transaction and time of update
                "trade_date": trade_date,  #Date of transaction (Form: 2025-11-05)
                "updated_at": now_cn.isoformat(),  #Update time (ISO format, time zone)
                #Expand Fields
                "full_symbol": self._get_full_symbol(code),
                "market_info": self._get_market_info(code),
                "data_source": "akshare",
                "last_sync": datetime.now(timezone.utc),
                "sync_status": "success"
            }

            logger.info(f"✅ {code}Real time line acquisition success: latest price ={quotes['price']}♪ Up and down ♪{quotes['change_percent']}%, barter={quotes['volume']}, turnover ={quotes['amount']}")
            return quotes

        except Exception as e:
            logger.error(f"Access{code}Timeline failed:{e}", exc_info=True)
            return None
    
    async def _get_realtime_quotes_data(self, code: str) -> Dict[str, Any]:
        """Get Real Time Line Data"""
        try:
            #Method 1: Obtain real-time information on Unit A
            def fetch_spot_data():
                return self.ak.stock_zh_a_spot_em()

            try:
                spot_df = await asyncio.to_thread(fetch_spot_data)

                if spot_df is not None and not spot_df.empty:
                    #Find Equities
                    stock_data = spot_df[spot_df['代码'] == code]

                    if not stock_data.empty:
                        row = stock_data.iloc[0]

                        #Parsing Line Data
                        return {
                            "name": str(row.get("名称", f"股票{code}")),
                            "price": self._safe_float(row.get("最新价", 0)),
                            "change": self._safe_float(row.get("涨跌额", 0)),
                            "change_percent": self._safe_float(row.get("涨跌幅", 0)),
                            "volume": self._safe_int(row.get("成交量", 0)),
                            "amount": self._safe_float(row.get("成交额", 0)),
                            "open": self._safe_float(row.get("今开", 0)),
                            "high": self._safe_float(row.get("最高", 0)),
                            "low": self._safe_float(row.get("最低", 0)),
                            "pre_close": self._safe_float(row.get("昨收", 0)),
                            #New: Financial indicators field
                            "turnover_rate": self._safe_float(row.get("换手率", None)),  #Exchange rate (%)
                            "volume_ratio": self._safe_float(row.get("量比", None)),  #Scale
                            "pe": self._safe_float(row.get("市盈率-动态", None)),  #Dynamic surplus
                            "pb": self._safe_float(row.get("市净率", None)),  #Net market rate
                            "total_mv": self._safe_float(row.get("总市值", None)),  #Total market value ($)
                            "circ_mv": self._safe_float(row.get("流通市值", None)),  #Market value in circulation ($)
                        }
            except Exception as e:
                logger.debug(f"Access{code}Unit A failed in real time:{e}")

            #Method 2: Attempt to obtain real-time data for single stocks
            def fetch_individual_spot():
                return self.ak.stock_zh_a_hist(symbol=code, period="daily", adjust="")

            try:
                hist_df = await asyncio.to_thread(fetch_individual_spot)
                if hist_df is not None and not hist_df.empty:
                    #Taking data from the latest day as current practice
                    latest_row = hist_df.iloc[-1]
                    return {
                        "name": f"股票{code}",
                        "price": self._safe_float(latest_row.get("收盘", 0)),
                        "change": 0,  #Historical data can't calculate the drop.
                        "change_percent": self._safe_float(latest_row.get("涨跌幅", 0)),
                        "volume": self._safe_int(latest_row.get("成交量", 0)),
                        "amount": self._safe_float(latest_row.get("成交额", 0)),
                        "open": self._safe_float(latest_row.get("开盘", 0)),
                        "high": self._safe_float(latest_row.get("最高", 0)),
                        "low": self._safe_float(latest_row.get("最低", 0)),
                        "pre_close": self._safe_float(latest_row.get("收盘", 0))
                    }
            except Exception as e:
                logger.debug(f"Access{code}Historic data as behavioral failure:{e}")

            return {}

        except Exception as e:
            logger.debug(f"Access{code}Timeline data failed:{e}")
            return {}
    
    def _safe_float(self, value: Any) -> float:
        """Convert safe to floating point"""
        try:
            if pd.isna(value) or value is None:
                return 0.0
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def _safe_int(self, value: Any) -> int:
        """Convert safe to integer"""
        try:
            if pd.isna(value) or value is None:
                return 0
            return int(float(value))
        except (ValueError, TypeError):
            return 0
    
    def _safe_str(self, value: Any) -> str:
        """Securely convert to string"""
        try:
            if pd.isna(value) or value is None:
                return ""
            return str(value)
        except:
            return ""

    async def get_historical_data(
        self,
        code: str,
        start_date: str,
        end_date: str,
        period: str = "daily"
    ) -> Optional[pd.DataFrame]:
        """Get Historical Status Data

Args:
code: stock code
Start date: Start date (YYYYY-MM-DD)
End date: End Date (YYYYY-MM-DD)
period: daily, weekly, monthly

Returns:
HistorylineDataFrame
"""
        if not self.connected:
            return None

        try:
            logger.debug(f"Access{code}Historical data:{start_date}Present.{end_date}")

            #Convert Periodic Format
            period_map = {
                "daily": "daily",
                "weekly": "weekly",
                "monthly": "monthly"
            }
            ak_period = period_map.get(period, "daily")

            #Formatting Date
            start_date_formatted = start_date.replace('-', '')
            end_date_formatted = end_date.replace('-', '')

            #Access to historical data
            def fetch_historical_data():
                return self.ak.stock_zh_a_hist(
                    symbol=code,
                    period=ak_period,
                    start_date=start_date_formatted,
                    end_date=end_date_formatted,
                    adjust="qfq"  #Former right of reinstatement
                )

            hist_df = await asyncio.to_thread(fetch_historical_data)

            if hist_df is None or hist_df.empty:
                logger.warning(f"⚠️ {code}History data is empty")
                return None

            #Standardized listing
            hist_df = self._standardize_historical_columns(hist_df, code)

            logger.debug(f"✅ {code}Historical data acquisition success:{len(hist_df)}Notes")
            return hist_df

        except Exception as e:
            logger.error(f"Access{code}Historical data failed:{e}")
            return None

    def _standardize_historical_columns(self, df: pd.DataFrame, code: str) -> pd.DataFrame:
        """Standardized historical data listing"""
        try:
            #Standardised Listing Map
            column_mapping = {
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'change_percent',
                '涨跌额': 'change',
                '换手率': 'turnover'
            }

            #Rename Column
            df = df.rename(columns=column_mapping)

            #Add Standard Fields
            df['code'] = code
            df['full_symbol'] = self._get_full_symbol(code)

            #Ensure date format
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])

            #Data type conversion
            numeric_columns = ['open', 'close', 'high', 'low', 'volume', 'amount']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

            return df

        except Exception as e:
            logger.error(f"Standardization{code}Historical data listing failed:{e}")
            return df

    async def get_financial_data(self, code: str) -> Dict[str, Any]:
        """Access to financial data

Args:
code: stock code

Returns:
Financial data dictionary
"""
        if not self.connected:
            return {}

        try:
            logger.debug(f"Access{code}Financial data...")

            financial_data = {}

            #1. Access to key financial indicators
            try:
                def fetch_financial_abstract():
                    return self.ak.stock_financial_abstract(symbol=code)

                main_indicators = await asyncio.to_thread(fetch_financial_abstract)
                if main_indicators is not None and not main_indicators.empty:
                    financial_data['main_indicators'] = main_indicators.to_dict('records')
                    logger.debug(f"✅ {code}Success in obtaining key financial indicators")
            except Exception as e:
                logger.debug(f"Access{code}Key financial indicators failed:{e}")

            #2. Access to balance sheets
            try:
                def fetch_balance_sheet():
                    return self.ak.stock_balance_sheet_by_report_em(symbol=code)

                balance_sheet = await asyncio.to_thread(fetch_balance_sheet)
                if balance_sheet is not None and not balance_sheet.empty:
                    financial_data['balance_sheet'] = balance_sheet.to_dict('records')
                    logger.debug(f"✅ {code}Balance sheet successful")
            except Exception as e:
                logger.debug(f"Access{code}Balance sheet failure:{e}")

            #3. Obtaining profit statements
            try:
                def fetch_income_statement():
                    return self.ak.stock_profit_sheet_by_report_em(symbol=code)

                income_statement = await asyncio.to_thread(fetch_income_statement)
                if income_statement is not None and not income_statement.empty:
                    financial_data['income_statement'] = income_statement.to_dict('records')
                    logger.debug(f"✅ {code}The profit statement was successful.")
            except Exception as e:
                logger.debug(f"Access{code}Loss of profit statement:{e}")

            #4. Access to cash flow statements
            try:
                def fetch_cash_flow():
                    return self.ak.stock_cash_flow_sheet_by_report_em(symbol=code)

                cash_flow = await asyncio.to_thread(fetch_cash_flow)
                if cash_flow is not None and not cash_flow.empty:
                    financial_data['cash_flow'] = cash_flow.to_dict('records')
                    logger.debug(f"✅ {code}Successful cash flow statement")
            except Exception as e:
                logger.debug(f"Access{code}Loss of cash flow statement:{e}")

            if financial_data:
                logger.debug(f"✅ {code}Financial data acquisition completed:{len(financial_data)}Data sets")
            else:
                logger.warning(f"⚠️ {code}No financial data obtained")

            return financial_data

        except Exception as e:
            logger.error(f"Access{code}Financial data failed:{e}")
            return {}

    async def get_market_status(self) -> Dict[str, Any]:
        """Access to market status information

Returns:
Market status information
"""
        try:
            #AKShare has no direct market status API.
            now = datetime.now()

            #Simple trade time judgement
            is_trading_time = (
                now.weekday() < 5 and  #Chile
                ((9 <= now.hour < 12) or (13 <= now.hour < 15))  #Time of transaction
            )

            return {
                "market_status": "open" if is_trading_time else "closed",
                "current_time": now.isoformat(),
                "data_source": "akshare",
                "trading_day": now.weekday() < 5
            }

        except Exception as e:
            logger.error(f"Access to markets failed:{e}")
            return {
                "market_status": "unknown",
                "current_time": datetime.now().isoformat(),
                "data_source": "akshare",
                "error": str(e)
            }

    def get_stock_news_sync(self, symbol: str = None, limit: int = 10) -> Optional[pd.DataFrame]:
        """Get Stock News (Sync version, return original DataFrame)

Args:
Symbol: Stock code, market news for None
Limited number of returns

Returns:
News DataFrame or None
"""
        if not self.is_available():
            return None

        try:
            import akshare as ak
            import json
            import time

            if symbol:
                #Access to a unit of information
                self.logger.debug(f"For AKshare News:{symbol}")

                #Standardised stock code
                symbol_6 = symbol.zfill(6)

                #Access to East Wealth News, add a retest mechanism
                max_retries = 3
                retry_delay = 1  #sec
                news_df = None

                for attempt in range(max_retries):
                    try:
                        news_df = ak.stock_news_em(symbol=symbol_6)
                        break  #Successfully jump out of retry cycle
                    except json.JSONDecodeError as e:
                        if attempt < max_retries - 1:
                            self.logger.warning(f"⚠️ {symbol}I don't think so.{attempt+1}It's not like it's the first time I've got news.{retry_delay}Try again in seconds...")
                            time.sleep(retry_delay)
                            retry_delay *= 2  #Index evading
                        else:
                            self.logger.error(f"❌ {symbol}Could not close temporary folder: %s{e}")
                            return None
                    except Exception as e:
                        if attempt < max_retries - 1:
                            self.logger.warning(f"⚠️ {symbol}I don't think so.{attempt+1}The news has failed:{e}，{retry_delay}Try again in seconds...")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                        else:
                            raise

                if news_df is not None and not news_df.empty:
                    self.logger.info(f"✅ {symbol}AKShare News Access Success:{len(news_df)}Article")
                    return news_df.head(limit) if limit else news_df
                else:
                    self.logger.warning(f"⚠️ {symbol}No AKShare news data available")
                    return None
            else:
                #Access to market news
                self.logger.debug("To AKShare Market News")
                news_df = ak.news_cctv()

                if news_df is not None and not news_df.empty:
                    self.logger.info(f"AKshare Market News was successful:{len(news_df)}Article")
                    return news_df.head(limit) if limit else news_df
                else:
                    self.logger.warning("No AKShare market news data available")
                    return None

        except Exception as e:
            self.logger.error(f"AKShare News Failed:{e}")
            return None

    async def get_stock_news(self, symbol: str = None, limit: int = 10) -> Optional[List[Dict[str, Any]]]:
        """Access to stock news (show, return structured list)

Args:
Symbol: Stock code, market news for None
Limited number of returns

Returns:
NewsList
"""
        if not self.is_available():
            return None

        try:
            import akshare as ak
            import json
            import os

            if symbol:
                #Access to a unit of information
                self.logger.debug(f"For AKshare News:{symbol}")

                #Standardised stock code
                symbol_6 = symbol.zfill(6)

                #Test for Docker Environment Medium
                is_docker = os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER') == 'true'

                #Access to East Wealth News, add a retest mechanism
                max_retries = 3
                retry_delay = 1  #sec
                news_df = None

                #If in Docker environment try to call API directly using curl cffi
                if is_docker:
                    try:
                        from curl_cffi import requests as curl_requests
                        self.logger.debug(f"Docker environment detected using curl cffi to call API")
                        news_df = await asyncio.to_thread(
                            self._get_stock_news_direct,
                            symbol=symbol_6,
                            limit=limit
                        )
                        if news_df is not None and not news_df.empty:
                            self.logger.info(f"✅ {symbol}Docker Environment Direct Call API Success")
                        else:
                            self.logger.warning(f"⚠️ {symbol}Docker environment directly calling API failed. Back to AKShare")
                            news_df = None  #Back to AKShare.
                    except ImportError:
                        self.logger.warning(f"Not installed. Back to AKshare.")
                        news_df = None
                    except Exception as e:
                        self.logger.warning(f"⚠️ {symbol}Docker environment directly calls API anomaly:{e}Back to Akshare.")
                        news_df = None

                #If a direct call failed or not in Docker environment, use AKShare
                if news_df is None:
                    for attempt in range(max_retries):
                        try:
                            news_df = await asyncio.to_thread(
                                ak.stock_news_em,
                                symbol=symbol_6
                            )
                            break  #Successfully jump out of retry cycle
                        except json.JSONDecodeError as e:
                            if attempt < max_retries - 1:
                                self.logger.warning(f"⚠️ {symbol}I don't think so.{attempt+1}It's not like it's the first time I've got news.{retry_delay}Try again in seconds...")
                                await asyncio.sleep(retry_delay)
                                retry_delay *= 2  #Index evading
                            else:
                                self.logger.error(f"❌ {symbol}Could not close temporary folder: %s{e}")
                                return []
                        except KeyError as e:
                            #Change in the Eastern Wealth Network interface or anti-pastoral interception and change in the structure of the returned field
                            if str(e) == "'cmsArticleWebOld'":
                                self.logger.error(f"❌ {symbol}AKShare news interface returned data structure anomaly: missing 'cmsArtileWebold 'field")
                                self.logger.error(f"This is usually due to: 1) anti-pastoral interception 2) Interfacing changes 3) Network problems")
                                self.logger.error(f"Recommendation: Check whether the AKShare version is up to date (current requirement > = 1.17.86)")
                                #Return empty list to avoid program crash
                                return []
                            else:
                                if attempt < max_retries - 1:
                                    self.logger.warning(f"⚠️ {symbol}I don't think so.{attempt+1}Could not close temporary folder: %s{e}，{retry_delay}Try again in seconds...")
                                    await asyncio.sleep(retry_delay)
                                    retry_delay *= 2
                                else:
                                    self.logger.error(f"❌ {symbol}Failed to get news (field error):{e}")
                                    return []
                        except Exception as e:
                            if attempt < max_retries - 1:
                                self.logger.warning(f"⚠️ {symbol}I don't think so.{attempt+1}The news has failed:{e}，{retry_delay}Try again in seconds...")
                                await asyncio.sleep(retry_delay)
                                retry_delay *= 2
                            else:
                                raise

                if news_df is not None and not news_df.empty:
                    news_list = []

                    for _, row in news_df.head(limit).iterrows():
                        title = str(row.get('新闻标题', '') or row.get('标题', ''))
                        content = str(row.get('新闻内容', '') or row.get('内容', ''))
                        summary = str(row.get('新闻摘要', '') or row.get('摘要', ''))

                        news_item = {
                            "symbol": symbol,
                            "title": title,
                            "content": content,
                            "summary": summary,
                            "url": str(row.get('新闻链接', '') or row.get('链接', '')),
                            "source": str(row.get('文章来源', '') or row.get('来源', '') or '东方财富'),
                            "author": str(row.get('作者', '') or ''),
                            "publish_time": self._parse_news_time(row.get('发布时间', '') or row.get('时间', '')),
                            "category": self._classify_news(content, title),
                            "sentiment": self._analyze_news_sentiment(content, title),
                            "sentiment_score": self._calculate_sentiment_score(content, title),
                            "keywords": self._extract_keywords(content, title),
                            "importance": self._assess_news_importance(content, title),
                            "data_source": "akshare"
                        }

                        #Filter empty header news
                        if news_item["title"]:
                            news_list.append(news_item)

                    self.logger.info(f"✅ {symbol}AKShare News Access Success:{len(news_list)}Article")
                    return news_list
                else:
                    self.logger.warning(f"⚠️ {symbol}No AKShare news data available")
                    return []
            else:
                #Access to market news
                self.logger.debug("To AKShare Market News")

                try:
                    #Access to financial information
                    news_df = await asyncio.to_thread(
                        ak.news_cctv,
                        limit=limit
                    )

                    if news_df is not None and not news_df.empty:
                        news_list = []

                        for _, row in news_df.iterrows():
                            title = str(row.get('title', '') or row.get('标题', ''))
                            content = str(row.get('content', '') or row.get('内容', ''))
                            summary = str(row.get('brief', '') or row.get('摘要', ''))

                            news_item = {
                                "title": title,
                                "content": content,
                                "summary": summary,
                                "url": str(row.get('url', '') or row.get('链接', '')),
                                "source": str(row.get('source', '') or row.get('来源', '') or 'CCTV财经'),
                                "author": str(row.get('author', '') or ''),
                                "publish_time": self._parse_news_time(row.get('time', '') or row.get('时间', '')),
                                "category": self._classify_news(content, title),
                                "sentiment": self._analyze_news_sentiment(content, title),
                                "sentiment_score": self._calculate_sentiment_score(content, title),
                                "keywords": self._extract_keywords(content, title),
                                "importance": self._assess_news_importance(content, title),
                                "data_source": "akshare"
                            }

                            if news_item["title"]:
                                news_list.append(news_item)

                        self.logger.info(f"AKshare Market News was successful:{len(news_list)}Article")
                        return news_list

                except Exception as e:
                    self.logger.debug(f"CCTV News Failed:{e}")

                return []

        except Exception as e:
            self.logger.error(f"== sync, corrected by elderman =={symbol}: {e}")
            return None

    def _parse_news_time(self, time_str: str) -> Optional[datetime]:
        """Parsing news time"""
        if not time_str:
            return datetime.utcnow()

        try:
            #Try multiple time formats
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%Y/%m/%d %H:%M:%S",
                "%Y/%m/%d %H:%M",
                "%Y/%m/%d",
                "%m-%d %H:%M",
                "%m/%d %H:%M"
            ]

            for fmt in formats:
                try:
                    parsed_time = datetime.strptime(str(time_str), fmt)

                    #If only the moon, the supplementary year
                    if fmt in ["%m-%d %H:%M", "%m/%d %H:%M"]:
                        current_year = datetime.now().year
                        parsed_time = parsed_time.replace(year=current_year)

                    return parsed_time
                except ValueError:
                    continue

            #If they fail, return to the current time.
            self.logger.debug(f"Could not close temporary folder: %s{time_str}")
            return datetime.utcnow()

        except Exception as e:
            self.logger.debug(f"News time anomaly:{e}")
            return datetime.utcnow()

    def _analyze_news_sentiment(self, content: str, title: str) -> str:
        """Analysis of news moods

Args:
Content:
title:

Returns:
Organisation
"""
        text = f"{title} {content}".lower()

        #Positive keywords
        positive_keywords = [
            '利好', '上涨', '增长', '盈利', '突破', '创新高', '买入', '推荐',
            '看好', '乐观', '强势', '大涨', '飙升', '暴涨', '涨停', '涨幅',
            '业绩增长', '营收增长', '净利润增长', '扭亏为盈', '超预期',
            '获批', '中标', '签约', '合作', '并购', '重组', '分红', '回购'
        ]

        #Negative keyword
        negative_keywords = [
            '利空', '下跌', '亏损', '风险', '暴跌', '卖出', '警告', '下调',
            '看空', '悲观', '弱势', '大跌', '跳水', '暴跌', '跌停', '跌幅',
            '业绩下滑', '营收下降', '净利润下降', '亏损', '低于预期',
            '被查', '违规', '处罚', '诉讼', '退市', '停牌', '商誉减值'
        ]

        positive_count = sum(1 for keyword in positive_keywords if keyword in text)
        negative_count = sum(1 for keyword in negative_keywords if keyword in text)

        if positive_count > negative_count:
            return 'positive'
        elif negative_count > positive_count:
            return 'negative'
        else:
            return 'neutral'

    def _calculate_sentiment_score(self, content: str, title: str) -> float:
        """Calculate Emotional Scores

Args:
Content:
title:

Returns:
Emotional score: -1.0 to 1.0
"""
        text = f"{title} {content}".lower()

        #Positive keyword weights
        positive_keywords = {
            '涨停': 1.0, '暴涨': 0.9, '大涨': 0.8, '飙升': 0.8,
            '创新高': 0.7, '突破': 0.6, '上涨': 0.5, '增长': 0.4,
            '利好': 0.6, '看好': 0.5, '推荐': 0.5, '买入': 0.6
        }

        #Negative keyword weight
        negative_keywords = {
            '跌停': -1.0, '暴跌': -0.9, '大跌': -0.8, '跳水': -0.8,
            '创新低': -0.7, '破位': -0.6, '下跌': -0.5, '下滑': -0.4,
            '利空': -0.6, '看空': -0.5, '卖出': -0.6, '警告': -0.5
        }

        score = 0.0

        #Calculate positive scores
        for keyword, weight in positive_keywords.items():
            if keyword in text:
                score += weight

        #Calculate negative scores
        for keyword, weight in negative_keywords.items():
            if keyword in text:
                score += weight

        #Normalize to [-1.0, 1.0]
        return max(-1.0, min(1.0, score / 3.0))

    def _extract_keywords(self, content: str, title: str) -> List[str]:
        """Extract Keywords

Args:
Content:
title:

Returns:
List of keywords
"""
        text = f"{title} {content}"

        #Common financial keywords
        common_keywords = [
            '股票', '公司', '市场', '投资', '业绩', '财报', '政策', '行业',
            '分析', '预测', '涨停', '跌停', '上涨', '下跌', '盈利', '亏损',
            '并购', '重组', '分红', '回购', '增持', '减持', '融资', 'IPO',
            '监管', '央行', '利率', '汇率', 'GDP', '通胀', '经济', '贸易',
            '科技', '互联网', '新能源', '医药', '房地产', '金融', '制造业'
        ]

        keywords = []
        for keyword in common_keywords:
            if keyword in text:
                keywords.append(keyword)

        return keywords[:10]  #Returns a maximum of 10 keywords

    def _assess_news_importance(self, content: str, title: str) -> str:
        """Assessing the importance of information

Args:
Content:
title:

Returns:
Importance level: high/media/low
"""
        text = f"{title} {content}".lower()

        #High-profile keywords
        high_importance_keywords = [
            '业绩', '财报', '年报', '季报', '重大', '公告', '监管', '政策',
            '并购', '重组', '退市', '停牌', '涨停', '跌停', '暴涨', '暴跌',
            '央行', '证监会', '交易所', '违规', '处罚', '立案', '调查'
        ]

        #Middle material keyword
        medium_importance_keywords = [
            '分析', '预测', '观点', '建议', '行业', '市场', '趋势', '机会',
            '研报', '评级', '目标价', '增持', '减持', '买入', '卖出',
            '合作', '签约', '中标', '获批', '分红', '回购'
        ]

        #Check for high importance
        if any(keyword in text for keyword in high_importance_keywords):
            return 'high'

        #Check medium importance
        if any(keyword in text for keyword in medium_importance_keywords):
            return 'medium'

        return 'low'

    def _classify_news(self, content: str, title: str) -> str:
        """Classified News

Args:
Content:
title:

Returns:
Category of information
"""
        text = f"{title} {content}".lower()

        #Company announcement
        if any(keyword in text for keyword in ['公告', '业绩', '财报', '年报', '季报']):
            return 'company_announcement'

        #Policy News
        if any(keyword in text for keyword in ['政策', '监管', '央行', '证监会', '国务院']):
            return 'policy_news'

        #Industry News
        if any(keyword in text for keyword in ['行业', '板块', '产业', '领域']):
            return 'industry_news'

        #Market News
        if any(keyword in text for keyword in ['市场', '指数', '大盘', '沪指', '深成指']):
            return 'market_news'

        #Studies
        if any(keyword in text for keyword in ['研报', '分析', '评级', '目标价', '机构']):
            return 'research_report'

        return 'general'


#Examples of global providers
_akshare_provider = None


def get_akshare_provider() -> AKShareProvider:
    """Get Global AKShare Provider Example"""
    global _akshare_provider
    if _akshare_provider is None:
        _akshare_provider = AKShareProvider()
    return _akshare_provider
