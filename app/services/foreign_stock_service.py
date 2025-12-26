"""Port and United States data services
RenewedStockService
🔥 Call API according to the data source priorities configured in the database
Request for removal of mechanism: prevent simultaneous calls to API
"""
from typing import Optional, Dict, List, Tuple
from datetime import datetime, timedelta
import logging
import json
import re
import asyncio
from collections import defaultdict

#Reuse existing cache system
from tradingagents.dataflows.cache import get_cache

#Reuse provider of existing data sources
from tradingagents.dataflows.providers.hk.hk_stock import HKStockProvider

logger = logging.getLogger(__name__)


class ForeignStockService:
    """Port and U.S. data services (re-use unified data source manager, dialed to database priorities)"""

    #Cache Time Configuration (sec)
    CACHE_TTL = {
        "HK": {
            "quote": 600,        #10 minutes (real time)
            "info": 86400,       #1 day (basic information)
            "kline": 7200,       #2 hours (K-line data)
        },
        "US": {
            "quote": 600,        #Ten minutes.
            "info": 86400,       #1 day
            "kline": 7200,       #Two hours.
        }
    }

    def __init__(self, db=None):
        #Use unified cache system (auto-selection MongoDB/Redis/File)
        self.cache = get_cache()

        #Source of data for initialized port units
        self.hk_provider = HKStockProvider()

        #Save database connection (for querying data source priorities)
        self.db = db

        #Request weight: Create a separate lock for each (market, code, data type)
        self._request_locks = defaultdict(asyncio.Lock)

        #Ongoing request cache (for sharing of results)
        self._pending_requests = {}

        logger.info("InitialStockService has been initialised (enabled to remove request)")
    
    async def get_quote(self, market: str, code: str, force_refresh: bool = False) -> Dict:
        """Get Real Time Lines

Args:
market: Market type (HK/US)
code: stock code
source refresh: whether to force refresh (jump cache)

Returns:
Real-time line data

Process:
1. Check for compulsory refreshing
2. Access from cache (Redis → MongoDB → File)
3. Cache outstanding data source API (priority)
4. Save to cache
"""
        if market == 'HK':
            return await self._get_hk_quote(code, force_refresh)
        elif market == 'US':
            return await self._get_us_quote(code, force_refresh)
        else:
            raise ValueError(f"不支持的市场类型: {market}")
    
    async def get_basic_info(self, market: str, code: str, force_refresh: bool = False) -> Dict:
        """Access to basic information

Args:
market: Market type (HK/US)
code: stock code
source refresh: whether to forcibly refresh

Returns:
Basic information data
"""
        if market == 'HK':
            return await self._get_hk_info(code, force_refresh)
        elif market == 'US':
            return await self._get_us_info(code, force_refresh)
        else:
            raise ValueError(f"不支持的市场类型: {market}")
    
    async def get_kline(self, market: str, code: str, period: str = 'day', 
                       limit: int = 120, force_refresh: bool = False) -> List[Dict]:
        """Get K-line data

Args:
market: Market type (HK/US)
code: stock code
period: Cycle (day/week/month)
number of data bars
source refresh: whether to forcibly refresh

Returns:
K-line Data List
"""
        if market == 'HK':
            return await self._get_hk_kline(code, period, limit, force_refresh)
        elif market == 'US':
            return await self._get_us_kline(code, period, limit, force_refresh)
        else:
            raise ValueError(f"不支持的市场类型: {market}")
    
    async def _get_hk_quote(self, code: str, force_refresh: bool = False) -> Dict:
        """Access to real-time accommodation (with heavy requests)
 Call API according to the data source priorities configured in the database
To prevent simultaneous calls to API
"""
        #1. Check the cache (unless mandatory updating)
        if not force_refresh:
            cache_key = self.cache.find_cached_stock_data(
                symbol=code,
                data_source="hk_realtime_quote"
            )

            if cache_key:
                cached_data = self.cache.load_stock_data(cache_key)
                if cached_data:
                    logger.info(f"Get Port Stock from Cache:{code}")
                    return self._parse_cached_data(cached_data, 'HK', code)

        #2. Requests for weight: use the lock to ensure that only one API is called at the same time
        request_key = f"HK_quote_{code}_{force_refresh}"
        lock = self._request_locks[request_key]

        async with lock:
            #Check the cache again (probably other requests were completed and data cached while waiting for lock)
            #Even if force refresh=True, check if any other simultaneous requests have just been completed
            cache_key = self.cache.find_cached_stock_data(
                symbol=code,
                data_source="hk_realtime_quote"
            )
            if cache_key:
                cached_data = self.cache.load_stock_data(cache_key)
                if cached_data:
                    #Check the cache time, if it's within the last second, indicating that the request has just been cached
                    try:
                        data_dict = json.loads(cached_data) if isinstance(cached_data, str) else cached_data
                        updated_at = data_dict.get('updated_at', '')
                        if updated_at:
                            cache_time = datetime.fromisoformat(updated_at)
                            time_diff = (datetime.now() - cache_time).total_seconds()
                            if time_diff < 1:  #Cache in 1 second.
                                logger.info(f"Use and request results:{code}(Cache time:{time_diff:.2f}Seconds ago)")
                                return self._parse_cached_data(cached_data, 'HK', code)
                    except Exception as e:
                        logger.debug(f"Can not open message{e}")

                    #Use cache if not mandatory
                    if not force_refresh:
                        logger.info(f"[moJI 0] [Drives] Get Hong Kong stock from the cache:{code}")
                        return self._parse_cached_data(cached_data, 'HK', code)

            logger.info(f"Here we go.{code} (force_refresh={force_refresh})")

            #3. Data source priorities from databases (using harmonized methods)
            source_priority = await self._get_source_priority('HK')

            #4. Piloting data sources in priority terms
            quote_data = None
            data_source = None

            #Data source name map (database name processing function)
            #Only these are valid data source names.
            source_handlers = {
                'yahoo_finance': ('yfinance', self._get_hk_quote_from_yfinance),
                'akshare': ('akshare', self._get_hk_quote_from_akshare),
            }

            #Filter Effective Data Sources and Heavy
            valid_priority = []
            seen = set()
            for source_name in source_priority:
                source_key = source_name.lower()
                #Keep only valid data sources
                if source_key in source_handlers and source_key not in seen:
                    seen.add(source_key)
                    valid_priority.append(source_name)

            if not valid_priority:
                logger.warning(f"⚠️ database does not have a valid port stock data source, using default order")
                valid_priority = ['yahoo_finance', 'akshare']

            logger.info(f"[HK active data source]{valid_priority}(Equities:{code})")

            for source_name in valid_priority:
                source_key = source_name.lower()
                handler_name, handler_func = source_handlers[source_key]
                try:
                    #Use asyncio.to thread to avoid blocking event cycles
                    quote_data = await asyncio.to_thread(handler_func, code)
                    data_source = handler_name

                    if quote_data:
                        logger.info(f"✅ {data_source}Successful access to Hong Kong stock:{code}")
                        break
                except Exception as e:
                    logger.warning(f"⚠️ {source_name}Failed to get (%1){code}): {e}")
                    continue

            if not quote_data:
                raise Exception(f"无法获取港股{code}的行情数据：所有数据源均失败")

            #Formatting data
            formatted_data = self._format_hk_quote(quote_data, code, data_source)

            #Save to cache
            self.cache.save_stock_data(
                symbol=code,
                data=json.dumps(formatted_data, ensure_ascii=False),
                data_source="hk_realtime_quote"
            )
            logger.info(f"The Hong Kong stock position has been compromised:{code}")

            return formatted_data

    async def _get_source_priority(self, market: str) -> List[str]:
        """Data source priorities from databases (harmonized methodology)
Re-enactment of Unified StockService
"""
        market_category_map = {
            "CN": "a_shares",
            "HK": "hk_stocks",
            "US": "us_stocks"
        }

        market_category_id = market_category_map.get(market)

        try:
            #Query from data groupings
            groupings = await self.db.datasource_groupings.find({
                "market_category_id": market_category_id,
                "enabled": True
            }).sort("priority", -1).to_list(length=None)

            if groupings:
                priority_list = [g["data_source_name"] for g in groupings]
                logger.info(f"📊 [{market}Data source priority] Read from database:{priority_list}")
                return priority_list
        except Exception as e:
            logger.warning(f"⚠️ [{market}Could not close temporary folder: %s{e}, using default order")

        #Default Priority
        default_priority = {
            "CN": ["tushare", "akshare", "baostock"],
            "HK": ["yfinance", "akshare"],
            "US": ["yfinance", "alpha_vantage", "finnhub"]
        }
        priority_list = default_priority.get(market, [])
        logger.info(f"📊 [{market}Data source priority] With default:{priority_list}")
        return priority_list

    def _get_hk_quote_from_yfinance(self, code: str) -> Dict:
        """Get Hong Kong Stock Exchange from yfinance"""
        quote_data = self.hk_provider.get_real_time_price(code)
        if not quote_data:
            raise Exception("无数据")
        return quote_data

    def _get_hk_quote_from_akshare(self, code: str) -> Dict:
        """Collecting Hong Kong stock from Akshare"""
        from tradingagents.dataflows.providers.hk.improved_hk import get_hk_stock_info_akshare
        info = get_hk_stock_info_akshare(code)
        if not info or 'error' in info:
            raise Exception("无数据")

        #Check for price data.
        if not info.get('price'):
            raise Exception("无价格数据")

        return info
    
    async def _get_us_quote(self, code: str, force_refresh: bool = False) -> Dict:
        """Get U.S. stock in real time.
 Call API according to the data source priorities configured in the database
To prevent simultaneous calls to API
"""
        #1. Check the cache (unless mandatory updating)
        if not force_refresh:
            cache_key = self.cache.find_cached_stock_data(
                symbol=code,
                data_source="us_realtime_quote"
            )

            if cache_key:
                cached_data = self.cache.load_stock_data(cache_key)
                if cached_data:
                    logger.info(f"⚡to get U.S. stock from the cache:{code}")
                    return self._parse_cached_data(cached_data, 'US', code)

        #2. Requests for weight: use the lock to ensure that only one API is called at the same time
        request_key = f"US_quote_{code}_{force_refresh}"
        lock = self._request_locks[request_key]

        async with lock:
            #Check the cache again (probably other requests were completed and data cached while waiting for lock)
            cache_key = self.cache.find_cached_stock_data(
                symbol=code,
                data_source="us_realtime_quote"
            )
            if cache_key:
                cached_data = self.cache.load_stock_data(cache_key)
                if cached_data:
                    #Check the cache time, if it's within the last second, indicating that the request has just been cached
                    try:
                        data_dict = json.loads(cached_data) if isinstance(cached_data, str) else cached_data
                        updated_at = data_dict.get('updated_at', '')
                        if updated_at:
                            cache_time = datetime.fromisoformat(updated_at)
                            time_diff = (datetime.now() - cache_time).total_seconds()
                            if time_diff < 1:  #Cache in 1 second.
                                logger.info(f"Use and request results:{code}(Cache time:{time_diff:.2f}Seconds ago)")
                                return self._parse_cached_data(cached_data, 'US', code)
                    except Exception as e:
                        logger.debug(f"Can not open message{e}")

                    #Use cache if not mandatory
                    if not force_refresh:
                        logger.info(f"♪ ⚡ ♪{code}")
                        return self._parse_cached_data(cached_data, 'US', code)

            logger.info(f"Here we go.{code} (force_refresh={force_refresh})")

            #3. Data source priorities from databases (using harmonized methods)
            source_priority = await self._get_source_priority('US')

            #4. Piloting data sources in priority terms
            quote_data = None
            data_source = None

            #Data source name map (database name processing function)
            #Only these are valid data source names: alpha vantage, yahoo finance, Finnishhub
            source_handlers = {
                'alpha_vantage': ('alpha_vantage', self._get_us_quote_from_alpha_vantage),
                'yahoo_finance': ('yfinance', self._get_us_quote_from_yfinance),
                'finnhub': ('finnhub', self._get_us_quote_from_finnhub),
            }

            #Filter Effective Data Sources and Heavy
            valid_priority = []
            seen = set()
            for source_name in source_priority:
                source_key = source_name.lower()
                #Keep only valid data sources
                if source_key in source_handlers and source_key not in seen:
                    seen.add(source_key)
                    valid_priority.append(source_name)

            if not valid_priority:
                logger.warning("⚠️ database does not have a valid US share data source configured, using default order")
                valid_priority = ['yahoo_finance', 'alpha_vantage', 'finnhub']

            logger.info(f"[US active data source]{valid_priority}(Equities:{code})")

            for source_name in valid_priority:
                source_key = source_name.lower()
                handler_name, handler_func = source_handlers[source_key]
                try:
                    #Use asyncio.to thread to avoid blocking event cycles
                    quote_data = await asyncio.to_thread(handler_func, code)
                    data_source = handler_name

                    if quote_data:
                        logger.info(f"✅ {data_source}Acquiring America's Equity Success:{code}")
                        break
                except Exception as e:
                    logger.warning(f"⚠️ {source_name}Failed to get (%1){code}): {e}")
                    continue

            if not quote_data:
                raise Exception(f"无法获取美股{code}的行情数据：所有数据源均失败")

            #Formatting data
            formatted_data = {
                'code': code,
                'name': quote_data.get('name', f'美股{code}'),
                'market': 'US',
                'price': quote_data.get('price'),
                'open': quote_data.get('open'),
                'high': quote_data.get('high'),
                'low': quote_data.get('low'),
                'volume': quote_data.get('volume'),
                'change_percent': quote_data.get('change_percent'),
                'trade_date': quote_data.get('trade_date'),
                'currency': quote_data.get('currency', 'USD'),
                'source': data_source,
                'updated_at': datetime.now().isoformat()
            }

            #Save to cache
            self.cache.save_stock_data(
                symbol=code,
                data=json.dumps(formatted_data, ensure_ascii=False),
                data_source="us_realtime_quote"
            )
            logger.info(f"The United States share has been saved:{code}")

            return formatted_data

    def _get_us_quote_from_yfinance(self, code: str) -> Dict:
        """Get the American stock from yfinance."""
        import yfinance as yf

        ticker = yf.Ticker(code)
        hist = ticker.history(period='1d')

        if hist.empty:
            raise Exception("无数据")

        latest = hist.iloc[-1]
        info = ticker.info

        return {
            'name': info.get('longName') or info.get('shortName'),
            'price': float(latest['Close']),
            'open': float(latest['Open']),
            'high': float(latest['High']),
            'low': float(latest['Low']),
            'volume': int(latest['Volume']),
            'change_percent': round(((latest['Close'] - latest['Open']) / latest['Open'] * 100), 2),
            'trade_date': hist.index[-1].strftime('%Y-%m-%d'),
            'currency': info.get('currency', 'USD')
        }

    def _get_us_quote_from_alpha_vantage(self, code: str) -> Dict:
        """Get US stock from Alpha Vantage"""
        try:
            from tradingagents.dataflows.providers.us.alpha_vantage_common import get_api_key, _make_api_request

            #Get API Key
            api_key = get_api_key()
            if not api_key:
                raise Exception("Alpha Vantage API Key 未配置")

            #Call GLOBAL QUOTE API
            params = {
                "symbol": code.upper(),
            }

            data = _make_api_request("GLOBAL_QUOTE", params)

            if not data or "Global Quote" not in data:
                raise Exception("Alpha Vantage 返回数据为空")

            quote = data["Global Quote"]

            if not quote:
                raise Exception("无数据")

            #Parsing data
            return {
                'symbol': quote.get('01. symbol', code),
                'price': float(quote.get('05. price', 0)),
                'open': float(quote.get('02. open', 0)),
                'high': float(quote.get('03. high', 0)),
                'low': float(quote.get('04. low', 0)),
                'volume': int(quote.get('06. volume', 0)),
                'latest_trading_day': quote.get('07. latest trading day', ''),
                'previous_close': float(quote.get('08. previous close', 0)),
                'change': float(quote.get('09. change', 0)),
                'change_percent': quote.get('10. change percent', '0%').rstrip('%'),
            }

        except Exception as e:
            logger.error(f"Alpha Vantage failed to access American equity:{e}")
            raise

    def _get_us_quote_from_finnhub(self, code: str) -> Dict:
        """Get the American stock from Finnhub."""
        try:
            import finnhub
            import os

            #Get API Key
            api_key = os.getenv('FINNHUB_API_KEY')
            if not api_key:
                raise Exception("Finnhub API Key 未配置")

            #Create Client
            client = finnhub.Client(api_key=api_key)

            #Get live quotes
            quote = client.quote(code.upper())

            if not quote or 'c' not in quote:
                raise Exception("无数据")

            #Parsing data
            return {
                'symbol': code.upper(),
                'price': quote.get('c', 0),  # current price
                'open': quote.get('o', 0),   # open price
                'high': quote.get('h', 0),   # high price
                'low': quote.get('l', 0),    # low price
                'previous_close': quote.get('pc', 0),  # previous close
                'change': quote.get('d', 0),  # change
                'change_percent': quote.get('dp', 0),  # change percent
                'timestamp': quote.get('t', 0),  # timestamp
            }

        except Exception as e:
            logger.error(f"Finnhub failed to access American equity:{e}")
            raise
    
    async def _get_hk_info(self, code: str, force_refresh: bool = False) -> Dict:
        """Access to basic information on port units
 Call API according to the data source priorities configured in the database
"""
        #1. Check the cache (unless mandatory updating)
        if not force_refresh:
            cache_key = self.cache.find_cached_stock_data(
                symbol=code,
                data_source="hk_basic_info"
            )

            if cache_key:
                cached_data = self.cache.load_stock_data(cache_key)
                if cached_data:
                    logger.info(f"⚡to obtain basic information on the port unit from the cache:{code}")
                    return self._parse_cached_data(cached_data, 'HK', code)

        #2. Data source priorities from databases
        source_priority = await self._get_source_priority('HK')

        #3. Piloting of data sources by priority
        info_data = None
        data_source = None

        #Data Source Name Map
        source_handlers = {
            'akshare': ('akshare', self._get_hk_info_from_akshare),
            'yahoo_finance': ('yfinance', self._get_hk_info_from_yfinance),
            'finnhub': ('finnhub', self._get_hk_info_from_finnhub),
        }

        #Filter Effective Data Sources and Heavy
        valid_priority = []
        seen = set()
        for source_name in source_priority:
            source_key = source_name.lower()
            if source_key in source_handlers and source_key not in seen:
                seen.add(source_key)
                valid_priority.append(source_name)

        if not valid_priority:
            logger.warning("⚠️ database does not have a valid port unit basic information source, using default order")
            valid_priority = ['akshare', 'yahoo_finance', 'finnhub']

        logger.info(f"[HK Basic Information Effective Data Source]{valid_priority}")

        for source_name in valid_priority:
            source_key = source_name.lower()
            handler_name, handler_func = source_handlers[source_key]
            try:
                #Use asyncio.to thread to avoid blocking event cycles
                import asyncio
                info_data = await asyncio.to_thread(handler_func, code)
                data_source = handler_name

                if info_data:
                    logger.info(f"✅ {data_source}Successful access to basic information on the Port Unit:{code}")
                    break
            except Exception as e:
                logger.warning(f"⚠️ {source_name}Could not close temporary folder: %s{e}")
                continue

        if not info_data:
            raise Exception(f"无法获取港股{code}的基础信息：所有数据源均失败")

        #4. Formatting data
        formatted_data = self._format_hk_info(info_data, code, data_source)

        #Save to cache
        self.cache.save_stock_data(
            symbol=code,
            data=json.dumps(formatted_data, ensure_ascii=False),
            data_source="hk_basic_info"
        )
        logger.info(f"Basic information on the Port Unit is on hold:{code}")

        return formatted_data

    async def _get_us_info(self, code: str, force_refresh: bool = False) -> Dict:
        """Access to basic United States information
 Call API according to the data source priorities configured in the database
"""
        #1. Check the cache (unless mandatory updating)
        if not force_refresh:
            cache_key = self.cache.find_cached_stock_data(
                symbol=code,
                data_source="us_basic_info"
            )

            if cache_key:
                cached_data = self.cache.load_stock_data(cache_key)
                if cached_data:
                    logger.info(f"⚡ for US stock base information from the cache:{code}")
                    return self._parse_cached_data(cached_data, 'US', code)

        #2. Data source priorities from databases
        source_priority = await self._get_source_priority('US')

        #3. Piloting of data sources by priority
        info_data = None
        data_source = None

        #Data Source Name Map
        source_handlers = {
            'alpha_vantage': ('alpha_vantage', self._get_us_info_from_alpha_vantage),
            'yahoo_finance': ('yfinance', self._get_us_info_from_yfinance),
            'finnhub': ('finnhub', self._get_us_info_from_finnhub),
        }

        #Filter Effective Data Sources and Heavy
        valid_priority = []
        seen = set()
        for source_name in source_priority:
            source_key = source_name.lower()
            if source_key in source_handlers and source_key not in seen:
                seen.add(source_key)
                valid_priority.append(source_name)

        if not valid_priority:
            logger.warning("⚠️ database does not have a valid US share data source configured, using default order")
            valid_priority = ['yahoo_finance', 'alpha_vantage', 'finnhub']

        logger.info(f"[US Basic Information Effective Data Source]{valid_priority}")

        for source_name in valid_priority:
            source_key = source_name.lower()
            handler_name, handler_func = source_handlers[source_key]
            try:
                #Use asyncio.to thread to avoid blocking event cycles
                import asyncio
                info_data = await asyncio.to_thread(handler_func, code)
                data_source = handler_name

                if info_data:
                    logger.info(f"✅ {data_source}Acquiring US stock base information successfully:{code}")
                    break
            except Exception as e:
                logger.warning(f"⚠️ {source_name}Could not close temporary folder: %s{e}")
                continue

        if not info_data:
            raise Exception(f"无法获取美股{code}的基础信息：所有数据源均失败")

        #Formatting data (field names matching the expectations of the front end)
        market_cap = info_data.get('market_cap')
        formatted_data = {
            'code': code,
            'name': info_data.get('name') or f'美股{code}',
            'market': 'US',
            'industry': info_data.get('industry'),
            'sector': info_data.get('sector'),
            #Front-end expectation total mv (in billions of yuan)
            'total_mv': market_cap / 1e8 if market_cap else None,
            #Front-end expectation
            'pe_ttm': info_data.get('pe_ratio'),
            'pe': info_data.get('pe_ratio'),
            #Front-end expectation pb
            'pb': info_data.get('pb_ratio'),
            #Front-end expectation ps (data not available)
            'ps': None,
            'ps_ttm': None,
            #Front-end expectations roe and debt ratio (no data available)
            'roe': None,
            'debt_ratio': None,
            'dividend_yield': info_data.get('dividend_yield'),
            'currency': info_data.get('currency', 'USD'),
            'source': data_source,
            'updated_at': datetime.now().isoformat()
        }

        #Save to cache
        self.cache.save_stock_data(
            symbol=code,
            data=json.dumps(formatted_data, ensure_ascii=False),
            data_source="us_basic_info"
        )
        logger.info(f"The U.S. share base information is cached:{code}")

        return formatted_data

    async def _get_hk_kline(self, code: str, period: str, limit: int, force_refresh: bool = False) -> List[Dict]:
        """Access to K-line data
 Call API according to the data source priorities configured in the database
"""
        #1. Check the cache (unless mandatory updating)
        cache_key_str = f"hk_kline_{period}_{limit}"
        if not force_refresh:
            cache_key = self.cache.find_cached_stock_data(
                symbol=code,
                data_source=cache_key_str
            )

            if cache_key:
                cached_data = self.cache.load_stock_data(cache_key)
                if cached_data:
                    logger.info(f"From the cache.{code}")
                    return self._parse_cached_kline(cached_data)

        #2. Data source priorities from databases
        source_priority = await self._get_source_priority('HK')

        #3. Piloting of data sources by priority
        kline_data = None
        data_source = None

        #Data Source Name Map
        source_handlers = {
            'akshare': ('akshare', self._get_hk_kline_from_akshare),
            'yahoo_finance': ('yfinance', self._get_hk_kline_from_yfinance),
            'finnhub': ('finnhub', self._get_hk_kline_from_finnhub),
        }

        #Filter Effective Data Sources and Heavy
        valid_priority = []
        seen = set()
        for source_name in source_priority:
            source_key = source_name.lower()
            if source_key in source_handlers and source_key not in seen:
                seen.add(source_key)
                valid_priority.append(source_name)

        if not valid_priority:
            logger.warning("⚠️ database does not contain a valid K-line source, using default order")
            valid_priority = ['akshare', 'yahoo_finance', 'finnhub']

        logger.info(f"[HK K-line active data source]{valid_priority}")

        for source_name in valid_priority:
            source_key = source_name.lower()
            handler_name, handler_func = source_handlers[source_key]
            try:
                #Use asyncio.to thread to avoid blocking event cycles
                import asyncio
                kline_data = await asyncio.to_thread(handler_func, code, period, limit)
                data_source = handler_name

                if kline_data:
                    logger.info(f"✅ {data_source}K-line success:{code}")
                    break
            except Exception as e:
                logger.warning(f"⚠️ {source_name}Could not close temporary folder: %s{e}")
                continue

        if not kline_data:
            raise Exception(f"无法获取港股{code}的K线数据：所有数据源均失败")

        #4. Save to cache
        self.cache.save_stock_data(
            symbol=code,
            data=json.dumps(kline_data, ensure_ascii=False),
            data_source=cache_key_str
        )
        logger.info(f"Port K-line has been cached:{code}")

        return kline_data

    async def _get_us_kline(self, code: str, period: str, limit: int, force_refresh: bool = False) -> List[Dict]:
        """Get the K-line data.
 Call API according to the data source priorities configured in the database
"""
        #1. Check the cache (unless mandatory updating)
        cache_key_str = f"us_kline_{period}_{limit}"
        if not force_refresh:
            cache_key = self.cache.find_cached_stock_data(
                symbol=code,
                data_source=cache_key_str
            )

            if cache_key:
                cached_data = self.cache.load_stock_data(cache_key)
                if cached_data:
                    logger.info(f"From the cache.{code}")
                    return self._parse_cached_kline(cached_data)

        #2. Data source priorities from databases
        source_priority = await self._get_source_priority('US')

        #3. Piloting of data sources by priority
        kline_data = None
        data_source = None

        #Data Source Name Map
        source_handlers = {
            'alpha_vantage': ('alpha_vantage', self._get_us_kline_from_alpha_vantage),
            'yahoo_finance': ('yfinance', self._get_us_kline_from_yfinance),
            'finnhub': ('finnhub', self._get_us_kline_from_finnhub),
        }

        #Filter Effective Data Sources and Heavy
        valid_priority = []
        seen = set()
        for source_name in source_priority:
            source_key = source_name.lower()
            if source_key in source_handlers and source_key not in seen:
                seen.add(source_key)
                valid_priority.append(source_name)

        if not valid_priority:
            logger.warning("⚠️ database does not have a valid US share data source configured, using default order")
            valid_priority = ['yahoo_finance', 'alpha_vantage', 'finnhub']

        logger.info(f"[US K-line active data source]{valid_priority}")

        for source_name in valid_priority:
            source_key = source_name.lower()
            handler_name, handler_func = source_handlers[source_key]
            try:
                #Use asyncio.to thread to avoid blocking event cycles
                import asyncio
                kline_data = await asyncio.to_thread(handler_func, code, period, limit)
                data_source = handler_name

                if kline_data:
                    logger.info(f"✅ {data_source}Acquiring K-line success:{code}")
                    break
            except Exception as e:
                logger.warning(f"⚠️ {source_name}Could not close temporary folder: %s{e}")
                continue

        if not kline_data:
            raise Exception(f"无法获取美股{code}的K线数据：所有数据源均失败")

        #4. Save to cache
        self.cache.save_stock_data(
            symbol=code,
            data=json.dumps(kline_data, ensure_ascii=False),
            data_source=cache_key_str
        )
        logger.info(f"The United States share line has clogged:{code}")

        return kline_data
    
    def _format_hk_quote(self, data: Dict, code: str, source: str) -> Dict:
        """Format Hong Kong Stockline Data"""
        return {
            'code': code,
            'name': data.get('name', f'港股{code}'),
            'market': 'HK',
            'price': data.get('price') or data.get('close'),
            'open': data.get('open'),
            'high': data.get('high'),
            'low': data.get('low'),
            'volume': data.get('volume'),
            'currency': data.get('currency', 'HKD'),
            'source': source,
            'trade_date': data.get('timestamp', datetime.now().strftime('%Y-%m-%d')),
            'updated_at': datetime.now().isoformat()
        }

    def _format_hk_info(self, data: Dict, code: str, source: str) -> Dict:
        """Formatting Basic Information for the Port Unit"""
        market_cap = data.get('market_cap')
        return {
            'code': code,
            'name': data.get('name', f'港股{code}'),
            'market': 'HK',
            'industry': data.get('industry'),
            'sector': data.get('sector'),
            #Front-end expectation total mv (in billions of yuan)
            'total_mv': market_cap / 1e8 if market_cap else None,
            #Front-end expectation
            'pe_ttm': data.get('pe_ratio'),
            'pe': data.get('pe_ratio'),
            #Front-end expectation pb
            'pb': data.get('pb_ratio'),
            #Front-end Expectations ps
            'ps': data.get('ps_ratio'),
            'ps_ttm': data.get('ps_ratio'),
            #Get roe and debt ratio from the financial indicators
            'roe': data.get('roe'),
            'debt_ratio': data.get('debt_ratio'),
            'dividend_yield': data.get('dividend_yield'),
            'currency': data.get('currency', 'HKD'),
            'source': source,
            'updated_at': datetime.now().isoformat()
        }

    def _parse_cached_data(self, cached_data: str, market: str, code: str) -> Dict:
        """Parsing Cache Data"""
        try:
            #Try to parse JSON
            if isinstance(cached_data, str):
                data = json.loads(cached_data)
            else:
                data = cached_data

            #Ensure that necessary fields are included
            if isinstance(data, dict):
                data['market'] = market
                data['code'] = code
                return data
            else:
                raise ValueError("缓存数据格式错误")
        except Exception as e:
            logger.warning(f"Could not close temporary folder: %s{e}")
            #Return empty data, trigger recovery
            return None

    def _parse_cached_kline(self, cached_data: str) -> List[Dict]:
        """Parsing cached Kline data"""
        try:
            #Try to parse JSON
            if isinstance(cached_data, str):
                data = json.loads(cached_data)
            else:
                data = cached_data

            #Make sure it's a list.
            if isinstance(data, list):
                return data
            else:
                raise ValueError("缓存K线数据格式错误")
        except Exception as e:
            logger.warning(f"Could not close temporary folder: %s{e}")
            #Return empty list, trigger retrieving
            return []

    def _get_us_info_from_yfinance(self, code: str) -> Dict:
        """Basic information on American stock from yfinance"""
        import yfinance as yf

        ticker = yf.Ticker(code)
        info = ticker.info

        if not info:
            raise Exception("无数据")

        return {
            'name': info.get('longName') or info.get('shortName'),
            'industry': info.get('industry'),
            'sector': info.get('sector'),
            'market_cap': info.get('marketCap'),
            'pe_ratio': info.get('trailingPE'),
            'pb_ratio': info.get('priceToBook'),
            'dividend_yield': info.get('dividendYield'),
            'currency': info.get('currency', 'USD'),
        }

    def _safe_float(self, value, default=None):
        """Safely convert to floating point numbers, processing the 'None ' string and empty values"""
        if value is None or value == '' or value == 'None' or value == 'N/A':
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _get_us_info_from_alpha_vantage(self, code: str) -> Dict:
        """Access to US stock base information from Alpha Vantage"""
        from tradingagents.dataflows.providers.us.alpha_vantage_common import get_api_key, _make_api_request

        #Get API Key
        api_key = get_api_key()
        if not api_key:
            raise Exception("Alpha Vantage API Key 未配置")

        #Call overVIEW API
        params = {"symbol": code.upper()}
        data = _make_api_request("OVERVIEW", params)

        if not data or not data.get('Symbol'):
            raise Exception("无数据")

        return {
            'name': data.get('Name'),
            'industry': data.get('Industry'),
            'sector': data.get('Sector'),
            'market_cap': self._safe_float(data.get('MarketCapitalization')),
            'pe_ratio': self._safe_float(data.get('TrailingPE')),
            'pb_ratio': self._safe_float(data.get('PriceToBookRatio')),
            'dividend_yield': self._safe_float(data.get('DividendYield')),
            'currency': 'USD',
        }

    def _get_us_info_from_finnhub(self, code: str) -> Dict:
        """Get U.S. stock base information from Finnhub"""
        import finnhub
        import os

        #Get API Key
        api_key = os.getenv('FINNHUB_API_KEY')
        if not api_key:
            raise Exception("Finnhub API Key 未配置")

        #Create Client
        client = finnhub.Client(api_key=api_key)

        #Access to corporate information
        profile = client.company_profile2(symbol=code.upper())

        if not profile:
            raise Exception("无数据")

        return {
            'name': profile.get('name'),
            'industry': profile.get('finnhubIndustry'),
            'sector': None,  #Finnhub does not provide secor
            'market_cap': profile.get('marketCapitalization') * 1000000 if profile.get('marketCapitalization') else None,  #Convert to United States dollars
            'pe_ratio': None,  #Finnhub profile does not provide PE directly
            'pb_ratio': None,  #Finnhub profile does not directly provide PB
            'dividend_yield': None,  #Finnhub program does not directly provide dividends
            'currency': profile.get('currency', 'USD'),
        }

    def _get_us_kline_from_yfinance(self, code: str, period: str, limit: int) -> List[Dict]:
        """Get K-line data from yfinance"""
        import yfinance as yf

        ticker = yf.Ticker(code)

        #Periodic Map
        period_map = {
            'day': '1d',
            'week': '1wk',
            'month': '1mo',
            '5m': '5m',
            '15m': '15m',
            '30m': '30m',
            '60m': '60m'
        }

        interval = period_map.get(period, '1d')
        hist = ticker.history(period=f'{limit}d', interval=interval)

        if hist.empty:
            raise Exception("无数据")

        #Formatting Data
        kline_data = []
        for date, row in hist.iterrows():
            date_str = date.strftime('%Y-%m-%d')
            kline_data.append({
                'date': date_str,
                'trade_date': date_str,  #The front side needs this field
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'volume': int(row['Volume'])
            })

        return kline_data

    def _get_us_kline_from_alpha_vantage(self, code: str, period: str, limit: int) -> List[Dict]:
        """Get K-line data from Alpha Vantage"""
        from tradingagents.dataflows.providers.us.alpha_vantage_common import get_api_key, _make_api_request
        import pandas as pd

        #Get API Key
        api_key = get_api_key()
        if not api_key:
            raise Exception("Alpha Vantage API Key 未配置")

        #Select API function according to cycle
        if period in ['5m', '15m', '30m', '60m']:
            function = "TIME_SERIES_INTRADAY"
            params = {
                "symbol": code.upper(),
                "interval": period,
                "outputsize": "full"
            }
            time_series_key = f"Time Series ({period})"
        else:
            function = "TIME_SERIES_DAILY"
            params = {
                "symbol": code.upper(),
                "outputsize": "full"
            }
            time_series_key = "Time Series (Daily)"

        data = _make_api_request(function, params)

        if not data or time_series_key not in data:
            raise Exception("无数据")

        time_series = data[time_series_key]

        #Convert to DataFrame
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df.index = pd.to_datetime(df.index)
        df = df.sort_index(ascending=False)  #The latest in front.

        #Limited number
        df = df.head(limit)

        #Formatting Data
        kline_data = []
        for date, row in df.iterrows():
            date_str = date.strftime('%Y-%m-%d')
            kline_data.append({
                'date': date_str,
                'trade_date': date_str,  #The front side needs this field
                'open': float(row['1. open']),
                'high': float(row['2. high']),
                'low': float(row['3. low']),
                'close': float(row['4. close']),
                'volume': int(row['5. volume'])
            })

        return kline_data

    def _get_us_kline_from_finnhub(self, code: str, period: str, limit: int) -> List[Dict]:
        """Get K-line data from Finnhub"""
        import finnhub
        import os
        from datetime import datetime, timedelta

        #Get API Key
        api_key = os.getenv('FINNHUB_API_KEY')
        if not api_key:
            raise Exception("Finnhub API Key 未配置")

        #Create Client
        client = finnhub.Client(api_key=api_key)

        #Calculate Date Range
        end_date = datetime.now()

        #Start date based on cycle
        if period == 'day':
            start_date = end_date - timedelta(days=limit)
            resolution = 'D'
        elif period == 'week':
            start_date = end_date - timedelta(weeks=limit)
            resolution = 'W'
        elif period == 'month':
            start_date = end_date - timedelta(days=limit * 30)
            resolution = 'M'
        elif period == '5m':
            start_date = end_date - timedelta(days=limit)
            resolution = '5'
        elif period == '15m':
            start_date = end_date - timedelta(days=limit)
            resolution = '15'
        elif period == '30m':
            start_date = end_date - timedelta(days=limit)
            resolution = '30'
        elif period == '60m':
            start_date = end_date - timedelta(days=limit)
            resolution = '60'
        else:
            start_date = end_date - timedelta(days=limit)
            resolution = 'D'

        #Get K-line data
        candles = client.stock_candles(
            code.upper(),
            resolution,
            int(start_date.timestamp()),
            int(end_date.timestamp())
        )

        if not candles or candles.get('s') != 'ok':
            raise Exception("无数据")

        #Formatting Data
        kline_data = []
        for i in range(len(candles['t'])):
            date_str = datetime.fromtimestamp(candles['t'][i]).strftime('%Y-%m-%d')
            kline_data.append({
                'date': date_str,
                'trade_date': date_str,  #The front side needs this field
                'open': float(candles['o'][i]),
                'high': float(candles['h'][i]),
                'low': float(candles['l'][i]),
                'close': float(candles['c'][i]),
                'volume': int(candles['v'][i])
            })

        return kline_data

    async def get_hk_news(self, code: str, days: int = 2, limit: int = 50) -> Dict:
        """Access to information in the Port Unit

Args:
code: stock code
Days: Backtrace days
Limited number of returns

Returns:
Dictionary containing newslists and data sources
"""
        from datetime import datetime, timedelta

        logger.info(f"We're starting to get information from the Port Unit:{code}, days={days}, limit={limit}")

        #1. Attempt to obtain from the cache
        cache_key_str = f"hk_news_{days}_{limit}"
        cache_key = self.cache.find_cached_stock_data(
            symbol=code,
            data_source=cache_key_str
        )

        if cache_key:
            cached_data = self.cache.load_stock_data(cache_key)
            if cached_data:
                logger.info(f"From the cache to the Port Unit News:{code}")
                return json.loads(cached_data)

        #2. Data source priorities from databases
        source_priority = await self._get_source_priority('HK')

        #3. Piloting of data sources by priority
        news_data = None
        data_source = None

        #Data Source Name Map
        source_handlers = {
            'akshare': ('akshare', self._get_hk_news_from_akshare),
            'finnhub': ('finnhub', self._get_hk_news_from_finnhub),
        }

        #Filter Effective Data Sources and Heavy
        valid_priority = []
        seen = set()
        for source_name in source_priority:
            source_key = source_name.lower()
            if source_key in source_handlers and source_key not in seen:
                seen.add(source_key)
                valid_priority.append(source_name)

        if not valid_priority:
            logger.warning("⚠️ database does not have a valid port information source in default order")
            valid_priority = ['akshare', 'finnhub']

        logger.info(f"[HK News Valid Data Source]{valid_priority}")

        for source_name in valid_priority:
            source_key = source_name.lower()
            handler_name, handler_func = source_handlers[source_key]
            try:
                #Use asyncio.to thread to avoid blocking event cycles
                import asyncio
                news_data = await asyncio.to_thread(handler_func, code, days, limit)
                data_source = handler_name

                if news_data:
                    logger.info(f"✅ {data_source}Access to information by the Port Unit was successful:{code}Back{len(news_data)}Article")
                    break
            except Exception as e:
                logger.warning(f"⚠️ {source_name}Access to news failed:{e}")
                continue

        if not news_data:
            logger.warning(f"Port Unit not available{code}News data: all data sources failed")
            news_data = []
            data_source = 'none'

        #4. Build return data
        result = {
            'code': code,
            'days': days,
            'limit': limit,
            'source': data_source,
            'items': news_data
        }

        #5. Cache data
        self.cache.save_stock_data(
            symbol=code,
            data=json.dumps(result, ensure_ascii=False),
            data_source=cache_key_str
        )

        return result

    async def get_us_news(self, code: str, days: int = 2, limit: int = 50) -> Dict:
        """Access to American News

Args:
code: stock code
Days: Backtrace days
Limited number of returns

Returns:
Dictionary containing newslists and data sources
"""
        from datetime import datetime, timedelta

        logger.info(f"Here we go.{code}, days={days}, limit={limit}")

        #1. Attempt to obtain from the cache
        cache_key_str = f"us_news_{days}_{limit}"
        cache_key = self.cache.find_cached_stock_data(
            symbol=code,
            data_source=cache_key_str
        )

        if cache_key:
            cached_data = self.cache.load_stock_data(cache_key)
            if cached_data:
                logger.info(f"From the cache to the U.S. News:{code}")
                return json.loads(cached_data)

        #2. Data source priorities from databases
        source_priority = await self._get_source_priority('US')

        #3. Piloting of data sources by priority
        news_data = None
        data_source = None

        #Data Source Name Map
        source_handlers = {
            'alpha_vantage': ('alpha_vantage', self._get_us_news_from_alpha_vantage),
            'finnhub': ('finnhub', self._get_us_news_from_finnhub),
        }

        #Filter Effective Data Sources and Heavy
        valid_priority = []
        seen = set()
        for source_name in source_priority:
            source_key = source_name.lower()
            if source_key in source_handlers and source_key not in seen:
                seen.add(source_key)
                valid_priority.append(source_name)

        if not valid_priority:
            logger.warning("⚠️ database does not have a valid USE information source in default order")
            valid_priority = ['alpha_vantage', 'finnhub']

        logger.info(f"[US News Effective Data Source]{valid_priority}")

        for source_name in valid_priority:
            source_key = source_name.lower()
            handler_name, handler_func = source_handlers[source_key]
            try:
                #Use asyncio.to thread to avoid blocking event cycles
                import asyncio
                news_data = await asyncio.to_thread(handler_func, code, days, limit)
                data_source = handler_name

                if news_data:
                    logger.info(f"✅ {data_source}This post is part of our special coverage Global Voices 2011.{code}Back{len(news_data)}Article")
                    break
            except Exception as e:
                logger.warning(f"⚠️ {source_name}Access to news failed:{e}")
                continue

        if not news_data:
            logger.warning(f"Can't get a U.S. share.{code}News data: all data sources failed")
            news_data = []
            data_source = 'none'

        #4. Build return data
        result = {
            'code': code,
            'days': days,
            'limit': limit,
            'source': data_source,
            'items': news_data
        }

        #5. Cache data
        self.cache.save_stock_data(
            symbol=code,
            data=json.dumps(result, ensure_ascii=False),
            data_source=cache_key_str
        )

        return result

    def _get_us_news_from_alpha_vantage(self, code: str, days: int, limit: int) -> List[Dict]:
        """Get U.S. News from Alpha Vantage"""
        from tradingagents.dataflows.providers.us.alpha_vantage_common import get_api_key, _make_api_request
        from datetime import datetime, timedelta

        #Get API Key
        api_key = get_api_key()
        if not api_key:
            raise Exception("Alpha Vantage API Key 未配置")

        #Calculate the time frame
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        #NEWS SENTIMENT API
        params = {
            "tickers": code.upper(),
            "time_from": start_date.strftime('%Y%m%dT%H%M'),
            "time_to": end_date.strftime('%Y%m%dT%H%M'),
            "sort": "LATEST",
            "limit": str(limit),
        }

        data = _make_api_request("NEWS_SENTIMENT", params)

        if not data or 'feed' not in data:
            raise Exception("无数据")

        #Format News Data
        news_list = []
        for article in data.get('feed', [])[:limit]:
            #Parsing Time
            time_published = article.get('time_published', '')
            try:
                #Alpha Vantage Time Format: 20240101T12000
                pub_time = datetime.strptime(time_published, '%Y%m%dT%H%M%S')
                pub_time_str = pub_time.strftime('%Y-%m-%d %H:%M:%S')
            except:
                pub_time_str = time_published

            #Take the emotional score of the stock.
            sentiment_score = None
            sentiment_label = article.get('overall_sentiment_label', 'Neutral')

            ticker_sentiment = article.get('ticker_sentiment', [])
            for ts in ticker_sentiment:
                if ts.get('ticker', '').upper() == code.upper():
                    sentiment_score = ts.get('ticker_sentiment_score')
                    sentiment_label = ts.get('ticker_sentiment_label', sentiment_label)
                    break

            news_list.append({
                'title': article.get('title', ''),
                'summary': article.get('summary', ''),
                'url': article.get('url', ''),
                'source': article.get('source', ''),
                'publish_time': pub_time_str,
                'sentiment': sentiment_label,
                'sentiment_score': sentiment_score,
            })

        return news_list

    def _get_us_news_from_finnhub(self, code: str, days: int, limit: int) -> List[Dict]:
        """From Finnhub to American News"""
        import finnhub
        import os
        from datetime import datetime, timedelta

        #Get API Key
        api_key = os.getenv('FINNHUB_API_KEY')
        if not api_key:
            raise Exception("Finnhub API Key 未配置")

        #Create Client
        client = finnhub.Client(api_key=api_key)

        #Calculate the time frame
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        #Access to corporate news
        news = client.company_news(
            code.upper(),
            _from=start_date.strftime('%Y-%m-%d'),
            to=end_date.strftime('%Y-%m-%d')
        )

        if not news:
            raise Exception("无数据")

        #Format News Data
        news_list = []
        for article in news[:limit]:
            #Parsing Timestamps
            timestamp = article.get('datetime', 0)
            pub_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

            news_list.append({
                'title': article.get('headline', ''),
                'summary': article.get('summary', ''),
                'url': article.get('url', ''),
                'source': article.get('source', ''),
                'publish_time': pub_time,
                'sentiment': None,  #Finnhub does not provide emotional analysis.
                'sentiment_score': None,
            })

        return news_list

    def _get_hk_news_from_finnhub(self, code: str, days: int, limit: int) -> List[Dict]:
        """Port Unit News from Finnhub"""
        import finnhub
        import os
        from datetime import datetime, timedelta

        #Get API Key
        api_key = os.getenv('FINNHUB_API_KEY')
        if not api_key:
            raise Exception("Finnhub API Key 未配置")

        #Create Client
        client = finnhub.Client(api_key=api_key)

        #Calculate the time frame
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        #Port stock code needs to add. HK suffix
        hk_symbol = f"{code}.HK" if not code.endswith('.HK') else code

        #Access to corporate news
        news = client.company_news(
            hk_symbol,
            _from=start_date.strftime('%Y-%m-%d'),
            to=end_date.strftime('%Y-%m-%d')
        )

        if not news:
            raise Exception("无数据")

        #Format News Data
        news_list = []
        for article in news[:limit]:
            #Parsing Timestamps
            timestamp = article.get('datetime', 0)
            pub_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

            news_list.append({
                'title': article.get('headline', ''),
                'summary': article.get('summary', ''),
                'url': article.get('url', ''),
                'source': article.get('source', ''),
                'publish_time': pub_time,
                'sentiment': None,  #Finnhub does not provide emotional analysis.
                'sentiment_score': None,
            })

        return news_list

    def _get_hk_info_from_akshare(self, code: str) -> Dict:
        """Access to basic information and financial indicators for the Port Unit from Akshare"""
        from tradingagents.dataflows.providers.hk.improved_hk import (
            get_hk_stock_info_akshare,
            get_hk_financial_indicators
        )

        #1. Access to basic information (including current prices)
        info = get_hk_stock_info_akshare(code)
        if not info or 'error' in info:
            raise Exception("无数据")

        #2. Access to financial indicators (EPS, BPS, ROE, liability ratio, etc.)
        financial_indicators = {}
        try:
            financial_indicators = get_hk_financial_indicators(code)
            logger.info(f"Access Port Unit{code}Success in financial indicators:{list(financial_indicators.keys())}")
        except Exception as e:
            logger.warning(f"Access Port Unit{code}Financial indicators failed:{e}")

        #3. Calculation of PE, PB, PS (calculation of reference analysis modules)
        current_price = info.get('price')  #Current price
        pe_ratio = None
        pb_ratio = None
        ps_ratio = None

        if current_price and financial_indicators:
            #Calculate PE = current price/ EPS TTM
            eps_ttm = financial_indicators.get('eps_ttm')
            if eps_ttm and eps_ttm > 0:
                pe_ratio = current_price / eps_ttm
                logger.info(f"PE:{current_price} / {eps_ttm} = {pe_ratio:.2f}")

            #Calculate PB = current / BPS
            bps = financial_indicators.get('bps')
            if bps and bps > 0:
                pb_ratio = current_price / bps
                logger.info(f"♪ 📊 ♪ Calculating PB:{current_price} / {bps} = {pb_ratio:.2f}")

            #Calculation of PS = market value / operating income (market value data required, not available for the time being)
            #ps ratio provisionally as None

        #4. Consolidation of data
        return {
            'name': info.get('name', f'港股{code}'),
            'market_cap': None,  #AKShare base information does not contain market value
            'industry': None,
            'sector': None,
            #🔥 Valuable indicators calculated
            'pe_ratio': pe_ratio,
            'pb_ratio': pb_ratio,
            'ps_ratio': ps_ratio,
            'dividend_yield': None,
            'currency': 'HKD',
            #🔥 from financial indicators
            'roe': financial_indicators.get('roe_avg'),  #Average net asset return
            'debt_ratio': financial_indicators.get('debt_asset_ratio'),  #Assets and liabilities ratio
        }

    def _get_hk_info_from_yfinance(self, code: str) -> Dict:
        """Basic information from Yahoo Finance"""
        import yfinance as yf

        ticker = yf.Ticker(f"{code}.HK")
        info = ticker.info

        return {
            'name': info.get('longName') or info.get('shortName') or f'港股{code}',
            'market_cap': info.get('marketCap'),
            'industry': info.get('industry'),
            'sector': info.get('sector'),
            'pe_ratio': info.get('trailingPE'),
            'pb_ratio': info.get('priceToBook'),
            'dividend_yield': info.get('dividendYield'),
            'currency': info.get('currency', 'HKD'),
        }

    def _get_hk_info_from_finnhub(self, code: str) -> Dict:
        """Basic information from Finnhub"""
        import finnhub
        import os

        #Get API Key
        api_key = os.getenv('FINNHUB_API_KEY')
        if not api_key:
            raise Exception("Finnhub API Key 未配置")

        #Create Client
        client = finnhub.Client(api_key=api_key)

        #Port stock code needs to add. HK suffix
        hk_symbol = f"{code}.HK" if not code.endswith('.HK') else code

        #Access to basic corporate information
        profile = client.company_profile2(symbol=hk_symbol)

        if not profile:
            raise Exception("无数据")

        return {
            'name': profile.get('name', f'港股{code}'),
            'market_cap': profile.get('marketCapitalization') * 1e6 if profile.get('marketCapitalization') else None,  #Finnhub returns millions of units.
            'industry': profile.get('finnhubIndustry'),
            'sector': None,
            'pe_ratio': None,
            'pb_ratio': None,
            'dividend_yield': None,
            'currency': profile.get('currency', 'HKD'),
        }

    def _get_hk_kline_from_akshare(self, code: str, period: str, limit: int) -> List[Dict]:
        """K-line data from Akshare"""
        import akshare as ak
        import pandas as pd
        from datetime import datetime, timedelta
        from tradingagents.dataflows.providers.hk.improved_hk import get_improved_hk_provider

        #Standardized Code
        provider = get_improved_hk_provider()
        normalized_code = provider._normalize_hk_symbol(code)

        #Directly use AKShare API
        df = ak.stock_hk_daily(symbol=normalized_code, adjust="qfq")

        if df is None or df.empty:
            raise Exception("无数据")

        #Filter Recent Data
        df = df.tail(limit)

        #Formatting Data
        kline_data = []
        for _, row in df.iterrows():
            #AKShare returns the list: date, open, close, high, low, volume
            date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])
            kline_data.append({
                'date': date_str,
                'trade_date': date_str,
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'volume': int(row['volume']) if 'volume' in row else 0
            })

        return kline_data

    def _get_hk_kline_from_yfinance(self, code: str, period: str, limit: int) -> List[Dict]:
        """K-line data from Yahoo Finance"""
        import yfinance as yf
        import pandas as pd

        ticker = yf.Ticker(f"{code}.HK")

        #Periodic Map
        period_map = {
            'day': '1d',
            'week': '1wk',
            'month': '1mo',
            '5m': '5m',
            '15m': '15m',
            '30m': '30m',
            '60m': '60m'
        }

        interval = period_map.get(period, '1d')
        hist = ticker.history(period=f'{limit}d', interval=interval)

        if hist.empty:
            raise Exception("无数据")

        #Formatting Data
        kline_data = []
        for date, row in hist.iterrows():
            date_str = date.strftime('%Y-%m-%d')
            kline_data.append({
                'date': date_str,
                'trade_date': date_str,
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'volume': int(row['Volume'])
            })

        return kline_data[-limit:]  #Return LastLimit

    def _get_hk_kline_from_finnhub(self, code: str, period: str, limit: int) -> List[Dict]:
        """K-line data from Finnhub"""
        import finnhub
        import os
        from datetime import datetime, timedelta

        #Get API Key
        api_key = os.getenv('FINNHUB_API_KEY')
        if not api_key:
            raise Exception("Finnhub API Key 未配置")

        #Create Client
        client = finnhub.Client(api_key=api_key)

        #Port stock code needs to add. HK suffix
        hk_symbol = f"{code}.HK" if not code.endswith('.HK') else code

        #Periodic Map
        resolution_map = {
            'day': 'D',
            'week': 'W',
            'month': 'M',
            '5m': '5',
            '15m': '15',
            '30m': '30',
            '60m': '60'
        }

        resolution = resolution_map.get(period, 'D')

        #Calculate the time frame
        end_time = int(datetime.now().timestamp())
        start_time = int((datetime.now() - timedelta(days=limit * 2)).timestamp())

        #Get K-line data
        candles = client.stock_candles(hk_symbol, resolution, start_time, end_time)

        if not candles or candles.get('s') != 'ok':
            raise Exception("无数据")

        #Formatting Data
        kline_data = []
        for i in range(len(candles['t'])):
            date_str = datetime.fromtimestamp(candles['t'][i]).strftime('%Y-%m-%d')
            kline_data.append({
                'date': date_str,
                'trade_date': date_str,
                'open': float(candles['o'][i]),
                'high': float(candles['h'][i]),
                'low': float(candles['l'][i]),
                'close': float(candles['c'][i]),
                'volume': int(candles['v'][i])
            })

        return kline_data[-limit:]  #Return LastLimit

    def _get_hk_news_from_akshare(self, code: str, days: int, limit: int) -> List[Dict]:
        """Port Unit News from Akshare"""
        try:
            import akshare as ak
            from datetime import datetime, timedelta

            #HKU News Interface for Akshare
            #Note: AKShare may not have a dedicated port desk news interface, which is used here.
            #If no suitable interface exists, throw out the anomaly and let the system try the next data. Source

            #Attempted access to information about the port unit (using information about the East Fortune Port)
            try:
                df = ak.stock_news_em(symbol=code)
                if df is None or df.empty:
                    raise Exception("无数据")

                #Format News Data
                news_list = []
                for _, row in df.head(limit).iterrows():
                    pub_time = row['发布时间'] if '发布时间' in row else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    news_list.append({
                        'title': row['新闻标题'] if '新闻标题' in row else '',
                        'summary': row['新闻内容'] if '新闻内容' in row else '',
                        'url': row['新闻链接'] if '新闻链接' in row else '',
                        'source': 'AKShare-东方财富',
                        'publish_time': pub_time,
                        'sentiment': None,
                        'sentiment_score': None,
                    })

                return news_list
            except Exception as e:
                logger.debug(f"AKShare The East Wealth Interface failed:{e}")
                raise Exception("AKShare 暂不支持港股新闻")

        except Exception as e:
            logger.warning(f"AKshare has failed to access Hong Kong News:{e}")
            raise

