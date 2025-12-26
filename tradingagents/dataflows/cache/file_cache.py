#!/usr/bin/env python3
"""Stock Data Cache Manager
Support local cache stock data, reduce API calls and increase response speed
"""

import os
import json
import pickle
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, Union, List
import hashlib

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')


class StockDataCache:
    """Stock Data Cache Manager - support U.S. and A.S. data cache optimization"""

    def __init__(self, cache_dir: str = None):
        """Initialise Cache Manager

Args:
Cache dir: Cache Directory Path, default to trapats/dataflows/data cache
"""
        if cache_dir is None:
            #Can not open message
            current_dir = Path(__file__).parent
            cache_dir = current_dir / "data_cache"

        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)

        #Create subdirectories - by market
        self.us_stock_dir = self.cache_dir / "us_stocks"
        self.china_stock_dir = self.cache_dir / "china_stocks"
        self.us_news_dir = self.cache_dir / "us_news"
        self.china_news_dir = self.cache_dir / "china_news"
        self.us_fundamentals_dir = self.cache_dir / "us_fundamentals"
        self.china_fundamentals_dir = self.cache_dir / "china_fundamentals"
        self.metadata_dir = self.cache_dir / "metadata"

        #Create all directories
        for dir_path in [self.us_stock_dir, self.china_stock_dir, self.us_news_dir,
                        self.china_news_dir, self.us_fundamentals_dir,
                        self.china_fundamentals_dir, self.metadata_dir]:
            dir_path.mkdir(exist_ok=True)

        #Cache Configuration - Different TTL for different markets
        self.cache_config = {
            'us_stock_data': {
                'ttl_hours': 2,  #US stock data cache 2 hours (taking into account API limitations)
                'max_files': 1000,
                'description': '美股历史数据'
            },
            'china_stock_data': {
                'ttl_hours': 1,  #Unit A data cache 1 hour (high real-time requirement)
                'max_files': 1000,
                'description': 'A股历史数据'
            },
            'us_news': {
                'ttl_hours': 6,  #Six hours in the News of America.
                'max_files': 500,
                'description': '美股新闻数据'
            },
            'china_news': {
                'ttl_hours': 4,  #4 hours news cache for Unit A
                'max_files': 500,
                'description': 'A股新闻数据'
            },
            'us_fundamentals': {
                'ttl_hours': 24,  #US stock base data cache 24 hours
                'max_files': 200,
                'description': '美股基本面数据'
            },
            'china_fundamentals': {
                'ttl_hours': 12,  #Basic A data cache for 12 hours
                'max_files': 200,
                'description': 'A股基本面数据'
            }
        }

        #Content Length Limit Configuration (file cache default unlimited)
        self.content_length_config = {
            'max_content_length': int(os.getenv('MAX_CACHE_CONTENT_LENGTH', '50000')),  #50K characters
            'long_text_providers': ['dashscope', 'openai', 'google'],  #Provider in support of long text
            'enable_length_check': os.getenv('ENABLE_CACHE_LENGTH_CHECK', 'false').lower() == 'true'  #File cache default unlimited
        }

        logger.info(f"Initialization of cache manager, cache catalogue:{self.cache_dir}")
        logger.info(f"Initialization of database cache manager completed")
        logger.info(f"US share data: ✅ configured")
        logger.info(f"Unit A data: ✅ configured")

    def _determine_market_type(self, symbol: str) -> str:
        """Market type determined by stock code"""
        import re

        #Whether or not to judge China A shares (6 figures)
        if re.match(r'^\d{6}$', str(symbol)):
            return 'china'
        else:
            return 'us'

    def _check_provider_availability(self) -> List[str]:
        """Check available LLM providers"""
        available_providers = []
        
        #Check DashScope
        dashscope_key = os.getenv("DASHSCOPE_API_KEY")
        if dashscope_key and dashscope_key.strip():
            available_providers.append('dashscope')
        
        #Check OpenAI
        openai_key = os.getenv("OPENAI_API_KEY")
        if openai_key and openai_key.strip():
            #Simple format check
            if openai_key.startswith('sk-') and len(openai_key) >= 40:
                available_providers.append('openai')
        
        #Check Google AI
        google_key = os.getenv("GOOGLE_API_KEY")
        if google_key and google_key.strip():
            available_providers.append('google')
        
        #Check Anthropic
        anthropic_key = os.getenv("ANTHROPIC_API_KEY")
        if anthropic_key and anthropic_key.strip():
            available_providers.append('anthropic')
        
        return available_providers

    def should_skip_cache_for_content(self, content: str, data_type: str = "unknown") -> bool:
        """Whether or not to skip the cache because the content is too long

Args:
Content to cache
Data type: Data type (for logs)

Returns:
Bool: Should Skip Cache
"""
        #If the length check is not enabled, go straight back to False
        if not self.content_length_config['enable_length_check']:
            return False
        
        #Check content length
        content_length = len(content)
        max_length = self.content_length_config['max_content_length']
        
        if content_length <= max_length:
            return False
        
        #It's too long to check if long text processing is available Business
        available_providers = self._check_provider_availability()
        long_text_providers = self.content_length_config['long_text_providers']
        
        #Found available long text providers
        available_long_providers = [p for p in available_providers if p in long_text_providers]
        
        if not available_long_providers:
            logger.warning(f"It's too long.{content_length:,}Character >{max_length:,}character) and no long text provider available, skip{data_type}Cache")
            logger.info(f"Available providers:{available_providers}")
            logger.info(f"Long text providers:{long_text_providers}")
            return True
        else:
            logger.info(f"Longer content (✅){content_length:,}Character) but long text providers available ({available_long_providers}Continue Cache")
            return False
    
    def _generate_cache_key(self, data_type: str, symbol: str, **kwargs) -> str:
        """Generate Cache Keys"""
        #Create a string with all parameters
        params_str = f"{data_type}_{symbol}"
        for key, value in sorted(kwargs.items()):
            params_str += f"_{key}_{value}"
        
        #Use MD5 to generate the only short identifier
        cache_key = hashlib.md5(params_str.encode()).hexdigest()[:12]
        return f"{symbol}_{data_type}_{cache_key}"
    
    def _get_cache_path(self, data_type: str, cache_key: str, file_format: str = "json", symbol: str = None) -> Path:
        """Acquire cache file paths - support market classification"""
        if symbol:
            market_type = self._determine_market_type(symbol)
        else:
            #Try extract market type from the cache key
            market_type = 'us' if not cache_key.startswith(('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 'china'

        #Select directories by data type and market type
        if data_type == "stock_data":
            base_dir = self.china_stock_dir if market_type == 'china' else self.us_stock_dir
        elif data_type == "news":
            base_dir = self.china_news_dir if market_type == 'china' else self.us_news_dir
        elif data_type == "fundamentals":
            base_dir = self.china_fundamentals_dir if market_type == 'china' else self.us_fundamentals_dir
        else:
            base_dir = self.cache_dir

        return base_dir / f"{cache_key}.{file_format}"
    
    def _get_metadata_path(self, cache_key: str) -> Path:
        """Path to getting metadata files"""
        return self.metadata_dir / f"{cache_key}_meta.json"
    
    def _save_metadata(self, cache_key: str, metadata: Dict[str, Any]):
        """Save metadata"""
        metadata_path = self._get_metadata_path(cache_key)
        metadata_path.parent.mkdir(parents=True, exist_ok=True)  #Ensure directory exists
        metadata['cached_at'] = datetime.now().isoformat()
        
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
    
    def _load_metadata(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Loading metadata"""
        metadata_path = self._get_metadata_path(cache_key)
        if not metadata_path.exists():
            return None
        
        try:
            with open(metadata_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return None
    
    def is_cache_valid(self, cache_key: str, max_age_hours: int = None, symbol: str = None, data_type: str = None) -> bool:
        """Check Cache Effectiveness - Support Smart TTL Configuration"""
        metadata = self._load_metadata(cache_key)
        if not metadata:
            return False

        #If no TTL is specified, automatically determine according to data type and market
        if max_age_hours is None:
            if symbol and data_type:
                market_type = self._determine_market_type(symbol)
                cache_type = f"{market_type}_{data_type}"
                max_age_hours = self.cache_config.get(cache_type, {}).get('ttl_hours', 24)
            else:
                #Get information from metadata
                symbol = metadata.get('symbol', '')
                data_type = metadata.get('data_type', 'stock_data')
                market_type = self._determine_market_type(symbol)
                cache_type = f"{market_type}_{data_type}"
                max_age_hours = self.cache_config.get(cache_type, {}).get('ttl_hours', 24)

        cached_at = datetime.fromisoformat(metadata['cached_at'])
        age = datetime.now() - cached_at

        is_valid = age.total_seconds() < max_age_hours * 3600

        if is_valid:
            market_type = self._determine_market_type(metadata.get('symbol', ''))
            cache_type = f"{market_type}_{metadata.get('data_type', 'stock_data')}"
            desc = self.cache_config.get(cache_type, {}).get('description', '数据')
            logger.info(f"Cache is active:{desc} - {metadata.get('symbol')}Remaining{max_age_hours - age.total_seconds()/3600:.1f}h)")

        return is_valid
    
    def save_stock_data(self, symbol: str, data: Union[pd.DataFrame, str],
                       start_date: str = None, end_date: str = None,
                       data_source: str = "unknown") -> str:
        """Store stock data to cache - Support U.S. and U.S.A. Catalogue Storage

Args:
symbol: stock code
Data: Stock data (DataFrame or string)
Start date: Start date
End date: End date
Data source: Data sources (e.g. "tdx", "yfinance", "finnhub")

Returns:
Cache key: Cache keys
"""
        #Check if content length needs to skip cache
        content_to_check = str(data)
        if self.should_skip_cache_for_content(content_to_check, "股票数据"):
            #Generates a virtual cache key but does not actually save
            market_type = self._determine_market_type(symbol)
            cache_key = self._generate_cache_key("stock_data", symbol,
                                               start_date=start_date,
                                               end_date=end_date,
                                               source=data_source,
                                               market=market_type,
                                               skipped=True)
            logger.info(f"🚫 Stock data skips the cache due to excessive content:{symbol} -> {cache_key}")
            return cache_key

        market_type = self._determine_market_type(symbol)
        cache_key = self._generate_cache_key("stock_data", symbol,
                                           start_date=start_date,
                                           end_date=end_date,
                                           source=data_source,
                                           market=market_type)

        #Save Data
        if isinstance(data, pd.DataFrame):
            cache_path = self._get_cache_path("stock_data", cache_key, "csv", symbol)
            cache_path.parent.mkdir(parents=True, exist_ok=True)  #Ensure directory exists
            data.to_csv(cache_path, index=True)
        else:
            cache_path = self._get_cache_path("stock_data", cache_key, "txt", symbol)
            cache_path.parent.mkdir(parents=True, exist_ok=True)  #Ensure directory exists
            with open(cache_path, 'w', encoding='utf-8') as f:
                f.write(str(data))

        #Save metadata
        metadata = {
            'symbol': symbol,
            'data_type': 'stock_data',
            'market_type': market_type,
            'start_date': start_date,
            'end_date': end_date,
            'data_source': data_source,
            'file_path': str(cache_path),
            'file_format': 'csv' if isinstance(data, pd.DataFrame) else 'txt',
            'content_length': len(content_to_check)
        }
        self._save_metadata(cache_key, metadata)

        #Fetch description information
        cache_type = f"{market_type}_stock_data"
        desc = self.cache_config.get(cache_type, {}).get('description', '股票数据')
        logger.info(f"💾 {desc}Cache:{symbol} ({data_source}) -> {cache_key}")
        return cache_key
    
    def load_stock_data(self, cache_key: str) -> Optional[Union[pd.DataFrame, str]]:
        """Loading stock data from cache"""
        metadata = self._load_metadata(cache_key)
        if not metadata:
            return None
        
        cache_path = Path(metadata['file_path'])
        if not cache_path.exists():
            return None
        
        try:
            if metadata['file_format'] == 'csv':
                return pd.read_csv(cache_path, index_col=0)
            else:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    return f.read()
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return None
    
    def find_cached_stock_data(self, symbol: str, start_date: str = None,
                              end_date: str = None, data_source: str = None,
                              max_age_hours: int = None) -> Optional[str]:
        """Find matching cache data - support smart market classification search

Args:
symbol: stock code
Start date: Start date
End date: End date
data source: data source
max age hours: maximum cache time (hours), use smart configuration for None

Returns:
Cache key: return the cache key if a valid cache is found, otherwise return the None
"""
        market_type = self._determine_market_type(symbol)

        #Use smart configuration if no TTL is specified
        if max_age_hours is None:
            cache_type = f"{market_type}_stock_data"
            max_age_hours = self.cache_config.get(cache_type, {}).get('ttl_hours', 24)

        #Generate Search Keys
        search_key = self._generate_cache_key("stock_data", symbol,
                                            start_date=start_date,
                                            end_date=end_date,
                                            source=data_source,
                                            market=market_type)

        #Check for exact match
        if self.is_cache_valid(search_key, max_age_hours, symbol, 'stock_data'):
            desc = self.cache_config.get(f"{market_type}_stock_data", {}).get('description', '数据')
            logger.info(f"I found the exact match.{desc}: {symbol} -> {search_key}")
            return search_key

        #If no exact match, find partial match (other caches of the same stock code)
        for metadata_file in self.metadata_dir.glob(f"*_meta.json"):
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)

                if (metadata.get('symbol') == symbol and
                    metadata.get('data_type') == 'stock_data' and
                    metadata.get('market_type') == market_type and
                    (data_source is None or metadata.get('data_source') == data_source)):

                    cache_key = metadata_file.stem.replace('_meta', '')
                    if self.is_cache_valid(cache_key, max_age_hours, symbol, 'stock_data'):
                        desc = self.cache_config.get(f"{market_type}_stock_data", {}).get('description', '数据')
                        logger.info(f"I found a partial match.{desc}: {symbol} -> {cache_key}")
                        return cache_key
            except Exception:
                continue

        desc = self.cache_config.get(f"{market_type}_stock_data", {}).get('description', '数据')
        logger.error(f"It's not working.{desc}Cache:{symbol}")
        return None
    
    def save_news_data(self, symbol: str, news_data: str, 
                      start_date: str = None, end_date: str = None,
                      data_source: str = "unknown") -> str:
        """Save news data to cache"""
        #Check if content length needs to skip cache
        if self.should_skip_cache_for_content(news_data, "新闻数据"):
            #Generates a virtual cache key but does not actually save
            cache_key = self._generate_cache_key("news", symbol,
                                               start_date=start_date,
                                               end_date=end_date,
                                               source=data_source,
                                               skipped=True)
            logger.info(f"News data jumped over cache due to excessive content:{symbol} -> {cache_key}")
            return cache_key

        cache_key = self._generate_cache_key("news", symbol,
                                           start_date=start_date,
                                           end_date=end_date,
                                           source=data_source)
        
        cache_path = self._get_cache_path("news", cache_key, "txt")
        cache_path.parent.mkdir(parents=True, exist_ok=True)  #Ensure directory exists
        with open(cache_path, 'w', encoding='utf-8') as f:
            f.write(news_data)
        
        metadata = {
            'symbol': symbol,
            'data_type': 'news',
            'start_date': start_date,
            'end_date': end_date,
            'data_source': data_source,
            'file_path': str(cache_path),
            'file_format': 'txt',
            'content_length': len(news_data)
        }
        self._save_metadata(cache_key, metadata)
        
        logger.info(f"News data cache:{symbol} ({data_source}) -> {cache_key}")
        return cache_key
    
    def save_fundamentals_data(self, symbol: str, fundamentals_data: str,
                              data_source: str = "unknown") -> str:
        """Save base face data to cache"""
        #Check if content length needs to skip cache
        if self.should_skip_cache_for_content(fundamentals_data, "基本面数据"):
            #Generates a virtual cache key but does not actually save
            market_type = self._determine_market_type(symbol)
            cache_key = self._generate_cache_key("fundamentals", symbol,
                                               source=data_source,
                                               market=market_type,
                                               date=datetime.now().strftime("%Y-%m-%d"),
                                               skipped=True)
            logger.info(f"Basic data jumped over cache due to excessive content:{symbol} -> {cache_key}")
            return cache_key

        market_type = self._determine_market_type(symbol)
        cache_key = self._generate_cache_key("fundamentals", symbol,
                                           source=data_source,
                                           market=market_type,
                                           date=datetime.now().strftime("%Y-%m-%d"))
        
        cache_path = self._get_cache_path("fundamentals", cache_key, "txt", symbol)
        cache_path.parent.mkdir(parents=True, exist_ok=True)  #Ensure directory exists
        with open(cache_path, 'w', encoding='utf-8') as f:
            f.write(fundamentals_data)
        
        metadata = {
            'symbol': symbol,
            'data_type': 'fundamentals',
            'data_source': data_source,
            'market_type': market_type,
            'file_path': str(cache_path),
            'file_format': 'txt',
            'content_length': len(fundamentals_data)
        }
        self._save_metadata(cache_key, metadata)
        
        desc = self.cache_config.get(f"{market_type}_fundamentals", {}).get('description', '基本面数据')
        logger.info(f"💼 {desc}Cache:{symbol} ({data_source}) -> {cache_key}")
        return cache_key
    
    def load_fundamentals_data(self, cache_key: str) -> Optional[str]:
        """Load basic face data from cache"""
        metadata = self._load_metadata(cache_key)
        if not metadata:
            return None
        
        cache_path = Path(metadata['file_path'])
        if not cache_path.exists():
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return None
    
    def find_cached_fundamentals_data(self, symbol: str, data_source: str = None,
                                    max_age_hours: int = None) -> Optional[str]:
        """Find matching base cache data

Args:
symbol: stock code
Data source: Data sources (e.g. "openai", "finnhub")
max age hours: maximum cache time (hours), use smart configuration for None

Returns:
Cache key: return the cache key if a valid cache is found, otherwise return the None
"""
        market_type = self._determine_market_type(symbol)
        
        #Use smart configuration if no TTL is specified
        if max_age_hours is None:
            cache_type = f"{market_type}_fundamentals"
            max_age_hours = self.cache_config.get(cache_type, {}).get('ttl_hours', 24)
        
        #Find matching caches
        for metadata_file in self.metadata_dir.glob(f"*_meta.json"):
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                
                if (metadata.get('symbol') == symbol and
                    metadata.get('data_type') == 'fundamentals' and
                    metadata.get('market_type') == market_type and
                    (data_source is None or metadata.get('data_source') == data_source)):
                    
                    cache_key = metadata_file.stem.replace('_meta', '')
                    if self.is_cache_valid(cache_key, max_age_hours, symbol, 'fundamentals'):
                        desc = self.cache_config.get(f"{market_type}_fundamentals", {}).get('description', '基本面数据')
                        logger.info(f"I found a match.{desc}Cache:{symbol} ({data_source}) -> {cache_key}")
                        return cache_key
            except Exception:
                continue
        
        desc = self.cache_config.get(f"{market_type}_fundamentals", {}).get('description', '基本面数据')
        logger.error(f"It's not working.{desc}Cache:{symbol} ({data_source})")
        return None
    
    def clear_old_cache(self, max_age_days: int = 7):
        """Clear Expired Cache"""
        cutoff_time = datetime.now() - timedelta(days=max_age_days)
        cleared_count = 0
        
        for metadata_file in self.metadata_dir.glob("*_meta.json"):
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                
                cached_at = datetime.fromisoformat(metadata['cached_at'])
                if cached_at < cutoff_time:
                    #Delete Data File
                    data_file = Path(metadata['file_path'])
                    if data_file.exists():
                        data_file.unlink()
                    
                    #Remove Metadata File
                    metadata_file.unlink()
                    cleared_count += 1
                    
            except Exception as e:
                logger.warning(f"Error cleaning cache:{e}")
        
        logger.info(f"Cleared{cleared_count}An expired cache file")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistical information"""
        stats = {
            'total_files': 0,
            'stock_data_count': 0,
            'news_count': 0,
            'fundamentals_count': 0,
            'total_size': 0,  #Bytes
            'total_size_mb': 0,  #MB (Reservations for compatibility)
            'skipped_count': 0  #Add: Number of Caches Skipped
        }

        total_size_bytes = 0

        #Cache file for statistical metadata
        metadata_files_count = 0
        for metadata_file in self.metadata_dir.glob("*_meta.json"):
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)

                data_type = metadata.get('data_type', 'unknown')
                if data_type == 'stock_data':
                    stats['stock_data_count'] += 1
                elif data_type == 'news':
                    stats['news_count'] += 1
                elif data_type == 'fundamentals':
                    stats['fundamentals_count'] += 1

                #Check if skipped cache (no actual file)
                data_file = Path(metadata.get('file_path', ''))
                if not data_file.exists():
                    stats['skipped_count'] += 1
                else:
                    #Calculate file size (bytes)
                    file_size = data_file.stat().st_size
                    total_size_bytes += file_size

                stats['total_files'] += 1
                metadata_files_count += 1

            except Exception:
                continue

        #If no metadata file is available, directly count the files in the cache directory (compatible with the old cache)
        if metadata_files_count == 0:
            logger.info("📊 was not found for metadata files, directly in the statistical cache directory")

            #Documents in statistical directories
            for stock_dir, data_type in [
                (self.us_stock_dir, 'us_stock'),
                (self.china_stock_dir, 'china_stock'),
                (self.us_news_dir, 'us_news'),
                (self.china_news_dir, 'china_news'),
                (self.us_fundamentals_dir, 'us_fundamentals'),
                (self.china_fundamentals_dir, 'china_fundamentals')
            ]:
                if stock_dir.exists():
                    for file_path in stock_dir.glob("*"):
                        if file_path.is_file():
                            try:
                                file_size = file_path.stat().st_size
                                total_size_bytes += file_size
                                stats['total_files'] += 1

                                #By type
                                if 'stock' in data_type:
                                    stats['stock_data_count'] += 1
                                elif 'news' in data_type:
                                    stats['news_count'] += 1
                                elif 'fundamentals' in data_type:
                                    stats['fundamentals_count'] += 1
                            except Exception:
                                continue

        stats['total_size'] = total_size_bytes  #Bytes
        stats['total_size_mb'] = round(total_size_bytes / (1024 * 1024), 2)  # MB
        return stats

    def get_content_length_config_status(self) -> Dict[str, Any]:
        """Get Content Length Configuration State"""
        available_providers = self._check_provider_availability()
        long_text_providers = self.content_length_config['long_text_providers']
        available_long_providers = [p for p in available_providers if p in long_text_providers]
        
        return {
            'enabled': self.content_length_config['enable_length_check'],
            'max_content_length': self.content_length_config['max_content_length'],
            'max_content_length_formatted': f"{self.content_length_config['max_content_length']:,}字符",
            'long_text_providers': long_text_providers,
            'available_providers': available_providers,
            'available_long_providers': available_long_providers,
            'has_long_text_support': len(available_long_providers) > 0,
            'will_skip_long_content': self.content_length_config['enable_length_check'] and len(available_long_providers) == 0
        }


#Global Cache instance
_cache_instance = None

def get_cache() -> StockDataCache:
    """Fetch global cache instance"""
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = StockDataCache()
    return _cache_instance
