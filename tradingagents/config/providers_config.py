"""Data source provider configuration management

Moving from TradingAGents/dataflows/providers config.py
Harmonized management of all data source provider configurations
"""
import os
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class DataSourceConfig:
    """Data Source Configuration Manager"""
    
    def __init__(self):
        self._configs = {}
        self._load_configs()
    
    def _load_configs(self):
        """Load all data sources configurations"""
        #Tushare Configuration
        self._configs["tushare"] = {
            "enabled": self._get_bool_env("TUSHARE_ENABLED", True),
            "token": os.getenv("TUSHARE_TOKEN", ""),
            "timeout": self._get_int_env("TUSHARE_TIMEOUT", 30),
            "rate_limit": self._get_float_env("TUSHARE_RATE_LIMIT", 0.1),
            "max_retries": self._get_int_env("TUSHARE_MAX_RETRIES", 3),
            "cache_enabled": self._get_bool_env("TUSHARE_CACHE_ENABLED", True),
            "cache_ttl": self._get_int_env("TUSHARE_CACHE_TTL", 3600),
        }
        
        #AKShare Configuration
        self._configs["akshare"] = {
            "enabled": self._get_bool_env("AKSHARE_ENABLED", True),
            "timeout": self._get_int_env("AKSHARE_TIMEOUT", 30),
            "rate_limit": self._get_float_env("AKSHARE_RATE_LIMIT", 0.2),
            "max_retries": self._get_int_env("AKSHARE_MAX_RETRIES", 3),
            "cache_enabled": self._get_bool_env("AKSHARE_CACHE_ENABLED", True),
            "cache_ttl": self._get_int_env("AKSHARE_CACHE_TTL", 1800),
        }
        
        #BaoStock Configuration
        self._configs["baostock"] = {
            "enabled": self._get_bool_env("BAOSTOCK_ENABLED", True),
            "timeout": self._get_int_env("BAOSTOCK_TIMEOUT", 30),
            "rate_limit": self._get_float_env("BAOSTOCK_RATE_LIMIT", 0.1),
            "max_retries": self._get_int_env("BAOSTOCK_MAX_RETRIES", 3),
            "cache_enabled": self._get_bool_env("BAOSTOCK_CACHE_ENABLED", True),
            "cache_ttl": self._get_int_env("BAOSTOCK_CACHE_TTL", 1800),
        }
        
        #Yahoo Finance Configuration
        self._configs["yahoo"] = {
            "enabled": self._get_bool_env("YAHOO_ENABLED", False),
            "timeout": self._get_int_env("YAHOO_TIMEOUT", 30),
            "rate_limit": self._get_float_env("YAHOO_RATE_LIMIT", 0.5),
            "max_retries": self._get_int_env("YAHOO_MAX_RETRIES", 3),
            "cache_enabled": self._get_bool_env("YAHOO_CACHE_ENABLED", True),
            "cache_ttl": self._get_int_env("YAHOO_CACHE_TTL", 300),
        }
        
        #Finnhub Configuration
        self._configs["finnhub"] = {
            "enabled": self._get_bool_env("FINNHUB_ENABLED", False),
            "api_key": os.getenv("FINNHUB_API_KEY", ""),
            "timeout": self._get_int_env("FINNHUB_TIMEOUT", 30),
            "rate_limit": self._get_float_env("FINNHUB_RATE_LIMIT", 1.0),
            "max_retries": self._get_int_env("FINNHUB_MAX_RETRIES", 3),
            "cache_enabled": self._get_bool_env("FINNHUB_CACHE_ENABLED", True),
            "cache_ttl": self._get_int_env("FINNHUB_CACHE_TTL", 300),
        }
        
        #Connect Configuration - Removed
        #TDX data source is no longer supported
        # self._configs["tdx"] = {
        #     "enabled": False,
        # }

        logger.debug("Data source configuration loaded")
    
    def get_provider_config(self, provider_name: str) -> Dict[str, Any]:
        """Get the configuration of the specified provider

Args:
program name: Provider name

Returns:
Configure Dictionary
"""
        config = self._configs.get(provider_name.lower(), {})
        if not config:
            logger.warning(f"Not found{provider_name}Configure")
        return config
    
    def is_provider_enabled(self, provider_name: str) -> bool:
        """Check if the provider is enabled"""
        config = self.get_provider_config(provider_name)
        return config.get("enabled", False)
    
    def get_all_enabled_providers(self) -> list:
        """Fetch all enabled provider names"""
        enabled = []
        for name, config in self._configs.items():
            if config.get("enabled", False):
                enabled.append(name)
        return enabled
    
    def _get_bool_env(self, key: str, default: bool) -> bool:
        """Get Boolean Environment Variables"""
        value = os.getenv(key, str(default)).lower()
        return value in ("true", "1", "yes", "on")
    
    def _get_int_env(self, key: str, default: int) -> int:
        """Get a whole environment variable"""
        try:
            return int(os.getenv(key, str(default)))
        except ValueError:
            return default
    
    def _get_float_env(self, key: str, default: float) -> float:
        """Fetch floating point environment variables"""
        try:
            return float(os.getenv(key, str(default)))
        except ValueError:
            return default


#Global Configuration Example
_config_instance = None

def get_data_source_config() -> DataSourceConfig:
    """Get global data source configuration examples"""
    global _config_instance
    if _config_instance is None:
        _config_instance = DataSourceConfig()
    return _config_instance

def get_provider_config(provider_name: str) -> Dict[str, Any]:
    """A simple function to get specified provider configuration"""
    config = get_data_source_config()
    return config.get_provider_config(provider_name)

