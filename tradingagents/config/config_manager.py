#!/usr/bin/env python3
"""Configure Manager
Manage API keys, model configuration, rate setting, etc.

DepreCATED: This module is obsolete and will be removed after 2026-03-31
Please use the new configuration system: app.services.config service.ConfigService
Migration guide: docs/DEPRECATION NOTICE.md
Moving scripts: scripts/migrate config to db.py
"""

import json
import os
import re
import warnings
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path
from dotenv import load_dotenv

#Send an abandoned warning
warnings.warn(
    "ConfigManager is deprecated and will be removed in version 2.0 (2026-03-31). "
    "Please use app.services.config_service.ConfigService instead. "
    "See docs/DEPRECATION_NOTICE.md for migration guide.",
    DeprecationWarning,
    stacklevel=2
)

#Import Unified Log System
from tradingagents.utils.logging_init import get_logger

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
#Run-time settings: read system time zones
from tradingagents.config.runtime_settings import get_timezone_name
logger = get_logger('agents')

#Import data model (avoiding circular import)
from .usage_models import UsageRecord, ModelConfig, PricingConfig

try:
    from .mongodb_storage import MongoDBStorage
    MONGODB_AVAILABLE = True
except ImportError as e:
    logger.error(f"Could not close temporary folder: %s{e}")
    import traceback
    logger.error(f"Stack:{traceback.format_exc()}")
    MONGODB_AVAILABLE = False
    MongoDBStorage = None
except Exception as e:
    logger.error(f"Could not close temporary folder: %s{e}")
    import traceback
    logger.error(f"Stack:{traceback.format_exc()}")
    MONGODB_AVAILABLE = False
    MongoDBStorage = None


class ConfigManager:
    """Configure Manager"""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)

        self.models_file = self.config_dir / "models.json"
        self.pricing_file = self.config_dir / "pricing.json"
        self.usage_file = self.config_dir / "usage.json"
        self.settings_file = self.config_dir / "settings.json"

        #Loading.env files (maintaining backward compatibility)
        self._load_env_file()

        #Initialize MongoDB storage (if available)
        self.mongodb_storage = None
        self._init_mongodb_storage()

        self._init_default_configs()

    def _load_env_file(self):
        """Loading.env files (maintaining backward compatibility)"""
        #Try loading.env files from the root directory
        project_root = Path(__file__).parent.parent.parent
        env_file = project_root / ".env"

        if env_file.exists():
            #🔧 [rehabilitation] override=False to ensure that environmental variables are prioritized above .env files
            #This way the environment variable in the Docker container will not be covered by the placeholder in the .env file
            logger.info(f"[ConfigManager] Loading.env documents:{env_file}")
            logger.info(f"[ConfigManager] Before loading DASHCOPE API KEY:{'Value' if os.getenv('DASHSCOPE_API_KEY') else 'Empty'}")

            load_dotenv(env_file, override=False)

            logger.info(f"[ConfigManager] Loaded up, DASHCOPE API KEY:{'Value' if os.getenv('DASHSCOPE_API_KEY') else 'Empty'}")

    def _get_env_api_key(self, provider: str) -> str:
        """Fetching API Keys from Environmental Variables"""
        env_key_map = {
            "dashscope": "DASHSCOPE_API_KEY",
            "openai": "OPENAI_API_KEY",
            "google": "GOOGLE_API_KEY",
            "anthropic": "ANTHROPIC_API_KEY",
            "deepseek": "DEEPSEEK_API_KEY"
        }

        env_key = env_key_map.get(provider.lower())
        if env_key:
            api_key = os.getenv(env_key, "")
            #Format validation of OpenAI keys ( always enabled)
            if provider.lower() == "openai" and api_key:
                if not self.validate_openai_api_key_format(api_key):
                    logger.warning(f"OpenAI API key format is incorrect and will be ignored:{api_key[:10]}...")
                    return ""
            return api_key
        return ""
    
    def validate_openai_api_key_format(self, api_key: str) -> bool:
        """Verify OpenAI API key format

        OpenAI API key format rule:
        Start with 'sk-'
        2. The total length is usually 51 words Arguments
        3. Include letters, numbers and possible special words Arguments

        Args:
            api key: API key to verify

        Returns:
            Bool: Is the format correct
        """
        if not api_key or not isinstance(api_key, str):
            return False
        
        #Check to start with 'sk- '
        if not api_key.startswith('sk-'):
            return False
        
        #Check length (openAI keys usually 51 characters)
        if len(api_key) != 51:
            return False
        
        #Check format: sk- should be followed by a combination of 48 characters
        pattern = r'^sk-[A-Za-z0-9]{48}$'
        if not re.match(pattern, api_key):
            return False
        
        return True
    
    def _init_mongodb_storage(self):
        """Initialize MongoDB storage"""
        logger.info("[ConfigManager] Start initializing MongoDB storage...")

        if not MONGODB_AVAILABLE:
            logger.warning("[ConfigManager] pymongo was not installed and could not be stored using MongoDB")
            return

        #Check to enable MongoDB storage
        use_mongodb_env = os.getenv("USE_MONGODB_STORAGE", "false")
        use_mongodb = use_mongodb_env.lower() == "true"

        logger.info(f"🔍 [ConfigManager] USE_MONGODB_STORAGE={use_mongodb_env}(Desert:{use_mongodb})")

        if not use_mongodb:
            logger.info("[ConfigManager] MongoDB storage is not enabled and will be stored using JSON files")
            return

        try:
            connection_string = os.getenv("MONGODB_CONNECTION_STRING")
            database_name = os.getenv("MONGODB_DATABASE_NAME", "tradingagents")

            logger.info(f"🔍 [ConfigManager] MONGODB_CONNECTION_STRING={'Set' if connection_string else 'Not set'}")
            logger.info(f"🔍 [ConfigManager] MONGODB_DATABASE_NAME={database_name}")

            if not connection_string:
                logger.error("[ConfigManager] MONGODB CONNECTION STRIING is not set and cannot initialize MongoDB storage")
                return

            logger.info(f"[ConfigManager] Creating the MongoDBStorage instance...")
            self.mongodb_storage = MongoDBStorage(
                connection_string=connection_string,
                database_name=database_name
            )

            if self.mongodb_storage.is_connected():
                logger.info(f"[ConfigManager] MongoDB storage enabled:{database_name}.token_usage")
            else:
                self.mongodb_storage = None
                logger.warning("[ConfigManager] MongoDB connection failed and will be stored using JSON files")

        except Exception as e:
            logger.error(f"[ConfigManager] MongoDB's initialization failed:{e}", exc_info=True)
            self.mongodb_storage = None

    def _init_default_configs(self):
        """Initialise Default Configuration"""
        #Default Model Configuration
        if not self.models_file.exists():
            default_models = [
                ModelConfig(
                    provider="dashscope",
                    model_name="qwen-turbo",
                    api_key="",
                    max_tokens=4000,
                    temperature=0.7
                ),
                ModelConfig(
                    provider="dashscope",
                    model_name="qwen-plus-latest",
                    api_key="",
                    max_tokens=8000,
                    temperature=0.7
                ),
                ModelConfig(
                    provider="openai",
                    model_name="gpt-3.5-turbo",
                    api_key="",
                    max_tokens=4000,
                    temperature=0.7,
                    enabled=False
                ),
                ModelConfig(
                    provider="openai",
                    model_name="gpt-4",
                    api_key="",
                    max_tokens=8000,
                    temperature=0.7,
                    enabled=False
                ),
                ModelConfig(
                    provider="google",
                    model_name="gemini-2.5-pro",
                    api_key="",
                    max_tokens=4000,
                    temperature=0.7,
                    enabled=False
                ),
                ModelConfig(
                    provider="deepseek",
                    model_name="deepseek-chat",
                    api_key="",
                    max_tokens=8000,
                    temperature=0.7,
                    enabled=False
                )
            ]
            self.save_models(default_models)
        
        #Default pricing configuration
        if not self.pricing_file.exists():
            default_pricing = [
                #Alibri pricing (RMB)
                PricingConfig("dashscope", "qwen-turbo", 0.002, 0.006, "CNY"),
                PricingConfig("dashscope", "qwen-plus-latest", 0.004, 0.012, "CNY"),
                PricingConfig("dashscope", "qwen-max", 0.02, 0.06, "CNY"),

                #DeepSeek Pricing (RMB)
                PricingConfig("deepseek", "deepseek-chat", 0.0014, 0.0028, "CNY"),
                PricingConfig("deepseek", "deepseek-coder", 0.0014, 0.0028, "CNY"),

                #OpenAI pricing (United States dollars)
                PricingConfig("openai", "gpt-3.5-turbo", 0.0015, 0.002, "USD"),
                PricingConfig("openai", "gpt-4", 0.03, 0.06, "USD"),
                PricingConfig("openai", "gpt-4-turbo", 0.01, 0.03, "USD"),

                #Google pricing (United States dollars)
                PricingConfig("google", "gemini-2.5-pro", 0.00025, 0.0005, "USD"),
                PricingConfig("google", "gemini-2.5-flash", 0.00025, 0.0005, "USD"),
                PricingConfig("google", "gemini-2.0-flash", 0.00025, 0.0005, "USD"),
                PricingConfig("google", "gemini-1.5-pro", 0.00025, 0.0005, "USD"),
                PricingConfig("google", "gemini-1.5-flash", 0.00025, 0.0005, "USD"),
                PricingConfig("google", "gemini-2.5-flash-lite-preview-06-17", 0.00025, 0.0005, "USD"),
                PricingConfig("google", "gemini-pro", 0.00025, 0.0005, "USD"),
                PricingConfig("google", "gemini-pro-vision", 0.00025, 0.0005, "USD"),
            ]
            self.save_pricing(default_pricing)
        
        #Default Settings
        if not self.settings_file.exists():
            #Import Default Data Directory Configuration
            import os
            default_data_dir = os.path.join(os.path.expanduser("~"), "Documents", "TradingAgents", "data")
            
            default_settings = {
                "default_provider": "dashscope",
                "default_model": "qwen-turbo",
                "enable_cost_tracking": True,
                "cost_alert_threshold": 100.0,  #Cost warning threshold
                "currency_preference": "CNY",
                "auto_save_usage": True,
                "max_usage_records": 10000,
                "data_dir": default_data_dir,  #Data Directory Configuration
                "cache_dir": os.path.join(default_data_dir, "cache"),  #Cache Directory
                "results_dir": os.path.join(os.path.expanduser("~"), "Documents", "TradingAgents", "results"),  #Results Directory
                "auto_create_dirs": True,  #Autocreate Directory
                "openai_enabled": False,  #Enable OpenAI models
            }
            self.save_settings(default_settings)
    
    def load_models(self) -> List[ModelConfig]:
        """Load model configuration, preferentially using the API key in.env"""
        try:
            with open(self.models_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                models = [ModelConfig(**item) for item in data]

                #Get Settings
                settings = self.load_settings()
                openai_enabled = settings.get("openai_enabled", False)

                #API Key in Consolidation.env (higher priority)
                for model in models:
                    env_api_key = self._get_env_api_key(model.provider)
                    if env_api_key:
                        model.api_key = env_api_key
                        #If there's an API key in .env, automatically activate the model
                        if not model.enabled:
                            model.enabled = True
                    
                    #Special processing OpenAI model
                    if model.provider.lower() == "openai":
                        #Check if OpenAI is enabled in configuration
                        if not openai_enabled:
                            model.enabled = False
                            logger.info(f"The OpenAI model has been disabled:{model.model_name}")
                        #Disable model if there is an API key but the format is incorrect (validation always enabled)
                        elif model.api_key and not self.validate_openai_api_key_format(model.api_key):
                            model.enabled = False
                            logger.warning(f"The OpenAI model is disabled because the key format is incorrect:{model.model_name}")

                return models
        except Exception as e:
            logger.error(f"Loading model configuration failed:{e}")
            return []
    
    def save_models(self, models: List[ModelConfig]):
        """Save Model Configuration"""
        try:
            data = [asdict(model) for model in models]
            with open(self.models_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Saving model configuration failed:{e}")
    
    def load_pricing(self) -> List[PricingConfig]:
        """Load pricing configuration"""
        try:
            with open(self.pricing_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return [PricingConfig(**item) for item in data]
        except Exception as e:
            logger.error(f"Load pricing configuration failed:{e}")
            return []
    
    def save_pricing(self, pricing: List[PricingConfig]):
        """Save pricing configuration"""
        try:
            data = [asdict(price) for price in pricing]
            with open(self.pricing_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Save pricing configuration failed:{e}")
    
    def load_usage_records(self) -> List[UsageRecord]:
        """Load Usage Record"""
        try:
            if not self.usage_file.exists():
                return []
            with open(self.usage_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return [UsageRecord(**item) for item in data]
        except Exception as e:
            logger.error(f"Cannot initialise Evolution's mail component.{e}")
            return []
    
    def save_usage_records(self, records: List[UsageRecord]):
        """Keep Usage Record"""
        try:
            data = [asdict(record) for record in records]
            with open(self.usage_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Failed to save usage record:{e}")
    
    def add_usage_record(self, provider: str, model_name: str, input_tokens: int,
                        output_tokens: int, session_id: str, analysis_type: str = "stock_analysis"):
        """Add Usage Record"""
        #Costing and currency units
        cost, currency = self.calculate_cost(provider, model_name, input_tokens, output_tokens)

        record = UsageRecord(
            timestamp=datetime.now(ZoneInfo(get_timezone_name())).isoformat(),
            provider=provider,
            model_name=model_name,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost=cost,
            currency=currency,
            session_id=session_id,
            analysis_type=analysis_type
        )

        #Detailed log: record location
        logger.info(f"[Token Records]{provider}/{model_name},Input={input_tokens},out ={output_tokens}, Cost ={cost:.4f}, session={session_id}")

        #Prefer MongoDB storage
        if self.mongodb_storage and self.mongodb_storage.is_connected():
            logger.info(f"[Token Record]{self.mongodb_storage.database_name}, set up:{self.mongodb_storage.collection_name})")
            success = self.mongodb_storage.save_usage_record(record)
            if success:
                logger.info(f"[Token Record] MongoDB saved successfully:{provider}/{model_name}")
                return record
            else:
                logger.error(f"[Token Record] MongoDB failed to save, back to JSON file storage")
        else:
            #Detailed log: Why MongoDB not used
            if self.mongodb_storage is None:
                logger.warning(f"[Token Record] MongoDB storage is not initialized (mongodb storage=None)")
                logger.warning(f"Please check the environment variable: USE MONGODB STORAGE={os.getenv('USE_MONGODB_STORAGE', 'Not set')}")
            elif not self.mongodb_storage.is_connected():
                logger.warning(f"[Token Record] MongoDB is not connected.")

            logger.info(f"[Token Recording]{self.usage_file}")

        #Back to JSON File Storage
        records = self.load_usage_records()
        records.append(record)

        #Limit the number of records
        settings = self.load_settings()
        max_records = settings.get("max_usage_records", 10000)
        if len(records) > max_records:
            records = records[-max_records:]

        self.save_usage_records(records)
        logger.info(f"[Token Record] JSON file saved successfully:{self.usage_file}")
        return record
    
    def calculate_cost(self, provider: str, model_name: str, input_tokens: int, output_tokens: int) -> tuple[float, str]:
        """Calculation of usage cost

        Returns:
            tuple [float, st]: (cost, currency unit)
        """
        pricing_configs = self.load_pricing()

        for pricing in pricing_configs:
            if pricing.provider == provider and pricing.model_name == model_name:
                input_cost = (input_tokens / 1000) * pricing.input_price_per_1k
                output_cost = (output_tokens / 1000) * pricing.output_price_per_1k
                total_cost = input_cost + output_cost
                return round(total_cost, 6), pricing.currency

        #Only output debug information when configuration is not found
        logger.warning(f"[calculate cost] No matching pricing configuration found:{provider}/{model_name}")
        logger.debug(f"[calculate cost]")
        for pricing in pricing_configs:
            logger.debug(f"⚠️ [calculate_cost]   - {pricing.provider}/{pricing.model_name}")

        return 0.0, "CNY"
    
    def load_settings(self) -> Dict[str, Any]:
        """Load settings, merge configurations in.env"""
        try:
            if self.settings_file.exists():
                with open(self.settings_file, 'r', encoding='utf-8') as f:
                    settings = json.load(f)
            else:
                #Create default settings if settings file does not exist
                settings = {
                    "default_provider": "dashscope",
                    "default_model": "qwen-turbo",
                    "enable_cost_tracking": True,
                    "cost_alert_threshold": 100.0,
                    "currency_preference": "CNY",
                    "auto_save_usage": True,
                    "max_usage_records": 10000,
                    "data_dir": os.path.join(os.path.expanduser("~"), "Documents", "TradingAgents", "data"),
                    "cache_dir": os.path.join(os.path.expanduser("~"), "Documents", "TradingAgents", "data", "cache"),
                    "results_dir": os.path.join(os.path.expanduser("~"), "Documents", "TradingAgents", "results"),
                    "auto_create_dirs": True,
                    "openai_enabled": False,
                }
                self.save_settings(settings)
        except Exception as e:
            logger.error(f"Loading settings failed:{e}")
            settings = {}

        #Merge other configurations in.env
        env_settings = {
            "finnhub_api_key": os.getenv("FINNHUB_API_KEY", ""),
            "reddit_client_id": os.getenv("REDDIT_CLIENT_ID", ""),
            "reddit_client_secret": os.getenv("REDDIT_CLIENT_SECRET", ""),
            "reddit_user_agent": os.getenv("REDDIT_USER_AGENT", ""),
            "results_dir": os.getenv("TRADINGAGENTS_RESULTS_DIR", ""),
            "log_level": os.getenv("TRADINGAGENTS_LOG_LEVEL", "INFO"),
            "data_dir": os.getenv("TRADINGAGENTS_DATA_DIR", ""),  #Data Directory Environment Variable
            "cache_dir": os.getenv("TRADINGAGENTS_CACHE_DIR", ""),  #Cache directory environment variable
        }

        #Add OpenAI-related configuration
        openai_enabled_env = os.getenv("OPENAI_ENABLED", "").lower()
        if openai_enabled_env in ["true", "false"]:
            env_settings["openai_enabled"] = openai_enabled_env == "true"

        #Overwrite only when environment variables exist and are not empty
        for key, value in env_settings.items():
            #For booleans, use directly
            if isinstance(value, bool):
                settings[key] = value
            #For strings, only cover non-empty hours
            elif value != "" and value is not None:
                settings[key] = value

        return settings

    def get_env_config_status(self) -> Dict[str, Any]:
        """Get.env configuration status"""
        return {
            "env_file_exists": (Path(__file__).parent.parent.parent / ".env").exists(),
            "api_keys": {
                "dashscope": bool(os.getenv("DASHSCOPE_API_KEY")),
                "openai": bool(os.getenv("OPENAI_API_KEY")),
                "google": bool(os.getenv("GOOGLE_API_KEY")),
                "anthropic": bool(os.getenv("ANTHROPIC_API_KEY")),
                "finnhub": bool(os.getenv("FINNHUB_API_KEY")),
            },
            "other_configs": {
                "reddit_configured": bool(os.getenv("REDDIT_CLIENT_ID") and os.getenv("REDDIT_CLIENT_SECRET")),
                "results_dir": os.getenv("TRADINGAGENTS_RESULTS_DIR", "./results"),
                "log_level": os.getenv("TRADINGAGENTS_LOG_LEVEL", "INFO"),
            }
        }

    def save_settings(self, settings: Dict[str, Any]):
        """Save Settings"""
        try:
            with open(self.settings_file, 'w', encoding='utf-8') as f:
                json.dump(settings, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Save settings failed:{e}")
    
    def get_enabled_models(self) -> List[ModelConfig]:
        """Fetch enabled models"""
        models = self.load_models()
        return [model for model in models if model.enabled and model.api_key]
    
    def get_model_by_name(self, provider: str, model_name: str) -> Optional[ModelConfig]:
        """Get model configuration by name"""
        models = self.load_models()
        for model in models:
            if model.provider == provider and model.model_name == model_name:
                return model
        return None
    
    def get_usage_statistics(self, days: int = 30) -> Dict[str, Any]:
        """Access to usage statistics"""
        #Prioritize MongoDB access to statistics
        if self.mongodb_storage and self.mongodb_storage.is_connected():
            try:
                #Access to basic statistics from MongoDB
                stats = self.mongodb_storage.get_usage_statistics(days)
                #Access to vendor statistics
                provider_stats = self.mongodb_storage.get_provider_statistics(days)
                
                if stats:
                    stats["provider_stats"] = provider_stats
                    stats["records_count"] = stats.get("total_requests", 0)
                    return stats
            except Exception as e:
                logger.error(f"MongoDB statistical access failed.{e}")
        
        #Back to JSON file count
        records = self.load_usage_records()
        
        #Filter Recent N Day Records
        from datetime import datetime, timedelta

        cutoff_date = datetime.now() - timedelta(days=days)
        
        recent_records = []
        for record in records:
            try:
                record_date = datetime.fromisoformat(record.timestamp)
                if record_date >= cutoff_date:
                    recent_records.append(record)
            except:
                continue
        
        #Statistics
        total_cost = sum(record.cost for record in recent_records)
        total_input_tokens = sum(record.input_tokens for record in recent_records)
        total_output_tokens = sum(record.output_tokens for record in recent_records)
        
        #By supplier
        provider_stats = {}
        for record in recent_records:
            if record.provider not in provider_stats:
                provider_stats[record.provider] = {
                    "cost": 0,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "requests": 0
                }
            provider_stats[record.provider]["cost"] += record.cost
            provider_stats[record.provider]["input_tokens"] += record.input_tokens
            provider_stats[record.provider]["output_tokens"] += record.output_tokens
            provider_stats[record.provider]["requests"] += 1
        
        return {
            "period_days": days,
            "total_cost": round(total_cost, 4),
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "total_requests": len(recent_records),
            "provider_stats": provider_stats,
            "records_count": len(recent_records)
        }
    
    def get_data_dir(self) -> str:
        """Access to Data Directory Path"""
        settings = self.load_settings()
        data_dir = settings.get("data_dir")
        if not data_dir:
            #Use default path if not configured
            data_dir = os.path.join(os.path.expanduser("~"), "Documents", "TradingAgents", "data")
        return data_dir

    def set_data_dir(self, data_dir: str):
        """Set Data Directory Path"""
        settings = self.load_settings()
        settings["data_dir"] = data_dir
        #Update cache directory also
        settings["cache_dir"] = os.path.join(data_dir, "cache")
        self.save_settings(settings)
        
        #Create directory if autocreate
        if settings.get("auto_create_dirs", True):
            self.ensure_directories_exist()

    def ensure_directories_exist(self):
        """Ensure that necessary directories exist"""
        settings = self.load_settings()
        
        directories = [
            settings.get("data_dir"),
            settings.get("cache_dir"),
            settings.get("results_dir"),
            os.path.join(settings.get("data_dir", ""), "finnhub_data"),
            os.path.join(settings.get("data_dir", ""), "finnhub_data", "news_data"),
            os.path.join(settings.get("data_dir", ""), "finnhub_data", "insider_sentiment"),
            os.path.join(settings.get("data_dir", ""), "finnhub_data", "insider_transactions")
        ]
        
        for directory in directories:
            if directory and not os.path.exists(directory):
                try:
                    os.makedirs(directory, exist_ok=True)
                    logger.info(f"Create directory:{directory}")
                except Exception as e:
                    logger.error(f"Failed to create directory{directory}: {e}")
    
    def set_openai_enabled(self, enabled: bool):
        """Set OpenAI model enabled"""
        settings = self.load_settings()
        settings["openai_enabled"] = enabled
        self.save_settings(settings)
        logger.info(f"🔧The OpenAI model has been set to:{enabled}")
    
    def is_openai_enabled(self) -> bool:
        """Check whether OpenAI models are enabled"""
        settings = self.load_settings()
        return settings.get("openai_enabled", False)
    
    def get_openai_config_status(self) -> Dict[str, Any]:
        """Get OpenAI configuration status"""
        openai_key = os.getenv("OPENAI_API_KEY", "")
        key_valid = self.validate_openai_api_key_format(openai_key) if openai_key else False
        
        return {
            "api_key_present": bool(openai_key),
            "api_key_valid_format": key_valid,
            "enabled": self.is_openai_enabled(),
            "models_available": self.is_openai_enabled() and key_valid,
            "api_key_preview": f"{openai_key[:10]}..." if openai_key else "未配置"
        }


class TokenTracker:
    """Token uses a tracker"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager

    def track_usage(self, provider: str, model_name: str, input_tokens: int,
                   output_tokens: int, session_id: str = None, analysis_type: str = "stock_analysis"):
        """Track Token for use"""
        if session_id is None:
            session_id = f"session_{datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y%m%d_%H%M%S')}"

        #Check if cost tracking is enabled
        settings = self.config_manager.load_settings()
        cost_tracking_enabled = settings.get("enable_cost_tracking", True)

        if not cost_tracking_enabled:
            return None

        #Add Usage Record
        record = self.config_manager.add_usage_record(
            provider=provider,
            model_name=model_name,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            session_id=session_id,
            analysis_type=analysis_type
        )

        #Check Cost Warning
        if record:
            self._check_cost_alert(record.cost)

        return record

    def _check_cost_alert(self, current_cost: float):
        """Check Cost Warning"""
        settings = self.config_manager.load_settings()
        threshold = settings.get("cost_alert_threshold", 100.0)

        #Get total cost today
        today_stats = self.config_manager.get_usage_statistics(1)
        total_today = today_stats["total_cost"]

        if total_today >= threshold:
            logger.warning(f"Cost warning: costs have reached today{total_today:.4f}, above the threshold{threshold}",
                          extra={'cost': total_today, 'threshold': threshold, 'event_type': 'cost_alert'})

    def get_session_cost(self, session_id: str) -> float:
        """Get Session Costs"""
        records = self.config_manager.load_usage_records()
        session_cost = sum(record.cost for record in records if record.session_id == session_id)
        return session_cost

    def estimate_cost(self, provider: str, model_name: str, estimated_input_tokens: int,
                     estimated_output_tokens: int) -> tuple[float, str]:
        """Estimated costs

        Returns:
            tuple [float, st]: (cost, currency unit)
        """
        return self.config_manager.calculate_cost(
            provider, model_name, estimated_input_tokens, estimated_output_tokens
        )




#Global Profile Manager Example - Configure using the root directory
def _get_project_config_dir():
    """Can not open message"""
    #Infer root directory from current file location
    current_file = Path(__file__)  # tradingagents/config/config_manager.py
    project_root = current_file.parent.parent.parent  #Level 3 up to the root directory
    return str(project_root / "config")

config_manager = ConfigManager(_get_project_config_dir())
token_tracker = TokenTracker(config_manager)
