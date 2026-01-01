"""Integrated configuration management system
Integration of configuration management for config/, tradencies/config/ and webapi
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import asyncio
from dataclasses import dataclass, asdict

from app.models.config import (
    LLMConfig, DataSourceConfig, DatabaseConfig, SystemConfig,
    ModelProvider, DataSourceType, DatabaseType
)


@dataclass
class ConfigPaths:
    """Profile Path"""
    root_config_dir: Path = Path("config")
    tradingagents_config_dir: Path = Path("tradingagents/config")
    webapi_config_dir: Path = Path("data/config")
    
    #Specific Profiles
    models_json: Path = root_config_dir / "models.json"
    settings_json: Path = root_config_dir / "settings.json"
    pricing_json: Path = root_config_dir / "pricing.json"
    verified_models_json: Path = root_config_dir / "verified_models.json"


class UnifiedConfigManager:
    """Unified Configuration Manager"""
    
    def __init__(self):
        self.paths = ConfigPaths()
        self._cache = {}
        self._last_modified = {}
        
    def _get_file_mtime(self, file_path: Path) -> float:
        """Get file change time"""
        try:
            return file_path.stat().st_mtime
        except FileNotFoundError:
            return 0.0
    
    def _is_cache_valid(self, cache_key: str, file_path: Path) -> bool:
        """Check if the cache is valid"""
        if cache_key not in self._cache:
            return False
        
        current_mtime = self._get_file_mtime(file_path)
        cached_mtime = self._last_modified.get(cache_key, 0)
        
        return current_mtime <= cached_mtime
    
    def _load_json_file(self, file_path: Path, cache_key: str = None) -> Dict[str, Any]:
        """Load JSON files to support caches"""
        if cache_key and self._is_cache_valid(cache_key, file_path):
            return self._cache[cache_key]
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if cache_key:
                self._cache[cache_key] = data
                self._last_modified[cache_key] = self._get_file_mtime(file_path)
            
            return data
        except FileNotFoundError:
            return {}
        except json.JSONDecodeError as e:
            print(f"配置文件格式错误 {file_path}: {e}")
            return {}
    
    def _save_json_file(self, file_path: Path, data: Dict[str, Any], cache_key: str = None):
        """Save JSON files"""
        #Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        if cache_key:
            self._cache[cache_key] = data
            self._last_modified[cache_key] = self._get_file_mtime(file_path)
    
    #== sync, corrected by elderman == @elder man
    
    def get_legacy_models(self) -> List[Dict[str, Any]]:
        """Get a model configuration for the traditional format"""
        return self._load_json_file(self.paths.models_json, "models")
    
    def get_llm_configs(self) -> List[LLMConfig]:
        """Get standardized LLM configuration"""
        legacy_models = self.get_legacy_models()
        llm_configs = []

        for model in legacy_models:
            try:
                #Directly use the provider string and no longer map to the count
                provider = model.get("provider", "openai")

                #Option A: Sensitive keys are not loaded from the document, and environmental variables/manufacturer catalogues are unified
                llm_config = LLMConfig(
                    provider=provider,
                    model_name=model.get("model_name", ""),
                    api_key="",
                    api_base=model.get("base_url"),
                    max_tokens=model.get("max_tokens", 4000),
                    temperature=model.get("temperature", 0.7),
                    enabled=model.get("enabled", True),
                    description=f"{model.get('provider', '')} {model.get('model_name', '')}"
                )
                llm_configs.append(llm_config)
            except Exception as e:
                print(f"转换模型配置失败: {model}, 错误: {e}")
                continue

        return llm_configs
    
    def save_llm_config(self, llm_config: LLMConfig) -> bool:
        """Save LLM configuration to traditional format"""
        try:
            legacy_models = self.get_legacy_models()

            #Directly using provider string, no more mapping
            #Option A: Do not write key when saving to file
            legacy_model = {
                "provider": llm_config.provider,
                "model_name": llm_config.model_name,
                "api_key": "",
                "base_url": llm_config.api_base,
                "max_tokens": llm_config.max_tokens,
                "temperature": llm_config.temperature,
                "enabled": llm_config.enabled
            }
            
            #Find and update the current configuration or add a new configuration
            updated = False
            for i, model in enumerate(legacy_models):
                if (model.get("provider") == legacy_model["provider"] and 
                    model.get("model_name") == legacy_model["model_name"]):
                    legacy_models[i] = legacy_model
                    updated = True
                    break
            
            if not updated:
                legacy_models.append(legacy_model)
            
            self._save_json_file(self.paths.models_json, legacy_models, "models")
            return True
            
        except Exception as e:
            print(f"保存LLM配置失败: {e}")
            return False
    
    #== sync, corrected by elderman == @elder man
    
    def get_system_settings(self) -> Dict[str, Any]:
        """Get System Settings"""
        return self._load_json_file(self.paths.settings_json, "settings")
    
    def save_system_settings(self, settings: Dict[str, Any]) -> bool:
        """Save System Settings (retain existing fields and add new field maps)"""
        try:
            print(f"📝 [unified_config] save_system_settings 被调用")
            print(f"📝 [unified_config] 接收到的 settings 包含 {len(settings)} 项")

            #Check key fields
            if "quick_analysis_model" in settings:
                print(f"  ✓ [unified_config] 包含 quick_analysis_model: {settings['quick_analysis_model']}")
            else:
                print(f"  ⚠️  [unified_config] 不包含 quick_analysis_model")

            if "deep_analysis_model" in settings:
                print(f"  ✓ [unified_config] 包含 deep_analysis_model: {settings['deep_analysis_model']}")
            else:
                print(f"  ⚠️  [unified_config] 不包含 deep_analysis_model")

            #Read existing configuration
            print(f"📖 [unified_config] 读取现有配置文件: {self.paths.settings_json}")
            current_settings = self.get_system_settings()
            print(f"📖 [unified_config] 现有配置包含 {len(current_settings)} 项")

            #Merge Configuration (new Configuration Overrides Old Configuration)
            merged_settings = current_settings.copy()
            merged_settings.update(settings)
            print(f"🔀 [unified_config] 合并后配置包含 {len(merged_settings)} 项")

            #Add field name map (new -> old field name)
            if "quick_analysis_model" in settings:
                merged_settings["quick_think_llm"] = settings["quick_analysis_model"]
                print(f"  ✓ [unified_config] 映射 quick_analysis_model -> quick_think_llm: {settings['quick_analysis_model']}")

            if "deep_analysis_model" in settings:
                merged_settings["deep_think_llm"] = settings["deep_analysis_model"]
                print(f"  ✓ [unified_config] 映射 deep_analysis_model -> deep_think_llm: {settings['deep_analysis_model']}")

            #Print configuration ultimately saved
            print(f"💾 [unified_config] 即将保存到文件:")
            if "quick_think_llm" in merged_settings:
                print(f"  ✓ quick_think_llm: {merged_settings['quick_think_llm']}")
            if "deep_think_llm" in merged_settings:
                print(f"  ✓ deep_think_llm: {merged_settings['deep_think_llm']}")
            if "quick_analysis_model" in merged_settings:
                print(f"  ✓ quick_analysis_model: {merged_settings['quick_analysis_model']}")
            if "deep_analysis_model" in merged_settings:
                print(f"  ✓ deep_analysis_model: {merged_settings['deep_analysis_model']}")

            #Save merged configuration
            print(f"💾 [unified_config] 保存到文件: {self.paths.settings_json}")
            self._save_json_file(self.paths.settings_json, merged_settings, "settings")
            print(f"✅ [unified_config] 配置保存成功")

            return True
        except Exception as e:
            print(f"❌ [unified_config] 保存系统设置失败: {e}")
            import traceback
            print(traceback.format_exc())
            return False
    
    def get_default_model(self) -> str:
        """Fetch default model (reverse compatible)"""
        settings = self.get_system_settings()
        #Prioritize the return of rapid analysis models and maintain backward compatibility
        return settings.get("quick_analysis_model", settings.get("default_model", "qwen-turbo"))

    def set_default_model(self, model_name: str) -> bool:
        """Set default model (reverse compatible)"""
        settings = self.get_system_settings()
        settings["quick_analysis_model"] = model_name
        return self.save_system_settings(settings)

    def get_quick_analysis_model(self) -> str:
        """Access rapid analysis models"""
        settings = self.get_system_settings()
        #Read new field names first and old field names if they do not exist (reverse compatibility)
        return settings.get("quick_analysis_model") or settings.get("quick_think_llm", "qwen-turbo")

    def get_deep_analysis_model(self) -> str:
        """Get Depth Analysis Model"""
        settings = self.get_system_settings()
        #Read new field names first and old field names if they do not exist (reverse compatibility)
        return settings.get("deep_analysis_model") or settings.get("deep_think_llm", "qwen-max")

    def set_analysis_models(self, quick_model: str, deep_model: str) -> bool:
        """Set Analytic Model"""
        settings = self.get_system_settings()
        settings["quick_analysis_model"] = quick_model
        settings["deep_analysis_model"] = deep_model
        return self.save_system_settings(settings)
    
    #== sync, corrected by elderman == @elder man
    
    def get_data_source_configs(self) -> List[DataSourceConfig]:
        """Fetching Data Source Configuration - Prioritize Reading from Database, Back to Hard Encoding (Sync Version)"""
        try:
            #🔥 Prioritize reading configurations from databases (using synchronous connections)
            from app.core.database import get_mongo_db_synchronous
            db = get_mongo_db_synchronous()
            config_collection = db.system_configs

            #Get the latest active configuration
            config_data = config_collection.find_one(
                {"is_active": True},
                sort=[("version", -1)]
            )

            if config_data and config_data.get('data_source_configs'):
                #Read from database to configuration
                data_source_configs = config_data.get('data_source_configs', [])
                print(f"✅ [unified_config] 从数据库读取到 {len(data_source_configs)} 个数据源配置")

                #Convert to DatasourceConfig Object
                result = []
                for ds_config in data_source_configs:
                    try:
                        result.append(DataSourceConfig(**ds_config))
                    except Exception as e:
                        print(f"⚠️ [unified_config] 解析数据源配置失败: {e}, 配置: {ds_config}")
                        continue

                #Sort by priority (the larger the number, the higher the priority)
                result.sort(key=lambda x: x.priority, reverse=True)
                return result
            else:
                print("⚠️ [unified_config] 数据库中没有数据源配置，使用硬编码配置")
        except Exception as e:
            print(f"⚠️ [unified_config] 从数据库读取数据源配置失败: {e}，使用硬编码配置")

        #Back to hard code configuration (compatibility)
        settings = self.get_system_settings()
        data_sources = []

        #AKShare (default enabled)
        akshare_config = DataSourceConfig(
            name="AKShare",
            type=DataSourceType.AKSHARE,
            endpoint="https://akshare.akfamily.xyz",
            enabled=True,
            priority=1,
            description="AKShare开源金融数据接口"
        )
        data_sources.append(akshare_config)

        #Tushare (if configured)
        if settings.get("tushare_token"):
            tushare_config = DataSourceConfig(
                name="Tushare",
                type=DataSourceType.TUSHARE,
                api_key=settings.get("tushare_token"),
                endpoint="http://api.tushare.pro",
                enabled=True,
                priority=2,
                description="Tushare专业金融数据接口"
            )
            data_sources.append(tushare_config)

        #Sort by priority
        data_sources.sort(key=lambda x: x.priority, reverse=True)
        return data_sources

    async def get_data_source_configs_async(self) -> List[DataSourceConfig]:
        """Fetching Data Source Configuration - Prioritize Reading from Databases, Back to Hard Encoding (Step Version)"""
        try:
            #🔥 Read configuration from the database as a matter of priority (using a walk-in connection)
            from app.core.database import get_mongo_db
            db = get_mongo_db()
            config_collection = db.system_configs

            #Get the latest active configuration
            config_data = await config_collection.find_one(
                {"is_active": True},
                sort=[("version", -1)]
            )

            if config_data and config_data.get('data_source_configs'):
                #Read from database to configuration
                data_source_configs = config_data.get('data_source_configs', [])
                print(f"✅ [unified_config] 从数据库读取到 {len(data_source_configs)} 个数据源配置")

                #Convert to DatasourceConfig Object
                result = []
                for ds_config in data_source_configs:
                    try:
                        result.append(DataSourceConfig(**ds_config))
                    except Exception as e:
                        print(f"⚠️ [unified_config] 解析数据源配置失败: {e}, 配置: {ds_config}")
                        continue

                #Sort by priority (the larger the number, the higher the priority)
                result.sort(key=lambda x: x.priority, reverse=True)
                return result
            else:
                print("⚠️ [unified_config] 数据库中没有数据源配置，使用硬编码配置")
        except Exception as e:
            print(f"⚠️ [unified_config] 从数据库读取数据源配置失败: {e}，使用硬编码配置")

        #Back to hard code configuration (compatibility)
        settings = self.get_system_settings()
        data_sources = []

        #AKShare (default enabled)
        akshare_config = DataSourceConfig(
            name="AKShare",
            type=DataSourceType.AKSHARE,
            endpoint="https://akshare.akfamily.xyz",
            enabled=True,
            priority=1,
            description="AKShare开源金融数据接口"
        )
        data_sources.append(akshare_config)

        #Tushare (if configured)
        if settings.get("tushare_token"):
            tushare_config = DataSourceConfig(
                name="Tushare",
                type=DataSourceType.TUSHARE,
                api_key=settings.get("tushare_token"),
                endpoint="http://api.tushare.pro",
                enabled=True,
                priority=2,
                description="Tushare专业金融数据接口"
            )
            data_sources.append(tushare_config)

        #Finnhub (if configured)
        if settings.get("finnhub_api_key"):
            finnhub_config = DataSourceConfig(
                name="Finnhub",
                type=DataSourceType.FINNHUB,
                api_key=settings.get("finnhub_api_key"),
                endpoint="https://finnhub.io/api/v1",
                enabled=True,
                priority=3,
                description="Finnhub股票数据接口"
            )
            data_sources.append(finnhub_config)

        return data_sources
    
    #== sync, corrected by elderman == @elder man
    
    def get_database_configs(self) -> List[DatabaseConfig]:
        """Get Database Configuration"""
        configs = []
        
        #MongoDB Configuration
        mongodb_config = DatabaseConfig(
            name="MongoDB主库",
            type=DatabaseType.MONGODB,
            host=os.getenv("MONGODB_HOST", "localhost"),
            port=int(os.getenv("MONGODB_PORT", "27017")),
            database=os.getenv("MONGODB_DATABASE", "tradingagents"),
            enabled=True,
            description="MongoDB主数据库"
        )
        configs.append(mongodb_config)
        
        #Redis Configuration
        redis_config = DatabaseConfig(
            name="Redis缓存",
            type=DatabaseType.REDIS,
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            database=os.getenv("REDIS_DB", "0"),
            enabled=True,
            description="Redis缓存数据库"
        )
        configs.append(redis_config)
        
        return configs
    
    #== sync, corrected by elderman == @elder man
    
    async def get_unified_system_config(self) -> SystemConfig:
        """Get a unified system configuration"""
        try:
            config = SystemConfig(
                config_name="统一系统配置",
                config_type="unified",
                llm_configs=self.get_llm_configs(),
                default_llm=self.get_default_model(),
                data_source_configs=self.get_data_source_configs(),
                default_data_source="AKShare",
                database_configs=self.get_database_configs(),
                system_settings=self.get_system_settings()
            )
            return config
        except Exception as e:
            print(f"获取统一配置失败: {e}")
            #Return Default Configuration
            return SystemConfig(
                config_name="默认配置",
                config_type="default",
                llm_configs=[],
                data_source_configs=[],
                database_configs=[],
                system_settings={}
            )
    
    def sync_to_legacy_format(self, system_config: SystemConfig) -> bool:
        """Sync Configuration to Traditional Format"""
        try:
            #Sync Model Configuration
            for llm_config in system_config.llm_configs:
                self.save_llm_config(llm_config)

            #Read existing settings.json
            current_settings = self.get_system_settings()

            #Sync System Settings (maintain existing fields, update only required fields)
            settings = current_settings.copy()

            #Map new field names to old field names
            if "quick_analysis_model" in system_config.system_settings:
                settings["quick_think_llm"] = system_config.system_settings["quick_analysis_model"]
                settings["quick_analysis_model"] = system_config.system_settings["quick_analysis_model"]

            if "deep_analysis_model" in system_config.system_settings:
                settings["deep_think_llm"] = system_config.system_settings["deep_analysis_model"]
                settings["deep_analysis_model"] = system_config.system_settings["deep_analysis_model"]

            if system_config.default_llm:
                settings["default_model"] = system_config.default_llm

            self.save_system_settings(settings)

            return True
        except Exception as e:
            print(f"同步配置到传统格式失败: {e}")
            return False


#Create global instance
unified_config = UnifiedConfigManager()
