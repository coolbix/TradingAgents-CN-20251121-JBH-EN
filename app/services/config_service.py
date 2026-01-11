"""Configure management services
"""

import time
import asyncio
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from app.utils.timezone import now_tz
from bson import ObjectId

from app.core.database import get_mongo_db_async
from app.core.unified_config import UNIFIED_CONFIG_MANAGER
from app.models.config_models import (
    SystemConfig, LLMConfig, DataSourceConfig, DatabaseConfig,
    ModelProvider, DataSourceType, DatabaseType, LLMProvider,
    MarketCategory, DataSourceGrouping, ModelCatalog, ModelInfo
)

logger = logging.getLogger(__name__)


class ConfigService:
    """Configure Management Services"""

    def __init__(self, db_manager=None):
        self.db = None
        self.db_manager = db_manager

    async def _get_db_async(self):
        """Get database connections"""
        if self.db is None:
            if self.db_manager and self.db_manager.mongo_db is not None:
                #If there are examples of DataManager, use them directly
                self.db = self.db_manager.mongo_db
            else:
                #Otherwise use global functions
                self.db = get_mongo_db_async()
        return self.db

    #== sync, corrected by elderman == @elder man

    async def get_market_categories(self) -> List[MarketCategory]:
        """Access to all market classifications"""
        try:
            db = await self._get_db_async()
            categories_collection = db.market_categories

            categories_data = await categories_collection.find({}).to_list(length=None)
            categories = [MarketCategory(**data) for data in categories_data]

            #Create default classification if no classification
            if not categories:
                categories = await self._create_default_market_categories()

            #Sort in order
            categories.sort(key=lambda x: x.sort_order)
            return categories
        except Exception as e:
            print(f"❌ Failed to get market categories: {e}")
            return []

    async def _create_default_market_categories(self) -> List[MarketCategory]:
        """Create default market classification"""
        default_categories = [
            MarketCategory(
                id="a_shares",
                name="a_shares",
                display_name="A股",
                description="中国A股市场数据源",
                enabled=True,
                sort_order=1
            ),
            MarketCategory(
                id="us_stocks",
                name="us_stocks",
                display_name="美股",
                description="美国股票市场数据源",
                enabled=True,
                sort_order=2
            ),
            MarketCategory(
                id="hk_stocks",
                name="hk_stocks",
                display_name="港股",
                description="香港股票市场数据源",
                enabled=True,
                sort_order=3
            ),
            MarketCategory(
                id="crypto",
                name="crypto",
                display_name="数字货币",
                description="数字货币市场数据源",
                enabled=True,
                sort_order=4
            ),
            MarketCategory(
                id="futures",
                name="futures",
                display_name="期货",
                description="期货市场数据源",
                enabled=True,
                sort_order=5
            )
        ]

        #Save to Database
        db = await self._get_db_async()
        categories_collection = db.market_categories

        for category in default_categories:
            await categories_collection.insert_one(category.model_dump())

        return default_categories

    async def add_market_category(self, category: MarketCategory) -> bool:
        """Add Market Classification"""
        try:
            db = await self._get_db_async()
            categories_collection = db.market_categories

            #Check if ID exists
            existing = await categories_collection.find_one({"id": category.id})
            if existing:
                return False

            await categories_collection.insert_one(category.model_dump())
            return True
        except Exception as e:
            print(f"❌ Failed to add market category: {e}")
            return False

    async def update_market_category(self, category_id: str, updates: Dict[str, Any]) -> bool:
        """Updating market classifications"""
        try:
            db = await self._get_db_async()
            categories_collection = db.market_categories

            updates["updated_at"] = now_tz()
            result = await categories_collection.update_one(
                {"id": category_id},
                {"$set": updates}
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"❌ Failed to update market category: {e}")
            return False

    async def delete_market_category(self, category_id: str) -> bool:
        """Remove Market Classification"""
        try:
            db = await self._get_db_async()
            categories_collection = db.market_categories
            groupings_collection = db.datasource_groupings

            #Check for data sources using this classification
            groupings_count = await groupings_collection.count_documents(
                {"market_category_id": category_id}
            )
            if groupings_count > 0:
                return False

            result = await categories_collection.delete_one({"id": category_id})
            return result.deleted_count > 0
        except Exception as e:
            print(f"❌ Failed to delete market category: {e}")
            return False

    #== sync, corrected by elderman == @elder man

    async def get_datasource_groupings(self) -> List[DataSourceGrouping]:
        """Get All Data Source Group Relationships"""
        try:
            db = await self._get_db_async()
            groupings_collection = db.datasource_groupings

            groupings_data = await groupings_collection.find({}).to_list(length=None)
            return [DataSourceGrouping(**data) for data in groupings_data]
        except Exception as e:
            print(f"❌ Failed to get data source groupings: {e}")
            return []

    async def add_datasource_to_category(self, grouping: DataSourceGrouping) -> bool:
        """Add data sources to classification"""
        try:
            db = await self._get_db_async()
            groupings_collection = db.datasource_groupings

            #Check for presence
            existing = await groupings_collection.find_one({
                "data_source_name": grouping.data_source_name,
                "market_category_id": grouping.market_category_id
            })
            if existing:
                return False

            await groupings_collection.insert_one(grouping.model_dump())
            return True
        except Exception as e:
            print(f"❌ Failed to add data source to category: {e}")
            return False

    async def remove_datasource_from_category(self, data_source_name: str, category_id: str) -> bool:
        """Remove data source from classification"""
        try:
            db = await self._get_db_async()
            groupings_collection = db.datasource_groupings

            result = await groupings_collection.delete_one({
                "data_source_name": data_source_name,
                "market_category_id": category_id
            })
            return result.deleted_count > 0
        except Exception as e:
            print(f"❌ Failed to remove data source from category: {e}")
            return False

    async def update_datasource_grouping(self, data_source_name: str, category_id: str, updates: Dict[str, Any]) -> bool:
        """Update data source group relationships

        Important: updating both the data groups and systems configs
        -Datasource groupings: for front-end presentation and management
        - system configs.data source configs: for priority judgement when actual data are obtained
        """
        try:
            db = await self._get_db_async()
            groupings_collection = db.datasource_groupings
            config_collection = db.system_configs

            #1. Update the datasource groupings collection
            updates["updated_at"] = now_tz()
            result = await groupings_collection.update_one(
                {
                    "data_source_name": data_source_name,
                    "market_category_id": category_id
                },
                {"$set": updates}
            )

            #Synchronize the system configs collection if priority is updated
            if "priority" in updates and result.modified_count > 0:
                #Get Current Activated Configuration
                config_data = await config_collection.find_one(
                    {"is_active": True},
                    sort=[("version", -1)]
                )

                if config_data:
                    data_source_configs = config_data.get("data_source_configs", [])

                    #Find and update the corresponding data source configuration
                    #Note: data source name may be "AKShare", while the name in config is also "AKShare"
                    #But type fields are lowercase "kshare"
                    updated = False
                    for ds_config in data_source_configs:
                        #Try matching name fields (priority) or type fields
                        if (ds_config.get("name") == data_source_name or
                            ds_config.get("type") == data_source_name.lower()):
                            ds_config["priority"] = updates["priority"]
                            updated = True
                            logger.info(f"✅ [Priority Sync] Updated data source in system configs: {data_source_name}, new priority: {updates['priority']}")
                            break

                    if updated:
                        #Update Profile Version
                        version = config_data.get("version", 0)
                        await config_collection.update_one(
                            {"_id": config_data["_id"]},
                            {
                                "$set": {
                                    "data_source_configs": data_source_configs,
                                    "version": version + 1,
                                    "updated_at": now_tz()
                                }
                            }
                        )
                        logger.info(f"[Priority Sync] System configs version updated: {version} -> {version + 1}")
                    else:
                        logger.warning(f"No matching data source configuration was found: {data_source_name}")

            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Failed to update the data source group relationship: {e}")
            return False

    async def update_category_datasource_order(self, category_id: str, ordered_datasources: List[Dict[str, Any]]) -> bool:
        """Update the sorting of data sources in the classification

        Important: updating both the data groups and systems configs
        -Datasource groupings: for front-end presentation and management
        - system configs.data source configs: for priority judgement when actual data are obtained
        """
        try:
            db = await self._get_db_async()
            groupings_collection = db.datasource_groupings
            config_collection = db.system_configs

            #1. Batch update of priorities in the group of data groupings
            for item in ordered_datasources:
                await groupings_collection.update_one(
                    {
                        "data_source_name": item["name"],
                        "market_category_id": category_id
                    },
                    {
                        "$set": {
                            "priority": item["priority"],
                            "updated_at": now_tz()
                        }
                    }
                )

            #Synchronized update of data source configs
            #Get Current Activated Configuration
            config_data = await config_collection.find_one(
                {"is_active": True},
                sort=[("version", -1)]
            )

            if config_data:
                #Build map of data source name to priority
                priority_map = {item["name"]: item["priority"] for item in ordered_datasources}

                #Update the priority of the corresponding data source in data source configs
                data_source_configs = config_data.get("data_source_configs", [])
                updated = False

                for ds_config in data_source_configs:
                    ds_name = ds_config.get("name")
                    if ds_name in priority_map:
                        ds_config["priority"] = priority_map[ds_name]
                        updated = True
                        print(f"📊 [Priority Sync] Updating priority for data source {ds_name} to {priority_map[ds_name]}")

                #Save database if updated
                if updated:
                    await config_collection.update_one(
                        {"_id": config_data["_id"]},
                        {
                            "$set": {
                                "data_source_configs": data_source_configs,
                                "updated_at": now_tz(),
                                "version": config_data.get("version", 0) + 1
                            }
                        }
                    )
                    print(f"✅ [Priority Sync] Synchronized system_configs collection, new version: {config_data.get('version', 0) + 1}")
                else:
                    print(f"⚠️ [Priority Sync] No data source configuration found to update")
            else:
                print(f"⚠️ [Priority Sync] Active system configuration not found")

            return True
        except Exception as e:
            print(f"❌ Failed to update category data source order: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def get_system_config_from_database(self) -> Optional[SystemConfig]:
        """Get System Configuration - Prioritize the most up-to-date data from the database"""
        try:
            #Get up-to-date configuration directly from the database to avoid cache problems
            db = await self._get_db_async()
            config_collection = db.system_configs

            config_data = await config_collection.find_one(
                {"is_active": True},
                sort=[("version", -1)]
            )

            if config_data:
                print(f"📊 Retrieved config from DB, Version: {config_data.get('version', 0)}, LLM Config Count: {len(config_data.get('llm_configs', []))}")
                return SystemConfig(**config_data)

            #Create default configuration if no configuration
            print("⚠️ No configuration in database, creating default configuration")
            return await self._create_default_config()

        except Exception as e:
            print(f"❌ Failed to retrieve configuration from database: {e}")

            #Try to get it from the Unified Configuration Manager as a last retreat
            try:
                unified_system_config = await UNIFIED_CONFIG_MANAGER.get_unified_system_config()
                if unified_system_config:
                    print("🔄 Fallback to unified configuration manager")
                    return unified_system_config
            except Exception as e2:
                print(f"Failed to retrieve from unified config as well: {e2}")

            return None
    
    async def _create_default_config(self) -> SystemConfig:
        """Create Default System Configuration"""
        default_config = SystemConfig(
            config_name="默认配置",
            config_type="system",
            llm_configs=[
                LLMConfig(
                    provider=ModelProvider.OPENAI,
                    model_name="gpt-3.5-turbo",
                    api_key="your-openai-api-key",
                    api_base="https://api.openai.com/v1",
                    max_tokens=4000,
                    temperature=0.7,
                    enabled=False,
                    description="OpenAI GPT-3.5 Turbo模型"
                ),
                LLMConfig(
                    provider=ModelProvider.ZHIPU,
                    model_name="glm-4",
                    api_key="your-zhipu-api-key",
                    api_base="https://open.bigmodel.cn/api/paas/v4",
                    max_tokens=4000,
                    temperature=0.7,
                    enabled=True,
                    description="智谱AI GLM-4模型（推荐）"
                ),
                LLMConfig(
                    provider=ModelProvider.QWEN,
                    model_name="qwen-turbo",
                    api_key="your-qwen-api-key",
                    api_base="https://dashscope.aliyuncs.com/compatible-mode/v1",
                    max_tokens=4000,
                    temperature=0.7,
                    enabled=False,
                    description="阿里云通义千问模型"
                )
            ],
            default_llm="glm-4",
            data_source_configs=[
                DataSourceConfig(
                    name="AKShare",
                    type=DataSourceType.AKSHARE,
                    endpoint="https://akshare.akfamily.xyz",
                    timeout=30,
                    rate_limit=100,
                    enabled=True,
                    priority=1,
                    description="AKShare开源金融数据接口"
                ),
                DataSourceConfig(
                    name="Tushare",
                    type=DataSourceType.TUSHARE,
                    api_key="your-tushare-token",
                    endpoint="http://api.tushare.pro",
                    timeout=30,
                    rate_limit=200,
                    enabled=False,
                    priority=2,
                    description="Tushare专业金融数据接口"
                )
            ],
            default_data_source="AKShare",
            database_configs=[
                DatabaseConfig(
                    name="MongoDB主库",
                    type=DatabaseType.MONGODB,
                    host="localhost",
                    port=27017,
                    database="tradingagents",
                    enabled=True,
                    description="MongoDB主数据库"
                ),
                DatabaseConfig(
                    name="Redis缓存",
                    type=DatabaseType.REDIS,
                    host="localhost",
                    port=6379,
                    database="0",
                    enabled=True,
                    description="Redis缓存数据库"
                )
            ],
            system_settings={
                "max_concurrent_tasks": 3,
                "default_analysis_timeout": 300,
                "enable_cache": True,
                "cache_ttl": 3600,
                "log_level": "INFO",
                "enable_monitoring": True,
                # Worker/Queue intervals
                "worker_heartbeat_interval_seconds": 30,
                "queue_poll_interval_seconds": 1.0,
                "queue_cleanup_interval_seconds": 60.0,
                # SSE intervals
                "sse_poll_timeout_seconds": 1.0,
                "sse_heartbeat_interval_seconds": 10,
                "sse_task_max_idle_seconds": 300,
                "sse_batch_poll_interval_seconds": 2.0,
                "sse_batch_max_idle_seconds": 600,
                # TradingAgents runtime intervals (optional; DB-managed)
                "ta_hk_min_request_interval_seconds": 2.0,
                "ta_hk_timeout_seconds": 60,
                "ta_hk_max_retries": 3,
                "ta_hk_rate_limit_wait_seconds": 60,
                "ta_hk_cache_ttl_seconds": 86400,
                #Add: TradingAgents Data Source Policy
                #Whether to read first from the app cache (Mongo collection stock basic info / market quotes)
                "ta_use_app_cache": False,
                "ta_china_min_api_interval_seconds": 0.5,
                "ta_us_min_api_interval_seconds": 1.0,
                "ta_google_news_sleep_min_seconds": 2.0,
                "ta_google_news_sleep_max_seconds": 6.0,
                "app_timezone": "Asia/Shanghai"
            }
        )
        
        #Save to Database
        await self.save_system_config(default_config)
        return default_config
    
    async def save_system_config(self, config: SystemConfig) -> bool:
        """Save System Configuration to Database"""
        try:
            print(f"💾 Starting to save config, LLM Config Count: {len(config.llm_configs)}")

            #Save to Database
            db = await self._get_db_async()
            config_collection = db.system_configs

            #Update Timetamp and Version
            config.updated_at = now_tz()
            config.version += 1

            #Set current active configuration to non-activated
            update_result = await config_collection.update_many(
                {"is_active": True},
                {"$set": {"is_active": False}}
            )
            print(f"📝 Disabled old configurations count: {update_result.modified_count}")

            #Insert a new configuration - Remove  id fields to automatically generate new MongoDB
            config_dict = config.model_dump(by_alias=True)
            if '_id' in config_dict:
                del config_dict['_id']  #Remove old  id to make MongoDB new

            #Print upcoming system settings
            system_settings = config_dict.get('system_settings', {})
            print(f"📝 Saving system_settings with {len(system_settings)} items")
            if 'quick_analysis_model' in system_settings:
                print(f"  ✓ Contains quick_analysis_model: {system_settings['quick_analysis_model']}")
            else:
                print(f"  ⚠️  Does not contain quick_analysis_model")
            if 'deep_analysis_model' in system_settings:
                print(f"  ✓ Contains deep_analysis_model: {system_settings['deep_analysis_model']}")
            else:
                print(f"  ⚠️  Does not contain deep_analysis_model")

            insert_result = await config_collection.insert_one(config_dict)
            print(f"📝 New Config ID: {insert_result.inserted_id}")

            #Verify Save Results
            saved_config = await config_collection.find_one({"_id": insert_result.inserted_id})
            if saved_config:
                print(f"✅ Config saved successfully, verified LLM config count: {len(saved_config.get('llm_configs', []))}")

                #Skip Unified Configuration Sync to avoid conflict
                # unified_config.sync_to_legacy_format(config)

                return True
            else:
                print("❌ Config save verification failed")
                return False

        except Exception as e:
            print(f"❌ Failed to save config: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def delete_llm_config(self, provider: str, model_name: str) -> bool:
        """Remove Large Model Configuration"""
        try:
            print(f"🗑️ Deleting LLM config - provider: {provider}, model_name: {model_name}")

            config = await self.get_system_config_from_database()
            if not config:
                print("❌ System configuration is empty")
                return False

            print(f"📊 Current LLM config count: {len(config.llm_configs)}")

            #Print all existing configurations
            for i, llm in enumerate(config.llm_configs):
                print(f"   {i+1}. provider: {llm.provider.value}, model_name: {llm.model_name}")

            #Find and remove specified LLM configuration
            original_count = len(config.llm_configs)

            #Use lighter matching conditions
            config.llm_configs = [
                llm for llm in config.llm_configs
                if not (str(llm.provider.value).lower() == provider.lower() and llm.model_name == model_name)
            ]

            new_count = len(config.llm_configs)
            print(f"🔄 Post-deletion config count: {new_count} (Original: {original_count})")

            if new_count == original_count:
                print(f"❌ No matching configuration found: {provider}/{model_name}")
                return False  #No configuration found to delete

            #Save updated configuration
            save_result = await self.save_system_config(config)
            print(f"💾 Save result: {save_result}")

            return save_result

        except Exception as e:
            print(f"❌ Failed to delete LLM config: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def set_default_llm(self, model_name: str) -> bool:
        """Set Default Large Model"""
        try:
            config = await self.get_system_config_from_database()
            if not config:
                return False

            #Check if the specified model exists
            model_exists = any(
                llm.model_name == model_name for llm in config.llm_configs
            )

            if not model_exists:
                return False

            config.default_llm = model_name
            return await self.save_system_config(config)

        except Exception as e:
            print(f"Failed to set default LLM: {e}")
            return False

    async def set_default_data_source(self, data_source_name: str) -> bool:
        """Set Default Data Source"""
        try:
            config = await self.get_system_config_from_database()
            if not config:
                return False

            #Checks whether the specified data source exists
            source_exists = any(
                ds.name == data_source_name for ds in config.data_source_configs
            )

            if not source_exists:
                return False

            config.default_data_source = data_source_name
            return await self.save_system_config(config)

        except Exception as e:
            print(f"Failed to set default data source: {e}")
            return False

    async def update_system_settings(self, settings: Dict[str, Any]) -> bool:
        """Update System Settings"""
        try:
            config = await self.get_system_config_from_database()
            if not config:
                return False

            #System Settings Before Printing Update
            print(f"📝 Pre-update system_settings contains {len(config.system_settings)} items")
            if 'quick_analysis_model' in config.system_settings:
                print(f"  ✓ Pre-update contains quick_analysis_model: {config.system_settings['quick_analysis_model']}")
            else:
                print(f"  ⚠️  Pre-update does not contain quick_analysis_model")

            #Update System Settings
            config.system_settings.update(settings)

            #Print updated system settings
            print(f"📝 Post-update system_settings contains {len(config.system_settings)} items")
            if 'quick_analysis_model' in config.system_settings:
                print(f"  ✓ Post-update contains quick_analysis_model: {config.system_settings['quick_analysis_model']}")
            else:
                print(f"  ⚠️  Post-update does not contain quick_analysis_model")
            if 'deep_analysis_model' in config.system_settings:
                print(f"  ✓ Post-update contains deep_analysis_model: {config.system_settings['deep_analysis_model']}")
            else:
                print(f"  ⚠️  Post-update does not contain deep_analysis_model")

            result = await self.save_system_config(config)

            #Synchronize to filesystem (for unified config)
            if result:
                try:
                    from app.core.unified_config import UNIFIED_CONFIG_MANAGER
                    UNIFIED_CONFIG_MANAGER.sync_to_legacy_format(config)
                    print(f"✅ System settings synchronized to filesystem")
                except Exception as e:
                    print(f"⚠️  Failed to sync system settings to filesystem: {e}")

            return result

        except Exception as e:
            print(f"Failed to update system settings: {e}")
            return False

    async def get_system_settings(self) -> Dict[str, Any]:
        """Get System Settings"""
        try:
            config = await self.get_system_config_from_database()
            if not config:
                return {}
            return config.system_settings
        except Exception as e:
            print(f"Failed to get system settings: {e}")
            return {}

    async def export_config(self) -> Dict[str, Any]:
        """Export Configuration"""
        try:
            config = await self.get_system_config_from_database()
            if not config:
                return {}

            #Convert to serialized dictionary format
            #Option A: Sensitization/cleaning of sensitive fields during export
            def _llm_sanitize(x: LLMConfig):
                d = x.model_dump()
                d["api_key"] = ""
                #Ensure default values for mandatory fields (preventing the export of None or empty strings)
                #Note: max tokens already has the right value in system configs, directly used
                if not d.get("max_tokens") or d.get("max_tokens") == "":
                    d["max_tokens"] = 4000
                if not d.get("temperature") and d.get("temperature") != 0:
                    d["temperature"] = 0.7
                if not d.get("timeout") or d.get("timeout") == "":
                    d["timeout"] = 180
                if not d.get("retry_times") or d.get("retry_times") == "":
                    d["retry_times"] = 3
                return d
            def _ds_sanitize(x: DataSourceConfig):
                d = x.model_dump()
                d["api_key"] = ""
                d["api_secret"] = ""
                return d
            def _db_sanitize(x: DatabaseConfig):
                d = x.model_dump()
                d["password"] = ""
                return d
            export_data = {
                "config_name": config.config_name,
                "config_type": config.config_type,
                "llm_configs": [_llm_sanitize(llm) for llm in config.llm_configs],
                "default_llm": config.default_llm,
                "data_source_configs": [_ds_sanitize(ds) for ds in config.data_source_configs],
                "default_data_source": config.default_data_source,
                "database_configs": [_db_sanitize(db) for db in config.database_configs],
                #Option A: Sensitization of system settings during export
                "system_settings": {k: (None if any(p in k.lower() for p in ("key","secret","password","token","client_secret")) else v) for k, v in (config.system_settings or {}).items()},
                "exported_at": now_tz().isoformat(),
                "version": config.version
            }

            return export_data

        except Exception as e:
            print(f"Failed to export config: {e}")
            return {}

    async def import_config(self, config_data: Dict[str, Any]) -> bool:
        """Import Configuration"""
        try:
            #Verify configuration data format
            if not self._validate_config_data(config_data):
                return False

            #Create a new system configuration (option A: ignore sensitive fields when importing)
            def _llm_sanitize_in(llm: Dict[str, Any]):
                d = dict(llm or {})
                d.pop("api_key", None)
                d["api_key"] = ""
                #Clear an empty string so that Pydantic uses the default value
                if d.get("max_tokens") == "" or d.get("max_tokens") is None:
                    d.pop("max_tokens", None)
                if d.get("temperature") == "" or d.get("temperature") is None:
                    d.pop("temperature", None)
                if d.get("timeout") == "" or d.get("timeout") is None:
                    d.pop("timeout", None)
                if d.get("retry_times") == "" or d.get("retry_times") is None:
                    d.pop("retry_times", None)
                return LLMConfig(**d)
            def _ds_sanitize_in(ds: Dict[str, Any]):
                d = dict(ds or {})
                d.pop("api_key", None)
                d.pop("api_secret", None)
                d["api_key"] = ""
                d["api_secret"] = ""
                return DataSourceConfig(**d)
            def _db_sanitize_in(db: Dict[str, Any]):
                d = dict(db or {})
                d.pop("password", None)
                d["password"] = ""
                return DatabaseConfig(**d)
            new_config = SystemConfig(
                config_name=config_data.get("config_name", "导入的配置"),
                config_type="imported",
                llm_configs=[_llm_sanitize_in(llm) for llm in config_data.get("llm_configs", [])],
                default_llm=config_data.get("default_llm"),
                data_source_configs=[_ds_sanitize_in(ds) for ds in config_data.get("data_source_configs", [])],
                default_data_source=config_data.get("default_data_source"),
                database_configs=[_db_sanitize_in(db) for db in config_data.get("database_configs", [])],
                system_settings=config_data.get("system_settings", {})
            )

            return await self.save_system_config(new_config)

        except Exception as e:
            print(f"Failed to import config: {e}")
            return False

    def _validate_config_data(self, config_data: Dict[str, Any]) -> bool:
        """Verify configuration data format"""
        try:
            required_fields = ["llm_configs", "data_source_configs", "database_configs", "system_settings"]
            for field in required_fields:
                if field not in config_data:
                    print(f"Config data missing required field: {field}")
                    return False

            return True

        except Exception as e:
            print(f"Failed to validate config data: {e}")
            return False

    async def migrate_legacy_config(self) -> bool:
        """Move traditional configuration"""
        try:
            #Here you can call the logic of moving scripts.
            #Or we're going to move here.
            from scripts.migrate_config_to_webapi import ConfigMigrator

            migrator = ConfigMigrator()
            return await migrator.migrate_all_configs()

        except Exception as e:
            print(f"Failed to migrate legacy config: {e}")
            return False
    
    async def update_llm_config(self, llm_config: LLMConfig) -> bool:
        """Update Large Model Configuration"""
        try:
            #Save directly to Unified Configuration Manager
            success = UNIFIED_CONFIG_MANAGER.save_llm_config(llm_config)
            if not success:
                return False

            #Update database configuration simultaneously
            config = await self.get_system_config_from_database()
            if not config:
                return False

            #Find and update corresponding LLM profiles
            for i, existing_config in enumerate(config.llm_configs):
                if existing_config.model_name == llm_config.model_name:
                    config.llm_configs[i] = llm_config
                    break
            else:
                #Add a new configuration if no existing
                config.llm_configs.append(llm_config)

            return await self.save_system_config(config)
        except Exception as e:
            print(f"Failed to update LLM config: {e}")
            return False
    
    async def test_llm_config(self, llm_config: LLMConfig) -> Dict[str, Any]:
        """Test Large Model Configuration - True Call API for Validation"""
        start_time = time.time()
        try:
            import requests

            #Get provider string values (compatible number count and string)
            provider_str = llm_config.provider.value if hasattr(llm_config.provider, 'value') else str(llm_config.provider)

            logger.info(f"Test the large model configuration:{provider_str} - {llm_config.model_name}")
            logger.info(f"API Foundation URL (model configuration):{llm_config.api_base}")

            #Get the plant configuration (for API Key and default base url)
            db = await self._get_db_async()
            providers_collection = db.llm_providers
            provider_data = await providers_collection.find_one({"name": provider_str})

            #1. Determine the API base URL
            api_base = llm_config.api_base
            if not api_base:
                #If model configuration does not have api base, get from manufacturer configuration
                if provider_data and provider_data.get("default_base_url"):
                    api_base = provider_data["default_base_url"]
                    logger.info(f"API base URL:{api_base}")
                else:
                    return {
                        "success": False,
                        "message": f"Neither model config nor provider config has API base URL set",
                        "response_time": time.time() - start_time,
                        "details": None
                    }

            #2. Validation API Key
            api_key = None
            if llm_config.api_key:
                api_key = llm_config.api_key
            else:
                #Get API Key from Plant Configuration
                if provider_data and provider_data.get("api_key"):
                    api_key = provider_data["api_key"]
                    logger.info(f"Getting the API key from the plant configuration")
                else:
                    #Try fetching from environmental variables
                    api_key = self._get_env_api_key(provider_str)
                    if api_key:
                        logger.info(f"Getting API keys from environmental variables")

            if not api_key or not self._is_valid_api_key(api_key):
                return {
                    "success": False,
                    "message": f"{provider_str} has no valid API key configured",
                    "response_time": time.time() - start_time,
                    "details": None
                }

            #3. Selection of test methods by type of manufacturer
            if provider_str == "google":
                #Google AI uses specific testing methods
                logger.info(f"Google AI test method")
                result = self._test_google_api(api_key, f"{provider_str} {llm_config.model_name}", api_base, llm_config.model_name)
                result["response_time"] = time.time() - start_time
                return result
            elif provider_str == "deepseek":
                #DeepSeek uses specific testing methods
                logger.info(f"Use the DeepSeek test method")
                result = self._test_deepseek_api(api_key, f"{provider_str} {llm_config.model_name}", llm_config.model_name)
                result["response_time"] = time.time() - start_time
                return result
            elif provider_str == "dashscope":
                #DashScope uses specific testing methods
                logger.info(f"Use the DashScope test method")
                result = self._test_dashscope_api(api_key, f"{provider_str} {llm_config.model_name}", llm_config.model_name)
                result["response_time"] = time.time() - start_time
                return result
            else:
                #Other manufacturers use OpenAI compatible testing methods
                logger.info(f"Use OpenAI compatibility test")

                #Build test request
                api_base_normalized = api_base.rstrip("/")

                #🔧 Smart version number processing: only if no version number is available /v1
                #Avoid re-adding URLs with existing version numbers (e. g. /v4 for spectro-AI) / v1
                import re
                if not re.search(r'/v\d+$', api_base_normalized):
                    #No version number at the end of URL, add /v1 (OpenAI standard)
                    api_base_normalized = api_base_normalized + "/v1"
                    logger.info(f"Add /v1 version number:{api_base_normalized}")
                else:
                    #URL already contains version number (e. g. / v4), not added
                    logger.info(f"Available version number detected, as it is:{api_base_normalized}")

                url = f"{api_base_normalized}/chat/completions"

                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {api_key}"
                }

                data = {
                    "model": llm_config.model_name,
                    "messages": [
                        {"role": "user", "content": "Hello, please respond with 'OK' if you can read this."}
                    ],
                    "max_tokens": 200,  #Increase to 200 to give enough space for reasoning models (e.g. o1/gpt-5)
                    "temperature": 0.1
                }

                logger.info(f"Send test requests to:{url}")
                logger.info(f"Using models:{llm_config.model_name}")
                logger.info(f"Data requested:{data}")

                #Send test request
                response = requests.post(url, json=data, headers=headers, timeout=15)
                response_time = time.time() - start_time

                logger.info(f"Response received: HTTP{response.status_code}")

                #Process response (openAI compatible plant only)
                if response.status_code == 200:
                    try:
                        result = response.json()
                        logger.info(f"Response to JSON:{result}")

                        if "choices" in result and len(result["choices"]) > 0:
                            content = result["choices"][0]["message"]["content"]
                            logger.info(f"Response:{content}")

                            if content and len(content.strip()) > 0:
                                logger.info(f"Test success:{content[:50]}")
                                return {
                                    "success": True,
                                    "message": f"Successfully connected to {provider_str} {llm_config.model_name}",
                                    "response_time": response_time,
                                    "details": {
                                        "provider": provider_str,
                                        "model": llm_config.model_name,
                                        "api_base": api_base,
                                        "response_preview": content[:100]
                                    }
                                }
                            else:
                                logger.warning(f"The API response is empty.")
                                return {
                                    "success": False,
                                    "message": "API response content is empty",
                                    "response_time": response_time,
                                    "details": None
                                }
                        else:
                            logger.warning(f"⚠️ API response format abnormal, missing choices field")
                            logger.warning(f"Response content:{result}")
                            return {
                                "success": False,
                                "message": "API response format abnormal",
                                "response_time": response_time,
                                "details": None
                            }
                    except Exception as e:
                        logger.error(f"The response failed:{e}")
                        logger.error(f"Reply to text:{response.text[:500]}")
                        return {
                            "success": False,
                            "message": f"Failed to parse response: {str(e)}",
                            "response_time": response_time,
                            "details": None
                        }
                elif response.status_code == 401:
                    return {
                        "success": False,
                        "message": "API key invalid or expired",
                        "response_time": response_time,
                        "details": None
                    }
                elif response.status_code == 403:
                    return {
                        "success": False,
                        "message": "API permission denied or quota exhausted",
                        "response_time": response_time,
                        "details": None
                    }
                elif response.status_code == 404:
                    return {
                        "success": False,
                        "message": f"API endpoint does not exist, check API base URL: {url}",
                        "response_time": response_time,
                        "details": None
                    }
                else:
                    try:
                        error_detail = response.json()
                        error_msg = error_detail.get("error", {}).get("message", f"HTTP {response.status_code}")
                        return {
                            "success": False,
                            "message": f"API test failed: {error_msg}",
                            "response_time": response_time,
                            "details": None
                        }
                    except:
                        return {
                        "success": False,
                        "message": f"API test failed: HTTP {response.status_code}",
                        "response_time": response_time,
                        "details": None
                    }

        except requests.exceptions.Timeout:
            response_time = time.time() - start_time
            return {
                "success": False,
                "message": "Connection timed out, check API base URL or network connectivity",
                "response_time": response_time,
                "details": None
            }
        except requests.exceptions.ConnectionError as e:
            response_time = time.time() - start_time
            return {
                "success": False,
                "message": f"Connection failed, check API base URL: {str(e)}",
                "response_time": response_time,
                "details": None
            }
        except Exception as e:
            response_time = time.time() - start_time
            logger.error(f"The large model configuration failed:{e}")
            return {
                "success": False,
                "message": f"Connection failed: {str(e)}",
                "response_time": response_time,
                "details": None
            }
    
    def _truncate_api_key(self, api_key: str, prefix_len: int = 6, suffix_len: int = 6) -> str:
        """Cut API Key for display

        Args:
            api key: Complete API Key
            prefix len: Keep prefix length
            suffix len: retain suffix length

        Returns:
            Post-cut API Key, e.g. 0f229a c550ec
        """
        if not api_key or len(api_key) <= prefix_len + suffix_len:
            return api_key

        return f"{api_key[:prefix_len]}...{api_key[-suffix_len:]}"

    async def test_data_source_config(self, ds_config: DataSourceConfig) -> Dict[str, Any]:
        """Test data source configuration - True call API for validation"""
        start_time = time.time()
        try:
            import requests
            import os

            ds_type = ds_config.type.value if hasattr(ds_config.type, 'value') else str(ds_config.type)

            logger.info(f"🧪 [TEST] Testing data source config: {ds_config.name} ({ds_type})")

            #🔥 Prefer to API Key in the configuration or, if not, access to the database
            api_key = ds_config.api_key
            used_db_credentials = False
            used_env_credentials = False

            logger.info(f"🔍 [TEST] Received API Key from config: {repr(api_key)} (type: {type(api_key).__name__}, length: {len(api_key) if api_key else 0})")

            #Testing according to different data source types
            if ds_type == "tushare":
                #🔥 If the configuration API Key contains "..." (cut marks), verify whether the original value is unmodified
                if api_key and "..." in api_key:
                    logger.info(f"🔍 [TEST] API Key contains '...' (truncated), checking if it matches database value")

                    #Get the full API Key from the database
                    system_config = await self.get_system_config_from_database()
                    db_config = None
                    if system_config:
                        for ds in system_config.data_source_configs:
                            if ds.name == ds_config.name:
                                db_config = ds
                                break

                    if db_config and db_config.api_key:
                        #Same cut-off process for complete API Key in database
                        truncated_db_key = self._truncate_api_key(db_config.api_key)
                        logger.info(f"🔍 [TEST] Database API Key truncated: {truncated_db_key}")
                        logger.info(f"🔍 [TEST] Received API Key: {api_key}")

                        #Compare post-cut values
                        if api_key == truncated_db_key:
                            #Same, indicating that the user has not changed and uses the full value in the database
                            api_key = db_config.api_key
                            used_db_credentials = True
                            logger.info(f"✅ [TEST] Truncated values match, using complete API Key from database (length: {len(api_key)})")
                        else:
                            #Different, indicating that the user has modified but incompletely
                            logger.error(f"❌ [TEST] Truncated API Key doesn't match database value, user may have modified it incorrectly")
                            return {
                                "success": False,
                                "message": "API Key Malformed: Truncation marker detected but does not match database value, please enter full API Key",
                                "response_time": time.time() - start_time,
                                "details": {
                                    "error": "truncated_key_mismatch",
                                    "received": api_key,
                                    "expected": truncated_db_key
                                }
                            }
                    else:
                        #There is no valid API Key in the database, trying to get it from environment variables
                        logger.info(f"⚠️  [TEST] No valid API Key in database, trying environment variable")
                        env_token = os.getenv('TUSHARE_TOKEN')
                        if env_token:
                            api_key = env_token.strip().strip('"').strip("'")
                            used_env_credentials = True
                            logger.info(f"🔑 [TEST] Using TUSHARE_TOKEN from environment (length: {len(api_key)})")
                        else:
                            logger.error(f"❌ [TEST] No valid API Key in database or environment")
                            return {
                                "success": False,
                                "message": "API Key Invalid: No valid Token configured in database or environment variables",
                                "response_time": time.time() - start_time,
                                "details": None
                            }

                #If API Key is empty, try to get it from a database or environment variable
                elif not api_key:
                    logger.info(f"⚠️  [TEST] API Key is empty, trying to get from database")

                    #Get the full API Key from the database
                    system_config = await self.get_system_config_from_database()
                    db_config = None
                    if system_config:
                        for ds in system_config.data_source_configs:
                            if ds.name == ds_config.name:
                                db_config = ds
                                break

                    if db_config and db_config.api_key and "..." not in db_config.api_key:
                        api_key = db_config.api_key
                        used_db_credentials = True
                        logger.info(f"🔑 [TEST] Using API Key from database (length: {len(api_key)})")
                    else:
                        #If not in the database, try to get it from environmental variables
                        logger.info(f"⚠️  [TEST] No valid API Key in database, trying environment variable")
                        env_token = os.getenv('TUSHARE_TOKEN')
                        if env_token:
                            api_key = env_token.strip().strip('"').strip("'")
                            used_env_credentials = True
                            logger.info(f"🔑 [TEST] Using TUSHARE_TOKEN from environment (length: {len(api_key)})")
                        else:
                            logger.error(f"❌ [TEST] No valid API Key in config, database, or environment")
                            return {
                                "success": False,
                                "message": "API Key Invalid: No valid Token configured in config, database, or environment",
                                "response_time": time.time() - start_time,
                                "details": None
                            }
                else:
                    #API Key is complete, directly used
                    logger.info(f"✅ [TEST] Using complete API Key from config (length: {len(api_key)})")

                #Test Tushare API
                try:
                    logger.info(f"🔌 [TEST] Calling Tushare API with token (length: {len(api_key)})")
                    import tushare as ts
                    ts.set_token(api_key)
                    pro = ts.pro_api()
                    #Get transaction calendar (light test)
                    df = pro.trade_cal(exchange='SSE', start_date='20240101', end_date='20240101')

                    if df is not None and len(df) > 0:
                        response_time = time.time() - start_time
                        logger.info(f"✅ [TEST] Tushare API call successful (response time: {response_time:.2f}s)")

                        #Build messages on which source vouchers are used
                        credential_source = "配置"
                        if used_db_credentials:
                            credential_source = "数据库"
                        elif used_env_credentials:
                            credential_source = "环境变量"

                        return {
                            "success": True,
                            "message": f"Successfully connected to Tushare data source (using credentials from {credential_source})",
                            "response_time": response_time,
                            "details": {
                                "type": ds_type,
                                "test_result": "获取交易日历成功",
                                "credential_source": credential_source,
                                "used_db_credentials": used_db_credentials,
                                "used_env_credentials": used_env_credentials
                            }
                        }
                    else:
                        logger.error(f"❌ [TEST] Tushare API returned empty data")
                        return {
                            "success": False,
                            "message": "Tushare API returned empty data",
                            "response_time": time.time() - start_time,
                            "details": None
                        }
                except ImportError:
                    logger.error(f"❌ [TEST] Tushare library not installed")
                    return {
                        "success": False,
                        "message": "Tushare library not installed, please run: pip install tushare",
                        "response_time": time.time() - start_time,
                        "details": None
                    }
                except Exception as e:
                    logger.error(f"❌ [TEST] Tushare API call failed: {e}")
                    return {
                        "success": False,
                        "message": f"Tushare API call failed: {str(e)}",
                        "response_time": time.time() - start_time,
                        "details": None
                    }

            elif ds_type == "akshare":
                #AKShare doesn't need API Key, direct test.
                try:
                    import akshare as ak
                    #Use lighter interface testing - get transaction calendars
                    #This interface is small, responsive, more suitable for testing connections.
                    df = ak.tool_trade_date_hist_sina()

                    if df is not None and len(df) > 0:
                        response_time = time.time() - start_time
                        return {
                            "success": True,
                            "message": f"Successfully connected to AKShare data source",
                            "response_time": response_time,
                            "details": {
                                "type": ds_type,
                                "test_result": f"Successfully retrieved trade calendar ({len(df)} records)"
                            }
                        }
                    else:
                        return {
                            "success": False,
                            "message": "AKShare API returned empty data",
                            "response_time": time.time() - start_time,
                            "details": None
                        }
                except ImportError:
                    return {
                        "success": False,
                        "message": "AKShare library not installed, please run: pip install akshare",
                        "response_time": time.time() - start_time,
                        "details": None
                    }
                except Exception as e:
                    return {
                        "success": False,
                        "message": f"AKShare API call failed: {str(e)}",
                        "response_time": time.time() - start_time,
                        "details": None
                    }

            elif ds_type == "baostock":
                #BaoStock does not need API Key, direct login
                try:
                    import baostock as bs
                    #Test Login
                    lg = bs.login()

                    if lg.error_code == '0':
                        #Login successful. Test to get data.
                        try:
                            #Get transaction calendar (light test)
                            rs = bs.query_trade_dates(start_date="2024-01-01", end_date="2024-01-01")

                            if rs.error_code == '0':
                                response_time = time.time() - start_time
                                bs.logout()
                                return {
                                    "success": True,
                                    "message": f"Successfully connected to BaoStock data source",
                                    "response_time": response_time,
                                    "details": {
                                        "type": ds_type,
                                        "test_result": "Login successful, trade calendar retrieval successful"
                                    }
                                }
                            else:
                                bs.logout()
                                return {
                                    "success": False,
                                    "message": f"BaoStock data retrieval failed: {rs.error_msg}",
                                    "response_time": time.time() - start_time,
                                    "details": None
                                }
                        except Exception as e:
                            bs.logout()
                            return {
                                "success": False,
                                "message": f"BaoStock data retrieval exception: {str(e)}",
                                "response_time": time.time() - start_time,
                                "details": None
                            }
                    else:
                        return {
                            "success": False,
                            "message": f"BaoStock login failed: {lg.error_msg}",
                            "response_time": time.time() - start_time,
                            "details": None
                        }
                except ImportError:
                    return {
                        "success": False,
                        "message": "BaoStock library not installed, please run: pip install baostock",
                        "response_time": time.time() - start_time,
                        "details": None
                    }
                except Exception as e:
                    return {
                        "success": False,
                        "message": f"BaoStock API call failed: {str(e)}",
                        "response_time": time.time() - start_time,
                        "details": None
                    }

            elif ds_type == "yahoo_finance":
                #Yahoo Finance Test
                if not ds_config.endpoint:
                    ds_config.endpoint = "https://query1.finance.yahoo.com"

                try:
                    url = f"{ds_config.endpoint}/v8/finance/chart/AAPL"
                    params = {"interval": "1d", "range": "1d"}
                    response = requests.get(url, params=params, timeout=10)

                    if response.status_code == 200:
                        data = response.json()
                        if "chart" in data and "result" in data["chart"]:
                            response_time = time.time() - start_time
                            return {
                                "success": True,
                                "message": f"Successfully connected to Yahoo Finance data source",
                                "response_time": response_time,
                                "details": {
                                    "type": ds_type,
                                    "endpoint": ds_config.endpoint,
                                    "test_result": "Successfully retrieved AAPL data"
                                }
                            }

                    return {
                        "success": False,
                        "message": f"Yahoo Finance API returned error: HTTP {response.status_code}",
                        "response_time": time.time() - start_time,
                        "details": None
                    }
                except Exception as e:
                    return {
                        "success": False,
                        "message": f"Yahoo Finance API call failed: {str(e)}",
                        "response_time": time.time() - start_time,
                        "details": None
                    }

            elif ds_type == "alpha_vantage":
                #🔥 If the configuration API Key contains "..." (cut marks), verify whether the original value is unmodified
                if api_key and "..." in api_key:
                    logger.info(f"🔍 [TEST] API Key contains '...' (truncated), checking if it matches database value")

                    #Get the full API Key from the database
                    system_config = await self.get_system_config_from_database()
                    db_config = None
                    if system_config:
                        for ds in system_config.data_source_configs:
                            if ds.name == ds_config.name:
                                db_config = ds
                                break

                    if db_config and db_config.api_key:
                        #Same cut-off process for complete API Key in database
                        truncated_db_key = self._truncate_api_key(db_config.api_key)
                        logger.info(f"🔍 [TEST] Database API Key truncated: {truncated_db_key}")
                        logger.info(f"🔍 [TEST] Received API Key: {api_key}")

                        #Compare post-cut values
                        if api_key == truncated_db_key:
                            #Same, indicating that the user has not changed and uses the full value in the database
                            api_key = db_config.api_key
                            used_db_credentials = True
                            logger.info(f"✅ [TEST] Truncated values match, using complete API Key from database (length: {len(api_key)})")
                        else:
                            #Different, indicating that the user has modified but incompletely
                            logger.error(f"❌ [TEST] Truncated API Key doesn't match database value")
                            return {
                                "success": False,
                                "message": "API Key Malformed: Truncation marker detected but does not match database value, please enter full API Key",
                                "response_time": time.time() - start_time,
                                "details": {
                                    "error": "truncated_key_mismatch",
                                    "received": api_key,
                                    "expected": truncated_db_key
                                }
                            }
                    else:
                        #There is no valid API Key in the database, trying to get it from environment variables
                        logger.info(f"⚠️  [TEST] No valid API Key in database, trying environment variable")
                        env_key = os.getenv('ALPHA_VANTAGE_API_KEY')
                        if env_key:
                            api_key = env_key.strip().strip('"').strip("'")
                            used_env_credentials = True
                            logger.info(f"🔑 [TEST] Using ALPHA_VANTAGE_API_KEY from environment (length: {len(api_key)})")
                        else:
                            logger.error(f"❌ [TEST] No valid API Key in database or environment")
                            return {
                                "success": False,
                                "message": "API Key Invalid: No valid API Key configured in database or environment variables",
                                "response_time": time.time() - start_time,
                                "details": None
                            }

                #If API Key is empty, try to get it from a database or environment variable
                elif not api_key:
                    logger.info(f"⚠️  [TEST] API Key is empty, trying to get from database")

                    #Get the full API Key from the database
                    system_config = await self.get_system_config_from_database()
                    db_config = None
                    if system_config:
                        for ds in system_config.data_source_configs:
                            if ds.name == ds_config.name:
                                db_config = ds
                                break

                    if db_config and db_config.api_key and "..." not in db_config.api_key:
                        api_key = db_config.api_key
                        used_db_credentials = True
                        logger.info(f"🔑 [TEST] Using API Key from database (length: {len(api_key)})")
                    else:
                        #If not in the database, try to get it from environmental variables
                        logger.info(f"⚠️  [TEST] No valid API Key in database, trying environment variable")
                        env_key = os.getenv('ALPHA_VANTAGE_API_KEY')
                        if env_key:
                            api_key = env_key.strip().strip('"').strip("'")
                            used_env_credentials = True
                            logger.info(f"🔑 [TEST] Using ALPHA_VANTAGE_API_KEY from environment (length: {len(api_key)})")
                        else:
                            logger.error(f"❌ [TEST] No valid API Key in config, database, or environment")
                            return {
                                "success": False,
                                "message": "API Key Invalid: No valid API Key configured in config, database, or environment",
                                "response_time": time.time() - start_time,
                                "details": None
                            }
                else:
                    #API Key is complete, directly used
                    logger.info(f"✅ [TEST] Using complete API Key from config (length: {len(api_key)})")

                #Test Alpha Vantage API
                endpoint = ds_config.endpoint or "https://www.alphavantage.co"
                url = f"{endpoint}/query"
                params = {
                    "function": "TIME_SERIES_INTRADAY",
                    "symbol": "IBM",
                    "interval": "5min",
                    "apikey": api_key
                }

                try:
                    logger.info(f"🔌 [TEST] Calling Alpha Vantage API with key (length: {len(api_key)})")
                    response = requests.get(url, params=params, timeout=10)

                    if response.status_code == 200:
                        data = response.json()
                        if "Time Series (5min)" in data or "Meta Data" in data:
                            response_time = time.time() - start_time
                            logger.info(f"✅ [TEST] Alpha Vantage API call successful (response time: {response_time:.2f}s)")

                            #Build messages on which source vouchers are used
                            credential_source = "配置"
                            if used_db_credentials:
                                credential_source = "数据库"
                            elif used_env_credentials:
                                credential_source = "环境变量"

                            return {
                                "success": True,
                                "message": f"Successfully connected to Alpha Vantage data source (using credentials from {credential_source})",
                                "response_time": response_time,
                                "details": {
                                    "type": ds_type,
                                    "endpoint": endpoint,
                                    "test_result": "API Key valid",
                                    "credential_source": credential_source,
                                    "used_db_credentials": used_db_credentials,
                                    "used_env_credentials": used_env_credentials
                                }
                            }
                        elif "Error Message" in data:
                            return {
                                "success": False,
                                "message": f"Alpha Vantage API Error: {data['Error Message']}",
                                "response_time": time.time() - start_time,
                                "details": None
                            }
                        elif "Note" in data:
                            return {
                                "success": False,
                                "message": "API rate limit exceeded, please try again later",
                                "response_time": time.time() - start_time,
                                "details": None
                            }

                    return {
                        "success": False,
                        "message": f"Alpha Vantage API returned error: HTTP {response.status_code}",
                        "response_time": time.time() - start_time,
                        "details": None
                    }
                except Exception as e:
                    return {
                        "success": False,
                        "message": f"Alpha Vantage API call failed: {str(e)}",
                        "response_time": time.time() - start_time,
                        "details": None
                    }

            else:
                #Other data source type - Try to get API Key from environmental variables (if needed)
                #Supported environmental variable mapping
                env_key_map = {
                    "finnhub": "FINNHUB_API_KEY",
                    "polygon": "POLYGON_API_KEY",
                    "iex": "IEX_API_KEY",
                    "quandl": "QUANDL_API_KEY",
                }

                #If API Key does not exist in the configuration, try to get it from the environment variable
                if ds_type in env_key_map and (not api_key or "..." in api_key):
                    env_var_name = env_key_map[ds_type]
                    env_key = os.getenv(env_var_name)
                    if env_key:
                        api_key = env_key.strip()
                        used_env_credentials = True
                        logger.info(f"From the environment variable{ds_type.upper()} API Key ({env_var_name})")

                #Basic Endpoint Test
                if ds_config.endpoint:
                    try:
                        #Add to request if API Key
                        headers = {}
                        params = {}

                        if api_key:
                            #Add API Key based on authentication of different data sources
                            if ds_type == "finnhub":
                                params["token"] = api_key
                            elif ds_type in ["polygon", "alpha_vantage"]:
                                params["apiKey"] = api_key
                            elif ds_type == "iex":
                                params["token"] = api_key
                            else:
                                #Default to use header authentication
                                headers["Authorization"] = f"Bearer {api_key}"

                        response = requests.get(ds_config.endpoint, params=params, headers=headers, timeout=10)
                        response_time = time.time() - start_time

                        if response.status_code < 500:
                            return {
                                "success": True,
                                "message": f"Successfully connected to data source {ds_config.name}",
                                "response_time": response_time,
                                "details": {
                                    "type": ds_type,
                                    "endpoint": ds_config.endpoint,
                                    "status_code": response.status_code,
                                    "used_env_credentials": used_env_credentials
                                }
                            }
                        else:
                            return {
                                "success": False,
                                "message": f"Data source returned server error: HTTP {response.status_code}",
                                "response_time": response_time,
                                "details": None
                            }
                    except Exception as e:
                        return {
                            "success": False,
                            "message": f"Connection failed: {str(e)}",
                            "response_time": time.time() - start_time,
                            "details": None
                        }
                else:
                    return {
                        "success": False,
                        "message": f"Unsupported data source type: {ds_type}, and no endpoint configured",
                        "response_time": time.time() - start_time,
                        "details": None
                    }

        except Exception as e:
            response_time = time.time() - start_time
            logger.error(f"Test source configuration failed:{e}")
            return {
                "success": False,
                "message": f"Connection failed: {str(e)}",
                "response_time": response_time,
                "details": None
            }
    
    async def test_database_config(self, db_config: DatabaseConfig) -> Dict[str, Any]:
        """Test Database Configuration - Real Connection Test"""
        start_time = time.time()
        try:
            db_type = db_config.type.value if hasattr(db_config.type, 'value') else str(db_config.type)

            logger.info(f"Test database configuration:{db_config.name} ({db_type})")
            logger.info(f"Contact address:{db_config.host}:{db_config.port}")

            #Testing according to different database types
            if db_type == "mongodb":
                try:
                    from motor.motor_asyncio import AsyncIOMotorClient
                    import os

                    #🔥 Prefer to complete connection information in environmental variables (including host, username, password)
                    host = db_config.host
                    port = db_config.port
                    username = db_config.username
                    password = db_config.password
                    database = db_config.database
                    auth_source = None
                    used_env_config = False

                    #Test for Docker Environment Medium
                    is_docker = os.path.exists('/.dockerenv') or os.getenv('DOCKER_CONTAINER') == 'true'

                    #If the configuration does not have a username password, try to get the complete configuration from the environment variable
                    if not username or not password:
                        env_host = os.getenv('MONGODB_HOST')
                        env_port = os.getenv('MONGODB_PORT')
                        env_username = os.getenv('MONGODB_USERNAME')
                        env_password = os.getenv('MONGODB_PASSWORD')
                        env_auth_source = os.getenv('MONGODB_AUTH_SOURCE', 'admin')

                        if env_username and env_password:
                            username = env_username
                            password = env_password
                            auth_source = env_auth_source
                            used_env_config = True

                            #Use it if there is a host configuration in the environment variable
                            if env_host:
                                host = env_host
                                #In the Docker environment, replace localhost with mongodb
                                if is_docker and host == 'localhost':
                                    host = 'mongodb'
                                    logger.info(f"Docker environment detected, replacing host from localhost with mongodb")

                            if env_port:
                                port = int(env_port)

                            logger.info(f"Use the MongoDB configuration of the environment variable (host=0){host}, port={port}, authSource={auth_source})")

                    #If no database name exists in the configuration, try to get it from the environment variable
                    if not database:
                        env_database = os.getenv('MONGODB_DATABASE')
                        if env_database:
                            database = env_database
                            logger.info(f"Using database names from environmental variables:{database}")

                    #Fetch authsource from connect parameters (if any)
                    if not auth_source and db_config.connection_params:
                        auth_source = db_config.connection_params.get('authSource')

                    #Build Connection String
                    if username and password:
                        connection_string = f"mongodb://{username}:{password}@{host}:{port}"
                    else:
                        connection_string = f"mongodb://{host}:{port}"

                    if database:
                        connection_string += f"/{database}"

                    #Add connecting parameters
                    params_list = []

                    #Add to parameter if you have authsource
                    if auth_source:
                        params_list.append(f"authSource={auth_source}")

                    #Add other connecting parameters
                    if db_config.connection_params:
                        for k, v in db_config.connection_params.items():
                            if k != 'authSource':  #Authsource has been added
                                params_list.append(f"{k}={v}")

                    if params_list:
                        connection_string += f"?{'&'.join(params_list)}"

                    logger.info(f"Connection string:{connection_string.replace(password or '', '***') if password else connection_string}")

                    #Create client and test connection
                    client = AsyncIOMotorClient(
                        connection_string,
                        serverSelectionTimeoutMS=5000  #Five-second timeout.
                    )

                    #Test access to the database if specified
                    if database:
                        #Test access to specified databases (does not require administrator privileges)
                        db = client[database]
                        #Try to list the pools (if there are no privileges to report errors)
                        collections = await db.list_collection_names()
                        test_result = f"数据库 '{database}' 可访问，包含 {len(collections)} 个集合"
                    else:
                        #Execute ping commands only if no database is specified
                        await client.admin.command('ping')
                        test_result = "连接成功"

                    response_time = time.time() - start_time

                    #Close Connection
                    client.close()

                    return {
                        "success": True,
                        "message": f"Successfully connected to MongoDB database",
                        "response_time": response_time,
                        "details": {
                            "type": db_type,
                            "host": host,
                            "port": port,
                            "database": database,
                            "auth_source": auth_source,
                            "test_result": test_result,
                            "used_env_config": used_env_config
                        }
                    }
                except ImportError:
                    return {
                        "success": False,
                        "message": "Motor library not installed, please run: pip install motor",
                        "response_time": time.time() - start_time,
                        "details": None
                    }
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"The MongoDB connection test failed:{error_msg}")

                    if "Authentication failed" in error_msg or "auth failed" in error_msg.lower():
                        message = "Authentication failed, check username and password"
                    elif "requires authentication" in error_msg.lower():
                        message = "Authentication required, please configure username and password"
                    elif "not authorized" in error_msg.lower():
                        message = "Permission denied, check user privileges"
                    elif "Connection refused" in error_msg:
                        message = "Connection refused, check host and port"
                    elif "timed out" in error_msg.lower():
                        message = "Connection timed out, check network and firewall settings"
                    elif "No servers found" in error_msg:
                        message = "Server not found, check host and port"
                    else:
                        message = f"Connection failed: {error_msg}"

                    return {
                        "success": False,
                        "message": message,
                        "response_time": time.time() - start_time,
                        "details": None
                    }

            elif db_type == "redis":
                try:
                    import redis.asyncio as aioredis
                    import os

                    #🔥 Prefer to complete Redis configuration of environment variables (including host, password)
                    host = db_config.host
                    port = db_config.port
                    password = db_config.password
                    database = db_config.database
                    used_env_config = False

                    #Test for Docker Environment Medium
                    is_docker = os.path.exists('/.dockerenv') or os.getenv('DOCKER_CONTAINER') == 'true'

                    #Try to get a complete configuration from an environment variable if the configuration does not have a password
                    if not password:
                        env_host = os.getenv('REDIS_HOST')
                        env_port = os.getenv('REDIS_PORT')
                        env_password = os.getenv('REDIS_PASSWORD')

                        if env_password:
                            password = env_password
                            used_env_config = True

                            #Use it if there is a host configuration in the environment variable
                            if env_host:
                                host = env_host
                                #In the Docker environment, replace localhost with redis
                                if is_docker and host == 'localhost':
                                    host = 'redis'
                                    logger.info(f"Docker environment detected, Redis host from localhost to redis")

                            if env_port:
                                port = int(env_port)

                            logger.info(f"Use the Redis configuration of the environment variable (host=){host}, port={port})")

                    #Try to get it from an environmental variable if the configuration does not have a database number
                    if database is None:
                        env_db = os.getenv('REDIS_DB')
                        if env_db:
                            database = int(env_db)
                            logger.info(f"📦 uses the Redis database number from the environment variable:{database}")

                    #Build connection parameters
                    redis_params = {
                        "host": host,
                        "port": port,
                        "decode_responses": True,
                        "socket_connect_timeout": 5
                    }

                    if password:
                        redis_params["password"] = password

                    if database is not None:
                        redis_params["db"] = int(database)

                    #Create Connection and Test
                    redis_client = await aioredis.from_url(
                        f"redis://{host}:{port}",
                        **redis_params
                    )

                    #Execute PING Command
                    await redis_client.ping()

                    #Get Server Information
                    info = await redis_client.info("server")

                    response_time = time.time() - start_time

                    #Close Connection
                    await redis_client.close()

                    return {
                        "success": True,
                        "message": f"Successfully connected to Redis database",
                        "response_time": response_time,
                        "details": {
                            "type": db_type,
                            "host": host,
                            "port": port,
                            "database": database,
                            "redis_version": info.get("redis_version", "unknown"),
                            "used_env_config": used_env_config
                        }
                    }
                except ImportError:
                    return {
                        "success": False,
                        "message": "Redis library not installed, please run: pip install redis",
                        "response_time": time.time() - start_time,
                        "details": None
                    }
                except Exception as e:
                    error_msg = str(e)
                    if "WRONGPASS" in error_msg or "Authentication" in error_msg:
                        message = "Authentication failed, check password"
                    elif "Connection refused" in error_msg:
                        message = "Connection refused, check host and port"
                    elif "timed out" in error_msg.lower():
                        message = "Connection timed out, check network and firewall settings"
                    else:
                        message = f"Connection failed: {error_msg}"

                    return {
                        "success": False,
                        "message": message,
                        "response_time": time.time() - start_time,
                        "details": None
                    }

            elif db_type == "mysql":
                try:
                    import aiomysql

                    #Create Connection
                    conn = await aiomysql.connect(
                        host=db_config.host,
                        port=db_config.port,
                        user=db_config.username,
                        password=db_config.password,
                        db=db_config.database,
                        connect_timeout=5
                    )

                    #Execute test queries
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT VERSION()")
                        version = await cursor.fetchone()

                    response_time = time.time() - start_time

                    #Close Connection
                    conn.close()

                    return {
                        "success": True,
                        "message": f"Successfully connected to MySQL database",
                        "response_time": response_time,
                        "details": {
                            "type": db_type,
                            "host": db_config.host,
                            "port": db_config.port,
                            "database": db_config.database,
                            "version": version[0] if version else "unknown"
                        }
                    }
                except ImportError:
                    return {
                        "success": False,
                        "message": "aiomysql library not installed, please run: pip install aiomysql",
                        "response_time": time.time() - start_time,
                        "details": None
                    }
                except Exception as e:
                    error_msg = str(e)
                    if "Access denied" in error_msg:
                        message = "Access denied, check username and password"
                    elif "Unknown database" in error_msg:
                        message = f"Database '{db_config.database}' does not exist"
                    elif "Can't connect" in error_msg:
                        message = "Cannot connect, check host and port"
                    else:
                        message = f"Connection failed: {error_msg}"

                    return {
                        "success": False,
                        "message": message,
                        "response_time": time.time() - start_time,
                        "details": None
                    }

            elif db_type == "postgresql":
                try:
                    import asyncpg

                    #Create Connection
                    conn = await asyncpg.connect(
                        host=db_config.host,
                        port=db_config.port,
                        user=db_config.username,
                        password=db_config.password,
                        database=db_config.database,
                        timeout=5
                    )

                    #Execute test queries
                    version = await conn.fetchval("SELECT version()")

                    response_time = time.time() - start_time

                    #Close Connection
                    await conn.close()

                    return {
                        "success": True,
                        "message": f"Successfully connected to PostgreSQL database",
                        "response_time": response_time,
                        "details": {
                            "type": db_type,
                            "host": db_config.host,
                            "port": db_config.port,
                            "database": db_config.database,
                            "version": version.split()[1] if version else "unknown"
                        }
                    }
                except ImportError:
                    return {
                        "success": False,
                        "message": "asyncpg library not installed, please run: pip install asyncpg",
                        "response_time": time.time() - start_time,
                        "details": None
                    }
                except Exception as e:
                    error_msg = str(e)
                    if "password authentication failed" in error_msg:
                        message = "Password authentication failed, check username and password"
                    elif "does not exist" in error_msg:
                        message = f"Database '{db_config.database}' does not exist"
                    elif "Connection refused" in error_msg:
                        message = "Connection refused, check host and port"
                    else:
                        message = f"Connection failed: {error_msg}"

                    return {
                        "success": False,
                        "message": message,
                        "response_time": time.time() - start_time,
                        "details": None
                    }

            elif db_type == "sqlite":
                try:
                    import aiosqlite

                    #Use file path for SQLite, do not need host/port
                    db_path = db_config.database or db_config.host

                    #Create Connection
                    async with aiosqlite.connect(db_path, timeout=5) as conn:
                        #Execute test queries
                        async with conn.execute("SELECT sqlite_version()") as cursor:
                            version = await cursor.fetchone()

                    response_time = time.time() - start_time

                    return {
                        "success": True,
                        "message": f"Successfully connected to SQLite database",
                        "response_time": response_time,
                        "details": {
                            "type": db_type,
                            "database": db_path,
                            "version": version[0] if version else "unknown"
                        }
                    }
                except ImportError:
                    return {
                        "success": False,
                        "message": "aiosqlite library not installed, please run: pip install aiosqlite",
                        "response_time": time.time() - start_time,
                        "details": None
                    }
                except Exception as e:
                    return {
                        "success": False,
                        "message": f"Connection failed: {str(e)}",
                        "response_time": time.time() - start_time,
                        "details": None
                    }

            else:
                return {
                    "success": False,
                    "message": f"Unsupported database type: {db_type}",
                    "response_time": time.time() - start_time,
                    "details": None
                }

        except Exception as e:
            response_time = time.time() - start_time
            logger.error(f"Test database configuration failed:{e}")
            return {
                "success": False,
                "message": f"Connection failed: {str(e)}",
                "response_time": response_time,
                "details": None
            }

    #== sync, corrected by elderman == @elder man

    async def add_database_config(self, db_config: DatabaseConfig) -> bool:
        """Add Database Configuration"""
        try:
            logger.info(f"Add database configuration:{db_config.name}")

            config = await self.get_system_config_from_database()
            if not config:
                logger.error("System configuration is empty")
                return False

            #Check if the same name configuration exists
            for existing_db in config.database_configs:
                if existing_db.name == db_config.name:
                    logger.error(f"❌ Database Configuration '{db_config.name}' Exists")
                    return False

            #Add New Profile
            config.database_configs.append(db_config)

            #Save Configuration
            result = await self.save_system_config(config)
            if result:
                logger.info(f"✅ Database Configuration '{db_config.name}'Add succeeded")
            else:
                logger.error(f"❌ Database Configuration '{db_config.name}'Add failed")

            return result

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            import traceback
            traceback.print_exc()
            return False

    async def update_database_config(self, db_config: DatabaseConfig) -> bool:
        """Update Database Configuration"""
        try:
            logger.info(f"Update the database configuration:{db_config.name}")

            config = await self.get_system_config_from_database()
            if not config:
                logger.error("System configuration is empty")
                return False

            #Find and update configuration
            found = False
            for i, existing_db in enumerate(config.database_configs):
                if existing_db.name == db_config.name:
                    config.database_configs[i] = db_config
                    found = True
                    break

            if not found:
                logger.error(f"❌ Database Configuration '{db_config.name}'None")
                return False

            #Save Configuration
            result = await self.save_system_config(config)
            if result:
                logger.info(f"✅ Database Configuration '{db_config.name}Update Successful")
            else:
                logger.error(f"❌ Database Configuration '{db_config.name}Update failed")

            return result

        except Exception as e:
            logger.error(f"Update of database configuration failed:{e}")
            import traceback
            traceback.print_exc()
            return False

    async def delete_database_config(self, db_name: str) -> bool:
        """Delete Database Configuration"""
        try:
            logger.info(f"Delete database configuration:{db_name}")

            config = await self.get_system_config_from_database()
            if not config:
                logger.error("System configuration is empty")
                return False

            #Original number recorded
            original_count = len(config.database_configs)

            #Remove specified configuration
            config.database_configs = [
                db for db in config.database_configs
                if db.name != db_name
            ]

            new_count = len(config.database_configs)

            if new_count == original_count:
                logger.error(f"❌ Database Configuration '{db_name}'None")
                return False

            #Save Configuration
            result = await self.save_system_config(config)
            if result:
                logger.info(f"✅ Database Configuration '{db_name}' Deletion succeeded")
            else:
                logger.error(f"❌ Database Configuration '{db_name}' Deletion failed")

            return result

        except Exception as e:
            logger.error(f"Delete database configuration failed:{e}")
            import traceback
            traceback.print_exc()
            return False

    async def get_database_config(self, db_name: str) -> Optional[DatabaseConfig]:
        """Get specified database configuration"""
        try:
            config = await self.get_system_config_from_database()
            if not config:
                return None

            for db in config.database_configs:
                if db.name == db_name:
                    return db

            return None

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return None

    async def get_database_configs(self) -> List[DatabaseConfig]:
        """Get All Database Configurations"""
        try:
            config = await self.get_system_config_from_database()
            if not config:
                return []

            return config.database_configs

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return []

    #== sync, corrected by elderman == @elder man

    async def get_model_catalog(self) -> List[ModelCatalog]:
        """Fetch all model directories"""
        try:
            db = await self._get_db_async()
            catalog_collection = db.model_catalog

            catalogs = []
            async for doc in catalog_collection.find():
                catalogs.append(ModelCatalog(**doc))

            return catalogs
        except Exception as e:
            print(f"Failed to get model catalog: {e}")
            return []

    async def get_provider_models(self, provider: str) -> Optional[ModelCatalog]:
        """Retrieving a model directory of specified manufacturers"""
        try:
            db = await self._get_db_async()
            catalog_collection = db.model_catalog

            doc = await catalog_collection.find_one({"provider": provider})
            if doc:
                return ModelCatalog(**doc)
            return None
        except Exception as e:
            print(f"Failed to get provider model catalog: {e}")
            return None

    async def save_model_catalog(self, catalog: ModelCatalog) -> bool:
        """Save or update the model directory"""
        try:
            db = await self._get_db_async()
            catalog_collection = db.model_catalog

            catalog.updated_at = now_tz()

            #Update or Insert
            result = await catalog_collection.replace_one(
                {"provider": catalog.provider},
                catalog.model_dump(by_alias=True, exclude={"id"}),
                upsert=True
            )

            return result.acknowledged
        except Exception as e:
            print(f"Failed to save model catalog: {e}")
            return False

    async def delete_model_catalog(self, provider: str) -> bool:
        """Remove model directory"""
        try:
            db = await self._get_db_async()
            catalog_collection = db.model_catalog

            result = await catalog_collection.delete_one({"provider": provider})
            return result.deleted_count > 0
        except Exception as e:
            print(f"Failed to delete model catalog: {e}")
            return False

    async def init_default_model_catalog(self) -> bool:
        """Initialize the default model directory"""
        try:
            db = await self._get_db_async()
            catalog_collection = db.model_catalog

            #Check if data is available
            count = await catalog_collection.count_documents({})
            if count > 0:
                print("Model catalog exists, skipping initialization")
                return True

            #Create Default Directory
            default_catalogs = self._get_default_model_catalog()

            for catalog_data in default_catalogs:
                catalog = ModelCatalog(**catalog_data)
                await self.save_model_catalog(catalog)

            print(f"✅ Initialized model catalogs for {len(default_catalogs)} providers")
            return True
        except Exception as e:
            print(f"Failed to initialize model catalog: {e}")
            return False

    def _get_default_model_catalog(self) -> List[Dict[str, Any]]:
        """Fetching default model directory data"""
        return [
            {
                "provider": "dashscope",
                "provider_name": "通义千问",
                "models": [
                    {
                        "name": "qwen-turbo",
                        "display_name": "Qwen Turbo - 快速经济 (1M上下文)",
                        "input_price_per_1k": 0.0003,
                        "output_price_per_1k": 0.0003,
                        "context_length": 1000000,
                        "currency": "CNY",
                        "description": "Qwen2.5-Turbo，支持100万tokens超长上下文"
                    },
                    {
                        "name": "qwen-plus",
                        "display_name": "Qwen Plus - 平衡推荐",
                        "input_price_per_1k": 0.0008,
                        "output_price_per_1k": 0.002,
                        "context_length": 32768,
                        "currency": "CNY"
                    },
                    {
                        "name": "qwen-plus-latest",
                        "display_name": "Qwen Plus Latest - 最新平衡",
                        "input_price_per_1k": 0.0008,
                        "output_price_per_1k": 0.002,
                        "context_length": 32768,
                        "currency": "CNY"
                    },
                    {
                        "name": "qwen-max",
                        "display_name": "Qwen Max - 最强性能",
                        "input_price_per_1k": 0.02,
                        "output_price_per_1k": 0.06,
                        "context_length": 8192,
                        "currency": "CNY"
                    },
                    {
                        "name": "qwen-max-latest",
                        "display_name": "Qwen Max Latest - 最新旗舰",
                        "input_price_per_1k": 0.02,
                        "output_price_per_1k": 0.06,
                        "context_length": 8192,
                        "currency": "CNY"
                    },
                    {
                        "name": "qwen-long",
                        "display_name": "Qwen Long - 长文本",
                        "input_price_per_1k": 0.0005,
                        "output_price_per_1k": 0.002,
                        "context_length": 1000000,
                        "currency": "CNY"
                    },
                    {
                        "name": "qwen-vl-plus",
                        "display_name": "Qwen VL Plus - 视觉理解",
                        "input_price_per_1k": 0.008,
                        "output_price_per_1k": 0.008,
                        "context_length": 8192,
                        "currency": "CNY"
                    },
                    {
                        "name": "qwen-vl-max",
                        "display_name": "Qwen VL Max - 视觉旗舰",
                        "input_price_per_1k": 0.02,
                        "output_price_per_1k": 0.02,
                        "context_length": 8192,
                        "currency": "CNY"
                    }
                ]
            },
            {
                "provider": "openai",
                "provider_name": "OpenAI",
                "models": [
                    {
                        "name": "gpt-4o",
                        "display_name": "GPT-4o - 最新旗舰",
                        "input_price_per_1k": 0.005,
                        "output_price_per_1k": 0.015,
                        "context_length": 128000,
                        "currency": "USD"
                    },
                    {
                        "name": "gpt-4o-mini",
                        "display_name": "GPT-4o Mini - 轻量旗舰",
                        "input_price_per_1k": 0.00015,
                        "output_price_per_1k": 0.0006,
                        "context_length": 128000,
                        "currency": "USD"
                    },
                    {
                        "name": "gpt-4-turbo",
                        "display_name": "GPT-4 Turbo - 强化版",
                        "input_price_per_1k": 0.01,
                        "output_price_per_1k": 0.03,
                        "context_length": 128000,
                        "currency": "USD"
                    },
                    {
                        "name": "gpt-4",
                        "display_name": "GPT-4 - 经典版",
                        "input_price_per_1k": 0.03,
                        "output_price_per_1k": 0.06,
                        "context_length": 8192,
                        "currency": "USD"
                    },
                    {
                        "name": "gpt-3.5-turbo",
                        "display_name": "GPT-3.5 Turbo - 经济版",
                        "input_price_per_1k": 0.0005,
                        "output_price_per_1k": 0.0015,
                        "context_length": 16385,
                        "currency": "USD"
                    }
                ]
            },
            {
                "provider": "google",
                "provider_name": "Google Gemini",
                "models": [
                    {
                        "name": "gemini-2.5-pro",
                        "display_name": "Gemini 2.5 Pro - 最新旗舰",
                        "input_price_per_1k": 0.00125,
                        "output_price_per_1k": 0.005,
                        "context_length": 1000000,
                        "currency": "USD"
                    },
                    {
                        "name": "gemini-2.5-flash",
                        "display_name": "Gemini 2.5 Flash - 最新快速",
                        "input_price_per_1k": 0.000075,
                        "output_price_per_1k": 0.0003,
                        "context_length": 1000000,
                        "currency": "USD"
                    },
                    {
                        "name": "gemini-1.5-pro",
                        "display_name": "Gemini 1.5 Pro - 专业版",
                        "input_price_per_1k": 0.00125,
                        "output_price_per_1k": 0.005,
                        "context_length": 2000000,
                        "currency": "USD"
                    },
                    {
                        "name": "gemini-1.5-flash",
                        "display_name": "Gemini 1.5 Flash - 快速版",
                        "input_price_per_1k": 0.000075,
                        "output_price_per_1k": 0.0003,
                        "context_length": 1000000,
                        "currency": "USD"
                    }
                ]
            },
            {
                "provider": "deepseek",
                "provider_name": "DeepSeek",
                "models": [
                    {
                        "name": "deepseek-chat",
                        "display_name": "DeepSeek Chat - 通用对话",
                        "input_price_per_1k": 0.0001,
                        "output_price_per_1k": 0.0002,
                        "context_length": 32768,
                        "currency": "CNY"
                    },
                    {
                        "name": "deepseek-coder",
                        "display_name": "DeepSeek Coder - 代码专用",
                        "input_price_per_1k": 0.0001,
                        "output_price_per_1k": 0.0002,
                        "context_length": 16384,
                        "currency": "CNY"
                    }
                ]
            },
            {
                "provider": "anthropic",
                "provider_name": "Anthropic Claude",
                "models": [
                    {
                        "name": "claude-3-5-sonnet-20241022",
                        "display_name": "Claude 3.5 Sonnet - 当前旗舰",
                        "input_price_per_1k": 0.003,
                        "output_price_per_1k": 0.015,
                        "context_length": 200000,
                        "currency": "USD"
                    },
                    {
                        "name": "claude-3-5-sonnet-20240620",
                        "display_name": "Claude 3.5 Sonnet (旧版)",
                        "input_price_per_1k": 0.003,
                        "output_price_per_1k": 0.015,
                        "context_length": 200000,
                        "currency": "USD"
                    },
                    {
                        "name": "claude-3-opus-20240229",
                        "display_name": "Claude 3 Opus - 强大性能",
                        "input_price_per_1k": 0.015,
                        "output_price_per_1k": 0.075,
                        "context_length": 200000,
                        "currency": "USD"
                    },
                    {
                        "name": "claude-3-sonnet-20240229",
                        "display_name": "Claude 3 Sonnet - 平衡版",
                        "input_price_per_1k": 0.003,
                        "output_price_per_1k": 0.015,
                        "context_length": 200000,
                        "currency": "USD"
                    },
                    {
                        "name": "claude-3-haiku-20240307",
                        "display_name": "Claude 3 Haiku - 快速版",
                        "input_price_per_1k": 0.00025,
                        "output_price_per_1k": 0.00125,
                        "context_length": 200000,
                        "currency": "USD"
                    }
                ]
            },
            {
                "provider": "qianfan",
                "provider_name": "百度千帆",
                "models": [
                    {
                        "name": "ernie-3.5-8k",
                        "display_name": "ERNIE 3.5 8K - 快速高效",
                        "input_price_per_1k": 0.0012,
                        "output_price_per_1k": 0.0012,
                        "context_length": 8192,
                        "currency": "CNY"
                    },
                    {
                        "name": "ernie-4.0-turbo-8k",
                        "display_name": "ERNIE 4.0 Turbo 8K - 强大推理",
                        "input_price_per_1k": 0.03,
                        "output_price_per_1k": 0.09,
                        "context_length": 8192,
                        "currency": "CNY"
                    },
                    {
                        "name": "ERNIE-Speed-8K",
                        "display_name": "ERNIE Speed 8K - 极速响应",
                        "input_price_per_1k": 0.0004,
                        "output_price_per_1k": 0.0004,
                        "context_length": 8192,
                        "currency": "CNY"
                    },
                    {
                        "name": "ERNIE-Lite-8K",
                        "display_name": "ERNIE Lite 8K - 轻量经济",
                        "input_price_per_1k": 0.0003,
                        "output_price_per_1k": 0.0006,
                        "context_length": 8192,
                        "currency": "CNY"
                    }
                ]
            },
            {
                "provider": "zhipu",
                "provider_name": "智谱AI",
                "models": [
                    {
                        "name": "glm-4",
                        "display_name": "GLM-4 - 旗舰版",
                        "input_price_per_1k": 0.1,
                        "output_price_per_1k": 0.1,
                        "context_length": 128000,
                        "currency": "CNY"
                    },
                    {
                        "name": "glm-4-plus",
                        "display_name": "GLM-4 Plus - 增强版",
                        "input_price_per_1k": 0.05,
                        "output_price_per_1k": 0.05,
                        "context_length": 128000,
                        "currency": "CNY"
                    },
                    {
                        "name": "glm-3-turbo",
                        "display_name": "GLM-3 Turbo - 快速版",
                        "input_price_per_1k": 0.001,
                        "output_price_per_1k": 0.001,
                        "context_length": 128000,
                        "currency": "CNY"
                    }
                ]
            }
        ]

    async def get_available_models(self) -> List[Dict[str, Any]]:
        """Get a list of available models (read from the database, return default data if empty)"""
        try:
            catalogs = await self.get_model_catalog()

            #Initialize the default directory if no data is available in the database
            if not catalogs:
                print("📦 Model catalog empty, initializing default catalog...")
                await self.init_default_model_catalog()
                catalogs = await self.get_model_catalog()

            #Convert to API Response Format
            result = []
            for catalog in catalogs:
                result.append({
                    "provider": catalog.provider,
                    "provider_name": catalog.provider_name,
                    "models": [
                        {
                            "name": model.name,
                            "display_name": model.display_name,
                            "description": model.description,
                            "context_length": model.context_length,
                            "input_price_per_1k": model.input_price_per_1k,
                            "output_price_per_1k": model.output_price_per_1k,
                            "is_deprecated": model.is_deprecated
                        }
                        for model in catalog.models
                    ]
                })

            return result
        except Exception as e:
            print(f"Failed to get model list: {e}")
            #Returns default data on failure
            return self._get_default_model_catalog()


    async def set_default_llm(self, model_name: str) -> bool:
        """Set Default Large Model"""
        try:
            config = await self.get_system_config_from_database()
            if not config:
                return False

            #Check if the model exists.
            model_exists = any(
                llm.model_name == model_name
                for llm in config.llm_configs
            )

            if not model_exists:
                return False

            config.default_llm = model_name
            return await self.save_system_config(config)
        except Exception as e:
            print(f"Failed to set default LLM: {e}")
            return False

    async def set_default_data_source(self, source_name: str) -> bool:
        """Set Default Data Source"""
        try:
            config = await self.get_system_config_from_database()
            if not config:
                return False

            #Check for data sources.
            source_exists = any(
                ds.name == source_name
                for ds in config.data_source_configs
            )

            if not source_exists:
                return False

            config.default_data_source = source_name
            return await self.save_system_config(config)
        except Exception as e:
            print(f"Failed to set default data source: {e}")
            return False

    #== sync, corrected by elderman ==

    async def get_llm_providers(self) -> List[LLMProvider]:
        """Access to all large model manufacturers (consolidated environmental variable configuration)"""
        try:
            db = await self._get_db_async()
            providers_collection = db.llm_providers

            providers_data = await providers_collection.find().to_list(length=None)
            providers = []

            logger.info(f"[get llm providers]{len(providers_data)}Vendors")

            for provider_data in providers_data:
                provider = LLMProvider(**provider_data)

                #API Key in the database is judged to be valid
                db_key_valid = self._is_valid_api_key(provider.api_key)
                logger.info(f"[get llm providers]{provider.display_name} ({provider.name}Database Key Valid={db_key_valid}")

                #Initialize extra config
                provider.extra_config = provider.extra_config or {}

                if not db_key_valid:
                    #Key in database is invalid, trying to get from environment variables
                    logger.info(f"[Get llm providers]{provider.name}API key...")
                    env_key = self._get_env_api_key(provider.name)
                    if env_key:
                        provider.api_key = env_key
                        provider.extra_config["source"] = "environment"
                        provider.extra_config["has_api_key"] = True
                        logger.info(f"[get llm providers]{provider.display_name}Get API Keys")
                    else:
                        provider.extra_config["has_api_key"] = False
                        logger.warning(f"[Get llm providers]{provider.display_name}Database configuration and environmental variables are not configured with valid API keys")
                else:
                    #Key in database is effective, using database configuration
                    provider.extra_config["source"] = "database"
                    provider.extra_config["has_api_key"] = True
                    logger.info(f"[get llm providers]{provider.display_name}API Key")

                providers.append(provider)

            logger.info(f"[get llm providers]{len(providers)}Vendors")
            return providers
        except Exception as e:
            logger.error(f"Can not get folder: %s: %s{e}", exc_info=True)
            return []

    def _is_valid_api_key(self, api_key: Optional[str]) -> bool:
        """Determines whether API Key is valid

        Conditions of validity:
        Key is not empty
        Key is not a placeholder (not beginning with 'your ' or 'your-', not ending with 'here')
        Key is not a cut-off key (does not contain '...')
        4. Key length > 10 (basic format validation)

        Args:
            api key: API Key to be validated

        Returns:
            Bool: True is valid, False is invalid
        """
        if not api_key:
            return False

        #Remove First End Space
        api_key = api_key.strip()

        #Check if empty
        if not api_key:
            return False

        #Check for placeholder (prefix)
        if api_key.startswith('your_') or api_key.startswith('your-'):
            return False

        #Check for placeholders (suffix)
        if api_key.endswith('_here') or api_key.endswith('-here'):
            return False

        #Check for cut-off keys (includes '...')
        if '...' in api_key:
            return False

        #Check length (most API Key > 10 characters)
        if len(api_key) <= 10:
            return False

        return True

    def _get_env_api_key(self, provider_name: str) -> Optional[str]:
        """Fetching API Keys from Environmental Variables"""
        import os

        #Environmental Variable Map
        env_key_mapping = {
            "openai": "OPENAI_API_KEY",
            "anthropic": "ANTHROPIC_API_KEY",
            "google": "GOOGLE_API_KEY",
            "zhipu": "ZHIPU_API_KEY",
            "deepseek": "DEEPSEEK_API_KEY",
            "dashscope": "DASHSCOPE_API_KEY",
            "qianfan": "QIANFAN_API_KEY",
            "azure": "AZURE_OPENAI_API_KEY",
            "siliconflow": "SILICONFLOW_API_KEY",
            "openrouter": "OPENROUTER_API_KEY",
            #Convergence Channel
            "302ai": "AI302_API_KEY",
            "oneapi": "ONEAPI_API_KEY",
            "newapi": "NEWAPI_API_KEY",
            "custom_aggregator": "CUSTOM_AGGREGATOR_API_KEY"
        }

        env_var = env_key_mapping.get(provider_name)
        if env_var:
            api_key = os.getenv(env_var)
            #Use of uniform authentication methods
            if self._is_valid_api_key(api_key):
                return api_key

        return None

    async def add_llm_provider(self, provider: LLMProvider) -> str:
        """Add Big Modeler"""
        try:
            db = await self._get_db_async()
            providers_collection = db.llm_providers

            #Check if the name of the factory exists.
            existing = await providers_collection.find_one({"name": provider.name})
            if existing:
                raise ValueError(f"Provider {provider.name} already exists")

            provider.created_at = now_tz()
            provider.updated_at = now_tz()

            #Fix: Delete  id field and allow MongoDB to automatically generate ObjectId
            provider_data = provider.model_dump(by_alias=True, exclude_unset=True)
            if "_id" in provider_data:
                del provider_data["_id"]

            result = await providers_collection.insert_one(provider_data)
            return str(result.inserted_id)
        except Exception as e:
            print(f"Failed to add provider: {e}")
            raise

    async def update_llm_provider(self, provider_id: str, update_data: Dict[str, Any]) -> bool:
        """Update the big modeler."""
        try:
            db = await self._get_db_async()
            providers_collection = db.llm_providers

            update_data["updated_at"] = now_tz()

            #Compatible processing: Try two types of objectId and string
            #Reason: Historical data may mix ObjectId and string as  id
            try:
                #Try first as ObjectId query
                result = await providers_collection.update_one(
                    {"_id": ObjectId(provider_id)},
                    {"$set": update_data}
                )

                #If not matched, try again as a string query
                if result.matched_count == 0:
                    result = await providers_collection.update_one(
                        {"_id": provider_id},
                        {"$set": update_data}
                    )
            except Exception:
                #If ObjectiveId conversion fails, ask directly with string
                result = await providers_collection.update_one(
                    {"_id": provider_id},
                    {"$set": update_data}
                )

            #Restoration: made count > 0 indicates that the record was found (even if not modified)
            #Modified count > 0 is only true when the field is actually modified
            #If records exist but have the same value, Modified count is 0, but this should not return 404
            return result.matched_count > 0
        except Exception as e:
            print(f"Failed to update provider: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def delete_llm_provider(self, provider_id: str) -> bool:
        """Remove Large Modeler"""
        try:
            print(f"🗑️ Deleting provider - provider_id: {provider_id}")
            print(f"🔍 ObjectId type: {type(ObjectId(provider_id))}")

            db = await self._get_db_async()
            providers_collection = db.llm_providers
            print(f"📊 Database: {db.name}, Collection: {providers_collection.name}")

            #List all the manufacturers' IDs and check the format.
            all_providers = await providers_collection.find({}, {"_id": 1, "display_name": 1}).to_list(length=None)
            print(f"📋 All provider IDs in database:")
            for p in all_providers:
                print(f"   - {p['_id']} ({type(p['_id'])}) - {p.get('display_name')}")
                if str(p['_id']) == provider_id:
                    print(f"   ✅ Found matching ID!")

            #Try different search methods
            print(f"🔍 Attempting lookup via ObjectId...")
            existing1 = await providers_collection.find_one({"_id": ObjectId(provider_id)})

            print(f"🔍 Attempting lookup via String...")
            existing2 = await providers_collection.find_one({"_id": provider_id})

            print(f"🔍 ObjectId lookup result: {existing1 is not None}")
            print(f"🔍 String lookup result: {existing2 is not None}")

            existing = existing1 or existing2
            if not existing:
                print(f"❌ Provider not found via either method: {provider_id}")
                return False

            print(f"✅ Found provider: {existing.get('display_name')}")

            #Delete using the found method
            if existing1:
                result = await providers_collection.delete_one({"_id": ObjectId(provider_id)})
            else:
                result = await providers_collection.delete_one({"_id": provider_id})

            success = result.deleted_count > 0

            print(f"🗑️ Deletion result: {success}, deleted_count: {result.deleted_count}")
            return success

        except Exception as e:
            print(f"❌ Failed to delete provider: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def toggle_llm_provider(self, provider_id: str, is_active: bool) -> bool:
        """Toggle large modeler state"""
        try:
            db = await self._get_db_async()
            providers_collection = db.llm_providers

            #Compatible processing: Try two types of objectId and string
            try:
                #Try first as ObjectId query
                result = await providers_collection.update_one(
                    {"_id": ObjectId(provider_id)},
                    {"$set": {"is_active": is_active, "updated_at": now_tz()}}
                )

                #If not matched, try again as a string query
                if result.matched_count == 0:
                    result = await providers_collection.update_one(
                        {"_id": provider_id},
                        {"$set": {"is_active": is_active, "updated_at": now_tz()}}
                    )
            except Exception:
                #If ObjectiveId conversion fails, ask directly with string
                result = await providers_collection.update_one(
                    {"_id": provider_id},
                    {"$set": {"is_active": is_active, "updated_at": now_tz()}}
                )

            return result.matched_count > 0
        except Exception as e:
            print(f"Failed to toggle provider status: {e}")
            return False

    async def init_aggregator_providers(self) -> Dict[str, Any]:
        """Initialized polymer channel plant configuration

        Returns:
            Initialization Results Statistics
        """
        from app.constants.model_capabilities import AGGREGATOR_PROVIDERS

        try:
            db = await self._get_db_async()
            providers_collection = db.llm_providers

            added_count = 0
            skipped_count = 0
            updated_count = 0

            for provider_name, config in AGGREGATOR_PROVIDERS.items():
                #API Key
                api_key = self._get_env_api_key(provider_name)

                #Check for presence
                existing = await providers_collection.find_one({"name": provider_name})

                if existing:
                    #Update if an API Key exists but is not available and the environment variable is available
                    if not existing.get("api_key") and api_key:
                        update_data = {
                            "api_key": api_key,
                            "is_active": True,  #Automatically enabled with API Key
                            "updated_at": now_tz()
                        }
                        await providers_collection.update_one(
                            {"name": provider_name},
                            {"$set": update_data}
                        )
                        updated_count += 1
                        print(f"✅ Updated API Key for aggregator channel {config['display_name']}")
                    else:
                        skipped_count += 1
                        print(f"⏭️ Aggregator channel {config['display_name']} exists, skipping")
                    continue

                #Create polymer channel plant configuration
                provider_data = {
                    "name": provider_name,
                    "display_name": config["display_name"],
                    "description": config["description"],
                    "website": config.get("website"),
                    "api_doc_url": config.get("api_doc_url"),
                    "default_base_url": config["default_base_url"],
                    "is_active": bool(api_key),  #Automatically enabled with API Key
                    "supported_features": ["chat", "completion", "function_calling", "streaming"],
                    "api_key": api_key or "",
                    "extra_config": {
                        "supported_providers": config.get("supported_providers", []),
                        "source": "environment" if api_key else "manual"
                    },
                    #Syndication channel identification
                    "is_aggregator": True,
                    "aggregator_type": "openai_compatible",
                    "model_name_format": config.get("model_name_format", "{provider}/{model}"),
                    "created_at": now_tz(),
                    "updated_at": now_tz()
                }

                provider = LLMProvider(**provider_data)
                #Fix: Delete  id field and allow MongoDB to automatically generate ObjectId
                insert_data = provider.model_dump(by_alias=True, exclude_unset=True)
                if "_id" in insert_data:
                    del insert_data["_id"]
                await providers_collection.insert_one(insert_data)
                added_count += 1

                if api_key:
                    print(f"✅ Added aggregator channel: {config['display_name']} (API Key retrieved from env)")
                else:
                    print(f"✅ Added aggregator channel: {config['display_name']} (Manual API Key config required)")

            message_parts = []
            if added_count > 0:
                message_parts.append(f"Successfully added {added_count} aggregator channels")
            if updated_count > 0:
                message_parts.append(f"Updated {updated_count}")
            if skipped_count > 0:
                message_parts.append(f"Skipped {skipped_count} existing")

            return {
                "success": True,
                "added": added_count,
                "updated": updated_count,
                "skipped": skipped_count,
                "message": ", ".join(message_parts) if message_parts else "No changes"
            }

        except Exception as e:
            print(f"❌ Failed to initialize aggregator channels: {e}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "error": str(e),
                "message": "Failed to initialize aggregator channels"
            }

    async def migrate_env_to_providers(self) -> Dict[str, Any]:
        """Migration of environmental variables to plant management"""
        import os

        try:
            db = await self._get_db_async()
            providers_collection = db.llm_providers

            #Pre-plant configuration
            default_providers = [
                {
                    "name": "openai",
                    "display_name": "OpenAI",
                    "description": "OpenAI是人工智能领域的领先公司，提供GPT系列模型",
                    "website": "https://openai.com",
                    "api_doc_url": "https://platform.openai.com/docs",
                    "default_base_url": "https://api.openai.com/v1",
                    "supported_features": ["chat", "completion", "embedding", "image", "vision", "function_calling", "streaming"]
                },
                {
                    "name": "anthropic",
                    "display_name": "Anthropic",
                    "description": "Anthropic专注于AI安全研究，提供Claude系列模型",
                    "website": "https://anthropic.com",
                    "api_doc_url": "https://docs.anthropic.com",
                    "default_base_url": "https://api.anthropic.com",
                    "supported_features": ["chat", "completion", "function_calling", "streaming"]
                },
                {
                    "name": "dashscope",
                    "display_name": "阿里云百炼",
                    "description": "阿里云百炼大模型服务平台，提供通义千问等模型",
                    "website": "https://bailian.console.aliyun.com",
                    "api_doc_url": "https://help.aliyun.com/zh/dashscope/",
                    "default_base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
                    "supported_features": ["chat", "completion", "embedding", "function_calling", "streaming"]
                },
                {
                    "name": "deepseek",
                    "display_name": "DeepSeek",
                    "description": "DeepSeek提供高性能的AI推理服务",
                    "website": "https://www.deepseek.com",
                    "api_doc_url": "https://platform.deepseek.com/api-docs",
                    "default_base_url": "https://api.deepseek.com",
                    "supported_features": ["chat", "completion", "function_calling", "streaming"]
                }
            ]

            migrated_count = 0
            updated_count = 0
            skipped_count = 0

            for provider_config in default_providers:
                #Fetching API Keys from Environmental Variables
                api_key = self._get_env_api_key(provider_config["name"])

                #Check for presence
                existing = await providers_collection.find_one({"name": provider_config["name"]})

                if existing:
                    #Update if an API key exists and the environment variable has a key
                    if not existing.get("api_key") and api_key:
                        update_data = {
                            "api_key": api_key,
                            "is_active": True,
                            "extra_config": {"migrated_from": "environment"},
                            "updated_at": now_tz()
                        }
                        await providers_collection.update_one(
                            {"name": provider_config["name"]},
                            {"$set": update_data}
                        )
                        updated_count += 1
                        print(f"✅ Updated API Key for provider {provider_config['display_name']}")
                    else:
                        skipped_count += 1
                        print(f"⏭️ Skipping provider {provider_config['display_name']} (Existing config)")
                    continue

                #Create new vendor configuration
                provider_data = {
                    **provider_config,
                    "api_key": api_key,
                    "is_active": bool(api_key),  #Automatically enabled with key
                    "extra_config": {"migrated_from": "environment"} if api_key else {},
                    "created_at": now_tz(),
                    "updated_at": now_tz()
                }

                await providers_collection.insert_one(provider_data)
                migrated_count += 1
                print(f"✅ Created provider {provider_config['display_name']}")

            total_changes = migrated_count + updated_count
            message_parts = []
            if migrated_count > 0:
                message_parts.append(f"Created {migrated_count} providers")
            if updated_count > 0:
                message_parts.append(f"Updated API Keys for {updated_count} providers")
            if skipped_count > 0:
                message_parts.append(f"Skipped {skipped_count} configured providers")

            if total_changes > 0:
                message = "Migration completed: " + ", ".join(message_parts)
            else:
                message = "All providers configured, no migration needed"

            return {
                "success": True,
                "migrated_count": migrated_count,
                "updated_count": updated_count,
                "skipped_count": skipped_count,
                "message": message
            }

        except Exception as e:
            print(f"Environment variable migration failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": "Environment variable migration failed"
            }

    async def test_provider_api(self, provider_id: str) -> dict:
        """Tester API Key"""
        try:
            print(f"🔍 Testing Provider API - provider_id: {provider_id}")

            db = await self._get_db_async()
            providers_collection = db.llm_providers

            #Compatible processing: Try two types of objectId and string
            from bson import ObjectId
            provider_data = None
            try:
                #Try first as ObjectId query
                provider_data = await providers_collection.find_one({"_id": ObjectId(provider_id)})
            except Exception:
                pass

            #If not found, try again as a string query
            if not provider_data:
                provider_data = await providers_collection.find_one({"_id": provider_id})

            if not provider_data:
                return {
                    "success": False,
                    "message": f"Provider not found (ID: {provider_id})"
                }

            provider_name = provider_data.get("name")
            api_key = provider_data.get("api_key")
            display_name = provider_data.get("display_name", provider_name)

            #API Key in the database is judged to be valid
            if not self._is_valid_api_key(api_key):
                #Key in database is invalid, trying to read from environment variables
                env_api_key = self._get_env_api_key(provider_name)
                if env_api_key:
                    api_key = env_api_key
                    print(f"✅ Database config invalid, retrieved API Key for {display_name} from environment")
                else:
                    return {
                        "success": False,
                        "message": f"{display_name} has no valid API Key configured (found in neither database nor environment)"
                    }
            else:
                print(f"✅ Using database-configured API Key for {display_name}")

            #Call the corresponding test function according to the manufacturer type
            test_result = await self._test_provider_connection(provider_name, api_key, display_name)

            return test_result

        except Exception as e:
            print(f"Failed to test provider API: {e}")
            return {
                "success": False,
                "message": f"Test failed: {str(e)}"
            }

    async def _test_provider_connection(self, provider_name: str, api_key: str, display_name: str) -> dict:
        """Test specific plant connections"""
        import asyncio

        try:
            #Aggregation channel (using OpenAI compatible API)
            if provider_name in ["302ai", "oneapi", "newapi", "custom_aggregator"]:
                #Get the manufacturer's base url
                db = await self._get_db_async()
                providers_collection = db.llm_providers
                provider_data = await providers_collection.find_one({"name": provider_name})
                base_url = provider_data.get("default_base_url") if provider_data else None
                return await asyncio.get_event_loop().run_in_executor(
                    None, self._test_openai_compatible_api, api_key, display_name, base_url, provider_name
                )
            elif provider_name == "google":
                #Get the manufacturer's base url
                db = await self._get_db_async()
                providers_collection = db.llm_providers
                provider_data = await providers_collection.find_one({"name": provider_name})
                base_url = provider_data.get("default_base_url") if provider_data else None
                return await asyncio.get_event_loop().run_in_executor(None, self._test_google_api, api_key, display_name, base_url)
            elif provider_name == "deepseek":
                return await asyncio.get_event_loop().run_in_executor(None, self._test_deepseek_api, api_key, display_name)
            elif provider_name == "dashscope":
                return await asyncio.get_event_loop().run_in_executor(None, self._test_dashscope_api, api_key, display_name)
            elif provider_name == "openrouter":
                return await asyncio.get_event_loop().run_in_executor(None, self._test_openrouter_api, api_key, display_name)
            elif provider_name == "openai":
                return await asyncio.get_event_loop().run_in_executor(None, self._test_openai_api, api_key, display_name)
            elif provider_name == "anthropic":
                return await asyncio.get_event_loop().run_in_executor(None, self._test_anthropic_api, api_key, display_name)
            elif provider_name == "qianfan":
                return await asyncio.get_event_loop().run_in_executor(None, self._test_qianfan_api, api_key, display_name)
            else:
                #OpenAI compatible API testing for unknown custom manufacturers
                logger.info(f"Use OpenAI compatible API to test custom manufacturers:{provider_name}")
                #Get the manufacturer's base url
                db = await self._get_db_async()
                providers_collection = db.llm_providers
                provider_data = await providers_collection.find_one({"name": provider_name})
                base_url = provider_data.get("default_base_url") if provider_data else None

                if not base_url:
                    return {
                        "success": False,
                        "message": f"Custom provider {display_name} has no API Base URL configured"
                    }

                return await asyncio.get_event_loop().run_in_executor(
                    None, self._test_openai_compatible_api, api_key, display_name, base_url, provider_name
                )
        except Exception as e:
            return {
                "success": False,
                "message": f"{display_name} Connection test failed: {str(e)}"
            }

    def _test_google_api(self, api_key: str, display_name: str, base_url: str = None, model_name: str = None) -> dict:
        """Test Google AI API"""
        try:
            import requests

            #Use default model if no model is specified
            if not model_name:
                model_name = "gemini-2.0-flash-exp"
                logger.info(f"No model specified, using default model:{model_name}")

            logger.info(f"[Google AI Testing]")
            logger.info(f"   display_name: {display_name}")
            logger.info(f"   model_name: {model_name}")
            logger.info(f"Base url (original):{base_url}")
            logger.info(f"api key Length:{len(api_key) if api_key else 0}")

            #Use configured base url or default
            if not base_url:
                base_url = "https://generativelanguage.googleapis.com/v1beta"
                logger.info(f"⚠️ base url is empty, using default values:{base_url}")

            #Remove end slash
            base_url = base_url.rstrip('/')
            logger.info(f"Base url (slash off):{base_url}")

            #If base url ends with /v1, replace with /v1beta (the correct peer of Google AI)
            if base_url.endswith('/v1'):
                base_url = base_url[:-3] + '/v1beta'
                logger.info(f"Replace /v1 with /v1beta:{base_url}")

            #Build a complete API endpoint (using a user profile model)
            url = f"{base_url}/models/{model_name}:generateContent?key={api_key}"

            logger.info(f"Final request for URL:{url.replace(api_key, '***')}")

            headers = {
                "Content-Type": "application/json"
            }

            #🔧 Add token to 2000 and avoid thinking patterns consumption leading to no output
            data = {
                "contents": [{
                    "parts": [{
                        "text": "Hello, please respond with 'OK' if you can read this."
                    }]
                }],
                "generationConfig": {
                    "maxOutputTokens": 2000,
                    "temperature": 0.1
                }
            }

            response = requests.post(url, json=data, headers=headers, timeout=15)

            print(f"📥 [Google AI Test] Response Status Code: {response.status_code}")

            if response.status_code == 200:
                #Print full response content for debugging
                print(f"📥 [Google AI Test] Response Content (First 1000 chars): {response.text[:1000]}")

                result = response.json()
                print(f"📥 [Google AI Test] Parsed JSON Structure:")
                print(f"   - Top-level keys: {list(result.keys())}")
                print(f"   - Contains 'candidates': {'candidates' in result}")
                if "candidates" in result:
                    print(f"   - candidates length: {len(result['candidates'])}")
                    if len(result['candidates']) > 0:
                        print(f"   - candidates[0] keys: {list(result['candidates'][0].keys())}")

                if "candidates" in result and len(result["candidates"]) > 0:
                    candidate = result["candidates"][0]
                    print(f"📥 [Google AI Test] candidate structure: {candidate}")

                    #Check FinnishReason
                    finish_reason = candidate.get("finishReason", "")
                    print(f"📥 [Google AI Test] finishReason: {finish_reason}")

                    if "content" in candidate:
                        content = candidate["content"]

                        #Check for parts
                        if "parts" in content and len(content["parts"]) > 0:
                            text = content["parts"][0].get("text", "")
                            print(f"📥 [Google AI Test] Extracted text: {text}")

                            if text and len(text.strip()) > 0:
                                return {
                                    "success": True,
                                    "message": f"{display_name} API连接测试成功"
                                }
                            else:
                                print(f"❌ [Google AI Test] Text is empty")
                                return {
                                    "success": False,
                                    "message": f"{display_name} API响应内容为空"
                                }
                        else:
                            #Contact does not have parts, probably for MAX TOKENS or other reasons
                            print(f"❌ [Google AI Test] No parts in content")
                            print(f"   content keys: {list(content.keys())}")

                            if finish_reason == "MAX_TOKENS":
                                return {
                                    "success": False,
                                    "message": f"{display_name} API response truncated (MAX_TOKENS), please increase maxOutputTokens config"
                                }
                            else:
                                return {
                                    "success": False,
                                    "message": f"{display_name} API response format abnormal (missing parts, finishReason: {finish_reason})"
                                }
                    else:
                        print(f"❌ [Google AI Test] Missing 'content' in candidate")
                        print(f"   candidate keys: {list(candidate.keys())}")
                        return {
                            "success": False,
                            "message": f"{display_name} API response format abnormal (missing content)"
                        }
                else:
                    print(f"❌ [Google AI Test] Missing candidates or candidates list is empty")
                    return {
                        "success": False,
                        "message": f"{display_name} API has no valid candidate response"
                    }
            elif response.status_code == 400:
                print(f"❌ [Google AI Test] 400 Error, response: {response.text[:500]}")
                try:
                    error_detail = response.json()
                    error_msg = error_detail.get("error", {}).get("message", "未知错误")
                    return {
                        "success": False,
                        "message": f"{display_name} API请求错误: {error_msg}"
                    }
                except:
                    return {
                        "success": False,
                        "message": f"{display_name} API请求格式错误"
                    }
            elif response.status_code == 403:
                print(f"❌ [Google AI Test] 403 Error, response: {response.text[:500]}")
                return {
                    "success": False,
                    "message": f"{display_name} API密钥无效或权限不足"
                }
            elif response.status_code == 503:
                print(f"❌ [Google AI Test] 503 Error, response: {response.text[:500]}")
                try:
                    error_detail = response.json()
                    error_code = error_detail.get("code", "")
                    error_msg = error_detail.get("message", "服务暂时不可用")

                    if error_code == "NO_KEYS_AVAILABLE":
                        return {
                            "success": False,
                            "message": f"{display_name} Relay service has no available keys, please try again later or contact provider"
                        }
                    else:
                        return {
                            "success": False,
                            "message": f"{display_name} Service temporarily unavailable: {error_msg}"
                        }
                except:
                    return {
                        "success": False,
                        "message": f"{display_name} 服务暂时不可用 (HTTP 503)"
                    }
            else:
                print(f"❌ [Google AI Test] {response.status_code} Error, response: {response.text[:500]}")
                return {
                    "success": False,
                    "message": f"{display_name} API测试失败: HTTP {response.status_code}"
                }

        except Exception as e:
            return {
                "success": False,
                "message": f"{display_name} API测试异常: {str(e)}"
            }

    def _test_deepseek_api(self, api_key: str, display_name: str, model_name: str = None) -> dict:
        """Test DeepSeek API"""
        try:
            import requests

            #Use default model if no model is specified
            if not model_name:
                model_name = "deepseek-chat"
                logger.info(f"No model specified, using default model:{model_name}")

            logger.info(f"[DeepSeek Test]{model_name}")

            url = "https://api.deepseek.com/chat/completions"

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }

            data = {
                "model": model_name,
                "messages": [
                    {"role": "user", "content": "你好，请简单介绍一下你自己。"}
                ],
                "max_tokens": 50,
                "temperature": 0.1
            }

            response = requests.post(url, json=data, headers=headers, timeout=10)

            if response.status_code == 200:
                result = response.json()
                if "choices" in result and len(result["choices"]) > 0:
                    content = result["choices"][0]["message"]["content"]
                    if content and len(content.strip()) > 0:
                        return {
                            "success": True,
                            "message": f"{display_name} API连接测试成功"
                        }
                    else:
                        return {
                            "success": False,
                            "message": f"{display_name} API响应为空"
                        }
                else:
                    return {
                        "success": False,
                        "message": f"{display_name} API响应格式异常"
                    }
            else:
                return {
                    "success": False,
                    "message": f"{display_name} API测试失败: HTTP {response.status_code}"
                }

        except Exception as e:
            return {
                "success": False,
                "message": f"{display_name} API测试异常: {str(e)}"
            }

    def _test_dashscope_api(self, api_key: str, display_name: str, model_name: str = None) -> dict:
        """Testing API."""
        try:
            import requests

            #Use default model if no model is specified
            if not model_name:
                model_name = "qwen-turbo"
                logger.info(f"No model specified, using default model:{model_name}")

            logger.info(f"Using models:{model_name}")

            #OpenAI compatible interface using Ali Yunpun
            url = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }

            data = {
                "model": model_name,
                "messages": [
                    {"role": "user", "content": "你好，请简单介绍一下你自己。"}
                ],
                "max_tokens": 50,
                "temperature": 0.1
            }

            response = requests.post(url, json=data, headers=headers, timeout=10)

            if response.status_code == 200:
                result = response.json()
                if "choices" in result and len(result["choices"]) > 0:
                    content = result["choices"][0]["message"]["content"]
                    if content and len(content.strip()) > 0:
                        return {
                            "success": True,
                            "message": f"{display_name} API连接测试成功"
                        }
                    else:
                        return {
                            "success": False,
                            "message": f"{display_name} API响应为空"
                        }
                else:
                    return {
                        "success": False,
                        "message": f"{display_name} API响应格式异常"
                    }
            else:
                return {
                    "success": False,
                    "message": f"{display_name} API测试失败: HTTP {response.status_code}"
                }

        except Exception as e:
            return {
                "success": False,
                "message": f"{display_name} API测试异常: {str(e)}"
            }

    def _test_openrouter_api(self, api_key: str, display_name: str) -> dict:
        """Test OpenRouter API"""
        try:
            import requests

            url = "https://openrouter.ai/api/v1/chat/completions"

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
                "HTTP-Referer": "https://tradingagents.cn",  #OpenRouter requests
                "X-Title": "TradingAgents-CN"
            }

            data = {
                "model": "meta-llama/llama-3.2-3b-instruct:free",  #Use of free models
                "messages": [
                    {"role": "user", "content": "你好，请简单介绍一下你自己。"}
                ],
                "max_tokens": 50,
                "temperature": 0.1
            }

            response = requests.post(url, json=data, headers=headers, timeout=15)

            if response.status_code == 200:
                result = response.json()
                if "choices" in result and len(result["choices"]) > 0:
                    content = result["choices"][0]["message"]["content"]
                    if content and len(content.strip()) > 0:
                        return {
                            "success": True,
                            "message": f"{display_name} API连接测试成功"
                        }
                    else:
                        return {
                            "success": False,
                            "message": f"{display_name} API响应为空"
                        }
                else:
                    return {
                        "success": False,
                        "message": f"{display_name} API响应格式异常"
                    }
            else:
                return {
                    "success": False,
                    "message": f"{display_name} API测试失败: HTTP {response.status_code}"
                }

        except Exception as e:
            return {
                "success": False,
                "message": f"{display_name} API测试异常: {str(e)}"
            }

    def _test_openai_api(self, api_key: str, display_name: str) -> dict:
        """Test OpenAI API"""
        try:
            import requests

            url = "https://api.openai.com/v1/chat/completions"

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }

            data = {
                "model": "gpt-3.5-turbo",
                "messages": [
                    {"role": "user", "content": "你好，请简单介绍一下你自己。"}
                ],
                "max_tokens": 50,
                "temperature": 0.1
            }

            response = requests.post(url, json=data, headers=headers, timeout=10)

            if response.status_code == 200:
                result = response.json()
                if "choices" in result and len(result["choices"]) > 0:
                    content = result["choices"][0]["message"]["content"]
                    if content and len(content.strip()) > 0:
                        return {
                            "success": True,
                            "message": f"{display_name} API连接测试成功"
                        }
                    else:
                        return {
                            "success": False,
                            "message": f"{display_name} API响应为空"
                        }
                else:
                    return {
                        "success": False,
                        "message": f"{display_name} API响应格式异常"
                    }
            else:
                return {
                    "success": False,
                    "message": f"{display_name} API测试失败: HTTP {response.status_code}"
                }

        except Exception as e:
            return {
                "success": False,
                "message": f"{display_name} API测试异常: {str(e)}"
            }

    def _test_anthropic_api(self, api_key: str, display_name: str) -> dict:
        """Test Anthropic API"""
        try:
            import requests

            url = "https://api.anthropic.com/v1/messages"

            headers = {
                "Content-Type": "application/json",
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01"
            }

            data = {
                "model": "claude-3-haiku-20240307",
                "max_tokens": 50,
                "messages": [
                    {"role": "user", "content": "你好，请简单介绍一下你自己。"}
                ]
            }

            response = requests.post(url, json=data, headers=headers, timeout=10)

            if response.status_code == 200:
                result = response.json()
                if "content" in result and len(result["content"]) > 0:
                    content = result["content"][0]["text"]
                    if content and len(content.strip()) > 0:
                        return {
                            "success": True,
                            "message": f"{display_name} API连接测试成功"
                        }
                    else:
                        return {
                            "success": False,
                            "message": f"{display_name} API响应为空"
                        }
                else:
                    return {
                        "success": False,
                        "message": f"{display_name} API响应格式异常"
                    }
            else:
                return {
                    "success": False,
                    "message": f"{display_name} API测试失败: HTTP {response.status_code}"
                }

        except Exception as e:
            return {
                "success": False,
                "message": f"{display_name} API测试异常: {str(e)}"
            }

    def _test_qianfan_api(self, api_key: str, display_name: str) -> dict:
        """Test 100 degrees aPI."""
        try:
            import requests

            #The new generation of thousands of sails uses OpenAI compatible interfaces
            url = "https://qianfan.baidubce.com/v2/chat/completions"

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }

            data = {
                "model": "ernie-3.5-8k",
                "messages": [
                    {"role": "user", "content": "你好，请简单介绍一下你自己。"}
                ],
                "max_tokens": 50,
                "temperature": 0.1
            }

            response = requests.post(url, json=data, headers=headers, timeout=15)

            if response.status_code == 200:
                result = response.json()
                if "choices" in result and len(result["choices"]) > 0:
                    content = result["choices"][0]["message"]["content"]
                    if content and len(content.strip()) > 0:
                        return {
                            "success": True,
                            "message": f"{display_name} API连接测试成功"
                        }
                    else:
                        return {
                            "success": False,
                            "message": f"{display_name} API响应为空"
                        }
                else:
                    return {
                        "success": False,
                        "message": f"{display_name} API响应格式异常"
                    }
            elif response.status_code == 401:
                return {
                    "success": False,
                    "message": f"{display_name} API密钥无效或已过期"
                }
            elif response.status_code == 403:
                return {
                    "success": False,
                    "message": f"{display_name} API权限不足或配额已用完"
                }
            else:
                try:
                    error_detail = response.json()
                    error_msg = error_detail.get("error", {}).get("message", f"HTTP {response.status_code}")
                    return {
                        "success": False,
                        "message": f"{display_name} API测试失败: {error_msg}"
                    }
                except:
                    return {
                        "success": False,
                        "message": f"{display_name} API测试失败: HTTP {response.status_code}"
                    }

        except Exception as e:
            return {
                "success": False,
                "message": f"{display_name} API测试异常: {str(e)}"
            }

    async def fetch_provider_models(self, provider_id: str) -> dict:
        """Fetch model list from the manufacturer API"""
        try:
            print(f"🔍 Fetching provider model list - provider_id: {provider_id}")

            db = await self._get_db_async()
            providers_collection = db.llm_providers

            #Compatible processing: Try two types of objectId and string
            from bson import ObjectId
            provider_data = None
            try:
                provider_data = await providers_collection.find_one({"_id": ObjectId(provider_id)})
            except Exception:
                pass

            if not provider_data:
                provider_data = await providers_collection.find_one({"_id": provider_id})

            if not provider_data:
                return {
                    "success": False,
                    "message": f"Provider not found (ID: {provider_id})"
                }

            provider_name = provider_data.get("name")
            api_key = provider_data.get("api_key")
            base_url = provider_data.get("default_base_url")
            display_name = provider_data.get("display_name", provider_name)

            #API Key in the database is judged to be valid
            if not self._is_valid_api_key(api_key):
                #Key in database is invalid, trying to read from environment variables
                env_api_key = self._get_env_api_key(provider_name)
                if env_api_key:
                    api_key = env_api_key
                    print(f"✅ Database config invalid, retrieved API Key for {display_name} from environment")
                else:
                    #/Models endpoints for certain polymer platforms (e. g. OpenRouter) do not need API Key
                    print(f"⚠️ {display_name} No valid API Key configured, attempting unauthenticated access")
            else:
                print(f"✅ Using database-configured API Key for {display_name}")

            if not base_url:
                return {
                    "success": False,
                    "message": f"{display_name} No API Base URL configured (default_base_url)"
                }

            #Call OpenAI compatible /v1/ models endpoint
            import asyncio
            result = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_models_from_api, api_key, base_url, display_name
            )

            return result

        except Exception as e:
            print(f"Failed to fetch model list: {e}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "message": f"Failed to fetch model list: {str(e)}"
            }

    def _fetch_models_from_api(self, api_key: str, base_url: str, display_name: str) -> dict:
        """Fetch model list from API"""
        try:
            import requests

            #🔧 Smart version number processing: only if no version number is available /v1
            #Avoid re-adding URLs with existing version numbers (e. g. /v4 for spectro-AI) / v1
            import re
            base_url = base_url.rstrip("/")
            if not re.search(r'/v\d+$', base_url):
                #No version number at the end of URL, add /v1 (OpenAI standard)
                base_url = base_url + "/v1"
                logger.info(f"[Fetch Model List] Adding /v1 version: {base_url}")
            else:
                #URL already contains version number (e. g. / v4), not added
                logger.info(f"[Fetch Model List] {base_url}")

            url = f"{base_url}/models"

            #Build Request Header
            headers = {}
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"
                print(f"🔍 Request URL: {url} (with API Key)")
            else:
                print(f"🔍 Request URL: {url} (without API Key)")

            response = requests.get(url, headers=headers, timeout=15)

            print(f"📊 Response Status Code: {response.status_code}")
            print(f"📊 Response Content: {response.text[:500]}...")

            if response.status_code == 200:
                result = response.json()
                print(f"📊 Response JSON Structure: {list(result.keys())}")

                if "data" in result and isinstance(result["data"], list):
                    all_models = result["data"]
                    print(f"📊 API returned {len(all_models)} models")

                    #Print the complete structure of previous models (for debugging price fields)
                    if all_models:
                        print(f"🔍 Complete structure of first model:")
                        import json
                        print(json.dumps(all_models[0], indent=2, ensure_ascii=False))

                    #Print all Anthropic models (for debugging)
                    anthropic_models = [m for m in all_models if "anthropic" in m.get("id", "").lower()]
                    if anthropic_models:
                        print(f"🔍 Anthropic Model List ({len(anthropic_models)} items):")
                        for m in anthropic_models[:20]:  #Only 20 before printing
                            print(f"   - {m.get('id')}")

                    #Filter: Only the usual models of mainstream large plants are retained
                    filtered_models = self._filter_popular_models(all_models)
                    print(f"✅ Retained {len(filtered_models)} popular models after filtering")

                    #Convert model format with price information
                    formatted_models = self._format_models_with_pricing(filtered_models)

                    return {
                        "success": True,
                        "models": formatted_models,
                        "message": f"Successfully retrieved {len(formatted_models)} popular models (filtered)"
                    }
                else:
                    print(f"❌ Response format abnormal, expected 'data' field to be a list")
                    return {
                        "success": False,
                        "message": f"{display_name} API response format abnormal (missing data field or incorrect format)"
                    }
            elif response.status_code == 401:
                return {
                    "success": False,
                    "message": f"{display_name} API密钥无效或已过期"
                }
            elif response.status_code == 403:
                return {
                    "success": False,
                    "message": f"{display_name} API权限不足"
                }
            else:
                try:
                    error_detail = response.json()
                    error_msg = error_detail.get("error", {}).get("message", f"HTTP {response.status_code}")
                    print(f"❌ API Error: {error_msg}")
                    return {
                        "success": False,
                        "message": f"{display_name} API request failed: {error_msg}"
                    }
                except:
                    print(f"❌ HTTP Error: {response.status_code}")
                    return {
                        "success": False,
                        "message": f"{display_name} API request failed: HTTP {response.status_code}, response: {response.text[:200]}"
                    }

        except Exception as e:
            print(f"❌ Exception: {e}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "message": f"{display_name} API request exception: {str(e)}"
            }

    def _format_models_with_pricing(self, models: list) -> list:
        """Formatting Model List with Price Information

        Support multiple price formats:
        1. OpenRouter: pricing.prompt/complement
        2. 302.ai: price.prompt/complement or price.input/output
        Other: possible lack of price information
        """
        formatted = []
        for model in models:
            model_id = model.get("id", "")
            model_name = model.get("name", model_id)

            #Try fetching price information from multiple fields
            input_price_per_1k = None
            output_price_per_1k = None

            #Method 1: OpenRouter Format (pricing.prompt/complement)
            pricing = model.get("pricing", {})
            if pricing:
                prompt_price = pricing.get("prompt", "0")  # USD per token
                completion_price = pricing.get("completion", "0")  # USD per token

                try:
                    if prompt_price and float(prompt_price) > 0:
                        input_price_per_1k = float(prompt_price) * 1000
                    if completion_price and float(completion_price) > 0:
                        output_price_per_1k = float(completion_price) * 1000
                except (ValueError, TypeError):
                    pass

            #Mode 2:302.ai format (price.prompt/complement or price.input/output)
            if not input_price_per_1k and not output_price_per_1k:
                price = model.get("price", {})
                if price and isinstance(price, dict):
                    #Try prompt/complement field
                    prompt_price = price.get("prompt") or price.get("input")
                    completion_price = price.get("completion") or price.get("output")

                    try:
                        if prompt_price and float(prompt_price) > 0:
                            #Assumptions per token, converted to per 1K tokens
                            input_price_per_1k = float(prompt_price) * 1000
                        if completion_price and float(completion_price) > 0:
                            output_price_per_1k = float(completion_price) * 1000
                    except (ValueError, TypeError):
                        pass

            #Get context length
            context_length = model.get("context_length")
            if not context_length:
                #Try fetching from top provider
                top_provider = model.get("top_provider", {})
                context_length = top_provider.get("context_length")

            #If not, try extrapolating from max complement tokens
            if not context_length:
                max_tokens = model.get("max_completion_tokens")
                if max_tokens and max_tokens > 0:
                    #Usually the context length is 4-8 times the maximum output
                    context_length = max_tokens * 4

            formatted_model = {
                "id": model_id,
                "name": model_name,
                "context_length": context_length,
                "input_price_per_1k": input_price_per_1k,
                "output_price_per_1k": output_price_per_1k,
            }

            formatted.append(formatted_model)

            #Print price information (for debugging)
            if input_price_per_1k or output_price_per_1k:
                print(f"💰 {model_id}: Input=${input_price_per_1k:.6f}/1K, Output=${output_price_per_1k:.6f}/1K")

        return formatted

    def _filter_popular_models(self, models: list) -> list:
        """Filter model list, only the usual model of the main plant"""
        import re

        #Only three plants are retained: OpenAI, Anthropic, Google
        popular_providers = [
            "openai",       # OpenAI
            "anthropic",    # Anthropic
            "google",       # Google
        ]

        #Common model name prefix (for identifying models without vendor prefixes)
        model_prefixes = {
            "gpt-": "openai",           # gpt-3.5-turbo, gpt-4, gpt-4o
            "o1-": "openai",            # o1-preview, o1-mini
            "claude-": "anthropic",     # claude-3-opus, claude-3-sonnet
            "gemini-": "google",        # gemini-pro, gemini-1.5-pro
            "gemini": "google",         #Gemini (without hyphenation)
        }

        #Excluded keywords
        exclude_keywords = [
            "preview",
            "experimental",
            "alpha",
            "beta",
            "free",
            "extended",
            "nitro",
            ":free",
            ":extended",
            "online",  #Exclude version with online search
            "instruct",  #Exclude instract version
        ]

        #Date format regular expression (matching 2024-05-13)
        date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}')

        filtered = []
        for model in models:
            model_id = model.get("id", "").lower()
            model_name = model.get("name", "").lower()

            #Check if they belong to the three main plants.
            #Mode 1: Model ID contains the name of the manufacturer (e.g. openai/gpt-4)
            is_popular_provider = any(provider in model_id for provider in popular_providers)

            #Mode 2: Model ID starts with a common prefix (e.g. gpt-4, claude-3-sonnet)
            if not is_popular_provider:
                for prefix, provider in model_prefixes.items():
                    if model_id.startswith(prefix):
                        is_popular_provider = True
                        print(f"🔍 Identified model prefix: {model_id} -> {provider}")
                        break

            if not is_popular_provider:
                continue

            #Check to include date (exclude old version with date)
            if date_pattern.search(model_id):
                print(f"⏭️ Skipping old dated version: {model_id}")
                continue

            #Check to include excluded keywords
            has_exclude_keyword = any(keyword in model_id or keyword in model_name for keyword in exclude_keywords)

            if has_exclude_keyword:
                print(f"⏭️ Skipping excluded keyword: {model_id}")
                continue

            #Keep the model
            print(f"✅ Retaining model: {model_id}")
            filtered.append(model)

        return filtered

    def _test_openai_compatible_api(self, api_key: str, display_name: str, base_url: str = None, provider_name: str = None) -> dict:
        """Test OpenAI Compatibility API (for aggregation channels and custom manufacturers)"""
        try:
            import requests

            #Use default if base url is not provided
            if not base_url:
                return {
                    "success": False,
                    "message": f"{display_name} No API Base URL configured (default_base_url)"
                }

            #🔧 Smart version number processing: only if no version number is available /v1
            #Avoid re-adding URLs with existing version numbers (e. g. /v4 for spectro-AI) / v1
            import re
            logger.info(f"[test API] Original base URL: {base_url}")
            base_url = base_url.rstrip("/")
            logger.info(f"[test API] After removing slash: {base_url}")

            if not re.search(r'/v\d+$', base_url):
                #No version number at the end of URL, add /v1 (OpenAI standard)
                base_url = base_url + "/v1"
                logger.info(f"[test API] Added /v1 version: {base_url}")
            else:
                #URL already contains version number (e. g. / v4), not added
                logger.info(f"[test API] {base_url}")

            url = f"{base_url}/chat/completions"
            logger.info(f"[test API] Final request URL: {url}")

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }

            #🔥Selection of suitable test models by different manufacturers
            test_model = "gpt-3.5-turbo"  #Default Model
            if provider_name == "siliconflow":
                #Silicon flows using a free Qwen model for testing
                test_model = "Qwen/Qwen2.5-7B-Instruct"
                logger.info(f"🔍 SiliconFlow using test model: {test_model}")
            elif provider_name == "zhipu":
                #The brain spectrum AI uses the glm-4 model for testing
                test_model = "glm-4"
                logger.info(f"🔍 Zhipu AI using test model: {test_model}")

            #Test using a generic model name
            #Aggregation channels usually support multiple models, using gpt-3.5-turbo as a test
            data = {
                "model": test_model,
                "messages": [
                    {"role": "user", "content": "Hello, please respond with 'OK' if you can read this."}
                ],
                "max_tokens": 200,  #Increase to 200 to give enough space for reasoning models (e.g. o1/gpt-5)
                "temperature": 0.1
            }

            response = requests.post(url, json=data, headers=headers, timeout=15)

            if response.status_code == 200:
                result = response.json()
                if "choices" in result and len(result["choices"]) > 0:
                    content = result["choices"][0]["message"]["content"]
                    if content and len(content.strip()) > 0:
                        return {
                            "success": True,
                            "message": f"{display_name} API连接测试成功"
                        }
                    else:
                        return {
                            "success": False,
                            "message": f"{display_name} API响应为空"
                        }
                else:
                    return {
                        "success": False,
                        "message": f"{display_name} API响应格式异常"
                    }
            elif response.status_code == 401:
                return {
                    "success": False,
                    "message": f"{display_name} API密钥无效或已过期"
                }
            elif response.status_code == 403:
                return {
                    "success": False,
                    "message": f"{display_name} API权限不足或配额已用完"
                }
            else:
                try:
                    error_detail = response.json()
                    error_msg = error_detail.get("error", {}).get("message", f"HTTP {response.status_code}")
                    logger.error(f"❌ [{display_name}] API test failed")
                    logger.error(f"Request URL: {url}")
                    logger.error(f"Status code: {response.status_code}")
                    logger.error(f"Error Details: {error_detail}")
                    return {
                        "success": False,
                        "message": f"{display_name} API测试失败: {error_msg}"
                    }
                except:
                    logger.error(f"❌ [{display_name}] API test failed")
                    logger.error(f"Request URL: {url}")
                    logger.error(f"Status code: {response.status_code}")
                    logger.error(f"Response content: {response.text[:500]}")
                    return {
                        "success": False,
                        "message": f"{display_name} API测试失败: HTTP {response.status_code}"
                    }

        except Exception as e:
            return {
                "success": False,
                "message": f"{display_name} API测试异常: {str(e)}"
            }


#Create global instance
CONFIG_SERVICE = ConfigService()