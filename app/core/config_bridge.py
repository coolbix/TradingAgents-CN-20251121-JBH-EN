"""Configure Bridge Modules
Bringing the configuration bridge of the unified configuration system to the environment variable for the TradingAgents Core Library
"""

import os
import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger("app.config_bridge")


def consolidate_configs_to_osenviron():
    """Bring the unified configurations to os.environ[] variable

    This function will:
    1. Read LLM Provider configurations from the database (API keys, overtime, temperature, etc.)
    2. Writing configuration to environmental variables
    3. Writing default models to environmental variables
    4. Writing data source configuration to environmental variables (API keys, timeout, retrying, etc.)
    5. Writing system time configuration to environmental variables

    So the TradingAgents Core Library can read the user profile data from the environment variable.
    """
    try:
        from app.core.unified_config import UNIFIED_CONFIG_MANAGER
        from app.services.config_service import CONFIG_SERVICE

        logger.info("Consolidating configurations to OS_ENVIRONMENT...")
        bridged_count = 0

        #Force to use MongoDB storage (tradingagents.token_usage) to store the LLM token usage statistics
        use_mongodb_storage = os.getenv("USE_MONGODB_STORAGE", "true")
        os.environ["USE_MONGODB_STORAGE"] = use_mongodb_storage
        #logger.info(f"== sync, corrected by elderman =={use_mongodb_storage}")
        bridged_count += 1

        #Bridge MongoDB Connection String
        mongodb_conn_str = os.getenv("MONGODB_CONNECTION_STRING")
        if mongodb_conn_str:
            os.environ["MONGODB_CONNECTION_STRING"] = mongodb_conn_str
            logger.info(f"✓ Bridge to MONGODB CONNECTION STRING{len(mongodb_conn_str)})")
            bridged_count += 1

        #Bridge MongoDB Database Name
        mongodb_db_name = os.getenv("MONGODB_DATABASE_NAME", "tradingagents")
        os.environ["MONGODB_DATABASE_NAME"] = mongodb_db_name
        #logger.info(f"== sync, corrected by elderman == @elder man{mongodb_db_name}")
        bridged_count += 1

        #-------------------------------------------------------------------------------------------------
        #1. Bridging LLM Provider configuration (basic API key)
        #-------------------------------------------------------------------------------------------------
        #🔧 [Priority].env file > Database vendor configuration
        #🔥 Modify: Read the plant configuration from llm providers in the database instead of from JSON files
        #Use the configuration in the database only if the environment variable does not exist or is a placeholder
        try:
            #Read the LLM Provider configuration using the sync MongoDB client
            from pymongo import MongoClient
            from app.core.config import SETTINGS
            from app.models.config_models import LLMProvider

            #Create a simultaneous MongoDB client
            client = MongoClient(SETTINGS.MONGO_URI)
            db = client[SETTINGS.MONGO_DB_NAME]
            providers_collection = db.llm_providers

            #Query All LLM Provider Configurations
            providers_data = list(providers_collection.find())
            providers = [LLMProvider(**data) for data in providers_data]

            logger.info(f"Read from the database: {len(providers)} LLM Providers Configuration")

            for provider in providers:
                if not provider.is_active:
                    logger.debug(f"The LLM Provider {provider.name}: Not enabled, Skip")
                    continue

                env_key = f"{provider.name.upper()}_API_KEY"
                existing_env_value = os.getenv(env_key)

                #Check that environment variables exist and are valid (not placeholders)
                if existing_env_value and not existing_env_value.startswith("your_"):
                    logger.info(f"{env_key} in the .env file(Long:{len(existing_env_value)})")
                    bridged_count += 1
                elif provider.api_key and not provider.api_key.startswith("your_"):
                    #Use database configuration only when environment variables do not exist or are placeholders
                    os.environ[env_key] = provider.api_key
                    logger.info(f"Using database LLM Provider configurations {env_key}(Long:{len(provider.api_key)})")
                    logger.info(f"The LLM Configuration in database is being used for {env_key}(Long:{len(provider.api_key)})")
                    bridged_count += 1
                else:
                    logger.debug(f"  ⏭️  {env_key} No valid API Key configured")

            #Close Sync Client
            client.close()

        except Exception as e:
            logger.error(f"Access to database configuration failed:{e}", exc_info=True)
            logger.warning("⚠️ will try to read configurations from JSON files as a backup scenario")

            #Backup scheme: read from JSON file
            llm_configs = UNIFIED_CONFIG_MANAGER.get_llm_configs()
            for llm_config in llm_configs:
                #Now it's a string type. It's no longer an anemic.
                env_key = f"{llm_config.provider.upper()}_API_KEY"
                existing_env_value = os.getenv(env_key)

                #Check that environment variables exist and are valid (not placeholders)
                if existing_env_value and not existing_env_value.startswith("your_"):
                    logger.info(f"In the .env file{env_key}(Long:{len(existing_env_value)})")
                    bridged_count += 1
                elif llm_config.enabled and llm_config.api_key:
                    #Use database configuration only when environment variables do not exist or are placeholders
                    if not llm_config.api_key.startswith("your_"):
                        os.environ[env_key] = llm_config.api_key
                        logger.info(f"✓ Using JSON Files{env_key}(Long:{len(llm_config.api_key)})")
                        bridged_count += 1
                    else:
                        logger.warning(f"  ⚠️  {env_key}Placeholders in .env and JSON files. Skip")
                else:
                    logger.debug(f"  ⏭️  {env_key}Not configured")

        #-------------------------------------------------------------------------------------------------
        #2. Bridge default model configuration
        #-------------------------------------------------------------------------------------------------
        default_model = UNIFIED_CONFIG_MANAGER.get_default_model()
        if default_model:
            os.environ['TRADINGAGENTS_DEFAULT_MODEL'] = default_model
            logger.info(f"✓ Bridge default model:{default_model}")
            bridged_count += 1

        quick_model = UNIFIED_CONFIG_MANAGER.get_quick_analysis_model()
        if quick_model:
            os.environ['TRADINGAGENTS_QUICK_MODEL'] = quick_model
            logger.info(f"✓ Bridge Rapid Analysis Model:{quick_model}")
            bridged_count += 1

        deep_model = UNIFIED_CONFIG_MANAGER.get_deep_analysis_model()
        if deep_model:
            os.environ['TRADINGAGENTS_DEEP_MODEL'] = deep_model
            logger.info(f"✓ Bridge depth analysis model:{deep_model}")
            bridged_count += 1

        #-------------------------------------------------------------------------------------------------
        #3. Bridging data source configuration (basic API key)
        #-------------------------------------------------------------------------------------------------
        #🔧 [priority].env files > database configuration
        #🔥 Modify: Read the data source configuration from the database's system configs collection instead of from the JSON file
        try:
            #Use sync MongoDB client reading system configuration
            from pymongo import MongoClient
            from app.core.config import SETTINGS
            from app.models.config_models import SystemConfig

            #Create a simultaneous MongoDB client
            client = MongoClient(SETTINGS.MONGO_URI)
            db = client[SETTINGS.MONGO_DB_NAME]
            config_collection = db.system_configs

            #Query the latest system configuration
            config_data = config_collection.find_one(
                {"is_active": True},
                sort=[("version", -1)]
            )

            if config_data and config_data.get('data_source_configs'):
                system_config = SystemConfig(**config_data)
                data_source_configs = system_config.data_source_configs
                logger.info(f"Read from the database{len(data_source_configs)}Data source configuration")
            else:
                logger.warning("⚠️ No data source configuration in database, using JSON file configuration")
                data_source_configs = UNIFIED_CONFIG_MANAGER.get_data_source_configs()

            #Close Sync Client
            client.close()

        except Exception as e:
            logger.error(f"Access to data source configuration from database failed:{e}", exc_info=True)
            logger.warning("⚠️ will try to read configurations from JSON files as a backup scenario")
            data_source_configs = UNIFIED_CONFIG_MANAGER.get_data_source_configs()

        for ds_config in data_source_configs:
            if ds_config.enabled and ds_config.api_key:
                # Tushare Token
                #🔥 Priority: Database Configuration > .env files (effective once modified by users in the Web backstage)
                if ds_config.type.value == 'tushare':
                    existing_token = os.getenv('TUSHARE_TOKEN')

                    #Prioritize database configuration
                    if ds_config.api_key and not ds_config.api_key.startswith("your_"):
                        os.environ['TUSHARE_TOKEN'] = ds_config.api_key
                        logger.info(f"Use Tushare ToKEN in the database{len(ds_config.api_key)})")
                        if existing_token and existing_token != ds_config.api_key:
                            logger.info(f"TUSHARE TOKEN")
                    #Decline to .env File Configuration
                    elif existing_token and not existing_token.startswith("your_"):
                        logger.info(f"✓ TUSHARE TOKEN in .env file{len(existing_token)})")
                        logger.info(f"ℹ️ not configured for effective TUSHARE TOKEN, using .env downscaling schemes")
                    else:
                        logger.warning(f"⚠️TUSHARE TOKEN is not configured in databases and .env")
                        continue
                    bridged_count += 1

                # FinnHub API Key
                #🔥 Priority: Database Configuration > .env files
                elif ds_config.type.value == 'finnhub':
                    existing_key = os.getenv('FINNHUB_API_KEY')

                    #Prioritize database configuration
                    if ds_config.api_key and not ds_config.api_key.startswith("your_"):
                        os.environ['FINNHUB_API_KEY'] = ds_config.api_key
                        logger.info(f"Use FINNHUB API KEY in the database (Longitude:{len(ds_config.api_key)})")
                        if existing_key and existing_key != ds_config.api_key:
                            logger.info(f"FINNHUB API KEY")
                    #Decline to .env File Configuration
                    elif existing_key and not existing_key.startswith("your_"):
                        logger.info(f"✓ FINNHUB API KEY in .env file{len(existing_key)})")
                        logger.info(f"No valid FINNHUB API KEY configured in ℹ️ database, using .env downscaling scheme")
                    else:
                        logger.warning(f"⚠️FINNHUB API KEY does not configure valid values in databases or .env")
                        continue
                    bridged_count += 1

        #-------------------------------------------------------------------------------------------------
        #4. Detailed configuration of bridge data sources (overtime, retest, cache, etc.)
        #-------------------------------------------------------------------------------------------------
        bridged_count += _bridge_datasource_details(data_source_configs)

        #-------------------------------------------------------------------------------------------------
        #5. Operation of the bridging system
        #-------------------------------------------------------------------------------------------------
        bridged_count += _bridge_system_settings()

        #-------------------------------------------------------------------------------------------------
        #6. Re-initiation of MongoDB storage in TradingAGents Library
        #-------------------------------------------------------------------------------------------------
        #Because the global config manager example was created when the module was imported, when the environment variable was not bridged Answer.
        try:
            from tradingagents.config.config_manager import CONFIG_MANAGER
            from tradingagents.config.mongodb_storage import MongoDBStorage
            logger.info("Re-initiation of transports MongoDB storage...")

            #Debug: Check the environment variable
            use_mongodb = os.getenv("USE_MONGODB_STORAGE", "false")
            mongodb_conn = os.getenv("MONGODB_CONNECTION_STRING", "未设置")
            mongodb_db = os.getenv("MONGODB_DATABASE_NAME", "tradingagents")
            logger.info(f"  📋 USE_MONGODB_STORAGE: {use_mongodb}")
            logger.info(f"  📋 MONGODB_CONNECTION_STRING: {mongodb_conn[:30]}..." if len(mongodb_conn) > 30 else f"  📋 MONGODB_CONNECTION_STRING: {mongodb_conn}")
            logger.info(f"  📋 MONGODB_DATABASE_NAME: {mongodb_db}")

            #Create the MongoDBStorage instance directly instead of calling  init mongodb storage()
            #This will capture more detailed error information.
            if use_mongodb.lower() == "true":
                try:
                    #🔍 Detailed log: display full connection string (for debugging)
                    logger.info(f"Connection string actually entered:{mongodb_conn}")
                    logger.info(f"Name of database actually entered:{mongodb_db}")

                    CONFIG_MANAGER.mongodb_storage = MongoDBStorage(
                        connection_string=mongodb_conn,
                        database_name=mongodb_db
                    )
                    if CONFIG_MANAGER.mongodb_storage.is_connected():
                        logger.info("✅Traditions MongoDB storage enabled")
                    else:
                        logger.warning("Could not close temporary folder: %s")
                        CONFIG_MANAGER.mongodb_storage = None
                except Exception as e:
                    logger.error(f"Could not close temporary folder: %s{e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    CONFIG_MANAGER.mongodb_storage = None
            else:
                logger.info("ℹ️USE MONGODB STORAGE is not enabled and will be stored using JSON files")
        except Exception as e:
            logger.error(f"Re-initiation of trafficagents MongoDB storage failed:{e}")
            import traceback
            logger.error(traceback.format_exc())

        #-------------------------------------------------------------------------------------------------
        #Synchronize pricing configuration to referig/pricing.json
        #-------------------------------------------------------------------------------------------------
        #Note: This needs to read the configuration from the database because the configuration in the file does not have pricing information
        #Synchronize pricing configuration using a step
        import asyncio
        try:
            loop = asyncio.get_running_loop()
            #Create a background job in a stept context
            task = loop.create_task(_sync_pricing_config_from_db())
            task.add_done_callback(_handle_sync_task_result)
            logger.info("🔄 Pricing configuration sync task created (backstage execution)")
        except RuntimeError:
            #Use asyncio.run, not in the variable context
            asyncio.run(_sync_pricing_config_from_db())

        logger.info(f"The bridge is complete.{bridged_count}Item Configuration")
        return True

    except Exception as e:
        logger.error(f"The bridge failed:{e}", exc_info=True)
        logger.warning("⚠️ TradingAgents will use configurations in .env files")
        return False


def _bridge_datasource_details(data_source_configs) -> int:
    """Configure bridge data source details to environmental variables

    Args:
        Data source configs: Data source configuration list

    Returns:
        Int: Number of configurations for bridges
    """
    bridged_count = 0

    for ds_config in data_source_configs:
        if not ds_config.enabled:
            continue

        #Note: Field name is type instead of source  type
        source_type = ds_config.type.value.upper()

        #Timeout
        if ds_config.timeout:
            env_key = f"{source_type}_TIMEOUT"
            os.environ[env_key] = str(ds_config.timeout)
            logger.debug(f"The bridge.{env_key}: {ds_config.timeout}")
            bridged_count += 1

        #Rate limit
        if ds_config.rate_limit:
            env_key = f"{source_type}_RATE_LIMIT"
            os.environ[env_key] = str(ds_config.rate_limit / 60.0)  #Convert to request per second
            logger.debug(f"The bridge.{env_key}: {ds_config.rate_limit / 60.0}")
            bridged_count += 1

        #Maximum number of retries (from config params)
        if ds_config.config_params and 'max_retries' in ds_config.config_params:
            env_key = f"{source_type}_MAX_RETRIES"
            os.environ[env_key] = str(ds_config.config_params['max_retries'])
            logger.debug(f"The bridge.{env_key}: {ds_config.config_params['max_retries']}")
            bridged_count += 1

        #Cache TTL (from config params)
        if ds_config.config_params and 'cache_ttl' in ds_config.config_params:
            env_key = f"{source_type}_CACHE_TTL"
            os.environ[env_key] = str(ds_config.config_params['cache_ttl'])
            logger.debug(f"The bridge.{env_key}: {ds_config.config_params['cache_ttl']}")
            bridged_count += 1

        #Whether to enable caches (retrieved from config params)
        if ds_config.config_params and 'cache_enabled' in ds_config.config_params:
            env_key = f"{source_type}_CACHE_ENABLED"
            os.environ[env_key] = str(ds_config.config_params['cache_enabled']).lower()
            logger.debug(f"The bridge.{env_key}: {ds_config.config_params['cache_enabled']}")
            bridged_count += 1

    if bridged_count > 0:
        logger.info(f"✓ Bridge data source detail configuration:{bridged_count}Item")

    return bridged_count


def _bridge_system_settings() -> int:
    """Configure the bridge system to the environment variable while running

    Returns:
        Int: Number of configurations for bridges
    """
    try:
        #Use synchronized MongoDB client
        from pymongo import MongoClient
        from app.core.config import SETTINGS

        #Create Sync client
        client = MongoClient(
            SETTINGS.MONGO_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )

        try:
            db = client[SETTINGS.MONGO_DB_NAME]
            #Read activated configuration from system configs
            config_doc = db.system_configs.find_one({"is_active": True})

            if not config_doc or 'system_settings' not in config_doc:
                logger.debug("The system is empty.")
                return 0

            system_settings = config_doc['system_settings']
        except Exception as e:
            logger.debug(f"Could not close temporary folder: %s{e}")
            import traceback
            logger.debug(traceback.format_exc())
            return 0
        finally:
            client.close()

        if not system_settings:
            logger.debug("The system is empty.")
            return 0

        logger.debug(f"Other Organiser{len(system_settings)}System Settings")
        bridged_count = 0

        #TradingAgents Runtime Configuration
        ta_settings = {
            'ta_hk_min_request_interval_seconds': 'TA_HK_MIN_REQUEST_INTERVAL_SECONDS',
            'ta_hk_timeout_seconds': 'TA_HK_TIMEOUT_SECONDS',
            'ta_hk_max_retries': 'TA_HK_MAX_RETRIES',
            'ta_hk_rate_limit_wait_seconds': 'TA_HK_RATE_LIMIT_WAIT_SECONDS',
            'ta_hk_cache_ttl_seconds': 'TA_HK_CACHE_TTL_SECONDS',
            'ta_use_app_cache': 'TA_USE_APP_CACHE',
        }

        #Token uses statistical configuration
        token_tracking_settings = {
            'enable_cost_tracking': 'ENABLE_COST_TRACKING',
            'auto_save_usage': 'AUTO_SAVE_USAGE',
        }

        for setting_key, env_key in ta_settings.items():
            #Check if the environment variable is set in the .env file
            env_value = os.getenv(env_key)
            if env_value is not None:
                #Setup in .env file. Prefer the value of .env
                logger.info(f"In the .env file{env_key}: {env_value}")
                bridged_count += 1
            elif setting_key in system_settings:
                #.env file not set, using value in database
                value = system_settings[setting_key]
                os.environ[env_key] = str(value).lower() if isinstance(value, bool) else str(value)
                logger.info(f"The bridge.{env_key}: {value}")
                bridged_count += 1
            else:
                logger.debug(f"Configure Keys{setting_key}Not in System Settings")

        #Bridge Token Use Statistical Configuration
        for setting_key, env_key in token_tracking_settings.items():
            if setting_key in system_settings:
                value = system_settings[setting_key]
                os.environ[env_key] = str(value).lower() if isinstance(value, bool) else str(value)
                logger.info(f"The bridge.{env_key}: {value}")
                bridged_count += 1
            else:
                logger.debug(f"Configure Keys{setting_key}Not in System Settings")

        #Time Zone Configuration
        if 'app_timezone' in system_settings:
            os.environ['APP_TIMEZONE'] = system_settings['app_timezone']
            logger.debug(f"APP TIMEZONE:{system_settings['app_timezone']}")
            bridged_count += 1

        #Currency preferences
        if 'currency_preference' in system_settings:
            os.environ['CURRENCY_PREFERENCE'] = system_settings['currency_preference']
            logger.debug(f"- CURRENCY PREFERNCE:{system_settings['currency_preference']}")
            bridged_count += 1

        if bridged_count > 0:
            logger.info(f"✓ Bridge system running time configuration:{bridged_count}Item")

        #Synchronize to filesystem (for unified config)
        try:
            print(f"🔄 [config_bridge] 准备同步系统设置到文件系统")
            print(f"🔄 [config_bridge] system_settings 包含 {len(system_settings)} 项")

            #Check key fields
            if "quick_analysis_model" in system_settings:
                print(f"  ✓ [config_bridge] 包含 quick_analysis_model: {system_settings['quick_analysis_model']}")
            else:
                print(f"  ⚠️  [config_bridge] 不包含 quick_analysis_model")

            if "deep_analysis_model" in system_settings:
                print(f"  ✓ [config_bridge] 包含 deep_analysis_model: {system_settings['deep_analysis_model']}")
            else:
                print(f"  ⚠️  [config_bridge] 不包含 deep_analysis_model")

            from app.core.unified_config import UNIFIED_CONFIG_MANAGER
            result = UNIFIED_CONFIG_MANAGER.save_system_settings(system_settings)

            if result:
                logger.info(f"System Settings Synchronized to File System")
                print(f"✅ [config_bridge] 系统设置同步成功")
            else:
                logger.warning(f"⚠️System Settings Sync returns False")
                print(f"⚠️  [config_bridge] 系统设置同步返回 False")
        except Exception as e:
            logger.warning(f"Could not close temporary folder: %s{e}")
            print(f"❌ [config_bridge] 同步系统设置到文件系统失败: {e}")
            import traceback
            print(traceback.format_exc())

        return bridged_count

    except Exception as e:
        logger.warning(f"The bridge setup failed:{e}")
        return 0


def get_bridged_api_key(provider: str) -> Optional[str]:
    """Fetch Bridge API Key

    Args:
        Provider: Provider name (e. g. openai, Deepseek, Dashscope)

    Returns:
        API key, if no returns
    """
    env_key = f"{provider.upper()}_API_KEY"
    return os.environ.get(env_key)


def get_bridged_model(model_type: str = "default") -> Optional[str]:
    """Getting Bridge Model Name

    Args:
        Model  type: Model type (default, quick, Deep)

    Returns:
        Model name, if none returns
    """
    if model_type == "quick":
        return os.environ.get('TRADINGAGENTS_QUICK_MODEL')
    elif model_type == "deep":
        return os.environ.get('TRADINGAGENTS_DEEP_MODEL')
    else:
        return os.environ.get('TRADINGAGENTS_DEFAULT_MODEL')


def clear_bridged_config():
    """Clear configuration of the bridge

    For testing or reloading configuration
    """
    keys_to_clear = [
        #Model Configuration
        'TRADINGAGENTS_DEFAULT_MODEL',
        'TRADINGAGENTS_QUICK_MODEL',
        'TRADINGAGENTS_DEEP_MODEL',
        #Data Source API Key
        'TUSHARE_TOKEN',
        'FINNHUB_API_KEY',
        #System Configuration
        'APP_TIMEZONE',
        'CURRENCY_PREFERENCE',
    ]

    #Clear all possible API keys
    providers = ['OPENAI', 'ANTHROPIC', 'GOOGLE', 'DEEPSEEK', 'DASHSCOPE', 'QIANFAN']
    for provider in providers:
        keys_to_clear.append(f'{provider}_API_KEY')

    #Clear Data Source Detail Configuration
    data_sources = ['TUSHARE', 'AKSHARE', 'FINNHUB']
    for ds in data_sources:
        keys_to_clear.extend([
            f'{ds}_TIMEOUT',
            f'{ds}_RATE_LIMIT',
            f'{ds}_MAX_RETRIES',
            f'{ds}_CACHE_TTL',
            f'{ds}_CACHE_ENABLED',
        ])

    #Clear TradingAgents Runtime Configuration
    ta_runtime_keys = [
        'TA_HK_MIN_REQUEST_INTERVAL_SECONDS',
        'TA_HK_TIMEOUT_SECONDS',
        'TA_HK_MAX_RETRIES',
        'TA_HK_RATE_LIMIT_WAIT_SECONDS',
        'TA_HK_CACHE_TTL_SECONDS',
        'TA_USE_APP_CACHE',
    ]
    keys_to_clear.extend(ta_runtime_keys)

    for key in keys_to_clear:
        if key in os.environ:
            del os.environ[key]
            logger.debug(f"Clear Environmental Variables:{key}")

    logger.info("All bridge configurations cleared")


def reload_bridged_config():
    """Reload bridge configuration

    For configuration updated and reconnect
    """
    logger.info("Reload the configuration bridge...")
    clear_bridged_config()
    return consolidate_configs_to_osenviron()


def _sync_pricing_config(llm_configs):
    """Synchronise pricing configuration to contact/pricing.json

    Args:
        llm configs: LLM configuration list
    """
    try:
        #Config directory to fetch root directory
        project_root = Path(__file__).parent.parent.parent
        config_dir = project_root / "config"
        config_dir.mkdir(exist_ok=True)

        pricing_file = config_dir / "pricing.json"

        #Build pricing configuration list
        pricing_configs = []
        for llm_config in llm_configs:
            if llm_config.enabled:
                pricing_config = {
                    #Now it's a string type. It's no longer an anemic.
                    "provider": llm_config.provider,
                    "model_name": llm_config.model_name,
                    "input_price_per_1k": llm_config.input_price_per_1k or 0.0,
                    "output_price_per_1k": llm_config.output_price_per_1k or 0.0,
                    "currency": llm_config.currency or "CNY"
                }
                pricing_configs.append(pricing_config)

        #Save to File
        with open(pricing_file, 'w', encoding='utf-8') as f:
            json.dump(pricing_configs, f, ensure_ascii=False, indent=2)

        logger.info(f"Synchronise pricing configuration to{pricing_file}: {len(pricing_configs)}Model")

    except Exception as e:
        logger.warning(f"Synchronized pricing configuration failed:{e}")


def sync_pricing_config_now():
    """Synchronize pricing configuration immediately (for real time synchronization after configuration update)

    Note: This function performs sync operations at step step back.
    """
    import asyncio

    try:
        #Create a background job if in the context of a step
        try:
            loop = asyncio.get_running_loop()
            #Create a back-office task in the context of a step (not awaiting completion)
            task = loop.create_task(_sync_pricing_config_from_db())
            #Add Callback to Record Errors
            task.add_done_callback(_handle_sync_task_result)
            logger.info("🔄 Pricing configuration sync task created (backstage execution)")
            return True
        except RuntimeError:
            #Use asyncio.run, not in the variable context
            asyncio.run(_sync_pricing_config_from_db())
            return True
    except Exception as e:
        logger.error(f"❌ >Sync pricing configuration failed immediately:{e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def _handle_sync_task_result(task):
    """Process the result of the sync task"""
    try:
        task.result()
    except Exception as e:
        logger.error(f"❌ Pricing configuration synchronized task execution failed:{e}")
        import traceback
        logger.error(traceback.format_exc())


async def _sync_pricing_config_from_db():
    """Synchronize pricing configuration from database (speech version)
    """
    try:
        from app.core.database import get_mongo_db_async
        from app.models.config_models import LLMConfig

        db = get_mongo_db_async()

        #Get the latest active configuration
        config = await db['system_configs'].find_one(
            {'is_active': True},
            sort=[('version', -1)]
        )

        if not config:
            logger.warning("No activated configuration found")
            return

        #Config directory to fetch root directory
        project_root = Path(__file__).parent.parent.parent
        config_dir = project_root / "config"
        config_dir.mkdir(exist_ok=True)

        pricing_file = config_dir / "pricing.json"

        #Build pricing configuration list
        pricing_configs = []
        for llm_config in config.get('llm_configs', []):
            if llm_config.get('enabled', False):
                #Reads a dictionary from a database using a string directly
                provider = llm_config.get('provider')

                #If provider is an item type, convert to a string
                if hasattr(provider, 'value'):
                    provider = provider.value

                pricing_config = {
                    "provider": provider,
                    "model_name": llm_config.get('model_name'),
                    "input_price_per_1k": llm_config.get('input_price_per_1k') or 0.0,
                    "output_price_per_1k": llm_config.get('output_price_per_1k') or 0.0,
                    "currency": llm_config.get('currency') or "CNY"
                }
                pricing_configs.append(pricing_config)

        #Save to File
        with open(pricing_file, 'w', encoding='utf-8') as f:
            json.dump(pricing_configs, f, ensure_ascii=False, indent=2)

        logger.info(f"Synchronise pricing configuration to{pricing_file}: {len(pricing_configs)}Model")

    except Exception as e:
        logger.error(f"Synchronized pricing configuration from database failed:{e}")
        import traceback
        logger.error(traceback.format_exc())


#Export Functions
__all__ = [
    'consolidate_configs_to_osenviron',
    'get_bridged_api_key',
    'get_bridged_model',
    'clear_bridged_config',
    'reload_bridged_config',
    'sync_pricing_config_now',
]

