"""Simplified stock analysis services
Directly call existing TradingAgendas analysis
"""

import asyncio
import uuid
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path
import sys

#Add root directory to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

#Initializing TradingAgents Log System
from tradingagents.utils.logging_init import init_logging
init_logging()

from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.default_config import DEFAULT_CONFIG
from app.models.analysis import (
    AnalysisTask, AnalysisStatus, SingleAnalysisRequest, AnalysisParameters
)
from app.models.user import PyObjectId
from app.models.notification import NotificationCreate
from bson import ObjectId
from app.core.database import get_mongo_db
from app.services.config_service import ConfigService
from app.services.memory_state_manager import get_memory_state_manager, TaskStatus
from app.services.redis_progress_tracker import RedisProgressTracker, get_progress_by_id
from app.services.progress_log_handler import register_analysis_tracker, unregister_analysis_tracker

#Share basic information acquisition (for additional display names)
try:
    from tradingagents.dataflows.data_source_manager import get_data_source_manager
    _data_source_manager = get_data_source_manager()
    def _get_stock_info_safe(stock_code: str):
        """Secure seal to obtain basic stock information"""
        return _data_source_manager.get_stock_basic_info(stock_code)
except Exception:
    _get_stock_info_safe = None

#Set Log
logger = logging.getLogger("app.services.simple_analysis_service")

#Examples of Configure Services
config_service = ConfigService()


async def get_provider_by_model_name(model_name: str) -> str:
    """Find the corresponding supplier from the database configuration according to the model name (speech version)

    Args:
        Model name: Model names, such as 'qwen-turbo', 'gpt-4', etc.

    Returns:
        st: Name of supplier, e.g. 'dashscope', 'openai', etc.
    """
    try:
        #Get the system configuration from the configuration service
        system_config = await config_service.get_system_config()
        if not system_config or not system_config.llm_configs:
            logger.warning(f"⚠️ System configuration empty with default vendor mapping")
            return _get_default_provider_by_model(model_name)

        #Find matching models in LLM configuration
        for llm_config in system_config.llm_configs:
            if llm_config.model_name == model_name:
                provider = llm_config.provider.value if hasattr(llm_config.provider, 'value') else str(llm_config.provider)
                logger.info(f"Find a model from the database{model_name}Vendors:{provider}")
                return provider

        #Use default map if not found in database
        logger.warning(f"No model found in database ⚠️{model_name}, using default map")
        return _get_default_provider_by_model(model_name)

    except Exception as e:
        logger.error(f"❌ to find model supplier failure:{e}")
        return _get_default_provider_by_model(model_name)


def get_provider_by_model_name_sync(model_name: str) -> str:
    """Find the corresponding supplier from the database configuration according to the model name (sync version)

    Args:
        Model name: Model names, such as 'qwen-turbo', 'gpt-4', etc.

    Returns:
        st: Name of supplier, e.g. 'dashscope', 'openai', etc.
    """
    provider_info = get_provider_and_url_by_model_sync(model_name)
    return provider_info["provider"]


def get_provider_and_url_by_model_sync(model_name: str) -> dict:
    """Find corresponding suppliers and API URLs from the database configuration according to the model name

    Args:
        Model name: Model names, such as 'qwen-turbo', 'gpt-4', etc.

    Returns:
        dict:   FMT 0 
    """
    try:
        #Direct query using Sync MongoDB client
        from pymongo import MongoClient
        from app.core.config import SETTINGS
        import os

        client = MongoClient(SETTINGS.MONGO_URI)
        db = client[SETTINGS.MONGO_DB_NAME]

        #Query the latest active configuration
        configs_collection = db.system_configs
        doc = configs_collection.find_one({"is_active": True}, sort=[("version", -1)])

        if doc and "llm_configs" in doc:
            llm_configs = doc["llm_configs"]

            for config_dict in llm_configs:
                if config_dict.get("model_name") == model_name:
                    provider = config_dict.get("provider")
                    api_base = config_dict.get("api_base")
                    model_api_key = config_dict.get("api_key")  #API Key for the model configuration

                    #Find plant configuration from llm providers
                    providers_collection = db.llm_providers
                    provider_doc = providers_collection.find_one({"name": provider})

                    #🔥 Determine API Key (priority: model configuration > plant configuration > environmental variable)
                    api_key = None
                    if model_api_key and model_api_key.strip() and model_api_key != "your-api-key":
                        api_key = model_api_key
                        logger.info(f"✅ [Sync Query] Using model configured API Key")
                    elif provider_doc and provider_doc.get("api_key"):
                        provider_api_key = provider_doc["api_key"]
                        if provider_api_key and provider_api_key.strip() and provider_api_key != "your-api-key":
                            api_key = provider_api_key
                            logger.info(f"✅ [Sync Query] Using the plant configuration of API Key")

                    #If there is no valid API Key in the database, try to get it from the environment variable
                    if not api_key:
                        api_key = _get_env_api_key_for_provider(provider)
                        if api_key:
                            logger.info(f"✅ [Sync Query] API Key for using environment variables")
                        else:
                            logger.warning(f"[Sync Query]{provider}API Key")

                    #Confirm
                    backend_url = None
                    if api_base:
                        backend_url = api_base
                        logger.info(f"[Sync Query] Model{model_name}Use custom API:{api_base}")
                    elif provider_doc and provider_doc.get("default_base_url"):
                        backend_url = provider_doc["default_base_url"]
                        logger.info(f"[Sync Query] Model{model_name}API:{backend_url}")
                    else:
                        backend_url = _get_default_backend_url(provider)
                        logger.warning(f"[Sync Query]{provider}No profile base url configured, with hard-coding default")

                    client.close()
                    return {
                        "provider": provider,
                        "backend_url": backend_url,
                        "api_key": api_key
                    }

        client.close()

        #Use default map if no model configuration is found in the database
        logger.warning(f"No model found in database ⚠️{model_name}, using default map")
        provider = _get_default_provider_by_model(model_name)

        #Try fetching data base url and API Key from the vendor configuration
        try:
            client = MongoClient(SETTINGS.MONGO_URI)
            db = client[SETTINGS.MONGO_DB_NAME]
            providers_collection = db.llm_providers
            provider_doc = providers_collection.find_one({"name": provider})

            backend_url = _get_default_backend_url(provider)
            api_key = None

            if provider_doc:
                if provider_doc.get("default_base_url"):
                    backend_url = provider_doc["default_base_url"]
                    logger.info(f"[Sync Query]{provider}BAR url:{backend_url}")

                if provider_doc.get("api_key"):
                    provider_api_key = provider_doc["api_key"]
                    if provider_api_key and provider_api_key.strip() and provider_api_key != "your-api-key":
                        api_key = provider_api_key
                        logger.info(f"[Sync Query]{provider}API Key")

            #If there is no API Key in the plant configuration, try to get it from the environment variable
            if not api_key:
                api_key = _get_env_api_key_for_provider(provider)
                if api_key:
                    logger.info(f"✅ [Sync Query] API Key for using environment variables")

            client.close()
            return {
                "provider": provider,
                "backend_url": backend_url,
                "api_key": api_key
            }
        except Exception as e:
            logger.warning(f"Could not close temporary folder: %s{e}")

        #Last retreat to the default URL for hard encoding and environment variable API Key
        return {
            "provider": provider,
            "backend_url": _get_default_backend_url(provider),
            "api_key": _get_env_api_key_for_provider(provider)
        }

    except Exception as e:
        logger.error(f"[Synthetic Query]{e}")
        provider = _get_default_provider_by_model(model_name)

        #Try fetching data base url and API Key from the vendor configuration
        try:
            from pymongo import MongoClient
            from app.core.config import SETTINGS

            client = MongoClient(SETTINGS.MONGO_URI)
            db = client[SETTINGS.MONGO_DB_NAME]
            providers_collection = db.llm_providers
            provider_doc = providers_collection.find_one({"name": provider})

            backend_url = _get_default_backend_url(provider)
            api_key = None

            if provider_doc:
                if provider_doc.get("default_base_url"):
                    backend_url = provider_doc["default_base_url"]
                    logger.info(f"[Sync Query]{provider}BAR url:{backend_url}")

                if provider_doc.get("api_key"):
                    provider_api_key = provider_doc["api_key"]
                    if provider_api_key and provider_api_key.strip() and provider_api_key != "your-api-key":
                        api_key = provider_api_key
                        logger.info(f"[Sync Query]{provider}API Key")

            #If there is no API Key in the plant configuration, try to get it from the environment variable
            if not api_key:
                api_key = _get_env_api_key_for_provider(provider)

            client.close()
            return {
                "provider": provider,
                "backend_url": backend_url,
                "api_key": api_key
            }
        except Exception as e2:
            logger.warning(f"Could not close temporary folder: %s{e2}")

        #Last retreat to the default URL for hard encoding and environment variable API Key
        return {
            "provider": provider,
            "backend_url": _get_default_backend_url(provider),
            "api_key": _get_env_api_key_for_provider(provider)
        }


def _get_env_api_key_for_provider(provider: str) -> str:
    """API Key from Environmental Variables

    Args:
        Provider: Name of supplier, such as 'google', 'dashscope', etc.

    Returns:
        str: API Key, return None if not found
    """
    import os

    env_key_map = {
        "google": "GOOGLE_API_KEY",
        "dashscope": "DASHSCOPE_API_KEY",
        "openai": "OPENAI_API_KEY",
        "deepseek": "DEEPSEEK_API_KEY",
        "anthropic": "ANTHROPIC_API_KEY",
        "openrouter": "OPENROUTER_API_KEY",
        "siliconflow": "SILICONFLOW_API_KEY",
        "qianfan": "QIANFAN_API_KEY",
        "302ai": "AI302_API_KEY",
    }

    env_key_name = env_key_map.get(provider.lower())
    if env_key_name:
        api_key = os.getenv(env_key_name)
        if api_key and api_key.strip() and api_key != "your-api-key":
            return api_key

    return None


def _get_default_backend_url(provider: str) -> str:
    """Return default backend url by supplier name

    Args:
        Provider: Name of supplier, such as 'google', 'dashscope', etc.

    Returns:
        str: Default backend url
    """
    default_urls = {
        "google": "https://generativelanguage.googleapis.com/v1beta",
        "dashscope": "https://dashscope.aliyuncs.com/api/v1",
        "openai": "https://api.openai.com/v1",
        "deepseek": "https://api.deepseek.com",
        "anthropic": "https://api.anthropic.com",
        "openrouter": "https://openrouter.ai/api/v1",
        "qianfan": "https://qianfan.baidubce.com/v2",
        "302ai": "https://api.302.ai/v1",
    }

    url = default_urls.get(provider, "https://dashscope.aliyuncs.com/compatible-mode/v1")
    logger.info(f"[default URL]{provider} -> {url}")
    return url


def _get_default_provider_by_model(model_name: str) -> str:
    """Returns the default vendor map by model name
    This is a backup scheme to be used when database queries fail
    """
    #Model name to the vendor 's default map
    model_provider_map = {
        #DashScope
        'qwen-turbo': 'dashscope',
        'qwen-plus': 'dashscope',
        'qwen-max': 'dashscope',
        'qwen-plus-latest': 'dashscope',
        'qwen-max-longcontext': 'dashscope',

        # OpenAI
        'gpt-3.5-turbo': 'openai',
        'gpt-4': 'openai',
        'gpt-4-turbo': 'openai',
        'gpt-4o': 'openai',
        'gpt-4o-mini': 'openai',

        # Google
        'gemini-pro': 'google',
        'gemini-2.0-flash': 'google',
        'gemini-2.0-flash-thinking-exp': 'google',

        # DeepSeek
        'deepseek-chat': 'deepseek',
        'deepseek-coder': 'deepseek',

        #Genre AI
        'glm-4': 'zhipu',
        'glm-3-turbo': 'zhipu',
        'chatglm3-6b': 'zhipu'
    }

    provider = model_provider_map.get(model_name, 'dashscope')  #Default use of Aliblanc
    logger.info(f"Using default maps:{model_name} -> {provider}")
    return provider


def create_analysis_config(
    research_depth,  #Support numbers (1-5) or strings ( "quick", "standard", "deep")
    selected_analysts: list,
    quick_model: str,
    deep_model: str,
    llm_provider: str,
    market_type: str = "A股",
    quick_model_config: dict = None,  #Add: Full configuration of fast models
    deep_model_config: dict = None    #Add: Full configuration of the depth model
) -> dict:
    """Create Analysis Configuration - Supporting Numerical and Chinese Levels

    Args:
        Research depth: Research depth, supporting numbers (1-5) or Chinese ( "quick", "basis", "standard", "deep", "full")
        list of selected analysts
        Quick model: Rapid Analysis Model
        Deep model: Depth Analysis Model
        Ilm provider: LLM supplier
        market type: Market type
        Quick model config: Full configuration of fast models (including max tokens, temperature, timeout etc.)
        Deep model config: complete configuration of depth models (including max tokens, temperature, timeout, etc.)

    Returns:
        dict: Full analytical configuration
    """
    #[Debugging]
    logger.info(f"[configuration creates]{research_depth}(Types:{type(research_depth).__name__})")

    #Map of Numerical to Chinese Level
    numeric_to_chinese = {
        1: "快速",
        2: "基础",
        3: "标准",
        4: "深度",
        5: "全面"
    }

    #Standardized research depth: supporting digital input
    if isinstance(research_depth, (int, float)):
        research_depth = int(research_depth)
        if research_depth in numeric_to_chinese:
            chinese_depth = numeric_to_chinese[research_depth]
            logger.info(f"🔢 [level conversion] Numerical grade{research_depth}→ Chinese rank '{chinese_depth}'")
            research_depth = chinese_depth
        else:
            logger.warning(f"Invalid numerical grade:{research_depth}, use default standard analysis")
            research_depth = "标准"
    elif isinstance(research_depth, str):
        #If a number is in string form, convert to integer
        if research_depth.isdigit():
            numeric_level = int(research_depth)
            if numeric_level in numeric_to_chinese:
                chinese_depth = numeric_to_chinese[numeric_level]
                logger.info(f"🔢 [class transformation] String numbers '{research_depth}' → Chinese rank '{chinese_depth}'")
                research_depth = chinese_depth
            else:
                logger.warning(f"Invalid string numerical level:{research_depth}, use default standard analysis")
                research_depth = "标准"
        #If it's already Chinese, use it directly.
        elif research_depth in ["快速", "基础", "标准", "深度", "全面"]:
            logger.info(f"📝 [level confirmation] For Chinese: '{research_depth}'")
        else:
            logger.warning(f"Unknown depth of study:{research_depth}, use default standard analysis")
            research_depth = "标准"
    else:
        logger.warning(f"Invalid study depth type:{type(research_depth)}, use default standard analysis")
        research_depth = "标准"

    #Full copy of the logic of the web directory, starting with DEFAULT CONFIG
    config = DEFAULT_CONFIG.copy()
    config["llm_provider"] = llm_provider
    config["deep_think_llm"] = deep_model
    config["quick_think_llm"] = quick_model

    #Reconfigure the configuration to the depth of the study - support 5 levels (consistent with Web interface)
    if research_depth == "快速":
        #Level 1 - Rapid analysis
        config["max_debate_rounds"] = 1
        config["max_risk_discuss_rounds"] = 1
        config["memory_enabled"] = False  #Disable memory to accelerate
        config["online_tools"] = True  #Harmonized use of online tools to avoid problems with offline tools
        logger.info(f"🔧 [Class 1 -- rapid analysis]{market_type}Use of harmonized tools to ensure correct and stable data sources")
        logger.info(f"Using the user profile model: Quick={quick_model}, deep={deep_model}")

    elif research_depth == "基础":
        #Level 2 - Basic analysis
        config["max_debate_rounds"] = 1
        config["max_risk_discuss_rounds"] = 1
        config["memory_enabled"] = True
        config["online_tools"] = True
        logger.info(f"🔧 [Class 2 -- basic analysis]{market_type}Use of online tools to obtain up-to-date data")
        logger.info(f"Using user-configured models: Quick={quick_model}, deep={deep_model}")

    elif research_depth == "标准":
        #Level 3 - Standard analysis (recommended)
        config["max_debate_rounds"] = 1
        config["max_risk_discuss_rounds"] = 2
        config["memory_enabled"] = True
        config["online_tools"] = True
        logger.info(f"🔧 [3 Level - Standard Analysis]{market_type}Balance speed and quality (recommended)")
        logger.info(f"Using the user profile model: Quick={quick_model}, deep={deep_model}")

    elif research_depth == "深度":
        #Level 4 - Depth analysis
        config["max_debate_rounds"] = 2
        config["max_risk_discuss_rounds"] = 2
        config["memory_enabled"] = True
        config["online_tools"] = True
        logger.info(f"🔧 [4-Depth analysis]{market_type}Multiple rounds of debate, in-depth research")
        logger.info(f"Using the user profile model: Quick={quick_model}, deep={deep_model}")

    elif research_depth == "全面":
        #Level 5 - Comprehensive analysis
        config["max_debate_rounds"] = 3
        config["max_risk_discuss_rounds"] = 3
        config["memory_enabled"] = True
        config["online_tools"] = True
        logger.info(f"🔧 [Class 5 -- comprehensive analysis]{market_type}Most comprehensive analysis, highest quality")
        logger.info(f"Using the user profile model: Quick={quick_model}, deep={deep_model}")

    else:
        #Default use of standard analysis
        logger.warning(f"Unknown depth of study:{research_depth}, using standard analysis")
        config["max_debate_rounds"] = 1
        config["max_risk_discuss_rounds"] = 2
        config["memory_enabled"] = True
        config["online_tools"] = True

    #🔧 get back url and API Key (priority: model configuration > plant configuration > environmental variable)
    try:
        #1️⃣ Prefer to access the database (api base, API Key and manufacturer's default base url, API Key)
        quick_provider_info = get_provider_and_url_by_model_sync(quick_model)
        deep_provider_info = get_provider_and_url_by_model_sync(deep_model)

        config["backend_url"] = quick_provider_info["backend_url"]
        config["quick_api_key"] = quick_provider_info.get("api_key")  #API Key to save fast-models
        config["deep_api_key"] = deep_provider_info.get("api_key")    #API Key for saving depth models

        logger.info(f"✅ with the database configuration back url:{quick_provider_info['backend_url']}")
        logger.info(f"Source: Model{quick_model}or plant{quick_provider_info['provider']}Default address")
        logger.info(f"API Key:{'Configured' if config['quick_api_key'] else 'Unconfigured (environmental variables will be used)'}")
        logger.info(f"Depth model API Key:{'Configured' if config['deep_api_key'] else 'Unconfigured (environmental variables will be used)'}")
    except Exception as e:
        logger.warning(f"Can not get folder: %s: %s{e}")
        #Back to the default URL for hard-coding, API Key will be read from the environment variable
        if llm_provider == "dashscope":
            config["backend_url"] = "https://dashscope.aliyuncs.com/api/v1"
        elif llm_provider == "deepseek":
            config["backend_url"] = "https://api.deepseek.com"
        elif llm_provider == "openai":
            config["backend_url"] = "https://api.openai.com/v1"
        elif llm_provider == "google":
            config["backend_url"] = "https://generativelanguage.googleapis.com/v1beta"
        elif llm_provider == "qianfan":
            config["backend_url"] = "https://aip.baidubce.com"
        else:
            #Unknown manufacturer, attempt to access the manufacturer's database
            logger.warning(f"Unknown manufacturer{llm_provider}, try to get configuration from the database")
            try:
                from pymongo import MongoClient
                from app.core.config import SETTINGS

                client = MongoClient(SETTINGS.MONGO_URI)
                db = client[SETTINGS.MONGO_DB_NAME]
                providers_collection = db.llm_providers
                provider_doc = providers_collection.find_one({"name": llm_provider})

                if provider_doc and provider_doc.get("default_base_url"):
                    config["backend_url"] = provider_doc["default_base_url"]
                    logger.info(f"Get custom manufacturers from the database{llm_provider}Back url:{config['backend_url']}")
                else:
                    #Use OpenAI compatible format as the last exit if not found in database
                    config["backend_url"] = "https://api.openai.com/v1"
                    logger.warning(f"No manufacturer found in database{llm_provider}, using the default OpenAI endpoint")

                client.close()
            except Exception as e2:
                logger.error(f"Could not close temporary folder: %s{e2}, use default OpenAI endpoint")
                config["backend_url"] = "https://api.openai.com/v1"

        logger.info(f"Use back url:{config['backend_url']}")

    #Add Analyst Configuration
    config["selected_analysts"] = selected_analysts
    config["debug"] = False

    #<🔧 Add research depth to configuration to enable tool functions to access analytical level information
    config["research_depth"] = research_depth

    #🔧 Add model configuration parameters (max tokens, temperature, timeout, retry times)
    if quick_model_config:
        config["quick_model_config"] = quick_model_config
        logger.info(f"[Quick Model Configuration]{quick_model_config.get('max_tokens')}, "
                   f"temperature={quick_model_config.get('temperature')}, "
                   f"timeout={quick_model_config.get('timeout')}, "
                   f"retry_times={quick_model_config.get('retry_times')}")

    if deep_model_config:
        config["deep_model_config"] = deep_model_config
        logger.info(f"[Depth Model Configuration]{deep_model_config.get('max_tokens')}, "
                   f"temperature={deep_model_config.get('temperature')}, "
                   f"timeout={deep_model_config.get('timeout')}, "
                   f"retry_times={deep_model_config.get('retry_times')}")

    logger.info(f"== sync, corrected by elderman == @elder man")
    logger.info(f"Depth of research:{research_depth}")
    logger.info(f"Debates on the following rounds:{config['max_debate_rounds']}")
    logger.info(f"Risk discussion rounds:{config['max_risk_discuss_rounds']}")
    logger.info(f"Memory function:{config['memory_enabled']}")
    logger.info(f"Online tools:{config['online_tools']}")
    logger.info(f"LLM supplier:{llm_provider}")
    logger.info(f"Fast model:{config['quick_think_llm']}")
    logger.info(f"Depth model:{config['deep_think_llm']}")
    logger.info(f"📋 ========================================")

    return config


class SimpleAnalysisService:
    """Simplified stock analysis services"""

    def __init__(self):
        self._trading_graph_cache = {}
        self.memory_manager = get_memory_state_manager()

        #Progress Tracker Cache
        self._progress_trackers: Dict[str, RedisProgressTracker] = {}

        #🔧 Create shared thread pools to support and deliver multiple analytical tasks
        #Default to perform up to 3 analytical tasks simultaneously (adjusted for server resources)
        import concurrent.futures
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=3)

        logger.info(f"SimpleAnalysisService ExampleID:{id(self)}")
        logger.info(f"[service initialization] memory manager example ID:{id(self.memory_manager)}")
        logger.info(f"🔧 [Initiation of services] Thresholds maximized: 3")

        #Setup WebSocket Manager
        #Simple stock name cache, less repeat queries
        self._stock_name_cache: Dict[str, str] = {}

        #Setup WebSocket Manager
        try:
            from app.services.websocket_manager import get_websocket_manager
            self.memory_manager.set_websocket_manager(get_websocket_manager())
        except ImportError:
            logger.warning("WebSocket Manager not available")

    async def _update_progress_async(self, task_id: str, progress: int, message: str):
        """Step up progress (RAM and MongoDB)"""
        try:
            #Update Memory
            await self.memory_manager.update_task_status(
                task_id=task_id,
                status=TaskStatus.RUNNING,
                progress=progress,
                message=message,
                current_step=message
            )

            #Update MongoDB
            from app.core.database import get_mongo_db
            from datetime import datetime
            db = get_mongo_db()
            await db.analysis_tasks.update_one(
                {"task_id": task_id},
                {
                    "$set": {
                        "progress": progress,
                        "current_step": message,
                        "message": message,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            logger.debug(f"✅ [by step update] Updated memory and MongoDB:{progress}%")
        except Exception as e:
            logger.warning(f"[Indistinct update] Failed:{e}")

    def _resolve_stock_name(self, code: Optional[str]) -> str:
        """Parsing stock name (with cache)"""
        if not code:
            return ""
        #Hit Cache
        if code in self._stock_name_cache:
            return self._stock_name_cache[code]
        name = None
        try:
            if _get_stock_info_safe:
                info = _get_stock_info_safe(code)
                if isinstance(info, dict):
                    name = info.get("name")
        except Exception as e:
            logger.warning(f"Could not close temporary folder: %s{code} - {e}")
        if not name:
            name = f"股票{code}"
        #Write Cache
        self._stock_name_cache[code] = name
        return name

    def _enrich_stock_names(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Complete the name of the stock for the task list (update locally)"""
        try:
            for t in tasks:
                code = t.get("stock_code") or t.get("stock_symbol")
                name = t.get("stock_name")
                if not name and code:
                    t["stock_name"] = self._resolve_stock_name(code)
        except Exception as e:
            logger.warning(f"An anomaly occurred when the name of the stock was completed:{e}")
        return tasks

    def _convert_user_id(self, user_id: str) -> PyObjectId:
        """Convert string userID to PyObjectId"""
        try:
            logger.info(f"Start switching user ID:{user_id}(Types:{type(user_id)})")

            #For admin users, use fixedObjectId
            if user_id == "admin":
                admin_object_id = ObjectId("507f1f77bcf86cd799439011")
                logger.info(f"Converting admin user ID:{user_id} -> {admin_object_id}")
                return PyObjectId(admin_object_id)
            else:
                #Try converting string to objectId
                object_id = ObjectId(user_id)
                logger.info(f"Other Organiser{user_id} -> {object_id}")
                return PyObjectId(object_id)
        except Exception as e:
            logger.error(f"User ID conversion failed:{user_id} -> {e}")
            #If conversion fails, create a new objectID
            new_object_id = ObjectId()
            logger.warning(f"⚠️ Generates new user ID:{new_object_id}")
            return PyObjectId(new_object_id)

    def _get_trading_graph(self, config: Dict[str, Any]) -> TradingAgentsGraph:
        """Fetching or Creating Action Examples

        Note: Create new examples each time to avoid data confusion at the time of execution
        It'll add some initialization costs, but it'll make it safe.

        The example of Trading Agencies Graph includes variable state (self.ticker, self.curr state, etc.).
        If multiple threads share the same example, this can lead to data confusion.
        """
        #🔧 [Soft Together] Creates new examples each time to avoid multi-wire sharing
        #No more caches because TradingAgentsGraph has variable case variables
        logger.info(f"🔧 Creates a new TradingAgents instance...")

        trading_graph = TradingAgentsGraph(
            selected_analysts=config.get("selected_analysts", ["market", "fundamentals"]),
            debug=config.get("debug", False),
            config=config
        )

        logger.info(f"✅ TradingAgents instance created successfully (example ID:{id(trading_graph)}）")

        return trading_graph

    async def create_analysis_task(
        self,
        user_id: str,
        request: SingleAnalysisRequest
    ) -> Dict[str, Any]:
        """Create analytical tasks (return immediately, no analysis performed)"""
        try:
            #Generate Task ID
            task_id = str(uuid.uuid4())

            #🔧 Use get symbol() to get stock codes (compatible symbol and stock code fields)
            stock_code = request.get_symbol()
            if not stock_code:
                raise ValueError("股票代码不能为空")

            logger.info(f"Other Organiser{task_id} - {stock_code}")
            logger.info(f"Memory Manager Example ID:{id(self.memory_manager)}")

            #Create Task Status in Memory
            task_state = await self.memory_manager.create_task(
                task_id=task_id,
                user_id=user_id,
                stock_code=stock_code,
                parameters=request.parameters.model_dump() if request.parameters else {},
                stock_name=(self._resolve_stock_name(stock_code) if hasattr(self, '_resolve_stock_name') else None),
            )

            logger.info(f"Mission status created:{task_state.task_id}")

            #Could not close temporary folder: %s
            verify_task = await self.memory_manager.get_task(task_id)
            if verify_task:
                logger.info(f"Could not close temporary folder: %s{verify_task.task_id}")
            else:
                logger.error(f"Could not close temporary folder: %s{task_id}")

            #Complete the stock name and write the initial record of the database task document
            code = stock_code
            name = self._resolve_stock_name(code) if hasattr(self, '_resolve_stock_name') else f"股票{code}"

            try:
                db = get_mongo_db()
                result = await db.analysis_tasks.update_one(
                    {"task_id": task_id},
                    {"$setOnInsert": {
                        "task_id": task_id,
                        "user_id": user_id,
                        "stock_code": code,
                        "stock_symbol": code,
                        "stock_name": name,
                        "status": "pending",
                        "progress": 0,
                        "created_at": datetime.utcnow(),
                    }},
                    upsert=True
                )

                if result.upserted_id or result.matched_count > 0:
                    logger.info(f"Mission saved to MongoDB:{task_id}")
                else:
                    logger.warning(f"MongoDB saves abnormally:{result.matched_count}, upserted={result.upserted_id}")

            except Exception as e:
                logger.error(f"Writing MongoDB to create the task failed:{e}")
                #The error should not be ignored here, because the absence of MongoDB records leads to failure of status queries
                #But in order not to interfere with mission performance, we recorded errors but continued to perform.
                import traceback
                logger.error(f"MongoDB saves detailed error:{traceback.format_exc()}")

            return {
                "task_id": task_id,
                "status": "pending",
                "message": "任务已创建，等待执行"
            }

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            raise

    async def execute_analysis_background(
        self,
        task_id: str,
        user_id: str,
        request: SingleAnalysisRequest
    ):
        """Conduct analytical missions backstage"""
        #🔧 Use get symbol() to get stock codes (compatible symbol and stock code fields)
        stock_code = request.get_symbol()

        #Add the outermost abnormal catch to ensure that all anomalies are recorded
        try:
            logger.info(f"[ENTRY] approach analysis background{task_id}")
            logger.info(f"🎯🎯🎯 [ENTRY] user_id={user_id}, stock_code={stock_code}")
        except Exception as entry_error:
            print(f"❌❌❌ [CRITICAL] 日志记录失败: {entry_error}")
            import traceback
            traceback.print_exc()

        progress_tracker = None
        try:
            logger.info(f"We'll start the analysis backstage:{task_id}")

            #Validation of stock code
            logger.info(f"Starting to verify the stock code:{stock_code}")
            from tradingagents.utils.stock_validator import prepare_stock_data_async
            from datetime import datetime

            #Market acquisition type
            market_type = request.parameters.market_type if request.parameters else "A股"

            #Get the parsing date and convert it to string format
            analysis_date = request.parameters.analysis_date if request.parameters else None
            if analysis_date:
                #Convert to string if datetime object
                if isinstance(analysis_date, datetime):
                    analysis_date = analysis_date.strftime('%Y-%m-%d')
                #If it's a string, make sure the format is correct.
                elif isinstance(analysis_date, str):
                    #Try parsing and reformatting to ensure uniform format
                    try:
                        parsed_date = datetime.strptime(analysis_date, '%Y-%m-%d')
                        analysis_date = parsed_date.strftime('%Y-%m-%d')
                    except ValueError:
                        #Use today if format is wrong
                        analysis_date = datetime.now().strftime('%Y-%m-%d')
                        logger.warning(f"Analysis date format is not correct.{analysis_date}")

            #🔥 Use a different version, directly await, to avoid cycle conflicts
            validation_result = await prepare_stock_data_async(
                stock_code=stock_code,
                market_type=market_type,
                period_days=30,
                analysis_date=analysis_date
            )

            if not validation_result.is_valid:
                error_msg = f"❌ 股票代码验证失败: {validation_result.error_message}"
                logger.error(error_msg)
                logger.error(f"Recommendations:{validation_result.suggestion}")

                #Build user-friendly error messages
                user_friendly_error = (
                    f"❌ 股票代码无效\n\n"
                    f"{validation_result.error_message}\n\n"
                    f"💡 {validation_result.suggestion}"
                )

                #Failed to update task status
                await self.memory_manager.update_task_status(
                    task_id=task_id,
                    status=AnalysisStatus.FAILED,
                    progress=0,
                    error_message=user_friendly_error
                )

                #Update MongoDB status
                await self._update_task_status(
                    task_id,
                    AnalysisStatus.FAILED,
                    0,
                    error_message=user_friendly_error
                )

                return

            logger.info(f"The stock code passes:{stock_code} - {validation_result.stock_name}")
            logger.info(f"Market type:{validation_result.market_type}")
            logger.info(f"Historical data:{'Yes.' if validation_result.has_historical_data else 'None'}")
            logger.info(f"Basic information:{'Yes.' if validation_result.has_basic_info else 'None'}")

            #Create Redis progress tracker in the online pool (avoid blocking event cycle)
            def create_progress_tracker():
                """Create progress tracker in an online process"""
                logger.info(f"Create progress tracker:{task_id}")
                tracker = RedisProgressTracker(
                    task_id=task_id,
                    analysts=request.parameters.selected_analysts or ["market", "fundamentals"],
                    research_depth=request.parameters.research_depth or "标准",
                    llm_provider="dashscope"
                )
                logger.info(f"The progress tracker has been created:{task_id}")
                return tracker

            progress_tracker = await asyncio.to_thread(create_progress_tracker)

            #Cache Progress Tracker
            self._progress_trackers[task_id] = progress_tracker

            #Register to Log Monitor
            register_analysis_tracker(task_id, progress_tracker)

            #Initialization progress (implemented online)
            await asyncio.to_thread(
                progress_tracker.update_progress,
                {
                    "progress_percentage": 10,
                    "last_message": "🚀 开始股票分析"
                }
            )

            #Update status as running
            await self.memory_manager.update_task_status(
                task_id=task_id,
                status=TaskStatus.RUNNING,
                progress=10,
                message="分析开始...",
                current_step="initialization"
            )

            #Synchronize MongoDB status update
            await self._update_task_status(task_id, AnalysisStatus.PROCESSING, 10)

            #Data preparation phase (executed online)
            await asyncio.to_thread(
                progress_tracker.update_progress,
                {
                    "progress_percentage": 20,
                    "last_message": "🔧 检查环境配置"
                }
            )
            await self.memory_manager.update_task_status(
                task_id=task_id,
                status=TaskStatus.RUNNING,
                progress=20,
                message="准备分析数据...",
                current_step="data_preparation"
            )

            #Synchronize MongoDB status update
            await self._update_task_status(task_id, AnalysisStatus.PROCESSING, 20)

            #Implementation of actual analysis
            result = await self._execute_analysis_sync(task_id, user_id, request, progress_tracker)

            #Mark progress tracker completed (executed online)
            await asyncio.to_thread(progress_tracker.mark_completed)

            #Save analysis to file and database
            try:
                logger.info(f"Start saving the analysis:{task_id}")
                await self._save_analysis_results_complete(task_id, result)
                logger.info(f"The results of the analysis are saved:{task_id}")
            except Exception as save_error:
                logger.error(f"Could not close temporary folder: %s{task_id} - {save_error}")
                #Save failure does not affect completion of analysis

            #Debugging: Checking to save to memory
            logger.info(f"[DEBUG] Result key to memory:{list(result.keys())}")
            logger.info(f"[DBUG] saved to memory:{bool(result.get('decision'))}")
            if result.get('decision'):
                logger.info(f"[DEBUG] About to save the content:{result['decision']}")

            #Update status complete
            await self.memory_manager.update_task_status(
                task_id=task_id,
                status=TaskStatus.COMPLETED,
                progress=100,
                message="分析完成",
                current_step="completed",
                result_data=result
            )

            #Synchronize update of MongoDB status complete
            await self._update_task_status(task_id, AnalysisStatus.COMPLETED, 100)

            #Creation Notification: Analyse Completed (Program B:REST+SSE)
            try:
                from app.services.notifications_service import get_notifications_service
                svc = get_notifications_service()
                summary = str(result.get("summary", ""))[:120]
                await svc.create_and_publish(
                    payload=NotificationCreate(
                        user_id=str(user_id),
                        type='analysis',
                        title=f"{request.stock_code} 分析完成",
                        content=summary,
                        link=f"/stocks/{request.stock_code}",
                        source='analysis'
                    )
                )
            except Exception as notif_err:
                logger.warning(f"Could not close temporary folder: %s{notif_err}")

            logger.info(f"Backstage analysis mission completed:{task_id}")

        except Exception as e:
            logger.error(f"Backstage analysis mission failed:{task_id} - {e}")

            #Format error messages as user-friendly tips
            from ..utils.error_formatter import ErrorFormatter

            #Gather context information
            error_context = {}
            if hasattr(request, 'parameters') and request.parameters:
                if hasattr(request.parameters, 'quick_model'):
                    error_context['model'] = request.parameters.quick_model
                if hasattr(request.parameters, 'deep_model'):
                    error_context['model'] = request.parameters.deep_model

            #Format error
            formatted_error = ErrorFormatter.format_error(str(e), error_context)

            #Build user-friendly error messages
            user_friendly_error = (
                f"{formatted_error['title']}\n\n"
                f"{formatted_error['message']}\n\n"
                f"💡 {formatted_error['suggestion']}"
            )

            #Tag Progress Tracker Failed
            if progress_tracker:
                progress_tracker.mark_failed(user_friendly_error)

            #Update status failed
            await self.memory_manager.update_task_status(
                task_id=task_id,
                status=TaskStatus.FAILED,
                progress=0,
                message="分析失败",
                current_step="failed",
                error_message=user_friendly_error
            )

            #Could not close temporary folder: %s
            await self._update_task_status(task_id, AnalysisStatus.FAILED, 0, user_friendly_error)
        finally:
            #Clear Progress Tracker Cache
            if task_id in self._progress_trackers:
                del self._progress_trackers[task_id]

            #Write-off from log monitoring
            unregister_analysis_tracker(task_id)

    async def _execute_analysis_sync(
        self,
        task_id: str,
        user_id: str,
        request: SingleAnalysisRequest,
        progress_tracker: Optional[RedisProgressTracker] = None
    ) -> Dict[str, Any]:
        """Synchronize execution analysis (run in shared thread pool)"""
        #🔧 Use shared thread pools to support multiple missions and execute them
        #Do not create a new thread pool every time, avoiding serial execution
        loop = asyncio.get_event_loop()
        logger.info(f"[Line pool]{task_id} - {request.stock_code}")
        result = await loop.run_in_executor(
            self._thread_pool,  #Use shared thread pool
            self._run_analysis_sync,
            task_id,
            user_id,
            request,
            progress_tracker
        )
        logger.info(f"[Line pool]{task_id}")
        return result

    def _run_analysis_sync(
        self,
        task_id: str,
        user_id: str,
        request: SingleAnalysisRequest,
        progress_tracker: Optional[RedisProgressTracker] = None
    ) -> Dict[str, Any]:
        """Synchronize the achievement of the analysis"""
        try:
            #Reinitiation of log system during online process
            from tradingagents.utils.logging_init import init_logging, get_logger
            init_logging()
            thread_logger = get_logger('analysis_thread')

            thread_logger.info(f"🔄 [Thread Pool] Starting analysis: {task_id} - {request.stock_code}")
            logger.info(f"[Line pool]{task_id} - {request.stock_code}")

            #🔧 Computes accurate progress based on the step weights of Redis ProcessTracker
            #Basic preparation stage (10%): 0.03 + 0.02 + 0.01 + 0.02 + 0.02 = 0.10
            #Step index 0-4 corresponds to 0-10%

            #Step update progress (call in online pool)
            def update_progress_sync(progress: int, message: str, step: str):
                """Synchronize progress in the online pool"""
                try:
                    #Update Redis progress tracker also
                    if progress_tracker:
                        progress_tracker.update_progress({
                            "progress_percentage": progress,
                            "last_message": message
                        })

                    #🔥 Update memory and MongoDB in sync to avoid cycle conflicts
                    #1. Update task status of memory (use new event cycle)
                    import asyncio
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        loop.run_until_complete(
                            self.memory_manager.update_task_status(
                                task_id=task_id,
                                status=TaskStatus.RUNNING,
                                progress=progress,
                                message=message,
                                current_step=step
                            )
                        )
                    finally:
                        loop.close()

                    #Update MongoDB (use synchronisation of client, avoiding incident cycle conflicts)
                    from pymongo import MongoClient
                    from app.core.config import SETTINGS
                    from datetime import datetime

                    sync_client = MongoClient(SETTINGS.MONGO_URI)
                    sync_db = sync_client[SETTINGS.MONGO_DB_NAME]

                    sync_db.analysis_tasks.update_one(
                        {"task_id": task_id},
                        {
                            "$set": {
                                "progress": progress,
                                "current_step": step,
                                "message": message,
                                "updated_at": datetime.utcnow()
                            }
                        }
                    )
                    sync_client.close()

                except Exception as e:
                    logger.warning(f"Progress update failed:{e}")

            #Configure Phase - Corresponding Step 3 "⚙️ Parameter Settings" (6-8%)
            update_progress_sync(7, "⚙️ 配置分析参数", "configuration")

            #Smart model selection logic
            from app.services.model_capability_service import get_model_capability_service
            capability_service = get_model_capability_service()

            research_depth = request.parameters.research_depth if request.parameters else "标准"

            #1. Check whether models have been specified at the front end
            if (request.parameters and
                hasattr(request.parameters, 'quick_analysis_model') and
                hasattr(request.parameters, 'deep_analysis_model') and
                request.parameters.quick_analysis_model and
                request.parameters.deep_analysis_model):

                #Use the model specified at the frontend
                quick_model = request.parameters.quick_analysis_model
                deep_model = request.parameters.deep_analysis_model

                logger.info(f"📝 [analytical service] User-designated model: Quick={quick_model}, deep={deep_model}")

                #Verify whether the model is appropriate
                validation = capability_service.validate_model_pair(
                    quick_model, deep_model, research_depth
                )

                if not validation["valid"]:
                    #Record warning
                    for warning in validation["warnings"]:
                        logger.warning(warning)

                    #If the model is inappropriate, switch to the recommended model.
                    logger.info(f"Automatically switch to recommended model...")
                    quick_model, deep_model = capability_service.recommend_models_for_depth(
                        research_depth
                    )
                    logger.info(f"Quick ={quick_model}, deep={deep_model}")
                else:
                    #Even if it's verified, the warning message is recorded.
                    for warning in validation["warnings"]:
                        logger.info(warning)
                    logger.info(f"✅ The user selected the model to verify: quick={quick_model}, deep={deep_model}")

            else:
                #2. Automatically recommend models
                quick_model, deep_model = capability_service.recommend_models_for_depth(
                    research_depth
                )
                logger.info(f"Auto-recommended model: Quick={quick_model}, deep={deep_model}")

            #🔧 Find the respective suppliers and API URLs according to fast and depth models
            quick_provider_info = get_provider_and_url_by_model_sync(quick_model)
            deep_provider_info = get_provider_and_url_by_model_sync(deep_model)

            quick_provider = quick_provider_info["provider"]
            deep_provider = deep_provider_info["provider"]
            quick_backend_url = quick_provider_info["backend_url"]
            deep_backend_url = deep_provider_info["backend_url"]

            logger.info(f"[Supplier Searching ]{quick_model}Corresponding suppliers:{quick_provider}")
            logger.info(f"[API Address]{quick_backend_url}")
            logger.info(f"Depth model{deep_model}Corresponding suppliers:{deep_provider}")
            logger.info(f"[API Address]{deep_backend_url}")

            #Check if two models come from the same plant.
            if quick_provider == deep_provider:
                logger.info(f"Two models from the same plant:{quick_provider}")
            else:
                logger.info(f"✅ [Mixed Mode] Quick Model{quick_provider}) and Depth Model ( ){deep_provider}From different manufacturers")

            #Market acquisition type
            market_type = request.parameters.market_type if request.parameters else "A股"
            logger.info(f"Use of market types:{market_type}")

            #Create analytical configuration (support hybrid mode)
            config = create_analysis_config(
                research_depth=research_depth,
                selected_analysts=request.parameters.selected_analysts if request.parameters else ["market", "fundamentals"],
                quick_model=quick_model,
                deep_model=deep_model,
                llm_provider=quick_provider,  #Vendors mainly using fast-track models
                market_type=market_type  #Market type with frontend
            )

            #Add Mixed Mode Configuration
            config["quick_provider"] = quick_provider
            config["deep_provider"] = deep_provider
            config["quick_backend_url"] = quick_backend_url
            config["deep_backend_url"] = deep_backend_url
            config["backend_url"] = quick_backend_url  #Maintain backward compatibility

            #Could not close temporary folder: %s
            logger.info(f"Rapid models in configuration:{config.get('quick_think_llm')}")
            logger.info(f"🔍 [model validation] deep model in configuration:{config.get('deep_think_llm')}")
            logger.info(f"The LLM supplier in the configuration:{config.get('llm_provider')}")

            #Initialisation Analysis Engine - Corresponding Step 4 "🚀 Start Engine" (8-10%)
            update_progress_sync(9, "🚀 初始化AI分析引擎", "engine_initialization")
            trading_graph = self._get_trading_graph(config)

            #Could not close temporary folder: %s
            logger.info(f"[engine validation]{trading_graph.config.get('quick_think_llm')}")
            logger.info(f"[engine validation]{trading_graph.config.get('deep_think_llm')}")

            #Prepare to analyze the data.
            start_time = datetime.now()

            #🔧 Use the analysis date passed from the frontend, if not the current date
            if request.parameters and hasattr(request.parameters, 'analysis_date') and request.parameters.analysis_date:
                #The frontend passes a datetime object or string
                if isinstance(request.parameters.analysis_date, datetime):
                    analysis_date = request.parameters.analysis_date.strftime("%Y-%m-%d")
                elif isinstance(request.parameters.analysis_date, str):
                    analysis_date = request.parameters.analysis_date
                else:
                    analysis_date = datetime.now().strftime("%Y-%m-%d")
                logger.info(f"📅 Use the analysis date specified at the frontend:{analysis_date}")
            else:
                analysis_date = datetime.now().strftime("%Y-%m-%d")
                logger.info(f"Use the current date as the date of analysis:{analysis_date}")

            #🔧 Smart date range processing: capture the latest 10 days of data, automate weekends/leaves Day
            #This ensures that even weekends or holidays will be able to get data on the last trading day.
            from tradingagents.utils.dataflow_utils import get_trading_date_range
            data_start_date, data_end_date = get_trading_date_range(analysis_date, lookback_days=10)

            logger.info(f"Analysis of target dates:{analysis_date}")
            logger.info(f"Data search range:{data_start_date}to{data_end_date}(During the last 10 days)")
            logger.info(f"Note: Access to data for 10 days automatically addresses weekends, holidays and data delays")

            #Start analysis - 10% progress, coming to analyst stage
            #Note: Do not manually set too much progress, let graph process callback update actual analysis progress
            update_progress_sync(10, "🤖 开始多智能体协作分析", "agent_analysis")

            #Starts a walker to simulate progress update
            import threading
            import time

            def simulate_progress():
                """Simulate internal progress of TradingAgendas"""
                try:
                    if not progress_tracker:
                        return

                    #Analyst phase - adjusted for the number of analysts selected
                    analysts = request.parameters.selected_analysts if request.parameters else ["market", "fundamentals"]

                    #Simulation analyst execution
                    for i, analyst in enumerate(analysts):
                        time.sleep(15)  #About 15 seconds per analyst.
                        if analyst == "market":
                            progress_tracker.update_progress("📊 市场分析师正在分析")
                        elif analyst == "fundamentals":
                            progress_tracker.update_progress("💼 基本面分析师正在分析")
                        elif analyst == "news":
                            progress_tracker.update_progress("📰 新闻分析师正在分析")
                        elif analyst == "social":
                            progress_tracker.update_progress("💬 社交媒体分析师正在分析")

                    #Research team phase
                    time.sleep(10)
                    progress_tracker.update_progress("🐂 看涨研究员构建论据")

                    time.sleep(8)
                    progress_tracker.update_progress("🐻 看跌研究员识别风险")

                    #Debate stage -- cycle of debate based on five levels
                    research_depth = request.parameters.research_depth if request.parameters else "标准"
                    if research_depth == "快速":
                        debate_rounds = 1
                    elif research_depth == "基础":
                        debate_rounds = 1
                    elif research_depth == "标准":
                        debate_rounds = 1
                    elif research_depth == "深度":
                        debate_rounds = 2
                    elif research_depth == "全面":
                        debate_rounds = 3
                    else:
                        debate_rounds = 1  #Default

                    for round_num in range(debate_rounds):
                        time.sleep(12)
                        progress_tracker.update_progress(f"🎯 研究辩论 第{round_num+1}轮")

                    time.sleep(8)
                    progress_tracker.update_progress("👔 研究经理形成共识")

                    #Traders phase
                    time.sleep(10)
                    progress_tracker.update_progress("💼 交易员制定策略")

                    #Risk management phase
                    time.sleep(8)
                    progress_tracker.update_progress("🔥 激进风险评估")

                    time.sleep(6)
                    progress_tracker.update_progress("🛡️ 保守风险评估")

                    time.sleep(6)
                    progress_tracker.update_progress("⚖️ 中性风险评估")

                    time.sleep(8)
                    progress_tracker.update_progress("🎯 风险经理制定策略")

                    #Final phase
                    time.sleep(5)
                    progress_tracker.update_progress("📡 信号处理")

                except Exception as e:
                    logger.warning(f"Progress simulation failed:{e}")

            #Start progress simulation thread
            progress_thread = threading.Thread(target=simulate_progress, daemon=True)
            progress_thread.start()

            #Defines a progress correction function to receive real-time progress from LangGraph
            #Node progress map (equivalent to the step weight of RedisProgressTracker)
            node_progress_map = {
                #Analyst stage (10%)
                "📊 市场分析师": 27.5,      #10% + 17.5% (Assuming 2 analysts)
                "💼 基本面分析师": 45,       # 10% + 35%
                "📰 新闻分析师": 27.5,       #If there were three analysts,
                "💬 社交媒体分析师": 27.5,   #If there were four analysts...
                #Research debate stage (45% 70%)
                "🐂 看涨研究员": 51.25,      # 45% + 6.25%
                "🐻 看跌研究员": 57.5,       # 45% + 12.5%
                "👔 研究经理": 70,           # 45% + 25%
                #Traders stage (70% → 78%)
                "💼 交易员决策": 78,         # 70% + 8%
                #Risk assessment phase (78% → 93%)
                "🔥 激进风险评估": 81.75,    # 78% + 3.75%
                "🛡️ 保守风险评估": 85.5,    # 78% + 7.5%
                "⚖️ 中性风险评估": 89.25,   # 78% + 11.25%
                "🎯 风险经理": 93,           # 78% + 15%
                #Final phase (93% → 100%)
                "📊 生成报告": 97,           # 93% + 4%
            }

            def graph_progress_callback(message: str):
                """Received LangGraph progress update

                Ensure that the step weights of RedisProgressTracker are consistent with the percentage of progress directly mapped by node name
                Note: Update only as progress increases and avoid covering the virtual step progress of RedisProgressTracker
                """
                try:
                    logger.info(f"[Graph progress is called] message={message}")
                    if not progress_tracker:
                        logger.warning(f"Noone can update progress")
                        return

                    #Find the percentage of progress corresponding to nodes
                    progress_pct = node_progress_map.get(message)

                    if progress_pct is not None:
                        #Get the current progress (using process data properties)
                        current_progress = progress_tracker.progress_data.get('progress_percentage', 0)

                        #Update only as progress increases, avoiding covering the progress of virtual steps
                        if int(progress_pct) > current_progress:
                            #Update Redis Progress Tracker
                            progress_tracker.update_progress({
                                'progress_percentage': int(progress_pct),
                                'last_message': message
                            })
                            logger.info(f"[Graph progress]{current_progress}% → {int(progress_pct)}% - {message}")

                            #Also update memory and MongoDB
                            try:
                                import asyncio
                                from datetime import datetime

                                #Try to fetch the currently running cycle of events
                                try:
                                    loop = asyncio.get_running_loop()
                                    #If in the event cycle, use Create task
                                    asyncio.create_task(
                                        self._update_progress_async(task_id, int(progress_pct), message)
                                    )
                                    logger.debug(f"[Graph progress]{int(progress_pct)}%")
                                except RuntimeError:
                                    #No running cycle, update MongoDB using sync
                                    from pymongo import MongoClient
                                    from app.core.config import SETTINGS

                                    #Create a simultaneous MongoDB client
                                    sync_client = MongoClient(SETTINGS.MONGO_URI)
                                    sync_db = sync_client[SETTINGS.MONGO_DB_NAME]

                                    #Synchronize MongoDB
                                    sync_db.analysis_tasks.update_one(
                                        {"task_id": task_id},
                                        {
                                            "$set": {
                                                "progress": int(progress_pct),
                                                "current_step": message,
                                                "message": message,
                                                "updated_at": datetime.utcnow()
                                            }
                                        }
                                    )
                                    sync_client.close()

                                    #Step up memory (create new event cycle)
                                    loop = asyncio.new_event_loop()
                                    asyncio.set_event_loop(loop)
                                    try:
                                        loop.run_until_complete(
                                            self.memory_manager.update_task_status(
                                                task_id=task_id,
                                                status=TaskStatus.RUNNING,
                                                progress=int(progress_pct),
                                                message=message,
                                                current_step=message
                                            )
                                        )
                                    finally:
                                        loop.close()

                                    logger.debug(f"[Graph progresses]{int(progress_pct)}%")
                            except Exception as sync_err:
                                logger.warning(f"Synchronising update failed:{sync_err}")
                        else:
                            #No progress, only updates
                            progress_tracker.update_progress({
                                'last_message': message
                            })
                            logger.info(f"[Graph progress]{current_progress}% >= {int(progress_pct)}Other Organiser, only updates:{message}")
                    else:
                        #Unknown Node, update only
                        logger.warning(f"[Graph progress] Unknown node:{message}, only update messages")
                        progress_tracker.update_progress({
                            'last_message': message
                        })

                except Exception as e:
                    logger.error(f"Graph progress has failed:{e}", exc_info=True)

            logger.info(f"Get ready to call...{graph_progress_callback}")

            #Implementation of physical analysis, transmission of progress back and task id
            state, decision = trading_graph.propagate(
                request.stock_code,
                analysis_date,
                progress_callback=graph_progress_callback,
                task_id=task_id
            )

            logger.info(f"Implementing graph.propagate")

            #Debugging: Checking the structure of the development
            logger.info(f"[DBUG] Decision type:{type(decision)}")
            logger.info(f"[DEBUG] Decision:{decision}")
            if isinstance(decision, dict):
                logger.info(f"[DEBUG] Decision:{list(decision.keys())}")
            elif hasattr(decision, '__dict__'):
                logger.info(f"[DEBUG] Commission Properties:{list(vars(decision).keys())}")

            #Process result
            if progress_tracker:
                progress_tracker.update_progress("📊 处理分析结果")
            update_progress_sync(90, "处理分析结果...", "result_processing")

            execution_time = (datetime.now() - start_time).total_seconds()

            #Extract reports fields from state
            reports = {}
            try:
                #Define all possible reporting fields
                report_fields = [
                    'market_report',
                    'sentiment_report',
                    'news_report',
                    'fundamentals_report',
                    'investment_plan',
                    'trader_investment_plan',
                    'final_trade_decision'
                ]

                #Extract report from state
                for field in report_fields:
                    if hasattr(state, field):
                        value = getattr(state, field, "")
                    elif isinstance(state, dict) and field in state:
                        value = state[field]
                    else:
                        value = ""

                    if isinstance(value, str) and len(value.strip()) > 10:  #Save only reports with actual content
                        reports[field] = value.strip()
                        logger.info(f"[REPORTS]{field}- Length:{len(value.strip())}")
                    else:
                        logger.debug(f"[REPORTS] Skip the report:{field}- It's empty or too short.")

                #Addressing the status of the research team debate report
                if hasattr(state, 'investment_debate_state') or (isinstance(state, dict) and 'investment_debate_state' in state):
                    debate_state = getattr(state, 'investment_debate_state', None) if hasattr(state, 'investment_debate_state') else state.get('investment_debate_state')
                    if debate_state:
                        #Extracting the history of multiple researchers
                        if hasattr(debate_state, 'bull_history'):
                            bull_content = getattr(debate_state, 'bull_history', "")
                        elif isinstance(debate_state, dict) and 'bull_history' in debate_state:
                            bull_content = debate_state['bull_history']
                        else:
                            bull_content = ""

                        if bull_content and len(bull_content.strip()) > 10:
                            reports['bull_researcher'] = bull_content.strip()
                            logger.info(f"[REPORTS] Extracting report: bull researcher - Length:{len(bull_content.strip())}")

                        #Extracting the history of empty researchers
                        if hasattr(debate_state, 'bear_history'):
                            bear_content = getattr(debate_state, 'bear_history', "")
                        elif isinstance(debate_state, dict) and 'bear_history' in debate_state:
                            bear_content = debate_state['bear_history']
                        else:
                            bear_content = ""

                        if bear_content and len(bear_content.strip()) > 10:
                            reports['bear_researcher'] = bear_content.strip()
                            logger.info(f"[REPORTS]{len(bear_content.strip())}")

                        #Decision-making by extracting research managers
                        if hasattr(debate_state, 'judge_decision'):
                            decision_content = getattr(debate_state, 'judge_decision', "")
                        elif isinstance(debate_state, dict) and 'judge_decision' in debate_state:
                            decision_content = debate_state['judge_decision']
                        else:
                            decision_content = str(debate_state)

                        if decision_content and len(decision_content.strip()) > 10:
                            reports['research_team_decision'] = decision_content.strip()
                            logger.info(f"[REPORTS]{len(decision_content.strip())}")

                #Process risk management team debate status report
                if hasattr(state, 'risk_debate_state') or (isinstance(state, dict) and 'risk_debate_state' in state):
                    risk_state = getattr(state, 'risk_debate_state', None) if hasattr(state, 'risk_debate_state') else state.get('risk_debate_state')
                    if risk_state:
                        #Extracting the history of radical analysts
                        if hasattr(risk_state, 'risky_history'):
                            risky_content = getattr(risk_state, 'risky_history', "")
                        elif isinstance(risk_state, dict) and 'risky_history' in risk_state:
                            risky_content = risk_state['risky_history']
                        else:
                            risky_content = ""

                        if risky_content and len(risky_content.strip()) > 10:
                            reports['risky_analyst'] = risky_content.strip()
                            logger.info(f"[REPORTS] Extracting report: risky analyst - Length:{len(risky_content.strip())}")

                        #Extract conservative analyst history
                        if hasattr(risk_state, 'safe_history'):
                            safe_content = getattr(risk_state, 'safe_history', "")
                        elif isinstance(risk_state, dict) and 'safe_history' in risk_state:
                            safe_content = risk_state['safe_history']
                        else:
                            safe_content = ""

                        if safe_content and len(safe_content.strip()) > 10:
                            reports['safe_analyst'] = safe_content.strip()
                            logger.info(f"[REPORTS]{len(safe_content.strip())}")

                        #Extract neutral analyst history
                        if hasattr(risk_state, 'neutral_history'):
                            neutral_content = getattr(risk_state, 'neutral_history', "")
                        elif isinstance(risk_state, dict) and 'neutral_history' in risk_state:
                            neutral_content = risk_state['neutral_history']
                        else:
                            neutral_content = ""

                        if neutral_content and len(neutral_content.strip()) > 10:
                            reports['neutral_analyst'] = neutral_content.strip()
                            logger.info(f"[REPORTS]{len(neutral_content.strip())}")

                        #Decision-making by Portfolio Manager
                        if hasattr(risk_state, 'judge_decision'):
                            risk_decision = getattr(risk_state, 'judge_decision', "")
                        elif isinstance(risk_state, dict) and 'judge_decision' in risk_state:
                            risk_decision = risk_state['judge_decision']
                        else:
                            risk_decision = str(risk_state)

                        if risk_decision and len(risk_decision.strip()) > 10:
                            reports['risk_management_decision'] = risk_decision.strip()
                            logger.info(f"[REPORTS] Extracting report: risk manage description - Length:{len(risk_decision.strip())}")

                logger.info(f"[REPORTS]{len(reports)}Reports:{list(reports.keys())}")

            except Exception as e:
                logger.warning(f"There was an error extracting reports:{e}")
                #Degraded to extract from detailed analysis
                try:
                    if isinstance(decision, dict):
                        for key, value in decision.items():
                            if isinstance(value, str) and len(value) > 50:
                                reports[key] = value
                        logger.info(f"📊 Downscaling: extracting from development{len(reports)}Report")
                except Exception as fallback_error:
                    logger.warning(f"The downgrading also failed:{fallback_error}")

            #🔥Formatization of data (reference web directory realization)
            formatted_decision = {}
            try:
                if isinstance(decision, dict):
                    #Processing target prices
                    target_price = decision.get('target_price')
                    if target_price is not None and target_price != 'N/A':
                        try:
                            if isinstance(target_price, str):
                                #Remove currency symbols and spaces
                                clean_price = target_price.replace('$', '').replace('¥', '').replace('￥', '').strip()
                                target_price = float(clean_price) if clean_price and clean_price != 'None' else None
                            elif isinstance(target_price, (int, float)):
                                target_price = float(target_price)
                            else:
                                target_price = None
                        except (ValueError, TypeError):
                            target_price = None
                    else:
                        target_price = None

                    #For investment proposal in English read Chinese
                    action_translation = {
                        'BUY': '买入',
                        'SELL': '卖出',
                        'HOLD': '持有',
                        'buy': '买入',
                        'sell': '卖出',
                        'hold': '持有'
                    }
                    action = decision.get('action', '持有')
                    chinese_action = action_translation.get(action, action)

                    formatted_decision = {
                        'action': chinese_action,
                        'confidence': decision.get('confidence', 0.5),
                        'risk_score': decision.get('risk_score', 0.3),
                        'target_price': target_price,
                        'reasoning': decision.get('reasoning', '暂无分析推理')
                    }

                    logger.info(f"[DBUG] formatted decision:{formatted_decision}")
                else:
                    #Deal with other types
                    formatted_decision = {
                        'action': '持有',
                        'confidence': 0.5,
                        'risk_score': 0.3,
                        'target_price': None,
                        'reasoning': '暂无分析推理'
                    }
                    logger.warning(f"⚠️ Decision is not a dictionary type:{type(decision)}")
            except Exception as e:
                logger.error(f"Formatting failure:{e}")
                formatted_decision = {
                    'action': '持有',
                    'confidence': 0.5,
                    'risk_score': 0.3,
                    'target_price': None,
                    'reasoning': '暂无分析推理'
                }

            #🔥 Generates summary and recommendation in a web catalogue
            summary = ""
            recommendation = ""

            #1. Prioritize the extraction of summy from the final trade deciation in the reports (consistent with the web directory)
            if isinstance(reports, dict) and 'final_trade_decision' in reports:
                final_decision_content = reports['final_trade_decision']
                if isinstance(final_decision_content, str) and len(final_decision_content) > 50:
                    #Extract the first 200 characters as summary (fully consistent with the web directory)
                    summary = final_decision_content[:200].replace('#', '').replace('*', '').strip()
                    if len(final_decision_content) > 200:
                        summary += "..."
                    logger.info(f"[SUMMARY] extracts a summary from final trade description:{len(summary)}Character")

            #If no financial trade description, extract from state
            if not summary and isinstance(state, dict):
                final_decision = state.get('final_trade_decision', '')
                if isinstance(final_decision, str) and len(final_decision) > 50:
                    summary = final_decision[:200].replace('#', '').replace('*', '').strip()
                    if len(final_decision) > 200:
                        summary += "..."
                    logger.info(f"[SUMMARY] Extract from state.final trade description:{len(summary)}Character")

            #3. Generating recommendation
            if isinstance(formatted_decision, dict):
                action = formatted_decision.get('action', '持有')
                target_price = formatted_decision.get('target_price')
                reasoning = formatted_decision.get('reasoning', '')

                #Generate investment recommendations
                recommendation = f"投资建议：{action}。"
                if target_price:
                    recommendation += f"目标价格：{target_price}元。"
                if reasoning:
                    recommendation += f"决策依据：{reasoning}"
                logger.info(f"[RECOMENDATION]{len(recommendation)}Character")

            #If not, extract from other reports
            if not summary and isinstance(reports, dict):
                #Try extracting summaries from other reports
                for report_name, content in reports.items():
                    if isinstance(content, str) and len(content) > 100:
                        summary = content[:200].replace('#', '').replace('*', '').strip()
                        if len(content) > 200:
                            summary += "..."
                        logger.info(f"[SUMMARY]{report_name}Extract summary:{len(summary)}Character")
                        break

            #5. Final standby options
            if not summary:
                summary = f"对{request.stock_code}的分析已完成，请查看详细报告。"
                logger.warning(f"[SUMMARY]")

            if not recommendation:
                recommendation = f"请参考详细分析报告做出投资决策。"
                logger.warning(f"[RECOMENDATION]")

            #Extract model information from decision-making
            model_info = decision.get('model_info', 'Unknown') if isinstance(decision, dict) else 'Unknown'

            #Build Results
            result = {
                "analysis_id": str(uuid.uuid4()),
                "stock_code": request.stock_code,
                "stock_symbol": request.stock_code,  #Add a stock symbol field to maintain compatibility
                "analysis_date": analysis_date,
                "summary": summary,
                "recommendation": recommendation,
                "confidence_score": formatted_decision.get("confidence", 0.0) if isinstance(formatted_decision, dict) else 0.0,
                "risk_level": "中等",  #Based on risk score
                "key_points": [],  #The key points can be extracted from reasoning.
                "detailed_analysis": decision,
                "execution_time": execution_time,
                "tokens_used": decision.get("tokens_used", 0) if isinstance(decision, dict) else 0,
                "state": state,
                #Add Analyst Information
                "analysts": request.parameters.selected_analysts if request.parameters else [],
                "research_depth": request.parameters.research_depth if request.parameters else "快速",
                #Add extracted report
                "reports": reports,
                #🔥Key fixation: add formatted decision field!
                "decision": formatted_decision,
                #Add Model Information Fields
                "model_info": model_info,
                #Performance indicator data
                "performance_metrics": state.get("performance_metrics", {}) if isinstance(state, dict) else {}
            }

            logger.info(f"The analysis is complete:{task_id}- Time-consuming.{execution_time:.2f}sec")

            #Debugging: check return structure
            logger.info(f"[DEBUG] Returning key:{list(result.keys())}")
            logger.info(f"[DEBUG] returns the data found in the report:{bool(result.get('decision'))}")
            if result.get('decision'):
                decision = result['decision']
                logger.info(f"[DEBUG] returns the content:{decision}")

            return result

        except Exception as e:
            logger.error(f"[Line pool]{task_id} - {e}")

            #Format error messages as user-friendly tips
            from ..utils.error_formatter import ErrorFormatter

            #Gather context information
            error_context = {}
            if request and hasattr(request, 'parameters') and request.parameters:
                if hasattr(request.parameters, 'quick_model'):
                    error_context['model'] = request.parameters.quick_model
                if hasattr(request.parameters, 'deep_model'):
                    error_context['model'] = request.parameters.deep_model

            #Format error
            formatted_error = ErrorFormatter.format_error(str(e), error_context)

            #Build user-friendly error messages
            user_friendly_error = (
                f"{formatted_error['title']}\n\n"
                f"{formatted_error['message']}\n\n"
                f"💡 {formatted_error['suggestion']}"
            )

            #Throw an anomaly containing friendly error information
            raise Exception(user_friendly_error) from e

    async def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get Task Status"""
        logger.info(f"Other Organiser{task_id}")
        logger.info(f"Current service example ID:{id(self)}")
        logger.info(f"Memory Manager Example ID:{id(self.memory_manager)}")

        #Force global memory manager instance (temporary solution)
        global_memory_manager = get_memory_state_manager()
        logger.info(f"Global memory manager example ID:{id(global_memory_manager)}")

        #Access to statistical information
        stats = await global_memory_manager.get_statistics()
        logger.info(f"Mission statistics in memory:{stats}")

        result = await global_memory_manager.get_task_dict(task_id)
        if result:
            logger.info(f"Found a mission:{task_id}- Status:{result.get('status')}")

            #Debugging: Check access to memory data
            result_data = result.get('result_data')
            logger.debug(f"[GET STATUS] result data exists:{bool(result_data)}")
            if result_data:
                logger.debug(f"[GET STATUS] result data:{list(result_data.keys())}")
                logger.debug(f"[GET STATUS]{bool(result_data.get('decision'))}")
                if result_data.get('decision'):
                    logger.debug(f"[GET STATUS] content:{result_data['decision']}")
            else:
                logger.debug(f"[GET STATUS] result data is empty or non-existent (mission running, normal)")

            #Prioritize detailed progress information from Redis
            redis_progress = get_progress_by_id(task_id)
            if redis_progress:
                logger.info(f"[Redis Progressing]{task_id}")

                #Extract the name and description of the current step from the steps array
                current_step_index = redis_progress.get('current_step', 0)
                steps = redis_progress.get('steps', [])
                current_step_name = redis_progress.get('current_step_name', '')
                current_step_description = redis_progress.get('current_step_description', '')

                #Extract from steps array if name/ description in Redis is empty
                if not current_step_name and steps and 0 <= current_step_index < len(steps):
                    current_step_info = steps[current_step_index]
                    current_step_name = current_step_info.get('name', '')
                    current_step_description = current_step_info.get('description', '')
                    logger.info(f"📋extracts current step information from the steps array:{current_step_index}, name={current_step_name}")

                #Merge Redis progress data
                result.update({
                    'progress': redis_progress.get('progress_percentage', result.get('progress', 0)),
                    'current_step': current_step_index,  #Use index instead of name
                    'current_step_name': current_step_name,  #Step Name
                    'current_step_description': current_step_description,  #Step Description
                    'message': redis_progress.get('last_message', result.get('message', '')),
                    'elapsed_time': redis_progress.get('elapsed_time', 0),
                    'remaining_time': redis_progress.get('remaining_time', 0),
                    'estimated_total_time': redis_progress.get('estimated_total_time', result.get('estimated_duration', 300)),  #🔧 fixation: using the estimated total duration in Redis
                    'steps': steps,
                    'start_time': result.get('start_time'),  #Keep format
                    'last_update': redis_progress.get('last_update', result.get('start_time'))
                })
            else:
                #If not available in Redis, try to get it from the progress tracker in memory
                if task_id in self._progress_trackers:
                    progress_tracker = self._progress_trackers[task_id]
                    progress_data = progress_tracker.to_dict()

                    #Merge progress tracker details
                    result.update({
                        'progress': progress_data['progress'],
                        'current_step': progress_data['current_step'],
                        'message': progress_data['message'],
                        'elapsed_time': progress_data['elapsed_time'],
                        'remaining_time': progress_data['remaining_time'],
                        'estimated_total_time': progress_data.get('estimated_total_time', 0),
                        'steps': progress_data['steps'],
                        'start_time': progress_data['start_time'],
                        'last_update': progress_data['last_update']
                    })
                    logger.info(f"Combining memory progress tracker data:{task_id}")
                else:
                    logger.info(f"No progress information found:{task_id}")
        else:
            logger.warning(f"No missions found:{task_id}")

        return result

    async def list_all_tasks(
        self,
        status: Optional[str] = None,
        limit: int = 20,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Other Organiser
        - Merge memory and MongoDB data
        - In reverse at the beginning.
        """
        try:
            task_status = None
            if status:
                try:
                    status_mapping = {
                        "processing": "running",
                        "pending": "pending",
                        "completed": "completed",
                        "failed": "failed",
                        "cancelled": "cancelled"
                    }
                    mapped_status = status_mapping.get(status, status)
                    task_status = TaskStatus(mapped_status)
                except ValueError:
                    logger.warning(f"Invalid status value:{status}")
                    task_status = None

            #1) Read all jobs from memory
            logger.info(f"[Tasks]{status}, limit={limit}, offset={offset}")
            tasks_in_mem = await self.memory_manager.list_all_tasks(
                status=task_status,
                limit=limit * 2,
                offset=0
            )
            logger.info(f"[Tasks] Memory returns:{len(tasks_in_mem)}")

            #2) Read tasks from MongoDB
            db = get_mongo_db()
            collection = db["analysis_tasks"]

            query = {}
            if task_status:
                query["status"] = task_status.value

            count = await collection.count_documents(query)
            logger.info(f"[Tasks]{count}")

            cursor = collection.find(query).sort("start_time", -1).limit(limit * 2)
            tasks_from_db = []
            async for doc in cursor:
                doc.pop("_id", None)
                tasks_from_db.append(doc)

            logger.info(f"[Tasks]{len(tasks_from_db)}")

            #3) Consolidation of tasks (RAM priority)
            task_dict = {}

            #Add a task first in MongoDB
            for task in tasks_from_db:
                task_id = task.get("task_id")
                if task_id:
                    task_dict[task_id] = task

            #Adds tasks in memory (override the same name in MongoDB)
            for task in tasks_in_mem:
                task_id = task.get("task_id")
                if task_id:
                    task_dict[task_id] = task

            #Convert to list and sort by time
            merged_tasks = list(task_dict.values())
            merged_tasks.sort(key=lambda x: x.get('start_time', ''), reverse=True)

            #Page Break
            results = merged_tasks[offset:offset + limit]

            #Complete stock names for results
            results = self._enrich_stock_names(results)
            logger.info(f"[Tasks]{len(results)}(RAM:{len(tasks_in_mem)}, MongoDB: {count})")
            return results
        except Exception as outer_e:
            logger.error(f"I'm sorry.{outer_e}", exc_info=True)
            return []

    async def list_user_tasks(
        self,
        user_id: str,
        status: Optional[str] = None,
        limit: int = 20,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Other Organiser
        - For processing status: priority from memory (real time progress)
        - For completed/failed/all status: merge memory and MongoDB data
        """
        try:
            task_status = None
            if status:
                try:
                    #The front-end is "processing," but Taskstatus uses "running."
                    #Map conversion required
                    status_mapping = {
                        "processing": "running",  #front-end processing, memory running
                        "pending": "pending",
                        "completed": "completed",
                        "failed": "failed",
                        "cancelled": "cancelled"
                    }
                    mapped_status = status_mapping.get(status, status)
                    task_status = TaskStatus(mapped_status)
                except ValueError:
                    logger.warning(f"Invalid status value:{status}")
                    task_status = None

            #1) Read jobs from memory
            logger.info(f"[Tasks]{user_id}, status={status} (mapped to {task_status}), limit={limit}, offset={offset}")
            tasks_in_mem = await self.memory_manager.list_user_tasks(
                user_id=user_id,
                status=task_status,
                limit=limit * 2,  #Read a little more. We'll combine the rest.
                offset=0  #Not many tasks in memory, read all
            )
            logger.info(f"[Tasks] Memory returns:{len(tasks_in_mem)}")

            #2) 🔧 For procising/running status, consolidation of MongoDB data is required to obtain an update on progress
            #Because graph process callback may have just updated MongoDB, and memory data may be old.

            #3) Read historical tasks from MongoDB (for consolidation or bottoming)
            logger.info(f"[Tasks]")
            mongo_tasks: List[Dict[str, Any]] = []
            count = 0
            try:
                db = get_mongo_db()

                #user id may be string or objectId, compatible
                uid_candidates: List[Any] = [user_id]

                #Special Process admin user
                if str(user_id) == 'admin':
                    #admin user: add fixed ObjectId and string forms
                    try:
                        from bson import ObjectId
                        admin_oid_str = '507f1f77bcf86cd799439011'
                        uid_candidates.append(ObjectId(admin_oid_str))
                        uid_candidates.append(admin_oid_str)  #Compatible String Storage
                        logger.info(f"📋 Admin Query, candidate ID: ['admin', ObjectId('{admin_oid_str}'), '{admin_oid_str}']")
                    except Exception as e:
                        logger.warning(f"⚠️ [Tasks] admin userObjectId failed to create:{e}")
                else:
                    #Normal user: trying to convert to
                    try:
                        from bson import ObjectId
                        uid_candidates.append(ObjectId(user_id))
                        logger.debug(f"📋 [Tasks]{user_id}")
                    except Exception as conv_err:
                        logger.warning(f"⚠️ [Tasks]{conv_err}")

                #Compatible user id and user fields First Name
                base_condition = {"$in": uid_candidates}
                or_conditions: List[Dict[str, Any]] = [
                    {"user_id": base_condition},
                    {"user": base_condition}
                ]
                query = {"$or": or_conditions}

                if task_status:
                    #Use a mapd state value (value of TaskStatus count)
                    query["status"] = task_status.value
                    logger.info(f"📋 [Tasks] Add status filter:{task_status.value}")

                logger.info(f"📋 [Tasks]{query}")
                #Read more data to merge
                cursor = db.analysis_tasks.find(query).sort("created_at", -1).limit(limit * 2)
                async for doc in cursor:
                    count += 1
                    #Compatible user id or user fields
                    user_field_val = doc.get("user_id", doc.get("user"))
                    #Multi-stock code field name: symbol, stock code, stock symbol
                    stock_code_value = doc.get("symbol") or doc.get("stock_code") or doc.get("stock_symbol")
                    item = {
                        "task_id": doc.get("task_id"),
                        "user_id": str(user_field_val) if user_field_val is not None else None,
                        "symbol": stock_code_value,  #Add symbol field (prior to front end)
                        "stock_code": stock_code_value,  #Compatible Fields
                        "stock_symbol": stock_code_value,  #Compatible Fields
                        "stock_name": doc.get("stock_name"),
                        "status": str(doc.get("status", "pending")),
                        "progress": int(doc.get("progress", 0) or 0),
                        "message": doc.get("message", ""),
                        "current_step": doc.get("current_step", ""),
                        "start_time": doc.get("started_at") or doc.get("created_at"),
                        "end_time": doc.get("completed_at"),
                        "parameters": doc.get("parameters", {}),
                        "execution_time": doc.get("execution_time"),
                        "tokens_used": doc.get("tokens_used"),
                        #Use memory manager fields here for compatibility frontends First Name
                        "result_data": doc.get("result"),
                    }
                    #Time format to ISO string (add time zone information)
                    for k in ("start_time", "end_time"):
                        if item.get(k) and hasattr(item[k], "isoformat"):
                            dt = item[k]
                            #If no time zone information is available, assumed UTC+8
                            if dt.tzinfo is None:
                                from datetime import timezone, timedelta
                                china_tz = timezone(timedelta(hours=8))
                                dt = dt.replace(tzinfo=china_tz)
                            item[k] = dt.isoformat()
                    mongo_tasks.append(item)

                logger.info(f"📋 [Tasks]{count}")
            except Exception as mongo_e:
                logger.error(f"❌ Could not close temporary folder: %s{mongo_e}", exc_info=True)
                #MongoDB query failed to continue using memory data

            #4) Merge memory and MongoDB data, heavy
            #Priority is given to progress data in MongoDB for procsing/runing status
            #Because graph process callback updates MongoDB directly, and memory data may be old
            task_dict = {}

            #Add memory tasks first
            for task in tasks_in_mem:
                task_id = task.get("task_id")
                if task_id:
                    task_dict[task_id] = task

            #Add a task in MongoDB
            #Use MongoDB progress data (update) for projecting/running status
            #For other states, skip if memory already exists (RAM priority)
            for task in mongo_tasks:
                task_id = task.get("task_id")
                if not task_id:
                    continue

                #If this task already exists in memory
                if task_id in task_dict:
                    mem_task = task_dict[task_id]
                    mongo_task = task

                    #Use MongoDB progress data if it is a process/running state
                    if mongo_task.get("status") in ["processing", "running"]:
                        #Keep basic information in memory but update progress related fields
                        mem_task["progress"] = mongo_task.get("progress", mem_task.get("progress", 0))
                        mem_task["message"] = mongo_task.get("message", mem_task.get("message", ""))
                        mem_task["current_step"] = mongo_task.get("current_step", mem_task.get("current_step", ""))
                        logger.debug(f"🔄 [Tasks]{task_id}, progress={mem_task['progress']}%")
                else:
                    #Cannot initialise Evolution's mail component.
                    task_dict[task_id] = task

            #Convert to list and sort by time
            merged_tasks = list(task_dict.values())
            merged_tasks.sort(key=lambda x: x.get('start_time', ''), reverse=True)

            #Page Break
            results = merged_tasks[offset:offset + limit]

            #🔥 Harmonized processing of time-zone information (ensure that all time-zone fields are marked)
            from datetime import timezone, timedelta
            china_tz = timezone(timedelta(hours=8))

            for task in results:
                for time_field in ("start_time", "end_time", "created_at", "started_at", "completed_at"):
                    value = task.get(time_field)
                    if value:
                        #If Datatime Object
                        if hasattr(value, "isoformat"):
                            #Add Time Zone Information if Naive Datetime
                            if value.tzinfo is None:
                                value = value.replace(tzinfo=china_tz)
                            task[time_field] = value.isoformat()
                        #Add time zone identification if string without time zone identification
                        elif isinstance(value, str) and value and not value.endswith(('Z', '+08:00', '+00:00')):
                            #Checks for time strings in ISO format
                            if 'T' in value or ' ' in value:
                                task[time_field] = value.replace(' ', 'T') + '+08:00'

            #Complete stock names for results
            results = self._enrich_stock_names(results)
            logger.info(f"📋 [Tasks]{len(results)}(RAM:{len(tasks_in_mem)}, MongoDB: {count})")
            return results
        except Exception as outer_e:
            logger.error(f"❌ I'm sorry.{outer_e}", exc_info=True)
            return []

    async def cleanup_zombie_tasks(self, max_running_hours: int = 2) -> Dict[str, Any]:
        """Clean-up of zombie missions (long-term process/running)

        Args:
            max running hours: Maximum run time (hours), tasks longer than that will be marked as failure

        Returns:
            Cleanup result statistics
        """
        try:
            #1) Clean-up of memory zombie missions
            memory_cleaned = await self.memory_manager.cleanup_zombie_tasks(max_running_hours)

            #2) Clean-up of zombie missions in MongoDB
            db = get_mongo_db()
            from datetime import timedelta
            cutoff_time = datetime.utcnow() - timedelta(hours=max_running_hours)

            #Find tasks in procising status for long periods
            zombie_filter = {
                "status": {"$in": ["processing", "running", "pending"]},
                "$or": [
                    {"started_at": {"$lt": cutoff_time}},
                    {"created_at": {"$lt": cutoff_time, "started_at": None}}
                ]
            }

            #Update as Failed
            update_result = await db.analysis_tasks.update_many(
                zombie_filter,
                {
                    "$set": {
                        "status": "failed",
                        "last_error": f"任务超时（运行时间超过 {max_running_hours} 小时）",
                        "completed_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow()
                    }
                }
            )

            mongo_cleaned = update_result.modified_count

            logger.info(f"🧹 We've cleared the zombie mission.{memory_cleaned}, MongoDB={mongo_cleaned}")

            return {
                "success": True,
                "memory_cleaned": memory_cleaned,
                "mongo_cleaned": mongo_cleaned,
                "total_cleaned": memory_cleaned + mongo_cleaned,
                "max_running_hours": max_running_hours
            }

        except Exception as e:
            logger.error(f"❌ The mission failed:{e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "memory_cleaned": 0,
                "mongo_cleaned": 0,
                "total_cleaned": 0
            }

    async def get_zombie_tasks(self, max_running_hours: int = 2) -> List[Dict[str, Any]]:
        """Fetch Zombie Job List (no cleanup, only query)

        Args:
            max runing hours: maximum running time (hours)

        Returns:
            Zombie Job List
        """
        try:
            db = get_mongo_db()
            from datetime import timedelta
            cutoff_time = datetime.utcnow() - timedelta(hours=max_running_hours)

            #Find tasks in procising status for long periods
            zombie_filter = {
                "status": {"$in": ["processing", "running", "pending"]},
                "$or": [
                    {"started_at": {"$lt": cutoff_time}},
                    {"created_at": {"$lt": cutoff_time, "started_at": None}}
                ]
            }

            cursor = db.analysis_tasks.find(zombie_filter).sort("created_at", -1)
            zombie_tasks = []

            async for doc in cursor:
                task = {
                    "task_id": doc.get("task_id"),
                    "user_id": str(doc.get("user_id", doc.get("user"))),
                    "stock_code": doc.get("stock_code"),
                    "stock_name": doc.get("stock_name"),
                    "status": doc.get("status"),
                    "created_at": doc.get("created_at").isoformat() if doc.get("created_at") else None,
                    "started_at": doc.get("started_at").isoformat() if doc.get("started_at") else None,
                    "running_hours": None
                }

                #Calculate running time
                start_time = doc.get("started_at") or doc.get("created_at")
                if start_time:
                    running_seconds = (datetime.utcnow() - start_time).total_seconds()
                    task["running_hours"] = round(running_seconds / 3600, 2)

                zombie_tasks.append(task)

            logger.info(f"📋 [Tasks] Other Organiser{len(zombie_tasks)}A zombie mission.")
            return zombie_tasks

        except Exception as e:
            logger.error(f"❌ The search mission failed:{e}", exc_info=True)
            return []



    async def _update_task_status(
        self,
        task_id: str,
        status: AnalysisStatus,
        progress: int,
        error_message: str = None
    ):
        """Update Task Status"""
        try:
            db = get_mongo_db()
            update_data = {
                "status": status,
                "progress": progress,
                "updated_at": datetime.utcnow()
            }

            if status == AnalysisStatus.PROCESSING and progress == 10:
                update_data["started_at"] = datetime.utcnow()
            elif status == AnalysisStatus.COMPLETED:
                update_data["completed_at"] = datetime.utcnow()
            elif status == AnalysisStatus.FAILED:
                update_data["last_error"] = error_message
                update_data["completed_at"] = datetime.utcnow()

            await db.analysis_tasks.update_one(
                {"task_id": task_id},
                {"$set": update_data}
            )

            logger.debug(f"📊 Mission status updated:{task_id} -> {status} ({progress}%)")

        except Exception as e:
            logger.error(f"❌ Could not close temporary folder: %s{task_id} - {e}")

    async def _save_analysis_result(self, task_id: str, result: Dict[str, Any]):
        """Save analytical results (original method)"""
        try:
            db = get_mongo_db()
            await db.analysis_tasks.update_one(
                {"task_id": task_id},
                {"$set": {"result": result}}
            )
            logger.debug(f"💾 The results of the analysis have been saved:{task_id}")
        except Exception as e:
            logger.error(f"❌ Could not close temporary folder: %s{task_id} - {e}")

    async def _save_analysis_result_web_style(self, task_id: str, result: Dict[str, Any]):
        """Save the results of the analysis - save them in the form of a web directory to anallysis reports"""
        try:
            db = get_mongo_db()

            #Generate analytical ID (consistent with web directory)
            from datetime import datetime
            timestamp = datetime.utcnow()  #Storage UTC time (standard practice)
            stock_symbol = result.get('stock_symbol') or result.get('stock_code', 'UNKNOWN')
            analysis_id = f"{stock_symbol}_{timestamp.strftime('%Y%m%d_%H%M%S')}"

            #Process reports fields - extract all analyses from state
            reports = {}
            if 'state' in result:
                try:
                    state = result['state']

                    #Define all possible reporting fields
                    report_fields = [
                        'market_report',
                        'sentiment_report',
                        'news_report',
                        'fundamentals_report',
                        'investment_plan',
                        'trader_investment_plan',
                        'final_trade_decision'
                    ]

                    #Extract report from state
                    for field in report_fields:
                        if hasattr(state, field):
                            value = getattr(state, field, "")
                        elif isinstance(state, dict) and field in state:
                            value = state[field]
                        else:
                            value = ""

                        if isinstance(value, str) and len(value.strip()) > 10:  #Save only reports with actual content
                            reports[field] = value.strip()

                    #Addressing the status of the research team debate report
                    if hasattr(state, 'investment_debate_state') or (isinstance(state, dict) and 'investment_debate_state' in state):
                        debate_state = getattr(state, 'investment_debate_state', None) if hasattr(state, 'investment_debate_state') else state.get('investment_debate_state')
                        if debate_state:
                            #Extracting the history of multiple researchers
                            if hasattr(debate_state, 'bull_history'):
                                bull_content = getattr(debate_state, 'bull_history', "")
                            elif isinstance(debate_state, dict) and 'bull_history' in debate_state:
                                bull_content = debate_state['bull_history']
                            else:
                                bull_content = ""

                            if bull_content and len(bull_content.strip()) > 10:
                                reports['bull_researcher'] = bull_content.strip()

                            #Extracting the history of empty researchers
                            if hasattr(debate_state, 'bear_history'):
                                bear_content = getattr(debate_state, 'bear_history', "")
                            elif isinstance(debate_state, dict) and 'bear_history' in debate_state:
                                bear_content = debate_state['bear_history']
                            else:
                                bear_content = ""

                            if bear_content and len(bear_content.strip()) > 10:
                                reports['bear_researcher'] = bear_content.strip()

                            #Decision-making by extracting research managers
                            if hasattr(debate_state, 'judge_decision'):
                                decision_content = getattr(debate_state, 'judge_decision', "")
                            elif isinstance(debate_state, dict) and 'judge_decision' in debate_state:
                                decision_content = debate_state['judge_decision']
                            else:
                                decision_content = str(debate_state)

                            if decision_content and len(decision_content.strip()) > 10:
                                reports['research_team_decision'] = decision_content.strip()

                    #Process risk management team debate status report
                    if hasattr(state, 'risk_debate_state') or (isinstance(state, dict) and 'risk_debate_state' in state):
                        risk_state = getattr(state, 'risk_debate_state', None) if hasattr(state, 'risk_debate_state') else state.get('risk_debate_state')
                        if risk_state:
                            #Extracting the history of radical analysts
                            if hasattr(risk_state, 'risky_history'):
                                risky_content = getattr(risk_state, 'risky_history', "")
                            elif isinstance(risk_state, dict) and 'risky_history' in risk_state:
                                risky_content = risk_state['risky_history']
                            else:
                                risky_content = ""

                            if risky_content and len(risky_content.strip()) > 10:
                                reports['risky_analyst'] = risky_content.strip()

                            #Extract conservative analyst history
                            if hasattr(risk_state, 'safe_history'):
                                safe_content = getattr(risk_state, 'safe_history', "")
                            elif isinstance(risk_state, dict) and 'safe_history' in risk_state:
                                safe_content = risk_state['safe_history']
                            else:
                                safe_content = ""

                            if safe_content and len(safe_content.strip()) > 10:
                                reports['safe_analyst'] = safe_content.strip()

                            #Extract neutral analyst history
                            if hasattr(risk_state, 'neutral_history'):
                                neutral_content = getattr(risk_state, 'neutral_history', "")
                            elif isinstance(risk_state, dict) and 'neutral_history' in risk_state:
                                neutral_content = risk_state['neutral_history']
                            else:
                                neutral_content = ""

                            if neutral_content and len(neutral_content.strip()) > 10:
                                reports['neutral_analyst'] = neutral_content.strip()

                            #Decision-making by Portfolio Manager
                            if hasattr(risk_state, 'judge_decision'):
                                risk_decision = getattr(risk_state, 'judge_decision', "")
                            elif isinstance(risk_state, dict) and 'judge_decision' in risk_state:
                                risk_decision = risk_state['judge_decision']
                            else:
                                risk_decision = str(risk_state)

                            if risk_decision and len(risk_decision.strip()) > 10:
                                reports['risk_management_decision'] = risk_decision.strip()

                    logger.info(f"📊 From the state{len(reports)}Reports:{list(reports.keys())}")

                except Exception as e:
                    logger.warning(f"⚠️ There was an error in handling reports in the state:{e}")
                    #Degraded to extract from detailed analysis
                    if 'detailed_analysis' in result:
                        try:
                            detailed_analysis = result['detailed_analysis']
                            if isinstance(detailed_analysis, dict):
                                for key, value in detailed_analysis.items():
                                    if isinstance(value, str) and len(value) > 50:
                                        reports[key] = value
                                logger.info(f"📊 Decline: extracted from detailed analysis{len(reports)}Report")
                        except Exception as fallback_error:
                            logger.warning(f"⚠️ The downgrading also failed:{fallback_error}")

            #🔥Infer market type based on stock code
            from tradingagents.utils.stock_utils import StockUtils
            market_info = StockUtils.get_market_info(stock_symbol)
            market_type_map = {
                "china_a": "A股",
                "hong_kong": "港股",
                "us": "美股",
                "unknown": "A股"  #Default to Unit A
            }
            market_type = market_type_map.get(market_info.get("market", "unknown"), "A股")
            logger.info(f"📊 Infer market type:{stock_symbol} -> {market_type}")

            #Can not open message
            stock_name = stock_symbol  #Default use of stock code
            try:
                if market_info.get("market") == "china_a":
                    #Unit A: Access to stock information using a unified interface
                    from tradingagents.dataflows.interface import get_china_stock_info_unified
                    stock_info = get_china_stock_info_unified(stock_symbol)
                    logger.debug(f"📊 Can not open message{stock_info[:200] if stock_info else 'None'}...")

                    if stock_info and "股票名称:" in stock_info:
                        stock_name = stock_info.split("股票名称:")[1].split("\n")[0].strip()
                        logger.info(f"✅ For the name of Unit A:{stock_symbol} -> {stock_name}")
                    else:
                        #Downscaling: attempt to obtain directly from the data source manager
                        logger.warning(f"⚠️ Could not close temporary folder: %s{stock_symbol}, try to downgrade")
                        try:
                            from tradingagents.dataflows.data_source_manager import get_china_stock_info_unified as get_info_dict
                            info_dict = get_info_dict(stock_symbol)
                            if info_dict and info_dict.get('name'):
                                stock_name = info_dict['name']
                                logger.info(f"✅ The downgrading programme successfully acquired the name of the stock:{stock_symbol} -> {stock_name}")
                        except Exception as fallback_e:
                            logger.error(f"❌ The demotion programme also failed:{fallback_e}")

                elif market_info.get("market") == "hong_kong":
                    #Port Unit: use of improved Port Unit tools
                    try:
                        from tradingagents.dataflows.providers.hk.improved_hk import get_hk_company_name_improved
                        stock_name = get_hk_company_name_improved(stock_symbol)
                        logger.info(f"📊 For the name of the Port Unit:{stock_symbol} -> {stock_name}")
                    except Exception:
                        clean_ticker = stock_symbol.replace('.HK', '').replace('.hk', '')
                        stock_name = f"港股{clean_ticker}"
                elif market_info.get("market") == "us":
                    #United States share: using simple mapping
                    us_stock_names = {
                        'AAPL': '苹果公司', 'TSLA': '特斯拉', 'NVDA': '英伟达',
                        'MSFT': '微软', 'GOOGL': '谷歌', 'AMZN': '亚马逊',
                        'META': 'Meta', 'NFLX': '奈飞'
                    }
                    stock_name = us_stock_names.get(stock_symbol.upper(), f"美股{stock_symbol}")
                    logger.info(f"📊 For United States share names:{stock_symbol} -> {stock_name}")
            except Exception as e:
                logger.warning(f"⚠️ Could not close temporary folder: %s{stock_symbol} - {e}")
                stock_name = stock_symbol

            #Build document (consistent with MongoDBReportManager in web directory)
            document = {
                "analysis_id": analysis_id,
                "stock_symbol": stock_symbol,
                "stock_name": stock_name,  #Add stock name field 🔥
                "market_type": market_type,  #Add market-type fields
                "model_info": result.get("model_info", "Unknown"),  #Add Model Information Fields
                "analysis_date": timestamp.strftime('%Y-%m-%d'),
                "timestamp": timestamp,
                "status": "completed",
                "source": "api",

                #Summary of findings
                "summary": result.get("summary", ""),
                "analysts": result.get("analysts", []),
                "research_depth": result.get("research_depth", 1),

                #Contents of report
                "reports": reports,

                #🔥Key fixation: add formatted decision field!
                "decision": result.get("decision", {}),

                #Metadata
                "created_at": timestamp,
                "updated_at": timestamp,

                #API-specific field
                "task_id": task_id,
                "recommendation": result.get("recommendation", ""),
                "confidence_score": result.get("confidence_score", 0.0),
                "risk_level": result.get("risk_level", "中等"),
                "key_points": result.get("key_points", []),
                "execution_time": result.get("execution_time", 0),
                "tokens_used": result.get("tokens_used", 0),

                #Performance indicator data
                "performance_metrics": result.get("performance_metrics", {})
            }

            #Save to anallysis reports (consistent with web directory)
            result_insert = await db.analysis_reports.insert_one(document)

            if result_insert.inserted_id:
                logger.info(f"✅ The analysis has been saved at the MongoDB analytical reports:{analysis_id}")

                #Updates also the result field of the analysis tasks collection to maintain API compatibility
                await db.analysis_tasks.update_one(
                    {"task_id": task_id},
                    {"$set": {"result": {
                        "analysis_id": analysis_id,
                        "stock_symbol": stock_symbol,
                        "stock_code": result.get('stock_code', stock_symbol),
                        "analysis_date": result.get('analysis_date'),
                        "summary": result.get("summary", ""),
                        "recommendation": result.get("recommendation", ""),
                        "confidence_score": result.get("confidence_score", 0.0),
                        "risk_level": result.get("risk_level", "中等"),
                        "key_points": result.get("key_points", []),
                        "detailed_analysis": result.get("detailed_analysis", {}),
                        "execution_time": result.get("execution_time", 0),
                        "tokens_used": result.get("tokens_used", 0),
                        "reports": reports,  #Include extracted report contents
                        #🔥Key fixation: add formatted decision field!
                        "decision": result.get("decision", {})
                    }}}
                )
                logger.info(f"💾 The results of the analysis have been saved (web style):{task_id}")
            else:
                logger.error("❌ The MongoDB insertion failed")

        except Exception as e:
            logger.error(f"❌ Could not close temporary folder: %s{task_id} - {e}")
            #Down to Simple Save
            try:
                simple_result = {
                    'task_id': task_id,
                    'success': result.get('success', True),
                    'error': str(e),
                    'completed_at': datetime.utcnow().isoformat()
                }
                await db.analysis_tasks.update_one(
                    {"task_id": task_id},
                    {"$set": {"result": simple_result}}
                )
                logger.info(f"💾 Save with a simplified result:{task_id}")
            except Exception as fallback_error:
                logger.error(f"❌ Simplified storage also failed:{task_id} - {fallback_error}")

    async def _save_analysis_results_complete(self, task_id: str, result: Dict[str, Any]):
        """Full analysis saved - full dual saving of web directory"""
        try:
            #Debug: Print all keys in result
            logger.info(f"🔍 All the keys in [debug] result:{list(result.keys())}")
            logger.info(f"🔍 [ Debug ] Stock code:{result.get('stock_code', 'NOT_FOUND')}")
            logger.info(f"🔍 [ Debug ] Stock symbol:{result.get('stock_symbol', 'NOT_FOUND')}")

            #Use stock symbol first, or if not stock code
            stock_symbol = result.get('stock_symbol') or result.get('stock_code', 'UNKNOWN')
            logger.info(f"💾starts the full preservation of the results:{stock_symbol}")

            #1. Save submodule reports to local directories
            logger.info(f"📁 Start saving submodule reports to local directories")
            local_files = await self._save_modular_reports_to_data_dir(result, stock_symbol)
            if local_files:
                logger.info(f"✅ Saved{len(local_files)}Local report document")
                for module, path in local_files.items():
                    logger.info(f"  - {module}: {path}")
            else:
                logger.warning(f"⚠️ Local Report File Saved Failed")

            #2. Preservation of analytical reports to databases
            logger.info(f"🗄️ [Databank Saves] Library")
            await self._save_analysis_result_web_style(task_id, result)
            logger.info(f"✅ [Databank Preservation] Analysis successfully saved to data Library")

            #3. Record-keeping results
            if local_files:
                logger.info(f"✅ Analytical reports are stored in databases and local files")
            else:
                logger.warning(f"⚠️ Database was saved successfully, but local files failed to be saved")

        except Exception as save_error:
            logger.error(f"❌ There was an error saving the analysis:{str(save_error)}")
            #Down to Database Only
            try:
                await self._save_analysis_result_web_style(task_id, result)
                logger.info(f"💾 Successfully saved (database only):{task_id}")
            except Exception as fallback_error:
                logger.error(f"❌ The downgrading was also unsuccessful:{task_id} - {fallback_error}")

    async def _save_modular_reports_to_data_dir(self, result: Dict[str, Any], stock_symbol: str) -> Dict[str, str]:
        """Save submodule reporting to data directory - fully using web directory file structure"""
        try:
            import os
            from pathlib import Path
            from datetime import datetime
            import json

            #Fetch project root directory
            project_root = Path(__file__).parent.parent.parent

            #Determines the result directory path - consistent with the web directory
            results_dir_env = os.getenv("TRADINGAGENTS_RESULTS_DIR")
            if results_dir_env:
                if not os.path.isabs(results_dir_env):
                    results_dir = project_root / results_dir_env
                else:
                    results_dir = Path(results_dir_env)
            else:
                #Default use of data directory instead of results directory
                results_dir = project_root / "data" / "analysis_results"

            #Create a stock-specific directory - fully follow the structure of the web directory
            analysis_date_raw = result.get('analysis_date', datetime.now())

            #Ensure that analysis date is a string format
            if isinstance(analysis_date_raw, datetime):
                analysis_date_str = analysis_date_raw.strftime('%Y-%m-%d')
            elif isinstance(analysis_date_raw, str):
                #If already a string, check the format
                try:
                    #Try to parse the date string to make sure the format is correct
                    parsed_date = datetime.strptime(analysis_date_raw, '%Y-%m-%d')
                    analysis_date_str = analysis_date_raw
                except ValueError:
                    #Use current date if format is incorrect
                    analysis_date_str = datetime.now().strftime('%Y-%m-%d')
            else:
                #Other type, use current date
                analysis_date_str = datetime.now().strftime('%Y-%m-%d')

            stock_dir = results_dir / stock_symbol / analysis_date_str
            reports_dir = stock_dir / "reports"
            reports_dir.mkdir(parents=True, exist_ok=True)

            #Create message tool.log file - aligned to web directory
            log_file = stock_dir / "message_tool.log"
            log_file.touch(exist_ok=True)

            logger.info(f"📁 Synchronising {reports_dir}")
            logger.info(f"🔍 [Debugs] Analysis date raw type:{type(analysis_date_raw)}, Value:{analysis_date_raw}")
            logger.info(f"🔍 [ Debugs ] anallysis date str:{analysis_date_str}")
            logger.info(f"🔍 [Debug ] Full path:{os.path.normpath(str(reports_dir))}")

            state = result.get('state', {})
            saved_files = {}

            #Defines the map of the reporting module - fully according to the definition of the web directory
            report_modules = {
                'market_report': {
                    'filename': 'market_report.md',
                    'title': f'{stock_symbol} 股票技术分析报告',
                    'state_key': 'market_report'
                },
                'sentiment_report': {
                    'filename': 'sentiment_report.md',
                    'title': f'{stock_symbol} 市场情绪分析报告',
                    'state_key': 'sentiment_report'
                },
                'news_report': {
                    'filename': 'news_report.md',
                    'title': f'{stock_symbol} 新闻事件分析报告',
                    'state_key': 'news_report'
                },
                'fundamentals_report': {
                    'filename': 'fundamentals_report.md',
                    'title': f'{stock_symbol} 基本面分析报告',
                    'state_key': 'fundamentals_report'
                },
                'investment_plan': {
                    'filename': 'investment_plan.md',
                    'title': f'{stock_symbol} 投资决策报告',
                    'state_key': 'investment_plan'
                },
                'trader_investment_plan': {
                    'filename': 'trader_investment_plan.md',
                    'title': f'{stock_symbol} 交易计划报告',
                    'state_key': 'trader_investment_plan'
                },
                'final_trade_decision': {
                    'filename': 'final_trade_decision.md',
                    'title': f'{stock_symbol} 最终投资决策',
                    'state_key': 'final_trade_decision'
                },
                'investment_debate_state': {
                    'filename': 'research_team_decision.md',
                    'title': f'{stock_symbol} 研究团队决策报告',
                    'state_key': 'investment_debate_state'
                },
                'risk_debate_state': {
                    'filename': 'risk_management_decision.md',
                    'title': f'{stock_symbol} 风险管理团队决策报告',
                    'state_key': 'risk_debate_state'
                }
            }

            #Save module reports - fully in the way of the web directory
            for module_key, module_info in report_modules.items():
                try:
                    state_key = module_info['state_key']
                    if state_key in state:
                        #Extract module contents
                        module_content = state[state_key]
                        if isinstance(module_content, str):
                            report_content = module_content
                        else:
                            report_content = str(module_content)

                        #Save to File - File Using Web Directory First Name
                        file_path = reports_dir / module_info['filename']
                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.write(report_content)

                        saved_files[module_key] = str(file_path)
                        logger.info(f"✅ Save module reports:{file_path}")

                except Exception as e:
                    logger.warning(f"⚠️ Save Modules{module_key}Failed:{e}")

            #Save final decision report - fully in the web directory
            decision = result.get('decision', {})
            if decision:
                decision_content = f"# {stock_symbol} 最终投资决策\n\n"

                if isinstance(decision, dict):
                    decision_content += f"## 投资建议\n\n"
                    decision_content += f"**行动**: {decision.get('action', 'N/A')}\n\n"
                    decision_content += f"**置信度**: {decision.get('confidence', 0):.1%}\n\n"
                    decision_content += f"**风险评分**: {decision.get('risk_score', 0):.1%}\n\n"
                    decision_content += f"**目标价位**: {decision.get('target_price', 'N/A')}\n\n"
                    decision_content += f"## 分析推理\n\n{decision.get('reasoning', '暂无分析推理')}\n\n"
                else:
                    decision_content += f"{str(decision)}\n\n"

                decision_file = reports_dir / "final_trade_decision.md"
                with open(decision_file, 'w', encoding='utf-8') as f:
                    f.write(decision_content)

                saved_files['final_trade_decision'] = str(decision_file)
                logger.info(f"✅ Save final decision:{decision_file}")

            #Save analytical metadata files - fully in the way of the web directory
            metadata = {
                'stock_symbol': stock_symbol,
                'analysis_date': analysis_date_str,
                'timestamp': datetime.now().isoformat(),
                'research_depth': result.get('research_depth', 1),
                'analysts': result.get('analysts', []),
                'status': 'completed',
                'reports_count': len(saved_files),
                'report_types': list(saved_files.keys())
            }

            metadata_file = reports_dir.parent / "analysis_metadata.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

            logger.info(f"✅ Can not open message{metadata_file}")
            logger.info(f"✅ Sub-module report saved and saved{len(saved_files)}File")
            logger.info(f"📁 Can not open message{os.path.normpath(str(reports_dir))}")

            return saved_files

        except Exception as e:
            logger.error(f"❌ Could not close temporary folder: %s{e}")
            import traceback
            logger.error(f"❌ Detailed error:{traceback.format_exc()}")
            return {}

#Repeated get task status method deleted, using memory version of line 469


#Examples of global services
_analysis_service = None

def get_simple_analysis_service() -> SimpleAnalysisService:
    """Examples of access to analytical services"""
    global _analysis_service
    if _analysis_service is None:
        logger.info("🔧 Can not open message")
        _analysis_service = SimpleAnalysisService()
    else:
        logger.info(f"🔧 [single] returns the existing SimpleAnalysisService example:{id(_analysis_service)}")
    return _analysis_service
