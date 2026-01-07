"""Equities Analysis Services
Packing existing TradingAgents analysis functions into API services
"""

import asyncio
import uuid
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable
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
from app.services.simple_analysis_service import create_analysis_config, get_provider_by_model_name
from app.models.analysis import (
    AnalysisParameters, AnalysisResult, AnalysisTask, AnalysisBatch,
    AnalysisStatus, BatchStatus, SingleAnalysisRequest, BatchAnalysisRequest
)
from app.models.user import PyObjectId
from bson import ObjectId
from app.core.database import get_mongo_db_async
from app.core.redis_client import get_redis_service, RedisKeys
from app.services.queue_service import QueueService
from app.core.database import get_redis_client_async
from app.services.redis_progress_tracker import RedisProgressTracker
from app.services.config_provider import provider as config_provider
from app.services.queue import DEFAULT_USER_CONCURRENT_LIMIT, GLOBAL_CONCURRENT_LIMIT, VISIBILITY_TIMEOUT_SECONDS
from app.services.usage_statistics_service import UsageStatisticsService
from app.models.config import UsageRecord

import logging
logger = logging.getLogger(__name__)


class AnalysisService:
    """Equities Analysis Services"""

    def __init__(self):
        #Get Redis client
        redis_client = get_redis_client_async()
        self.queue_service = QueueService(redis_client)
        #Initial use of statistical services
        self.usage_service = UsageStatisticsService()
        self._trading_graph_cache = {}
        #Progress Tracker Cache
        self._progress_trackers: Dict[str, RedisProgressTracker] = {}

    def _convert_user_id(self, user_id: str) -> PyObjectId:
        """Convert string userID to PyObjectId"""
        try:
            logger.info(f"Start switching user ID:{user_id}(Types:{type(user_id)})")

            #For admin users, use fixedObjectId
            if user_id == "admin":
                #Use fixedObjectId as admin user ID
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
        """Get or create examples of TradingAgents maps (with caches) - consistent with single share analysis"""
        config_key = json.dumps(config, sort_keys=True)

        if config_key not in self._trading_graph_cache:
            #Directly use the full configuration and no longer merge DEFAULT CONFIG (because file analysis config has been processed)
            #This is consistent with the way in which the single analytical services and the web catalogues are conducted.
            self._trading_graph_cache[config_key] = TradingAgentsGraph(
                selected_analysts=config.get("selected_analysts", ["market", "fundamentals"]),
                debug=config.get("debug", False),
                config=config
            )

            logger.info(f"Other Organiser{config.get('llm_provider', 'default')}")

        return self._trading_graph_cache[config_key]

    def _execute_analysis_sync_with_progress(self, task: AnalysisTask, progress_tracker: RedisProgressTracker) -> AnalysisResult:
        """Synchronize analytical tasks (run in online pools, track progress)"""
        try:
            #Reinitiation of log system during online process
            from tradingagents.utils.logging_init import init_logging, get_logger
            init_logging()
            thread_logger = get_logger('analysis_thread')

            thread_logger.info(f"[Line pool]{task.task_id} - {task.symbol}")
            logger.info(f"[Line pool]{task.task_id} - {task.symbol}")

            #Environmental inspection
            progress_tracker.update_progress("🔧 检查环境配置")

            #Create full configuration using standard configuration functions
            from app.core.unified_config import unified_config

            quick_model = getattr(task.parameters, 'quick_analysis_model', None) or unified_config.get_quick_analysis_model()
            deep_model = getattr(task.parameters, 'deep_analysis_model', None) or unified_config.get_deep_analysis_model()

            #🔧 Read the full configuration parameters of the model from the MongoDB database (rather than from the JSON file)
            quick_model_config = None
            deep_model_config = None

            try:
                from pymongo import MongoClient
                from app.core.config import SETTINGS

                #Use sync MongoDB client
                client = MongoClient(SETTINGS.MONGO_URI)
                db = client[SETTINGS.MONGO_DB_NAME]
                collection = db.system_configs

                #Query the latest active configuration
                doc = collection.find_one({"is_active": True}, sort=[("version", -1)])

                if doc and "llm_configs" in doc:
                    llm_configs = doc["llm_configs"]
                    logger.info(f"Read from MongoDB{len(llm_configs)}Model Configuration")

                    for llm_config in llm_configs:
                        if llm_config.get("model_name") == quick_model:
                            quick_model_config = {
                                "max_tokens": llm_config.get("max_tokens", 4000),
                                "temperature": llm_config.get("temperature", 0.7),
                                "timeout": llm_config.get("timeout", 180),
                                "retry_times": llm_config.get("retry_times", 3),
                                "api_base": llm_config.get("api_base")
                            }
                            logger.info(f"Read fast model configuration:{quick_model}")
                            logger.info(f"   max_tokens={quick_model_config['max_tokens']}, temperature={quick_model_config['temperature']}")
                            logger.info(f"   timeout={quick_model_config['timeout']}, retry_times={quick_model_config['retry_times']}")
                            logger.info(f"   api_base={quick_model_config['api_base']}")

                        if llm_config.get("model_name") == deep_model:
                            deep_model_config = {
                                "max_tokens": llm_config.get("max_tokens", 4000),
                                "temperature": llm_config.get("temperature", 0.7),
                                "timeout": llm_config.get("timeout", 180),
                                "retry_times": llm_config.get("retry_times", 3),
                                "api_base": llm_config.get("api_base")
                            }
                            logger.info(f"Read depth model configuration:{deep_model} - {deep_model_config}")
                else:
                    logger.warning("No system configuration found in MongoDB. Default parameters will be used")
            except Exception as e:
                logger.warning(f"Reading model configuration from MongoDB failed:{e}, default parameters will be used")

            #Cost estimates
            progress_tracker.update_progress("💰 预估分析成本")

            #Find suppliers according to model name dynamics (sync version)
            llm_provider = "dashscope"  #Default use dashscope

            #Parameter Configuration
            progress_tracker.update_progress("⚙️ 配置分析参数")

            #Create full configuration using standard configuration functions
            from app.services.simple_analysis_service import create_analysis_config
            config = create_analysis_config(
                research_depth=task.parameters.research_depth,
                selected_analysts=task.parameters.selected_analysts or ["market", "fundamentals"],
                quick_model=quick_model,
                deep_model=deep_model,
                llm_provider=llm_provider,
                market_type=getattr(task.parameters, 'market_type', "A股"),
                quick_model_config=quick_model_config,  #Transfer Model Configuration
                deep_model_config=deep_model_config     #Transfer Model Configuration
            )

            #Start the engine.
            progress_tracker.update_progress("🚀 初始化AI分析引擎")

            #Fetching Action Examples
            trading_graph = self._get_trading_graph(config)

            #Implementation analysis
            from datetime import timezone
            start_time = datetime.now(timezone.utc)
            analysis_date = task.parameters.analysis_date or datetime.now().strftime("%Y-%m-%d")

            #Create Progress Replay function
            def progress_callback(message: str):
                progress_tracker.update_progress(message)

            #Call existing analytical methods (sync call, pass back progress)
            _, decision = trading_graph.propagate(task.symbol, analysis_date, progress_callback)

            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()

            #Generate Report
            progress_tracker.update_progress("📊 生成分析报告")

            #Extract model information from decision-making
            model_info = decision.get('model_info', 'Unknown') if isinstance(decision, dict) else 'Unknown'

            #Build Results
            result = AnalysisResult(
                analysis_id=str(uuid.uuid4()),
                summary=decision.get("summary", ""),
                recommendation=decision.get("recommendation", ""),
                confidence_score=decision.get("confidence_score", 0.0),
                risk_level=decision.get("risk_level", "中等"),
                key_points=decision.get("key_points", []),
                detailed_analysis=decision,
                execution_time=execution_time,
                tokens_used=decision.get("tokens_used", 0),
                model_info=model_info  #Add Model Information Fields
            )

            logger.info(f"Analysis mission completed:{task.task_id}- Time-consuming.{execution_time:.2f}sec")
            return result

        except Exception as e:
            logger.error(f"[Line pool]{task.task_id} - {e}")
            raise

    def _execute_analysis_sync(self, task: AnalysisTask) -> AnalysisResult:
        """Synchronize analytical tasks (run in an online pool)"""
        try:
            logger.info(f"[Line pool]{task.task_id} - {task.symbol}")

            #Create full configuration using standard configuration functions
            from app.core.unified_config import unified_config

            quick_model = getattr(task.parameters, 'quick_analysis_model', None) or unified_config.get_quick_analysis_model()
            deep_model = getattr(task.parameters, 'deep_analysis_model', None) or unified_config.get_deep_analysis_model()

            #🔧 Read the full configuration parameters of the model from the MongoDB database (rather than from the JSON file)
            quick_model_config = None
            deep_model_config = None

            try:
                from pymongo import MongoClient
                from app.core.config import SETTINGS

                #Use sync MongoDB client
                client = MongoClient(SETTINGS.MONGO_URI)
                db = client[SETTINGS.MONGO_DB_NAME]
                collection = db.system_configs

                #Query the latest active configuration
                doc = collection.find_one({"is_active": True}, sort=[("version", -1)])

                if doc and "llm_configs" in doc:
                    llm_configs = doc["llm_configs"]
                    logger.info(f"Read from MongoDB{len(llm_configs)}Model Configuration")

                    for llm_config in llm_configs:
                        if llm_config.get("model_name") == quick_model:
                            quick_model_config = {
                                "max_tokens": llm_config.get("max_tokens", 4000),
                                "temperature": llm_config.get("temperature", 0.7),
                                "timeout": llm_config.get("timeout", 180),
                                "retry_times": llm_config.get("retry_times", 3),
                                "api_base": llm_config.get("api_base")
                            }
                            logger.info(f"Read fast model configuration:{quick_model}")
                            logger.info(f"   max_tokens={quick_model_config['max_tokens']}, temperature={quick_model_config['temperature']}")
                            logger.info(f"   timeout={quick_model_config['timeout']}, retry_times={quick_model_config['retry_times']}")
                            logger.info(f"   api_base={quick_model_config['api_base']}")

                        if llm_config.get("model_name") == deep_model:
                            deep_model_config = {
                                "max_tokens": llm_config.get("max_tokens", 4000),
                                "temperature": llm_config.get("temperature", 0.7),
                                "timeout": llm_config.get("timeout", 180),
                                "retry_times": llm_config.get("retry_times", 3),
                                "api_base": llm_config.get("api_base")
                            }
                            logger.info(f"Read depth model configuration:{deep_model} - {deep_model_config}")
                else:
                    logger.warning("No system configuration found in MongoDB. Default parameters will be used")
            except Exception as e:
                logger.warning(f"Reading model configuration from MongoDB failed:{e}, default parameters will be used")

            #Find suppliers according to model name dynamics (sync version)
            llm_provider = "dashscope"  #Default use dashscope

            #Create full configuration using standard configuration functions
            from app.services.simple_analysis_service import create_analysis_config
            config = create_analysis_config(
                research_depth=task.parameters.research_depth,
                selected_analysts=task.parameters.selected_analysts or ["market", "fundamentals"],
                quick_model=quick_model,
                deep_model=deep_model,
                llm_provider=llm_provider,
                market_type=getattr(task.parameters, 'market_type', "A股"),
                quick_model_config=quick_model_config,  #Transfer Model Configuration
                deep_model_config=deep_model_config     #Transfer Model Configuration
            )

            #Fetching Action Examples
            trading_graph = self._get_trading_graph(config)

            #Implementation analysis
            from datetime import timezone
            start_time = datetime.now(timezone.utc)
            analysis_date = task.parameters.analysis_date or datetime.now().strftime("%Y-%m-%d")

            #Call existing analytical methods (synchronous calls)
            _, decision = trading_graph.propagate(task.symbol, analysis_date)

            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()

            #Extract model information from decision-making
            model_info = decision.get('model_info', 'Unknown') if isinstance(decision, dict) else 'Unknown'

            #Build Results
            result = AnalysisResult(
                analysis_id=str(uuid.uuid4()),
                summary=decision.get("summary", ""),
                recommendation=decision.get("recommendation", ""),
                confidence_score=decision.get("confidence_score", 0.0),
                risk_level=decision.get("risk_level", "中等"),
                key_points=decision.get("key_points", []),
                detailed_analysis=decision,
                execution_time=execution_time,
                tokens_used=decision.get("tokens_used", 0),
                model_info=model_info  #Add Model Information Fields
            )

            logger.info(f"Analysis mission completed:{task.task_id}- Time-consuming.{execution_time:.2f}sec")
            return result

        except Exception as e:
            logger.error(f"[Line pool]{task.task_id} - {e}")
            raise

    async def _execute_single_analysis_async(self, task: AnalysisTask):
        """Step on a single unit analysis mission (run backstage without blocking the main route)"""
        progress_tracker = None
        try:
            logger.info(f"The analysis mission began:{task.task_id} - {task.symbol}")

            #Create Progress Tracker
            progress_tracker = RedisProgressTracker(
                task_id=task.task_id,
                analysts=task.parameters.selected_analysts or ["market", "fundamentals"],
                research_depth=task.parameters.research_depth or "标准",
                llm_provider="dashscope"
            )

            #Cache Progress Tracker
            self._progress_trackers[task.task_id] = progress_tracker

            #Initialization progress
            progress_tracker.update_progress("🚀 开始股票分析")
            await self._update_task_status_with_tracker(task.task_id, AnalysisStatus.PROCESSING, progress_tracker)

            #Perform analysis in the online pool to avoid blocking the cycle of events
            import asyncio
            import concurrent.futures

            loop = asyncio.get_event_loop()

            #Run synchronized analysis codes using a thread pool implementer
            with concurrent.futures.ThreadPoolExecutor() as executor:
                result = await loop.run_in_executor(
                    executor,
                    self._execute_analysis_sync_with_progress,
                    task,
                    progress_tracker
                )

            #Tag Completed
            progress_tracker.mark_completed("✅ 分析完成")
            await self._update_task_status_with_tracker(task.task_id, AnalysisStatus.COMPLETED, progress_tracker, result)

            #Record token usage
            try:
                #Get the model information used
                quick_model = getattr(task.parameters, 'quick_analysis_model', None)
                deep_model = getattr(task.parameters, 'deep_analysis_model', None)

                #Prioritize the use of depth analysis models or, if not, rapid analysis models
                model_name = deep_model or quick_model or "qwen-plus"

                #Identification of suppliers by model name
                from app.services.simple_analysis_service import get_provider_by_model_name
                provider = get_provider_by_model_name(model_name)

                #Recording of usage
                await self._record_token_usage(task, result, provider, model_name)
            except Exception as e:
                logger.error(f"Recording token failed:{e}")

            logger.info(f"Analysis mission completed:{task.task_id}")

        except Exception as e:
            logger.error(f"The analysis mission failed:{task.task_id} - {e}")

            #Tag failed
            if progress_tracker:
                progress_tracker.mark_failed(str(e))
                await self._update_task_status_with_tracker(task.task_id, AnalysisStatus.FAILED, progress_tracker)
            else:
                await self._update_task_status(task.task_id, AnalysisStatus.FAILED, 0, str(e))
        finally:
            #Clear Progress Tracker Cache
            if task.task_id in self._progress_trackers:
                del self._progress_trackers[task.task_id]

    async def submit_single_analysis(
        self,
        user_id: str,
        request: SingleAnalysisRequest
    ) -> Dict[str, Any]:
        """Submission of single unit analysis assignments"""
        try:
            logger.info(f"Start submitting single unit analysis assignments")
            logger.info(f"User ID:{user_id}(Types:{type(user_id)})")

            #Get stock code (old field compatible)
            stock_symbol = request.get_symbol()
            logger.info(f"Stock code:{stock_symbol}")
            logger.info(f"Analysis parameters:{request.parameters}")

            #Generate Task ID
            task_id = str(uuid.uuid4())
            logger.info(f"Other Organiser{task_id}")

            #Convert User ID
            converted_user_id = self._convert_user_id(user_id)
            logger.info(f"User ID after conversion:{converted_user_id}(Types:{type(converted_user_id)})")

            #Create analytical task
            logger.info(f"Start creating AnalysisTask objects...")

            #Read merged system settings (ENV priority DB) for filling models with simultaneous/overtime configuration
            try:
                effective_settings = await config_provider.get_effective_system_settings()
            except Exception:
                effective_settings = {}

            #Filling model in analytical parameters (if requested, not visible)
            params = request.parameters or AnalysisParameters()
            if not getattr(params, 'quick_analysis_model', None):
                params.quick_analysis_model = effective_settings.get("quick_analysis_model", "qwen-turbo")
            if not getattr(params, 'deep_analysis_model', None):
                params.deep_analysis_model = effective_settings.get("deep_analysis_model", "qwen-max")

            #Application system level combined with visibility timeout (if available)
            try:
                self.queue_service.user_concurrent_limit = int(effective_settings.get("max_concurrent_tasks", DEFAULT_USER_CONCURRENT_LIMIT))
                self.queue_service.global_concurrent_limit = int(effective_settings.get("max_concurrent_tasks", GLOBAL_CONCURRENT_LIMIT))
                self.queue_service.visibility_timeout = int(effective_settings.get("default_analysis_timeout", VISIBILITY_TIMEOUT_SECONDS))
            except Exception:
                #Use the default value.
                pass

            task = AnalysisTask(
                task_id=task_id,
                user_id=converted_user_id,
                symbol=stock_symbol,
                stock_code=stock_symbol,  #Compatible Fields
                parameters=params,
                status=AnalysisStatus.PENDING
            )
            logger.info(f"AnalysisTask object created successfully")

            #Can not open message
            logger.info(f"Can not open message")
            db = get_mongo_db_async()
            task_dict = task.model_dump(by_alias=True)
            logger.info(f"Mission Dictionary:{task_dict}")
            await db.analysis_tasks.insert_one(task_dict)
            logger.info(f"✅ Tasks saved to database")

            #Single unit analysis: implemented directly backstage (without blocking API response)
            logger.info(f"We'll start an analysis backstage...")

            #Create background task without waiting for completion
            import asyncio
            background_task = asyncio.create_task(
                self._execute_single_analysis_async(task)
            )

            #Let it run backstage without waiting for the mission to be completed.
            logger.info(f"Backstage mission started. Mission ID:{task_id}")

            logger.info(f"Single unit analysis mission submitted:{task_id} - {stock_symbol}")

            return {
                "task_id": task_id,
                "symbol": stock_symbol,
                "stock_code": stock_symbol,  #Compatible Fields
                "status": AnalysisStatus.PENDING,
                "message": "任务已在后台启动"
            }
            
        except Exception as e:
            logger.error(f"Failed to submit single unit analysis task:{e}")
            raise
    
    async def submit_batch_analysis(
        self, 
        user_id: str, 
        request: BatchAnalysisRequest
    ) -> Dict[str, Any]:
        """Submission of batch analysis assignments"""
        try:
            #Generate Batch ID
            batch_id = str(uuid.uuid4())
            
            #Convert User ID
            converted_user_id = self._convert_user_id(user_id)

            #Read system settings, fill model parameters and apply simultaneous/overtime configuration
            try:
                effective_settings = await config_provider.get_effective_system_settings()
            except Exception:
                effective_settings = {}

            params = request.parameters or AnalysisParameters()
            if not getattr(params, 'quick_analysis_model', None):
                params.quick_analysis_model = effective_settings.get("quick_analysis_model", "qwen-turbo")
            if not getattr(params, 'deep_analysis_model', None):
                params.deep_analysis_model = effective_settings.get("deep_analysis_model", "qwen-max")

            try:
                self.queue_service.user_concurrent_limit = int(effective_settings.get("max_concurrent_tasks", DEFAULT_USER_CONCURRENT_LIMIT))
                self.queue_service.global_concurrent_limit = int(effective_settings.get("max_concurrent_tasks", GLOBAL_CONCURRENT_LIMIT))
                self.queue_service.visibility_timeout = int(effective_settings.get("default_analysis_timeout", VISIBILITY_TIMEOUT_SECONDS))
            except Exception:
                pass

            #Create Batch Record
            #Retrieving list of stock codes (old field compatible)
            stock_symbols = request.get_symbols()

            batch = AnalysisBatch(
                batch_id=batch_id,
                user_id=converted_user_id,
                title=request.title,
                description=request.description,
                total_tasks=len(stock_symbols),
                parameters=params,
                status=BatchStatus.PENDING
            )

            #Other Organiser
            tasks = []
            for symbol in stock_symbols:
                task_id = str(uuid.uuid4())
                task = AnalysisTask(
                    task_id=task_id,
                    batch_id=batch_id,
                    user_id=converted_user_id,
                    symbol=symbol,
                    stock_code=symbol,  #Compatible Fields
                    parameters=batch.parameters,
                    status=AnalysisStatus.PENDING
                )
                tasks.append(task)
            
            #Save to Database
            db = get_mongo_db_async()
            await db.analysis_batches.insert_one(batch.dict(by_alias=True))
            await db.analysis_tasks.insert_many([task.dict(by_alias=True) for task in tasks])
            
            #Submit Tasks to Queue
            for task in tasks:
                #Prepare Queue Parameters (Direct Pass Analysis Parameters, No Embedded)
                queue_params = task.parameters.dict() if task.parameters else {}

                #Add Task Metadata
                queue_params.update({
                    "task_id": task.task_id,
                    "symbol": task.symbol,
                    "stock_code": task.symbol,  #Compatible Fields
                    "user_id": str(task.user_id),
                    "batch_id": task.batch_id,
                    "created_at": task.created_at.isoformat() if task.created_at else None
                })

                #Call Queue Service
                await self.queue_service.enqueue_task(
                    user_id=str(converted_user_id),
                    symbol=task.symbol,
                    params=queue_params,
                    batch_id=task.batch_id
                )
            
            logger.info(f"Batch analysis missions have been submitted:{batch_id} - {len(tasks)}Equities")
            
            return {
                "batch_id": batch_id,
                "total_tasks": len(tasks),
                "status": BatchStatus.PENDING,
                "message": f"已提交{len(tasks)}个分析任务到队列"
            }
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            raise
    
    async def execute_analysis_task(
        self, 
        task: AnalysisTask,
        progress_callback: Optional[Callable[[int, str], None]] = None
    ) -> AnalysisResult:
        """Individual analytical tasks performed"""
        try:
            logger.info(f"An analytical mission began:{task.task_id} - {task.symbol}")
            
            #Update Task Status
            await self._update_task_status(task.task_id, AnalysisStatus.PROCESSING, 0)
            
            if progress_callback:
                progress_callback(10, "初始化分析引擎...")
            
            #Create complete configuration using standard configuration functions - consistent with single unit analysis
            from app.core.unified_config import unified_config

            quick_model = getattr(task.parameters, 'quick_analysis_model', None) or unified_config.get_quick_analysis_model()
            deep_model = getattr(task.parameters, 'deep_analysis_model', None) or unified_config.get_deep_analysis_model()

            #🔧 Read full configuration parameters of the model from the database
            quick_model_config = None
            deep_model_config = None
            llm_configs = unified_config.get_llm_configs()

            for llm_config in llm_configs:
                if llm_config.model_name == quick_model:
                    quick_model_config = {
                        "max_tokens": llm_config.max_tokens,
                        "temperature": llm_config.temperature,
                        "timeout": llm_config.timeout,
                        "retry_times": llm_config.retry_times,
                        "api_base": llm_config.api_base
                    }

                if llm_config.model_name == deep_model:
                    deep_model_config = {
                        "max_tokens": llm_config.max_tokens,
                        "temperature": llm_config.temperature,
                        "timeout": llm_config.timeout,
                        "retry_times": llm_config.retry_times,
                        "api_base": llm_config.api_base
                    }

            #Find supply according to model name dynamics Business
            llm_provider = await get_provider_by_model_name(quick_model)

            #Create full configuration using standard configuration functions
            config = create_analysis_config(
                research_depth=task.parameters.research_depth,
                selected_analysts=task.parameters.selected_analysts or ["market", "fundamentals"],
                quick_model=quick_model,
                deep_model=deep_model,
                llm_provider=llm_provider,
                market_type=getattr(task.parameters, 'market_type', "A股"),
                quick_model_config=quick_model_config,  #Transfer Model Configuration
                deep_model_config=deep_model_config     #Transfer Model Configuration
            )
            
            if progress_callback:
                progress_callback(30, "创建分析图...")
            
            #Fetching Action Examples
            trading_graph = self._get_trading_graph(config)
            
            if progress_callback:
                progress_callback(50, "执行股票分析...")
            
            #Implementation analysis
            start_time = datetime.utcnow()
            analysis_date = task.parameters.analysis_date or datetime.now().strftime("%Y-%m-%d")
            
            #Access to existing analytical methods
            _, decision = trading_graph.propagate(task.symbol, analysis_date)
            
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            
            if progress_callback:
                progress_callback(80, "处理分析结果...")

            #Extract model information from decision-making
            model_info = decision.get('model_info', 'Unknown') if isinstance(decision, dict) else 'Unknown'

            #Build Results
            result = AnalysisResult(
                analysis_id=str(uuid.uuid4()),
                summary=decision.get("summary", ""),
                recommendation=decision.get("recommendation", ""),
                confidence_score=decision.get("confidence_score", 0.0),
                risk_level=decision.get("risk_level", "中等"),
                key_points=decision.get("key_points", []),
                detailed_analysis=decision,
                execution_time=execution_time,
                tokens_used=decision.get("tokens_used", 0),
                model_info=model_info  #Add Model Information Fields
            )

            if progress_callback:
                progress_callback(100, "分析完成")

            #Update Task Status
            await self._update_task_status(task.task_id, AnalysisStatus.COMPLETED, 100, result)

            #Record token usage
            try:
                #Recording of usage
                await self._record_token_usage(task, result, llm_provider, deep_model or quick_model)
            except Exception as e:
                logger.error(f"Recording token failed:{e}")

            logger.info(f"Analytical tasks accomplished:{task.task_id}- Time-consuming.{execution_time:.2f}sec")

            return result
            
        except Exception as e:
            logger.error(f"Failed to perform analytical tasks:{task.task_id} - {e}")
            
            #Failed to update task status
            error_result = AnalysisResult(error_message=str(e))
            await self._update_task_status(task.task_id, AnalysisStatus.FAILED, 0, error_result)
            
            raise
    
    async def _update_task_status(
        self,
        task_id: str,
        status: AnalysisStatus,
        progress: int,
        result: Optional[AnalysisResult] = None,
    ) -> None:
        """Update task status (commission to split utility function)"""
        try:
            from app.services.analysis.status_update_utils import perform_update_task_status
            await perform_update_task_status(task_id, status, progress, result)
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{task_id} - {e}")

    async def _update_task_status_with_tracker(
        self,
        task_id: str,
        status: AnalysisStatus,
        progress_tracker: RedisProgressTracker,
        result: Optional[AnalysisResult] = None,
    ) -> None:
        """Update task status using progress tracker (trust to split utility function)"""
        try:
            from app.services.analysis.status_update_utils import perform_update_task_status_with_tracker
            await perform_update_task_status_with_tracker(task_id, status, progress_tracker, result)
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{task_id} - {e}")

    async def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get Task Status"""
        try:
            #Check the track of progress in memory first Device
            if task_id in self._progress_trackers:
                progress_tracker = self._progress_trackers[task_id]
                progress_data = progress_tracker.to_dict()

                #Get Task Basic Information from Database
                db = get_mongo_db_async()
                task = await db.analysis_tasks.find_one({"task_id": task_id})

                if task:
                    #Merge database information and progress tracker information
                    return {
                        "task_id": task_id,
                        "user_id": task.get("user_id"),
                        "symbol": task.get("stock_symbol") or task.get("symbol"),
                        "stock_code": task.get("stock_symbol") or task.get("symbol"),  #Compatible Fields
                        "status": progress_data["status"],
                        "progress": progress_data["progress"],
                        "current_step": progress_data["current_step"],
                        "message": progress_data["message"],
                        "elapsed_time": progress_data["elapsed_time"],
                        "remaining_time": progress_data["remaining_time"],
                        "estimated_total_time": progress_data.get("estimated_total_time", 0),
                        "steps": progress_data["steps"],
                        "start_time": progress_data["start_time"],
                        "end_time": None,
                        "last_update": progress_data["last_update"],
                        "parameters": task.get("parameters", {}),
                        "execution_time": None,
                        "tokens_used": None,
                        "result_data": task.get("result"),
                        "error_message": None
                    }

            #Get from the Redis cache
            redis_service = get_redis_service()
            progress_key = RedisKeys.TASK_PROGRESS.format(task_id=task_id)
            cached_status = await redis_service.get_json(progress_key)

            if cached_status:
                return cached_status

            #Fetch from database
            db = get_mongo_db_async()
            task = await db.analysis_tasks.find_one({"task_id": task_id})

            if task:
                #Calculate time used
                elapsed_time = 0
                remaining_time = 0
                estimated_total_time = 0

                if task.get("started_at"):
                    from datetime import datetime
                    start_time = task.get("started_at")
                    if task.get("completed_at"):
                        #Task completed
                        elapsed_time = (task.get("completed_at") - start_time).total_seconds()
                        estimated_total_time = elapsed_time  #The total time taken to complete the task is the time taken.
                        remaining_time = 0
                    else:
                        #Mission in progress
                        elapsed_time = (datetime.utcnow() - start_time).total_seconds()

                        #The estimated duration of the task used, or default value if not available (5 minutes)
                        estimated_total_time = task.get("estimated_duration", 300)

                        #Projected balance = total estimated time - time taken
                        remaining_time = max(0, estimated_total_time - elapsed_time)

                return {
                    "task_id": task_id,
                    "status": task.get("status"),
                    "progress": task.get("progress", 0),
                    "current_step": task.get("current_step", ""),
                    "message": task.get("message", ""),
                    "elapsed_time": elapsed_time,
                    "remaining_time": remaining_time,
                    "estimated_total_time": estimated_total_time,
                    "start_time": task.get("started_at").isoformat() if task.get("started_at") else None,
                    "updated_at": task.get("updated_at", "").isoformat() if task.get("updated_at") else None,
                    "result_data": task.get("result")
                }

            return None

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{task_id} - {e}")
            return None
    
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel Task"""
        try:
            #Update Task Status
            await self._update_task_status(task_id, AnalysisStatus.CANCELLED, 0)
            
            #Remove from queue (if still in queue)
            await self.queue_service.remove_task(task_id)
            
            logger.info(f"Other Organiser{task_id}")
            return True
            
        except Exception as e:
            logger.error(f"Can not open message{task_id} - {e}")
            return False

    async def _record_token_usage(
        self,
        task: AnalysisTask,
        result: AnalysisResult,
        provider: str,
        model_name: str
    ):
        """Record token usage"""
        try:
            #Extract token information from the result
            #Note: This requires to obtain actual token usage from LLM responses
            #Current estimate used
            input_tokens = result.tokens_used // 2 if result.tokens_used > 0 else 0
            output_tokens = result.tokens_used - input_tokens if result.tokens_used > 0 else 0

            #Use default estimation if no token is used
            if result.tokens_used == 0:
                #Estimates based on type of analysis
                input_tokens = 2000  #Default input token
                output_tokens = 1000  #Default output token

            #Get Model Price Configuration
            from app.services.config_service import config_service
            config = await config_service.get_system_config()

            #Find corresponding LLM profiles
            llm_config = None
            if config and config.llm_configs:
                for cfg in config.llm_configs:
                    if cfg.provider == provider and cfg.model_name == model_name:
                        llm_config = cfg
                        break

            #Costing
            cost = 0.0
            currency = "CNY"  #Default currency units
            if llm_config:
                input_price = llm_config.input_price_per_1k or 0.0
                output_price = llm_config.output_price_per_1k or 0.0
                cost = (input_tokens / 1000 * input_price) + (output_tokens / 1000 * output_price)
                currency = llm_config.currency or "CNY"

            #Create Usage Record
            usage_record = UsageRecord(
                timestamp=datetime.now().isoformat(),
                provider=provider,
                model_name=model_name,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cost=cost,
                currency=currency,
                session_id=task.task_id,
                analysis_type="stock_analysis",
                stock_code=task.symbol
            )

            #Save to Database
            success = await self.usage_service.add_usage_record(usage_record)

            if success:
                logger.info(f"Recording of usage costs:{provider}/{model_name} - ¥{cost:.4f}")
            else:
                logger.warning(f"Failed to record usage costs")

        except Exception as e:
            logger.error(f"Recording token failed:{e}")


#Global analysis of service examples (delayed initialization)
analysis_service: Optional[AnalysisService] = None


def get_analysis_service() -> AnalysisService:
    """Examples of access to analytical services (delayed initialization)"""
    global analysis_service
    if analysis_service is None:
        analysis_service = AnalysisService()
    return analysis_service
