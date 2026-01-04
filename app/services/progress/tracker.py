"""Progress tracker (transitional period)
- Import RedisRegressTracker class temporarily from old modules
- Provide realization of get process by id within this module (conform with old, amend cls references)
"""
from typing import Any, Dict, Optional, List
import json
import os
import logging
import time



logger = logging.getLogger("app.services.progress.tracker")

from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class AnalysisStep:
    """Analysis step data class"""
    name: str
    description: str
    status: str = "pending"  # pending, current, completed, failed
    weight: float = 0.1  #weights for calculating progress
    start_time: Optional[float] = None
    end_time: Optional[float] = None


def safe_serialize(data):
    """Secure sequenced, handling non-serialized objects"""
    if isinstance(data, dict):
        return {k: safe_serialize(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [safe_serialize(item) for item in data]
    elif isinstance(data, (str, int, float, bool, type(None))):
        return data
    elif hasattr(data, '__dict__'):
        return safe_serialize(data.__dict__)
    else:
        return str(data)



class RedisProgressTracker:
    """Redis progress tracker"""

    def __init__(self, task_id: str, analysts: List[str], research_depth: str, llm_provider: str):
        self.task_id = task_id
        self.analysts = analysts
        self.research_depth = research_depth
        self.llm_provider = llm_provider

        #Redis Connection
        self.redis_client = None
        self.use_redis = self._init_redis()

        #Progress data
        self.progress_data = {
            'task_id': task_id,
            'status': 'running',
            'progress_percentage': 0.0,
            'current_step': 0,  #Index of current steps (numbers)
            'total_steps': 0,
            'current_step_name': '初始化',
            'current_step_description': '准备开始分析',
            'last_message': '分析任务已启动',
            'start_time': time.time(),
            'last_update': time.time(),
            'elapsed_time': 0.0,
            'remaining_time': 0.0,
            'steps': []
        }

        #Generate analytical steps
        self.analysis_steps = self._generate_dynamic_steps()
        self.progress_data['total_steps'] = len(self.analysis_steps)
        self.progress_data['steps'] = [asdict(step) for step in self.analysis_steps]

        #🔧 Calculates and sets the estimated total duration
        base_total_time = self._get_base_total_time()
        self.progress_data['estimated_total_time'] = base_total_time
        self.progress_data['remaining_time'] = base_total_time  #= total duration

        #Save Initial Status
        self._save_progress()

        logger.info(f"[Redis Progressing]{task_id}, steps:{len(self.analysis_steps)}")

    def _init_redis(self) -> bool:
        """Initialize Redis Connection"""
        try:
            #Check REDIS ENABLED environment variables
            redis_enabled = os.getenv('REDIS_ENABLED', 'false').lower() == 'true'
            if not redis_enabled:
                logger.info(f"📊 [Redis Progress]")
                return False

            import redis

            #Redis configuration from environment variable
            redis_host = os.getenv('REDIS_HOST', 'localhost')
            redis_port = int(os.getenv('REDIS_PORT', 6379))
            redis_password = os.getenv('REDIS_PASSWORD', None)
            redis_db = int(os.getenv('REDIS_DB', 0))

            #Create Redis Connection
            if redis_password:
                self.redis_client = redis.Redis(
                    host=redis_host,
                    port=redis_port,
                    password=redis_password,
                    db=redis_db,
                    decode_responses=True
                )
            else:
                self.redis_client = redis.Redis(
                    host=redis_host,
                    port=redis_port,
                    db=redis_db,
                    decode_responses=True
                )

            #Test Connection
            self.redis_client.ping()
            logger.info(f"[Redis progresses]{redis_host}:{redis_port}")
            return True
        except Exception as e:
            logger.warning(f"[Redis Progress] Redis connection failed, using file storage:{e}")
            return False

    def _generate_dynamic_steps(self) -> List[AnalysisStep]:
        """Analysis steps based on the number of analysts and depth of research"""
        steps: List[AnalysisStep] = []
        #1) Basic preparation stage (10%)
        steps.extend([
            AnalysisStep("📋 准备阶段", "验证股票代码，检查数据源可用性", "pending", 0.03),
            AnalysisStep("🔧 环境检查", "检查API密钥配置，确保数据获取正常", "pending", 0.02),
            AnalysisStep("💰 成本估算", "根据分析深度预估API调用成本", "pending", 0.01),
            AnalysisStep("⚙️ 参数设置", "配置分析参数和AI模型选择", "pending", 0.02),
            AnalysisStep("🚀 启动引擎", "初始化AI分析引擎，准备开始分析", "pending", 0.02),
        ])
        #2) Analyst team stage (35%) - parallel
        analyst_weight = 0.35 / max(len(self.analysts), 1)
        for analyst in self.analysts:
            info = self._get_analyst_step_info(analyst)
            steps.append(AnalysisStep(info["name"], info["description"], "pending", analyst_weight))
        #3) Research team debate phase (25%)
        rounds = self._get_debate_rounds()
        debate_weight = 0.25 / (3 + rounds)
        steps.extend([
            AnalysisStep("🐂 看涨研究员", "基于分析师报告构建买入论据", "pending", debate_weight),
            AnalysisStep("🐻 看跌研究员", "识别潜在风险和问题", "pending", debate_weight),
        ])
        for i in range(rounds):
            steps.append(AnalysisStep(f"🎯 研究辩论 第{i+1}轮", "多头空头研究员深度辩论", "pending", debate_weight))
        steps.append(AnalysisStep("👔 研究经理", "综合辩论结果，形成研究共识", "pending", debate_weight))
        #4) Trading team phase (8%)
        steps.append(AnalysisStep("💼 交易员决策", "基于研究结果制定具体交易策略", "pending", 0.08))
        #5) Risk management team phase (15%)
        risk_weight = 0.15 / 4
        steps.extend([
            AnalysisStep("🔥 激进风险评估", "从激进角度评估投资风险", "pending", risk_weight),
            AnalysisStep("🛡️ 保守风险评估", "从保守角度评估投资风险", "pending", risk_weight),
            AnalysisStep("⚖️ 中性风险评估", "从中性角度评估投资风险", "pending", risk_weight),
            AnalysisStep("🎯 风险经理", "综合风险评估，制定风险控制策略", "pending", risk_weight),
        ])
        #6. Final decision-making stage (7%)
        steps.extend([
            AnalysisStep("📡 信号处理", "处理所有分析结果，生成交易信号", "pending", 0.04),
            AnalysisStep("📊 生成报告", "整理分析结果，生成完整报告", "pending", 0.03),
        ])
        return steps

    def _get_debate_rounds(self) -> int:
        """Getting debate rounds based on research depth"""
        if self.research_depth == "快速":
            return 1
        if self.research_depth == "标准":
            return 2
        return 3

    def _get_analyst_step_info(self, analyst: str) -> Dict[str, str]:
        """Get analyst step information (name and description)"""
        mapping = {
            'market': {"name": "📊 市场分析师", "description": "分析股价走势、成交量、技术指标等市场表现"},
            'fundamentals': {"name": "💼 基本面分析师", "description": "分析公司财务状况、盈利能力、成长性等基本面"},
            'news': {"name": "📰 新闻分析师", "description": "分析相关新闻、公告、行业动态对股价的影响"},
            'social': {"name": "💬 社交媒体分析师", "description": "分析社交媒体讨论、网络热度、散户情绪等"},
        }
        return mapping.get(analyst, {"name": f"🔍 {analyst}分析师", "description": f"进行{analyst}相关的专业分析"})

    def _estimate_step_time(self, step: AnalysisStep) -> float:
        """Estimated step implementation time (sec)"""
        return self._get_base_total_time() * step.weight

    def _get_base_total_time(self) -> float:
        """Total estimated length (s) based on number of analysts, depth of research, model type

        Algorithmic design thinking (based on actual test data):
        1. Actual: Level 4 depth + 3 analysts = 11 minutes (661 seconds)
        2. Measurements: 1 level of speed = 4-5 minutes
        3. Measurement: Level 2 base = 5-6 minutes
        4. Co-processing between analysts, not linear overlay
        """

        #Supporting 5 levels of analysis
        depth_map = {
            "快速": 1,  #Level 1 - Rapid analysis
            "基础": 2,  #Level 2 - Basic analysis
            "标准": 3,  #Level 3 - Standard analysis (recommended)
            "深度": 4,  #Level 4 - Depth analysis
            "全面": 5   #Level 5 - Comprehensive analysis
        }
        d = depth_map.get(self.research_depth, 3)  #Default Standard Analysis

        #📊 Base time based on actual test data (sec)
        #This is the basis of the individual analyst.
        base_time_per_depth = {
            1: 150,  #Level 1: 2.5 minutes (4-5 minutes measured as multiple analysts)
            2: 180,  #Level 2: 3 minutes (5-6 minutes measured for multiple analysts)
            3: 240,  #Level 3: 4 minutes (front end: 6-10 minutes)
            4: 330,  #Level 4: 5.5 minutes (actual: 3 analysts 11 minutes, reverse about 5.5 minutes each)
            5: 480   #Level 5: 8 minutes (front end: 15-25 minutes)
        }.get(d, 240)

        #📈 Analysiser ' s quantitative impact factor (based on actual test data)
        #Actual: Level 4 + 3 Analysts = 11 minutes = 660 seconds
        #Inverse: 330 seconds * multiplication = 660 seconds > multiplication = 2.0
        analyst_count = len(self.analysts)
        if analyst_count == 1:
            analyst_multiplier = 1.0
        elif analyst_count == 2:
            analyst_multiplier = 1.5  #Two analysts, about 1.5 times the time.
        elif analyst_count == 3:
            analyst_multiplier = 2.0  #3 analysts approximately twice the time (actual validation)
        elif analyst_count == 4:
            analyst_multiplier = 2.4  #Four analysts, about 2.4 times the time.
        else:
            analyst_multiplier = 2.4 + (analyst_count - 4) * 0.3  #30 per cent increase in 1 additional analyst

        #🚀 Model Speed Impact (based on actual tests)
        model_mult = {
            'dashscope': 1.0,  #Alibri's speed is right.
            'deepseek': 0.8,   #DeepSeek is faster.
            'google': 1.2      #Google's slow.
        }.get(self.llm_provider, 1.0)

        #Calculate total time
        total_time = base_time_per_depth * analyst_multiplier * model_mult

        return total_time

    def _calculate_time_estimates(self) -> tuple[float, float, float]:
        """Return (elapsed, returning, restored, restored total)"""
        now = time.time()
        start = self.progress_data.get('start_time', now)
        elapsed = now - start
        pct = self.progress_data.get('progress_percentage', 0)
        base_total = self._get_base_total_time()

        if pct >= 100:
            #Task completed
            est_total = elapsed
            remaining = 0
        else:
            #Use the estimated total duration (fixed)
            est_total = base_total
            #Projected balance = total estimated time - time taken
            remaining = max(0, est_total - elapsed)

        return elapsed, remaining, est_total

    @staticmethod
    def _calculate_static_time_estimates(progress_data: dict) -> dict:
        """Static: calculation of time estimates for progress data available"""
        if 'start_time' not in progress_data or not progress_data['start_time']:
            return progress_data
        now = time.time()
        elapsed = now - progress_data['start_time']
        progress_data['elapsed_time'] = elapsed
        pct = progress_data.get('progress_percentage', 0)

        if pct >= 100:
            #Task completed
            est_total = elapsed
            remaining = 0
        else:
            #Use the estimated total length (fixed value) or default value if not
            est_total = progress_data.get('estimated_total_time', 300)
            #Projected balance = total estimated time - time taken
            remaining = max(0, est_total - elapsed)

        progress_data['estimated_total_time'] = est_total
        progress_data['remaining_time'] = remaining
        return progress_data

    def update_progress(self, progress_update: Any) -> Dict[str, Any]:
        """update progress and persist; accepts dict or plain message string"""
        try:
            if isinstance(progress_update, dict):
                self.progress_data.update(progress_update)
            elif isinstance(progress_update, str):
                self.progress_data['last_message'] = progress_update
                self.progress_data['last_update'] = time.time()
            else:
                # try to coerce iterable of pairs; otherwise fallback to string
                try:
                    self.progress_data.update(dict(progress_update))
                except Exception:
                    self.progress_data['last_message'] = str(progress_update)
                    self.progress_data['last_update'] = time.time()

            #Automatically update step status based on percentage progress
            progress_pct = self.progress_data.get('progress_percentage', 0)
            self._update_steps_by_progress(progress_pct)

            #Fetch the index of the current step
            current_step_index = self._detect_current_step()
            self.progress_data['current_step'] = current_step_index

            #Update the name and description of the current step
            if 0 <= current_step_index < len(self.analysis_steps):
                current_step_obj = self.analysis_steps[current_step_index]
                self.progress_data['current_step_name'] = current_step_obj.name
                self.progress_data['current_step_description'] = current_step_obj.description

            elapsed, remaining, est_total = self._calculate_time_estimates()
            self.progress_data['elapsed_time'] = elapsed
            self.progress_data['remaining_time'] = remaining
            self.progress_data['estimated_total_time'] = est_total

            #Update steps in project data
            self.progress_data['steps'] = [asdict(step) for step in self.analysis_steps]

            self._save_progress()
            logger.debug(f"[RedisProgress] updated: {self.task_id} - {self.progress_data.get('progress_percentage', 0)}%")
            return self.progress_data
        except Exception as e:
            logger.error(f"[RedisProgress] update failed: {self.task_id} - {e}")
            return self.progress_data

    def _update_steps_by_progress(self, progress_pct: float) -> None:
        """Automatically update step status based on percentage progress"""
        try:
            cumulative_weight = 0.0
            current_time = time.time()

            for step in self.analysis_steps:
                step_start_pct = cumulative_weight
                step_end_pct = cumulative_weight + (step.weight * 100)

                if progress_pct >= step_end_pct:
                    #Steps completed
                    if step.status != 'completed':
                        step.status = 'completed'
                        step.end_time = current_time
                elif progress_pct > step_start_pct:
                    #Steps under way
                    if step.status != 'current':
                        step.status = 'current'
                        step.start_time = current_time
                else:
                    #Steps not started
                    if step.status not in ('pending', 'failed'):
                        step.status = 'pending'

                cumulative_weight = step_end_pct
        except Exception as e:
            logger.debug(f"[RedisProgress] update steps by progress failed: {e}")

    def _detect_current_step(self) -> int:
        """detect current step index by status"""
        try:
            #Prefer steps with `current ' status
            for index, step in enumerate(self.analysis_steps):
                if step.status == 'current':
                    return index
            #If 'current 'is not available, find the first 'pending' step
            for index, step in enumerate(self.analysis_steps):
                if step.status == 'pending':
                    return index
            #If all is done, return the index for the last step
            for index, step in enumerate(reversed(self.analysis_steps)):
                if step.status == 'completed':
                    return len(self.analysis_steps) - 1 - index
            return 0
        except Exception as e:
            logger.debug(f"[RedisProgress] detect current step failed: {e}")
            return 0

    def _find_step_by_name(self, step_name: str) -> Optional[AnalysisStep]:
        for step in self.analysis_steps:
            if step.name == step_name:
                return step
        return None

    def _find_step_by_pattern(self, pattern: str) -> Optional[AnalysisStep]:
        for step in self.analysis_steps:
            if pattern in step.name:
                return step
        return None

    def _save_progress(self) -> None:
        try:
            progress_copy = self.to_dict()
            serialized = json.dumps(progress_copy)
            if self.use_redis and self.redis_client:
                key = f"progress:{self.task_id}"
                self.redis_client.set(key, serialized)
                self.redis_client.expire(key, 3600)
            else:
                os.makedirs("./data/progress", exist_ok=True)
                with open(f"./data/progress/{self.task_id}.json", 'w', encoding='utf-8') as f:
                    f.write(serialized)
        except Exception as e:
            logger.error(f"[RedisProgress] save progress failed: {self.task_id} - {e}")

    def mark_completed(self) -> Dict[str, Any]:
        try:
            self.progress_data['progress_percentage'] = 100
            self.progress_data['status'] = 'completed'
            self.progress_data['completed'] = True
            self.progress_data['completed_time'] = time.time()
            for step in self.analysis_steps:
                if step.status != 'failed':
                    step.status = 'completed'
                    step.end_time = step.end_time or time.time()
            self._save_progress()
            return self.progress_data
        except Exception as e:
            logger.error(f"[RedisProgress] mark completed failed: {self.task_id} - {e}")
            return self.progress_data

    def mark_failed(self, reason: str = "") -> Dict[str, Any]:
        try:
            self.progress_data['status'] = 'failed'
            self.progress_data['failed'] = True
            self.progress_data['failed_reason'] = reason
            self.progress_data['completed_time'] = time.time()
            for step in self.analysis_steps:
                if step.status not in ('completed', 'failed'):
                    step.status = 'failed'
                    step.end_time = step.end_time or time.time()
            self._save_progress()
            return self.progress_data
        except Exception as e:
            logger.error(f"[RedisProgress] mark failed failed: {self.task_id} - {e}")
            return self.progress_data

    def to_dict(self) -> Dict[str, Any]:
        try:
            return {
                'task_id': self.task_id,
                'analysts': self.analysts,
                'research_depth': self.research_depth,
                'llm_provider': self.llm_provider,
                'steps': [asdict(step) for step in self.analysis_steps],
                'start_time': self.progress_data.get('start_time'),
                'elapsed_time': self.progress_data.get('elapsed_time', 0),
                'remaining_time': self.progress_data.get('remaining_time', 0),
                'estimated_total_time': self.progress_data.get('estimated_total_time', 0),
                'progress_percentage': self.progress_data.get('progress_percentage', 0),
                'status': self.progress_data.get('status', 'pending'),
                'current_step': self.progress_data.get('current_step')
            }
        except Exception as e:
            logger.error(f"[RedisProgress] to_dict failed: {self.task_id} - {e}")
            return self.progress_data





def get_progress_by_id(task_id: str) -> Optional[Dict[str, Any]]:
    """Get progress according to task ID (conform with old, amend cls references)"""
    try:
        #Check REDIS ENABLED environment variables
        redis_enabled = os.getenv('REDIS_ENABLED', 'false').lower() == 'true'

        #If Redis is enabled, try Redis first.
        if redis_enabled:
            try:
                import redis

                #Redis configuration from environment variable
                redis_host = os.getenv('REDIS_HOST', 'localhost')
                redis_port = int(os.getenv('REDIS_PORT', 6379))
                redis_password = os.getenv('REDIS_PASSWORD', None)
                redis_db = int(os.getenv('REDIS_DB', 0))

                #Create Redis Connection
                if redis_password:
                    redis_client = redis.Redis(
                        host=redis_host,
                        port=redis_port,
                        password=redis_password,
                        db=redis_db,
                        decode_responses=True
                    )
                else:
                    redis_client = redis.Redis(
                        host=redis_host,
                        port=redis_port,
                        db=redis_db,
                        decode_responses=True
                    )

                key = f"progress:{task_id}"
                data = redis_client.get(key)
                if data:
                    progress_data = json.loads(data)
                    progress_data = RedisProgressTracker._calculate_static_time_estimates(progress_data)
                    return progress_data
            except Exception as e:
                logger.debug(f"[Redis Progressing]{e}")

        #Try reading from files
        progress_file = f"./data/progress/{task_id}.json"
        if os.path.exists(progress_file):
            with open(progress_file, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)
                progress_data = RedisProgressTracker._calculate_static_time_estimates(progress_data)
                return progress_data

        #Try backup file location
        backup_file = f"./data/progress_{task_id}.json"
        if os.path.exists(backup_file):
            with open(backup_file, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)
                progress_data = RedisProgressTracker._calculate_static_time_estimates(progress_data)
                return progress_data

        return None

    except Exception as e:
        logger.error(f"[Redis Progressing]{task_id} - {e}")
        return None
