"""Progress Log Processor
Monitor the log output of TradingAgents and automatically update progress tracking Device
"""

import logging
import re
import threading
from typing import Dict, Optional
from .tracker import RedisProgressTracker

logger = logging.getLogger("app.services.progress_log_handler")


class ProgressLogHandler(logging.Handler):
    """Progress log processor, monitor TradingAgendas logs and update progress"""

    def __init__(self):
        super().__init__()
        self._trackers: Dict[str, RedisProgressTracker] = {}
        self._lock = threading.Lock()

        #Log Mode Match
        self.progress_patterns = {
            #Basic phase
            r"验证.*股票代码|检查.*数据源": "📋 准备阶段",
            r"检查.*API.*密钥|环境.*配置": "🔧 环境检查",
            r"预估.*成本|成本.*估算": "💰 成本估算",
            r"配置.*参数|参数.*设置": "⚙️ 参数设置",
            r"初始化.*引擎|启动.*引擎": "🚀 启动引擎",

            #Analyst phase
            r"市场分析师.*开始|开始.*市场分析|市场.*数据.*分析": "📊 市场分析师正在分析",
            r"基本面分析师.*开始|开始.*基本面分析|财务.*数据.*分析": "💼 基本面分析师正在分析",
            r"新闻分析师.*开始|开始.*新闻分析|新闻.*数据.*分析": "📰 新闻分析师正在分析",
            r"社交媒体分析师.*开始|开始.*社交媒体分析|情绪.*分析": "💬 社交媒体分析师正在分析",

            #Research team phase
            r"看涨研究员|多头研究员|bull.*researcher": "🐂 看涨研究员构建论据",
            r"看跌研究员|空头研究员|bear.*researcher": "🐻 看跌研究员识别风险",
            r"研究.*辩论|辩论.*开始|debate.*start": "🎯 研究辩论进行中",
            r"研究经理|research.*manager": "👔 研究经理形成共识",

            #Trading team phase
            r"交易员.*决策|trader.*decision|制定.*交易策略": "💼 交易员制定策略",

            #Risk management phase
            r"激进.*风险|risky.*risk": "🔥 激进风险评估",
            r"保守.*风险|conservative.*risk": "🛡️ 保守风险评估",
            r"中性.*风险|neutral.*risk": "⚖️ 中性风险评估",
            r"风险经理|risk.*manager": "🎯 风险经理制定策略",

            #Final phase
            r"信号处理|signal.*process": "📡 信号处理",
            r"生成.*报告|report.*generat": "📊 生成报告",
            r"分析.*完成|analysis.*complet": "✅ 分析完成",
        }

        logger.info("📊 [Progress log] Log processor initialised")

    def register_tracker(self, task_id: str, tracker: RedisProgressTracker):
        """Register progress tracker"""
        with self._lock:
            self._trackers[task_id] = tracker
            logger.info(f"[Progress log]{task_id}")

    def unregister_tracker(self, task_id: str):
        """Write-off progress tracker"""
        with self._lock:
            if task_id in self._trackers:
                del self._trackers[task_id]
                logger.info(f"[Progress log] Write-off trackers:{task_id}")

    def emit(self, record):
        """Processing log records"""
        try:
            message = record.getMessage()

            #Check to see if it's our concern.
            progress_message = self._extract_progress_message(message)
            if not progress_message:
                return

            #Find matching trackers (reduce lock holding time)
            trackers_copy = {}
            with self._lock:
                trackers_copy = self._trackers.copy()

            #Processing tracker updates outside the lock
            for task_id, tracker in trackers_copy.items():
                try:
                    #Check the tracker status
                    if hasattr(tracker, 'progress_data') and tracker.progress_data.get('status') == 'running':
                        tracker.update_progress(progress_message)
                        logger.debug(f"[Progress log] Update progress:{task_id} -> {progress_message}")
                        break  #Only update the first match tracker Device
                except Exception as e:
                    logger.warning(f"Update failed:{task_id} - {e}")

        except Exception as e:
            #Do not let journal processor errors affect the master program
            logger.error(f"[Progress log] Log processing error:{e}")

    def _extract_progress_message(self, message: str) -> Optional[str]:
        """Can not open message"""
        message_lower = message.lower()

        #Check to include progress-related keywords
        progress_keywords = [
            "开始", "完成", "分析", "处理", "执行", "生成",
            "start", "complete", "analysis", "process", "execute", "generate"
        ]

        if not any(keyword in message_lower for keyword in progress_keywords):
            return None

        #Match specific progress patterns
        for pattern, progress_msg in self.progress_patterns.items():
            if re.search(pattern, message_lower):
                return progress_msg

        return None

    def _extract_stock_symbol(self, message: str) -> Optional[str]:
        """Extract stock code from message"""
        #Match common stock code formats
        patterns = [
            r'\b(\d{6})\b',  #6-digit (Unit A)
            r'\b([A-Z]{1,5})\b',  #1-5 capital letters (United States share)
            r'\b(\d{4,5}\.HK)\b',  #Port Unit Format
        ]

        for pattern in patterns:
            match = re.search(pattern, message)
            if match:
                return match.group(1)

        return None


#Global log processor instance
_progress_log_handler = None
_handler_lock = threading.Lock()


def get_progress_log_handler() -> ProgressLogHandler:
    """Get a global progress log processor instance"""
    global _progress_log_handler

    with _handler_lock:
        if _progress_log_handler is None:
            _progress_log_handler = ProgressLogHandler()

            #Add processor to relevant log recorder
            loggers_to_monitor = [
                "agents",
                "tradingagents",
                "agents.analysts",
                "agents.researchers",
                "agents.traders",
                "agents.managers",
                "agents.risk_mgmt",
            ]

            for logger_name in loggers_to_monitor:
                target_logger = logging.getLogger(logger_name)
                target_logger.addHandler(_progress_log_handler)
                target_logger.setLevel(logging.INFO)

            logger.info(f"[Progress log]{len(loggers_to_monitor)}Log recorder")

    return _progress_log_handler


def register_analysis_tracker(task_id: str, tracker: RedisProgressTracker):
    """Register Analytical Tracker to Log Monitor"""
    handler = get_progress_log_handler()
    handler.register_tracker(task_id, tracker)


def unregister_analysis_tracker(task_id: str):
    """Write-off analysis tracking from log monitoring Device"""
    handler = get_progress_log_handler()
    handler.unregister_tracker(task_id)

