#!/usr/bin/env python3
"""Log system initialization module
Initialize the Unified Log system on application startup
"""

import os
import sys
from pathlib import Path
from typing import Optional

#Add root directory to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from tradingagents.utils.logging_manager import setup_logging, get_logger


def init_logging(config_override: Optional[dict] = None) -> None:
    """Initialise Project Log System

Args:
config override: optional configuration overlay
"""
    #Setup Log System
    logger_manager = setup_logging(config_override)
    
    #Get Initializing Logs
    logger = get_logger('tradingagents.init')
    
    #Record initialised information
    logger.info("Initialization of TradingAgents-CN log system completed")
    logger.info(f"Log catalogue:{logger_manager.config.get('handlers', {}).get('file', {}).get('directory', 'N/A')}")
    logger.info(f"Log level:{logger_manager.config.get('level', 'INFO')}")
    
    #Docker Environment Special Treatment
    if logger_manager.config.get('docker', {}).get('enabled', False):
        logger.info("Docker environment detected using optimal packaging configuration")
    
    #Recording environmental information
    logger.debug(f"Python version:{sys.version}")
    logger.debug(f"Working directory:{os.getcwd()}")
    logger.debug(f"Environmental variable: DOCKER CONTAINER={os.getenv('DOCKER_CONTAINER', 'false')}")


def get_session_logger(session_id: str, module_name: str = 'session') -> 'logging.Logger':
    """Get Session-specific logs

Args:
session id: sessionID
Modeule name: module name

Returns:
Configure Logs
"""
    logger_name = f"{module_name}.{session_id[:8]}"  #Use first eight session ID
    
    #Add Session ID to all log records
    class SessionAdapter:
        def __init__(self, logger, session_id):
            self.logger = logger
            self.session_id = session_id
        
        def debug(self, msg, *args, **kwargs):
            kwargs.setdefault('extra', {})['session_id'] = self.session_id
            return self.logger.debug(msg, *args, **kwargs)
        
        def info(self, msg, *args, **kwargs):
            kwargs.setdefault('extra', {})['session_id'] = self.session_id
            return self.logger.info(msg, *args, **kwargs)
        
        def warning(self, msg, *args, **kwargs):
            kwargs.setdefault('extra', {})['session_id'] = self.session_id
            return self.logger.warning(msg, *args, **kwargs)
        
        def error(self, msg, *args, **kwargs):
            kwargs.setdefault('extra', {})['session_id'] = self.session_id
            return self.logger.error(msg, *args, **kwargs)
        
        def critical(self, msg, *args, **kwargs):
            kwargs.setdefault('extra', {})['session_id'] = self.session_id
            return self.logger.critical(msg, *args, **kwargs)
    
    return SessionAdapter(logger, session_id)


def log_startup_info():
    """Recording startup information for application"""
    logger = get_logger('tradingagents.startup')
    
    logger.info("=" * 60)
    logger.info("TradingAgendas-CN")
    logger.info("=" * 60)
    
    #System Information
    import platform
    logger.info(f"System:{platform.system()} {platform.release()}")
    logger.info(f"🐍 Python: {platform.python_version()}")
    
    #Environmental information
    env_info = {
        'DOCKER_CONTAINER': os.getenv('DOCKER_CONTAINER', 'false'),
        'TRADINGAGENTS_LOG_LEVEL': os.getenv('TRADINGAGENTS_LOG_LEVEL', 'INFO'),
        'TRADINGAGENTS_LOG_DIR': os.getenv('TRADINGAGENTS_LOG_DIR', './logs'),
    }
    
    for key, value in env_info.items():
        logger.info(f"🔧 {key}: {value}")
    
    logger.info("=" * 60)


def log_shutdown_info():
    """Record closed information for application"""
    logger = get_logger('tradingagents.shutdown')
    
    logger.info("=" * 60)
    logger.info("TradingAgents-CN off")
    logger.info("=" * 60)


#Easy Functions
def setup_web_logging():
    """Set up Web application dedicated log"""
    init_logging()
    log_startup_info()
    return get_logger('web')


def setup_analysis_logging(session_id: str):
    """Set a dedicated analytical log"""
    return get_session_logger(session_id, 'analysis')


def setup_dataflow_logging():
    """Set data stream dedicated log"""
    return get_logger('dataflows')


def setup_llm_logging():
    """Set up a special log for LLM adapter"""
    return get_logger('llm_adapters')


if __name__ == "__main__":
    #Test Log System
    init_logging()
    log_startup_info()
    
    #Test logs for different modules
    web_logger = setup_web_logging()
    web_logger.info("Web module log test")
    
    analysis_logger = setup_analysis_logging("test-session-123")
    analysis_logger.info("Analyzing module log tests")
    
    dataflow_logger = setup_dataflow_logging()
    dataflow_logger.info("Data stream module log testing")
    
    llm_logger = setup_llm_logging()
    llm_logger.info("LLM adapter module log testing")
    
    log_shutdown_info()
