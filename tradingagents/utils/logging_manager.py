#!/usr/bin/env python3
"""Unified Log Manager
Provide project-level log configuration and management functions
"""

import logging
import logging.handlers
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union
import json
import inspect
import toml

#Note: You can't import yourself here. This can cause circular import.
#Use a standard library self-starter to avoid undefined references before the log system is initiated
_bootstrap_logger = logging.getLogger("tradingagents.logging_manager")

_CLASSNAME_FACTORY_INSTALLED = False
_ORIGINAL_RECORD_FACTORY = logging.getLogRecordFactory()


def _find_classname_from_stack() -> str:
    frame = inspect.currentframe()
    try:
        while frame:
            module_name = frame.f_globals.get("__name__", "")
            if module_name.startswith("logging"):
                frame = frame.f_back
                continue
            if "self" in frame.f_locals:
                return frame.f_locals["self"].__class__.__name__
            if "cls" in frame.f_locals and isinstance(frame.f_locals["cls"], type):
                return frame.f_locals["cls"].__name__
            frame = frame.f_back
    finally:
        del frame
    return "-"


def _install_classname_record_factory() -> None:
    global _CLASSNAME_FACTORY_INSTALLED
    if _CLASSNAME_FACTORY_INSTALLED:
        return

    def record_factory(*args, **kwargs):
        record = _ORIGINAL_RECORD_FACTORY(*args, **kwargs)
        if not hasattr(record, "classname"):
            record.classname = _find_classname_from_stack()
        return record

    logging.setLogRecordFactory(record_factory)
    _CLASSNAME_FACTORY_INSTALLED = True


class ColoredFormatter(logging.Formatter):
    """Colour log formatter"""
    
    #ANSI colour code
    COLORS = {
        'DEBUG': '\033[36m',    #Cyan
        'INFO': '\033[32m',     #Green
        'WARNING': '\033[33m',  #Yellow
        'ERROR': '\033[31m',    #Red
        'CRITICAL': '\033[35m', #Purple
        'RESET': '\033[0m'      #Reset
    }
    
    def format(self, record):
        #Add Colour
        if hasattr(record, 'levelname') and record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{self.COLORS['RESET']}"
        
        return super().format(record)


class StructuredFormatter(logging.Formatter):
    """Structured log formatter (JSON format)"""
    
    def format(self, record):
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'classname': getattr(record, 'classname', '-')
        }
        
        #Add Extra Fields
        if hasattr(record, 'session_id'):
            log_entry['session_id'] = record.session_id
        if hasattr(record, 'analysis_type'):
            log_entry['analysis_type'] = record.analysis_type
        if hasattr(record, 'stock_symbol'):
            log_entry['stock_symbol'] = record.stock_symbol
        if hasattr(record, 'cost'):
            log_entry['cost'] = record.cost
        if hasattr(record, 'tokens'):
            log_entry['tokens'] = record.tokens
            
        return json.dumps(log_entry, ensure_ascii=False)


class TradingAgentsLogger:
    """TradingAgents Unified Log Manager"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or self._load_default_config()
        self.loggers: Dict[str, logging.Logger] = {}
        self._setup_logging()
    
    def _load_default_config(self) -> Dict[str, Any]:
        """Load the default log configuration"""
        #Try loading from profile
        config = self._load_config_file()
        if config:
            return config

        #Get configurations from environmental variables
        log_level = os.getenv('TRADINGAGENTS_LOG_LEVEL', 'INFO').upper()
        log_dir = os.getenv('TRADINGAGENTS_LOG_DIR', './logs')

        return {
            'level': log_level,
            'format': {
                'console': '%(asctime)s | %(name)-20s | %(levelname)-8s | %(classname)-20s | %(message)s',
                'file': '%(asctime)s | %(name)-20s | %(levelname)-8s | %(classname)-20s | %(module)s:%(funcName)s:%(lineno)d | %(message)s',
                'structured': 'json'
            },
            'handlers': {
                'console': {
                    'enabled': True,
                    'colored': True,
                    'level': log_level
                },
                'file': {
                    'enabled': True,
                    'level': 'DEBUG',
                    'max_size': '10MB',
                    'backup_count': 5,
                    'directory': log_dir
                },
                'error': {
                    'enabled': True,
                    'level': 'WARNING',  #Only WarNING and above
                    'max_size': '10MB',
                    'backup_count': 5,
                    'directory': log_dir,
                    'filename': 'error.log'
                },
                'structured': {
                    'enabled': False,  #By default close, enabled by environment variables
                    'level': 'INFO',
                    'directory': log_dir
                }
            },
            'loggers': {
                'tradingagents': {'level': log_level},
                'web': {'level': log_level},
                'streamlit': {'level': 'WARNING'},  #Streamlit has a lot of logs set as WARNING
                'urllib3': {'level': 'WARNING'},    #HTTP requests more logs
                'requests': {'level': 'WARNING'},
                'matplotlib': {'level': 'WARNING'}
            },
            'docker': {
                'enabled': os.getenv('DOCKER_CONTAINER', 'false').lower() == 'true',
                'stdout_only': True  #Docker environment only output to stdout
            }
        }

    def _load_config_file(self) -> Optional[Dict[str, Any]]:
        """Load log configuration from profile"""
        #Set profile path
        config_paths = [
            'config/logging_docker.toml' if os.getenv('DOCKER_CONTAINER') == 'true' else None,
            'config/logging.toml',
            './logging.toml'
        ]

        for config_path in config_paths:
            if config_path and Path(config_path).exists():
                try:
                    with open(config_path, 'r', encoding='utf-8') as f:
                        config_data = toml.load(f)

                    #Convert configuration format
                    return self._convert_toml_config(config_data)
                except Exception as e:
                    _bootstrap_logger.warning(f"Warning: Cannot load profile{config_path}: {e}")
                    continue

        return None

    def _convert_toml_config(self, toml_config: Dict[str, Any]) -> Dict[str, Any]:
        """Convert TOML configuration to internal configuration format"""
        logging_config = toml_config.get('logging', {})

        #Check Docker environment.
        is_docker = (
            os.getenv('DOCKER_CONTAINER') == 'true' or
            logging_config.get('docker', {}).get('enabled', False)
        )

        return {
            'level': logging_config.get('level', 'INFO'),
            'format': logging_config.get('format', {}),
            'handlers': logging_config.get('handlers', {}),
            'loggers': logging_config.get('loggers', {}),
            'docker': {
                'enabled': is_docker,
                'stdout_only': logging_config.get('docker', {}).get('stdout_only', True)
            },
            'performance': logging_config.get('performance', {}),
            'security': logging_config.get('security', {}),
            'business': logging_config.get('business', {})
        }
    
    def _setup_logging(self):
        """Setup Log System"""
        _install_classname_record_factory()
        #Create Log Directory
        if self.config['handlers']['file']['enabled']:
            log_dir = Path(self.config['handlers']['file']['directory'])
            log_dir.mkdir(parents=True, exist_ok=True)
        
        #Set root log level
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, self.config['level']))
        
        #Clear existing processor
        root_logger.handlers.clear()
        
        #Add Processor
        self._add_console_handler(root_logger)

        if not self.config['docker']['enabled'] or not self.config['docker']['stdout_only']:
            self._add_file_handler(root_logger)
            self._add_error_handler(root_logger)  #Add Error Log Processor
            if self.config['handlers']['structured']['enabled']:
                self._add_structured_handler(root_logger)
        
        #Configure Specific Logs
        self._configure_specific_loggers()
    
    def _add_console_handler(self, logger: logging.Logger):
        """Add Console Processor"""
        if not self.config['handlers']['console']['enabled']:
            return
            
        console_handler = logging.StreamHandler(sys.stdout)
        console_level = getattr(logging, self.config['handlers']['console']['level'])
        console_handler.setLevel(console_level)
        
        #Select Formatter
        if self.config['handlers']['console']['colored'] and sys.stdout.isatty():
            formatter = ColoredFormatter(self.config['format']['console'])
        else:
            formatter = logging.Formatter(self.config['format']['console'])
        
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    def _add_file_handler(self, logger: logging.Logger):
        """Add File Processor"""
        if not self.config['handlers']['file']['enabled']:
            return

        log_dir = Path(self.config['handlers']['file']['directory'])
        log_file = log_dir / 'tradingagents.log'

        #Rotating Filehandler
        max_size = self._parse_size(self.config['handlers']['file']['max_size'])
        backup_count = self.config['handlers']['file']['backup_count']

        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_size,
            backupCount=backup_count,
            encoding='utf-8'
        )

        file_level = getattr(logging, self.config['handlers']['file']['level'])
        file_handler.setLevel(file_level)

        formatter = logging.Formatter(self.config['format']['file'])
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    def _add_error_handler(self, logger: logging.Logger):
        """Add error log processor (Warning and above only)"""
        #Check if the bug processor is enabled
        error_config = self.config['handlers'].get('error', {})
        if not error_config.get('enabled', True):
            return

        log_dir = Path(error_config.get('directory', self.config['handlers']['file']['directory']))
        error_log_file = log_dir / error_config.get('filename', 'error.log')

        #Rotating Filehandler
        max_size = self._parse_size(error_config.get('max_size', '10MB'))
        backup_count = error_config.get('backup_count', 5)

        error_handler = logging.handlers.RotatingFileHandler(
            error_log_file,
            maxBytes=max_size,
            backupCount=backup_count,
            encoding='utf-8'
        )

        #Only Warning and above (WARNING, ERRO, CRITICAL)
        error_level = getattr(logging, error_config.get('level', 'WARNING'))
        error_handler.setLevel(error_level)

        formatter = logging.Formatter(self.config['format']['file'])
        error_handler.setFormatter(formatter)
        logger.addHandler(error_handler)
    
    def _add_structured_handler(self, logger: logging.Logger):
        """Add Structured Log Processor"""
        log_dir = Path(self.config['handlers']['structured']['directory'])
        log_file = log_dir / 'tradingagents_structured.log'
        
        structured_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=self._parse_size('10MB'),
            backupCount=3,
            encoding='utf-8'
        )
        
        structured_level = getattr(logging, self.config['handlers']['structured']['level'])
        structured_handler.setLevel(structured_level)
        
        formatter = StructuredFormatter()
        structured_handler.setFormatter(formatter)
        logger.addHandler(structured_handler)
    
    def _configure_specific_loggers(self):
        """Configure specific logs"""
        for logger_name, logger_config in self.config['loggers'].items():
            logger = logging.getLogger(logger_name)
            level = getattr(logging, logger_config['level'])
            logger.setLevel(level)
    
    def _parse_size(self, size_str: str) -> int:
        """Parsing size strings (e.g. '10MB') as bytes"""
        size_str = size_str.upper()
        if size_str.endswith('KB'):
            return int(size_str[:-2]) * 1024
        elif size_str.endswith('MB'):
            return int(size_str[:-2]) * 1024 * 1024
        elif size_str.endswith('GB'):
            return int(size_str[:-2]) * 1024 * 1024 * 1024
        else:
            return int(size_str)
    
    def get_logger(self, name: str) -> logging.Logger:
        """Gets a log folder with the given name"""
        if name not in self.loggers:
            self.loggers[name] = logging.getLogger(name)
        return self.loggers[name]
    
    def log_analysis_start(self, logger: logging.Logger, stock_symbol: str, analysis_type: str, session_id: str):
        """Record analysis started."""
        logger.info(
            f"We'll start the analysis.{stock_symbol}type:{analysis_type}",
            extra={
                'stock_symbol': stock_symbol,
                'analysis_type': analysis_type,
                'session_id': session_id,
                'event_type': 'analysis_start',
                'timestamp': datetime.now().isoformat()
            }
        )

    def log_analysis_complete(self, logger: logging.Logger, stock_symbol: str, analysis_type: str,
                            session_id: str, duration: float, cost: float = 0):
        """Records analysis complete."""
        logger.info(
            f"Analysis completed - stocks:{stock_symbol}, time consuming:{duration:.2f}s, cost:{cost:.4f}",
            extra={
                'stock_symbol': stock_symbol,
                'analysis_type': analysis_type,
                'session_id': session_id,
                'duration': duration,
                'cost': cost,
                'event_type': 'analysis_complete',
                'timestamp': datetime.now().isoformat()
            }
        )

    def log_module_start(self, logger: logging.Logger, module_name: str, stock_symbol: str,
                        session_id: str, **extra_data):
        """Records module starts analysis"""
        logger.info(
            f"[module starts]{module_name}- Stock:{stock_symbol}",
            extra={
                'module_name': module_name,
                'stock_symbol': stock_symbol,
                'session_id': session_id,
                'event_type': 'module_start',
                'timestamp': datetime.now().isoformat(),
                **extra_data
            }
        )

    def log_module_complete(self, logger: logging.Logger, module_name: str, stock_symbol: str,
                           session_id: str, duration: float, success: bool = True,
                           result_length: int = 0, **extra_data):
        """Document module completed analysis"""
        status = "✅ 成功" if success else "❌ 失败"
        logger.info(
            f"[module finished]{module_name} - {status}- Stock:{stock_symbol}, time consuming:{duration:.2f}s",
            extra={
                'module_name': module_name,
                'stock_symbol': stock_symbol,
                'session_id': session_id,
                'duration': duration,
                'success': success,
                'result_length': result_length,
                'event_type': 'module_complete',
                'timestamp': datetime.now().isoformat(),
                **extra_data
            }
        )

    def log_module_error(self, logger: logging.Logger, module_name: str, stock_symbol: str,
                        session_id: str, duration: float, error: str, **extra_data):
        """Record module analysis error"""
        logger.error(
            f"[modular error]{module_name}- Stock:{stock_symbol}, time consuming:{duration:.2f}s, error:{error}",
            extra={
                'module_name': module_name,
                'stock_symbol': stock_symbol,
                'session_id': session_id,
                'duration': duration,
                'error': error,
                'event_type': 'module_error',
                'timestamp': datetime.now().isoformat(),
                **extra_data
            },
            exc_info=True
        )
    
    def log_token_usage(self, logger: logging.Logger, provider: str, model: str, 
                       input_tokens: int, output_tokens: int, cost: float, session_id: str):
        """Record Token's use"""
        logger.info(
            f"Token uses--{provider}/{model}: Input ={input_tokens},out ={output_tokens}, Cost ={cost:.6f}",
            extra={
                'provider': provider,
                'model': model,
                'tokens': {'input': input_tokens, 'output': output_tokens},
                'cost': cost,
                'session_id': session_id,
                'event_type': 'token_usage'
            }
        )


#Examples of global log manager
_logger_manager: Optional[TradingAgentsLogger] = None


def get_logger_manager() -> TradingAgentsLogger:
    """Fetch global log manager instance"""
    global _logger_manager
    if _logger_manager is None:
        _logger_manager = TradingAgentsLogger()
    return _logger_manager


def get_logger(name: str) -> logging.Logger:
    """Retrieving a journal with a given name (fast function)"""
    return get_logger_manager().get_logger(name)


def setup_logging(config: Optional[Dict[str, Any]] = None):
    """Set the project log system (fast function)"""
    global _logger_manager
    _logger_manager = TradingAgentsLogger(config)
    return _logger_manager
