#!/usr/bin/env python3
"""Tool Call Log Decorator
Add a unified log record for all tool calls
"""

import time
import functools
from typing import Any, Dict, Optional, Callable
from datetime import datetime
from zoneinfo import ZoneInfo
from tradingagents.config.runtime_settings import get_timezone_name


from tradingagents.utils.logging_init import get_logger

#Import Log Module
from tradingagents.utils.logging_manager import get_logger, get_logger_manager
logger = get_logger('agents')

#Tools to call logs
tool_logger = get_logger("tools")


def log_tool_call(tool_name: Optional[str] = None, log_args: bool = True, log_result: bool = False):
    """Tool Call Log Decorator

Args:
tool name: Tool name, use function name if not available
log args: Whether to record parameters
log result: record return results (note: may contain large amounts of data)
"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            #Determine Tool Name
            name = tool_name or getattr(func, '__name__', 'unknown_tool')

            #Record start time
            start_time = time.time()

            #Prepare Parameter Information
            args_info = {}
            if log_args:
                #Record location parameters
                if args:
                    args_info['args'] = [str(arg)[:100] + '...' if len(str(arg)) > 100 else str(arg) for arg in args]

                #Record keyword parameters
                if kwargs:
                    args_info['kwargs'] = {
                        k: str(v)[:100] + '...' if len(str(v)) > 100 else str(v)
                        for k, v in kwargs.items()
                    }

            #Record tool call start
            tool_logger.info(
                f"[tool calls]{name}- Let's go.",
                extra={
                    'tool_name': name,
                    'event_type': 'tool_call_start',
                    'timestamp': datetime.now(ZoneInfo(get_timezone_name())).isoformat(),
                    'args_info': args_info if log_args else None
                }
            )

            try:
                #Execute Tool Functions
                result = func(*args, **kwargs)

                #Calculate implementation time
                duration = time.time() - start_time

                #Prepare result information
                result_info = None
                if log_result and result is not None:
                    result_str = str(result)
                    result_info = result_str[:200] + '...' if len(result_str) > 200 else result_str

                #Record tool call successful
                tool_logger.info(
                    f"[tool calls]{name}- Done.{duration:.2f}s)",
                    extra={
                        'tool_name': name,
                        'event_type': 'tool_call_success',
                        'duration': duration,
                        'result_info': result_info if log_result else None,
                        'timestamp': datetime.now(ZoneInfo(get_timezone_name())).isoformat()
                    }
                )

                return result

            except Exception as e:
                #Calculate implementation time
                duration = time.time() - start_time

                #Record tool call failed
                tool_logger.error(
                    f"[tool calls]{name}- Failed.{duration:.2f}s): {str(e)}",
                    extra={
                        'tool_name': name,
                        'event_type': 'tool_call_error',
                        'duration': duration,
                        'error': str(e),
                        'timestamp': datetime.now(ZoneInfo(get_timezone_name())).isoformat()
                    },
                    exc_info=True
                )

                #Releasing anomaly.
                raise

        return wrapper
    return decorator


def log_data_source_call(source_name: str):
    """Data source call dedicated log decorator

Args:
Source name (e.g. tushare, kshare, yfinance)
"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()

            #Stock extraction codes (usually the first parameter)
            symbol = args[0] if args else kwargs.get('symbol', kwargs.get('ticker', 'unknown'))

            #Record data source call start
            tool_logger.info(
                f"[Data source]{source_name}- Get it.{symbol}Data",
                extra={
                    'data_source': source_name,
                    'symbol': symbol,
                    'event_type': 'data_source_call',
                    'timestamp': datetime.now(ZoneInfo(get_timezone_name())).isoformat()
                }
            )

            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time

                #Check for success
                success = result and "❌" not in str(result) and "错误" not in str(result)

                if success:
                    tool_logger.info(
                        f"[Data source]{source_name} - {symbol}Data acquisition success (time-consuming:{duration:.2f}s)",
                        extra={
                            'data_source': source_name,
                            'symbol': symbol,
                            'event_type': 'data_source_success',
                            'duration': duration,
                            'data_size': len(str(result)) if result else 0,
                            'timestamp': datetime.now(ZoneInfo(get_timezone_name())).isoformat()
                        }
                    )
                else:
                    tool_logger.warning(
                        f"[Data source]{source_name} - {symbol}Data acquisition failed (time:{duration:.2f}s)",
                        extra={
                            'data_source': source_name,
                            'symbol': symbol,
                            'event_type': 'data_source_failure',
                            'duration': duration,
                            'timestamp': datetime.now(ZoneInfo(get_timezone_name())).isoformat()
                        }
                    )

                return result

            except Exception as e:
                duration = time.time() - start_time

                tool_logger.error(
                    f"[Data source]{source_name} - {symbol}Data acquisition anomaly (time-consuming:{duration:.2f}s): {str(e)}",
                    extra={
                        'data_source': source_name,
                        'symbol': symbol,
                        'event_type': 'data_source_error',
                        'duration': duration,
                        'error': str(e),
                        'timestamp': datetime.now(ZoneInfo(get_timezone_name())).isoformat()
                    },
                    exc_info=True
                )

                raise

        return wrapper
    return decorator


def log_llm_call(provider: str, model: str):
    """LLM calls a dedicated log decorationer

Args:
Provider: LLM providers (e.g. openai, Deepseek, toongyi, etc.)
Model name
"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()

            #Record LLM call start
            tool_logger.info(
                f"[LLM calling]{provider}/{model}- Let's go.",
                extra={
                    'llm_provider': provider,
                    'llm_model': model,
                    'event_type': 'llm_call_start',
                    'timestamp': datetime.now(ZoneInfo(get_timezone_name())).isoformat()
                }
            )

            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time

                tool_logger.info(
                    f"[LLM calling]{provider}/{model}- Done.{duration:.2f}s)",
                    extra={
                        'llm_provider': provider,
                        'llm_model': model,
                        'event_type': 'llm_call_success',
                        'duration': duration,
                        'timestamp': datetime.now(ZoneInfo(get_timezone_name())).isoformat()
                    }
                )

                return result

            except Exception as e:
                duration = time.time() - start_time

                tool_logger.error(
                    f"[LLM calling]{provider}/{model}- Failed.{duration:.2f}s): {str(e)}",
                    extra={
                        'llm_provider': provider,
                        'llm_model': model,
                        'event_type': 'llm_call_error',
                        'duration': duration,
                        'error': str(e),
                        'timestamp': datetime.now(ZoneInfo(get_timezone_name())).isoformat()
                    },
                    exc_info=True
                )

                raise

        return wrapper
    return decorator


#Easy Functions
def log_tool_usage(tool_name: str, symbol: str = None, **extra_data):
    """Easy function to record tool usage

Args:
tool name: Tool name
symbol: stock code (optional)
**extra data: extra data
"""
    extra = {
        'tool_name': tool_name,
        'event_type': 'tool_usage',
        'timestamp': datetime.now(ZoneInfo(get_timezone_name())).isoformat(),
        **extra_data
    }

    if symbol:
        extra['symbol'] = symbol

    tool_logger.info(f"[Tool use]{tool_name}", extra=extra)


def log_analysis_step(step_name: str, symbol: str, **extra_data):
    """A simple function to record the analysis steps

Args:
step name: step name
symbol: stock code
**extra data: extra data
"""
    extra = {
        'step_name': step_name,
        'symbol': symbol,
        'event_type': 'analysis_step',
        'timestamp': datetime.now(ZoneInfo(get_timezone_name())).isoformat(),
        **extra_data
    }

    tool_logger.info(f"[analytical step]{step_name} - {symbol}", extra=extra)


def log_analysis_module(module_name: str, session_id: str = None):
    """Analyzing module log decorations
Autorecord the start and end of the module

Args:
Modeule name: module name (e. g. market analyst, fundamentals analyst, etc.)
session id: Session ID (optional)
"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            #Try extracting stock codes from parameters
            symbol = None

            #Special handling: parameter structure of signal processing module
            if module_name == "graph_signal_processing":
                #Signal processing module: protocol signal (self, full signal, stock symbol=None)
                if len(args) >= 3:  # self, full_signal, stock_symbol
                    symbol = str(args[2]) if args[2] else None
                elif 'stock_symbol' in kwargs:
                    symbol = str(kwargs['stock_symbol']) if kwargs['stock_symbol'] else None
            else:
                if args:
                    #Check if the first parameter is the state dictionary (case of analyst nodes)
                    first_arg = args[0]
                    if isinstance(first_arg, dict) and 'company_of_interest' in first_arg:
                        symbol = str(first_arg['company_of_interest'])
                    #Check if the first parameter is stock code
                    elif isinstance(first_arg, str) and len(first_arg) <= 10:
                        symbol = first_arg

            #Find stock codes from kwargs
            if not symbol:
                for key in ['symbol', 'ticker', 'stock_code', 'stock_symbol', 'company_of_interest']:
                    if key in kwargs:
                        symbol = str(kwargs[key])
                        break

            #If not found, use default
            if not symbol:
                symbol = 'unknown'

            #Generate Session ID
            actual_session_id = session_id or f"session_{int(time.time())}"

            #Record module start
            logger_manager = get_logger_manager()

            start_time = time.time()

            logger_manager.log_module_start(
                tool_logger, module_name, symbol, actual_session_id,
                function_name=func.__name__,
                args_count=len(args),
                kwargs_keys=list(kwargs.keys())
            )

            try:
                #Execute Analytical Functions
                result = func(*args, **kwargs)

                #Calculate implementation time
                duration = time.time() - start_time

                #Record module completed
                result_length = len(str(result)) if result else 0
                logger_manager.log_module_complete(
                    tool_logger, module_name, symbol, actual_session_id,
                    duration, success=True, result_length=result_length,
                    function_name=func.__name__
                )

                return result

            except Exception as e:
                #Calculate implementation time
                duration = time.time() - start_time

                #Record module error
                logger_manager.log_module_error(
                    tool_logger, module_name, symbol, actual_session_id,
                    duration, str(e),
                    function_name=func.__name__
                )

                #Releasing anomaly.
                raise

        return wrapper
    return decorator


def log_analyst_module(analyst_type: str):
    """Special Decorator for Analyst Modules

Args:
Analyst type: Analyst type of analyst (e.g. market, fundamentals, technical, scientific, etc.)
"""
    return log_analysis_module(f"{analyst_type}_analyst")


def log_graph_module(graph_type: str):
    """Special Decorator for the Figure Processing Module

Args:
graph type: Figure processing type (e.g., signature processing, workflow)
"""
    return log_analysis_module(f"graph_{graph_type}")


def log_dataflow_module(dataflow_type: str):
    """Data stream module special decorator

Args:
Dataflow type: data stream type (e.g. carche, interface, protocol, etc.)
"""
    return log_analysis_module(f"dataflow_{dataflow_type}")
