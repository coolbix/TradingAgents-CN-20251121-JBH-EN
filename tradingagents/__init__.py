#!/usr/bin/env python3
"""TradingAgents-CN Core Module

This is a multi-intellectual-based stock analysis system that supports the integrated analysis of Unit A, Port and United States shares.
"""

__version__ = "1.0.0-preview"
__author__ = "TradingAgents-CN Team"
__description__ = "Multi-agent stock analysis system for Chinese markets"

#Import Core Module
try:
    from .config import config_manager
    from .utils import logging_manager
except ImportError:
    #If import fails, the basic functionality of the module is not affected
    pass

__all__ = [
    "__version__",
    "__author__", 
    "__description__"
]