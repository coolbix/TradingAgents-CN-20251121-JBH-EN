#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Stock data API interface
Provide simple and easy-to-use stock data acquisition interfaces with full downgrading mechanisms
"""

from typing import Dict, List, Optional, Any
from .stock_data_service import get_stock_data_service

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')

def get_stock_info(stock_code: str) -> Optional[Dict[str, Any]]:
    """Access to basic information on individual stocks

    Args:
        Stock code: Stock code (e. g. '000001')

    Returns:
        Dict: Stock information, with the words code, name, market, class
        If fetching failed, return the dictionary containing the error field

    Example:
    >info = get stock info
    >print (info ['name']) # Output:
    """
    service = get_stock_data_service()
    return service.get_stock_basic_info(stock_code)

def get_all_stocks() -> List[Dict[str, Any]]:
    """Get All Stock Lists

    Returns:
        List [Dict]: List of stocks, each element containing basic stock information
        If fetching failed, return the dictionary containing the error field

    Example:
    > stocks = get all stocks()
    I don't know, logger.info.
    """
    service = get_stock_data_service()
    result = service.get_stock_basic_info()
    
    if isinstance(result, list):
        return result
    elif isinstance(result, dict) and 'error' in result:
        return [result]  #Synchronising folder
    else:
        return []

def get_stock_data(stock_code: str, start_date: str, end_date: str) -> str:
    """Access to stock history data (degrading mechanism)

    Args:
        Stock code: Stock code
        Start date: Start date 'YYYYY-MM-DD'
        End date: End Date 'YYYY-MM-DD'

    Returns:
        str: Formatted Stock Data Reports

    Example:
    >data = get stock data
    >print(data)
    """
    service = get_stock_data_service()
    return service.get_stock_data_with_fallback(stock_code, start_date, end_date)

def search_stocks_by_name(name: str) -> List[Dict[str, Any]]:
    """Search stocks by stock name (MugoDB support required)

    Args:
        Name: Stock name keyword

    Returns:
        List [Dict]: Match list of shares

    Example:
    >Results =search stocks by name
    For stock in results:
    Logger.info(f" FMT 0: FMT 1 ")
    """
    #This function needs MongoDB support and is being implemented in the same manner as it was.
    try:
        from ..examples.stock_query_examples import EnhancedStockQueryService

        service = EnhancedStockQueryService()
        return service.query_stocks_by_name(name)
    except Exception as e:
        return [{'error': f'名称搜索功能不可用: {str(e)}'}]

def check_data_sources() -> Dict[str, Any]:
    """Check data source status

    Returns:
        Dict: Availability of data sources

    Example:
    == sync, corrected by elderman ==
    logger.info (f "MongoDB available:   FT 0 ")
    logger.info (f "Uniform Data Interface Available:   FMT 1 ")
    """
    service = get_stock_data_service()
    
    return {
        'mongodb_available': service.db_manager is not None and service.db_manager.mongodb_db is not None,
        'unified_api_available': True,  #Uniform interfaces are always available
        'enhanced_fetcher_available': True,  #This usually works.
        'fallback_mode': service.db_manager is None or service.db_manager.mongodb_db is None,
        'recommendation': (
            "所有数据源正常" if service.db_manager and service.db_manager.mongodb_db 
            else "建议配置MongoDB以获得最佳性能，当前使用统一数据接口降级模式"
        )
    }