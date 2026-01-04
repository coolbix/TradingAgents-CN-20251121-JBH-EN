#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Stock data API interface
Provide easy access to stock data to support complete downgrading mechanisms
"""

import sys
import os
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')

#Add DataFlows Directory to Path
dataflows_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dataflows')
if dataflows_path not in sys.path:
    sys.path.append(dataflows_path)

#Import Unified Log System
from tradingagents.utils.logging_init import get_logger

try:
    from stock_data_service import get_stock_data_service

    SERVICE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Equities data services are not available:{e}")
    SERVICE_AVAILABLE = False

def get_stock_info(stock_code: str) -> Dict[str, Any]:
    """Access to basic information on individual stocks

    Args:
        Stock code: Stock code (e. g. '000001')

    Returns:
        Dict: Basic information on stocks

    Example:
    >info = get stock info
    Peace Bank
    """
    if not SERVICE_AVAILABLE:
        return {
            'error': '股票数据服务不可用',
            'code': stock_code,
            'suggestion': '请检查服务配置'
        }
    
    service = get_stock_data_service()
    result = service.get_stock_basic_info(stock_code)
    
    if result is None:
        return {
            'error': f'未找到股票{stock_code}的信息',
            'code': stock_code,
            'suggestion': '请检查股票代码是否正确'
        }
    
    return result

def get_all_stocks() -> List[Dict[str, Any]]:
    """Access to basic information on all stocks

    Returns:
        List [Dict]: List of basic information for all stocks

    Example:
    > stocks = get all stocks()
    I don't know, logger.info.
    """
    if not SERVICE_AVAILABLE:
        return [{
            'error': '股票数据服务不可用',
            'suggestion': '请检查服务配置'
        }]
    
    service = get_stock_data_service()
    result = service.get_stock_basic_info()
    
    if result is None or (isinstance(result, dict) and 'error' in result):
        return [{
            'error': '无法获取股票列表',
            'suggestion': '请检查网络连接和数据库配置'
        }]
    
    return result if isinstance(result, list) else [result]

def get_stock_data(stock_code: str, start_date: str = None, end_date: str = None) -> str:
    """Access to stock history data (degrading mechanism)

    Args:
        Stock code: Stock code
        Start date: Start date (format: YYYY-MM-DD), default 30 days ago
        End date: End date (format: YYYY-MM-DD), default to today

    Returns:
        str: Stock data string expression or error information

    Example:
    >data = get stock data
    >print(data)
    """
    if not SERVICE_AVAILABLE:
        return "❌ 股票数据服务不可用，请检查服务配置"
    
    #Set Default Date
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    service = get_stock_data_service()
    return service.get_stock_data_with_fallback(stock_code, start_date, end_date)

def search_stocks(keyword: str) -> List[Dict[str, Any]]:
    """Search stocks by keyword

    Args:
        Keyword: Search for keywords (part of stock code or name)

    Returns:
        List [Dict]: Matching list of shares

    Example:
    {\\bord0\\shad0\\alphaH3D}Results =search stocks
    For stock in results:
    Logger.info(f" FMT 0: FMT 1 ")
    """
    all_stocks = get_all_stocks()
    
    if not all_stocks or (len(all_stocks) == 1 and 'error' in all_stocks[0]):
        return all_stocks
    
    #Search for matching stocks
    matches = []
    keyword_lower = keyword.lower()
    
    for stock in all_stocks:
        if 'error' in stock:
            continue
            
        code = stock.get('code', '').lower()
        name = stock.get('name', '').lower()
        
        if keyword_lower in code or keyword_lower in name:
            matches.append(stock)
    
    return matches

def get_market_summary() -> Dict[str, Any]:
    """Access to market overview information

    Returns:
        Dict: Market statistics

    Example:
    >Summarry = get market summary()
    Loger.info (f "Quantity of market shares:   FT 0 ")
    """
    all_stocks = get_all_stocks()
    
    if not all_stocks or (len(all_stocks) == 1 and 'error' in all_stocks[0]):
        return {
            'error': '无法获取市场数据',
            'suggestion': '请检查网络连接和数据库配置'
        }
    
    #Statistical market information
    shanghai_count = 0
    shenzhen_count = 0
    category_stats = {}
    
    for stock in all_stocks:
        if 'error' in stock:
            continue
            
        market = stock.get('market', '')
        category = stock.get('category', '未知')
        
        if market == '上海':
            shanghai_count += 1
        elif market == '深圳':
            shenzhen_count += 1
        
        category_stats[category] = category_stats.get(category, 0) + 1
    
    return {
        'total_count': len([s for s in all_stocks if 'error' not in s]),
        'shanghai_count': shanghai_count,
        'shenzhen_count': shenzhen_count,
        'category_stats': category_stats,
        'data_source': all_stocks[0].get('source', 'unknown') if all_stocks else 'unknown',
        'updated_at': datetime.now().isoformat()
    }

def check_service_status() -> Dict[str, Any]:
    """Check service status

    Returns:
        Dict: Service status information

    Example:
    == sync, corrected by elderman == @elder man
    Logger.info (f "MongoDB State:   FT 0 ")
    """
    if not SERVICE_AVAILABLE:
        return {
            'service_available': False,
            'error': '股票数据服务不可用',
            'suggestion': '请检查服务配置和依赖'
        }
    
    service = get_stock_data_service()
    
    #Check MongoDB status
    mongodb_status = 'disconnected'
    if service.db_manager:
        try:
            #Try checking the connection status of the database manager
            if hasattr(service.db_manager, 'is_mongodb_available') and service.db_manager.is_mongodb_available():
                mongodb_status = 'connected'
            elif hasattr(service.db_manager, 'mongodb_client') and service.db_manager.mongodb_client:
                #Try executing a simple query to test the connection
                service.db_manager.mongodb_client.admin.command('ping')
                mongodb_status = 'connected'
            else:
                mongodb_status = 'unavailable'
        except Exception:
            mongodb_status = 'error'
    
    #Check UDI status
    unified_api_status = 'unavailable'
    try:
        #Try getting a stock information to test a uniform interface
        test_result = service.get_stock_basic_info('000001')
        if test_result and 'error' not in test_result:
            unified_api_status = 'available'
        else:
            unified_api_status = 'limited'
    except Exception:
        unified_api_status = 'error'
    
    return {
        'service_available': True,
        'mongodb_status': mongodb_status,
        'unified_api_status': unified_api_status,
        'data_sources_available': ['tushare', 'akshare', 'baostock'],
        'fallback_available': True,
        'checked_at': datetime.now().isoformat()
    }

#A convenient alias function
get_stock = get_stock_info  #Alias
get_stocks = get_all_stocks  #Alias
search = search_stocks  #Alias
status = check_service_status  #Alias

if __name__ == '__main__':
    #Simple command line test
    logger.debug(f"Stock data API testing")
    logger.info(f"=" * 50)
    
    #Check service status
    logger.info(f"Service status check:")
    status_info = check_service_status()
    for key, value in status_info.items():
        logger.info(f"  {key}: {value}")
    
    #Test for individual stock information
    logger.info(f"Can not open message")
    stock_info = get_stock_info('000001')
    if 'error' not in stock_info:
        logger.info(f"Code:{stock_info.get('code')}")
        logger.info(f"Name:{stock_info.get('name')}")
        logger.info(f"Market:{stock_info.get('market')}")
        logger.info(f"Category:{stock_info.get('category')}")
        logger.info(f"Data source:{stock_info.get('source')}")
    else:
        logger.error(f"Error:{stock_info.get('error')}")
    
    #Test search function
    logger.debug(f"Search for 'Peace' related stocks:")
    search_results = search_stocks('平安')
    for i, stock in enumerate(search_results[:3]):  #Show the first three results only
        if 'error' not in stock:
            logger.info(f"  {i+1}. {stock.get('code')}")

    #Test market overview
    logger.info(f"Market overview:")
    summary = get_market_summary()
    if 'error' not in summary:
        logger.info(f"Total equities:{summary.get('total_count')}")
        logger.info(f"Market shares:{summary.get('shanghai_count')}")
        logger.info(f"Deep market shares:{summary.get('shenzhen_count')}")
        logger.info(f"Data source:{summary.get('data_source')}")
    else:
        logger.error(f"Error:{summary.get('error')}")