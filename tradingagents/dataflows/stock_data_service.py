#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Common stock data acquisition services
Complete downgrading mechanism for the MongoDB-> Tushare data interface
"""

import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')

try:
    from tradingagents.config.database_manager import get_database_manager
    DATABASE_MANAGER_AVAILABLE = True
except ImportError:
    DATABASE_MANAGER_AVAILABLE = False

try:
    import sys
    import os
    #Add utils directory to path
    utils_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'utils')
    if utils_path not in sys.path:
        sys.path.append(utils_path)
    from enhanced_stock_list_fetcher import enhanced_fetch_stock_list
    ENHANCED_FETCHER_AVAILABLE = True
except ImportError:
    ENHANCED_FETCHER_AVAILABLE = False

logger = logging.getLogger(__name__)

class StockDataService:
    """Common stock data acquisition services
Complete downscaling mechanism: MongoDB ->Tushare data interface ->Cache -> Error processing
"""
    
    def __init__(self):
        self.db_manager = None
        self._init_services()
    
    def _init_services(self):
        """Initialization services"""
        #Try Initializing Database Manager
        if DATABASE_MANAGER_AVAILABLE:
            try:
                self.db_manager = get_database_manager()
                if self.db_manager.is_mongodb_available():
                    logger.info(f"The MongoDB connection was successful.")
                else:
                    logger.error(f"⚠️ MongoDB connection failed, using other data sources")
            except Exception as e:
                logger.error(f"The initialization of the database manager failed:{e}")
                self.db_manager = None
    
    def get_stock_basic_info(self, stock_code: str = None) -> Optional[Dict[str, Any]]:
        """Access to basic equity information (individual or total stocks)

Args:
Stock code: Stock code, return all shares if None

Returns:
Dict: Basic information on stocks
"""
        logger.info(f"Access to basic stock information:{stock_code or 'All stocks'}")
        
        #1. Priority access from MongoDB
        if self.db_manager and self.db_manager.is_mongodb_available():
            try:
                result = self._get_from_mongodb(stock_code)
                if result:
                    logger.info(f"From MongoDB:{len(result) if isinstance(result, list) else 1}Notes")
                    return result
            except Exception as e:
                logger.error(f"Could not close temporary folder: %s{e}")
        
        #2. Degrade to enhanced receiver
        logger.info(f"MongoDB is not available, downgraded to enhanced receiver")
        if ENHANCED_FETCHER_AVAILABLE:
            try:
                result = self._get_from_enhanced_fetcher(stock_code)
                if result:
                    logger.info(f"✅ from enhanced acquisition:{len(result) if isinstance(result, list) else 1}Notes")
                    #Try cache to MongoDB (if available)
                    self._cache_to_mongodb(result)
                    return result
            except Exception as e:
                logger.error(f"Enhanced receiver query failed:{e}")
        
        #3. Final downgrading programme
        logger.error(f"All data sources are not available")
        return self._get_fallback_data(stock_code)
    
    def _get_from_mongodb(self, stock_code: str = None) -> Optional[Dict[str, Any]]:
        """Get data from MongoDB"""
        try:
            mongodb_client = self.db_manager.get_mongodb_client()
            if not mongodb_client:
                return None

            db = mongodb_client[self.db_manager.mongodb_config["database"]]
            collection = db['stock_basic_info']

            if stock_code:
                #Get a single stock
                result = collection.find_one({'code': stock_code})
                return result if result else None
            else:
                #Get all shares.
                cursor = collection.find({})
                results = list(cursor)
                return results if results else None

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return None
    
    def _get_from_enhanced_fetcher(self, stock_code: str = None) -> Optional[Dict[str, Any]]:
        """Get data from the enhanced receiver"""
        try:
            if stock_code:
                #Get a single stock information - use an enhanced receiver to get all shares and screen them Select
                stock_df = enhanced_fetch_stock_list(
                    type_='stock',
                    enable_server_failover=True,
                    max_retries=3
                )
                
                if stock_df is not None and not stock_df.empty:
                    #Find specified stock code
                    stock_row = stock_df[stock_df['code'] == stock_code]
                    if not stock_row.empty:
                        row = stock_row.iloc[0]
                        return {
                            'code': row.get('code', stock_code),
                            'name': row.get('name', ''),
                            'market': row.get('market', self._get_market_name(stock_code)),
                            'category': row.get('category', self._get_stock_category(stock_code)),
                            'source': 'enhanced_fetcher',
                            'updated_at': datetime.now().isoformat()
                        }
                    else:
                        #If not found, return basic information
                        return {
                            'code': stock_code,
                            'name': '',
                            'market': self._get_market_name(stock_code),
                            'category': self._get_stock_category(stock_code),
                            'source': 'enhanced_fetcher',
                            'updated_at': datetime.now().isoformat()
                        }
            else:
                #Get All Stock Lists
                stock_df = enhanced_fetch_stock_list(
                    type_='stock',
                    enable_server_failover=True,
                    max_retries=3
                )
                
                if stock_df is not None and not stock_df.empty:
                    #Convert to dictionary list
                    results = []
                    for _, row in stock_df.iterrows():
                        results.append({
                            'code': row.get('code', ''),
                            'name': row.get('name', ''),
                            'market': row.get('market', ''),
                            'category': row.get('category', ''),
                            'source': 'enhanced_fetcher',
                            'updated_at': datetime.now().isoformat()
                        })
                    return results
                    
        except Exception as e:
            logger.error(f"Enhancement of accesser query failed:{e}")
            return None
    
    def _cache_to_mongodb(self, data: Any) -> bool:
        """Cache Data to MongoDB"""
        if not self.db_manager or not self.db_manager.mongodb_db:
            return False
        
        try:
            collection = self.db_manager.mongodb_db['stock_basic_info']
            
            if isinstance(data, list):
                #Batch Insert
                for item in data:
                    collection.update_one(
                        {'code': item['code']},
                        {'$set': item},
                        upsert=True
                    )
                logger.info(f"Cached{len(data)}Record to MongoDB")
            elif isinstance(data, dict):
                #Single Insert
                collection.update_one(
                    {'code': data['code']},
                    {'$set': data},
                    upsert=True
                )
                logger.info(f"Accomplished stocks{data['code']}To MongoDB.")
            
            return True
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return False
    
    def _get_fallback_data(self, stock_code: str = None) -> Dict[str, Any]:
        """Last downgrade data"""
        if stock_code:
            return {
                'code': stock_code,
                'name': f'股票{stock_code}',
                'market': self._get_market_name(stock_code),
                'category': '未知',
                'source': 'fallback',
                'updated_at': datetime.now().isoformat(),
                'error': '所有数据源都不可用'
            }
        else:
            return {
                'error': '无法获取股票列表，请检查网络连接和数据库配置',
                'suggestion': '请确保MongoDB已配置或网络连接正常以访问Tushare数据接口'
            }
    
    def _get_market_name(self, stock_code: str) -> str:
        """The market is judged by stock code."""
        if stock_code.startswith(('60', '68', '90')):
            return '上海'
        elif stock_code.startswith(('00', '30', '20')):
            return '深圳'
        else:
            return '未知'
    
    def _get_stock_category(self, stock_code: str) -> str:
        """Category by stock code"""
        if stock_code.startswith('60'):
            return '沪市主板'
        elif stock_code.startswith('68'):
            return '科创板'
        elif stock_code.startswith('00'):
            return '深市主板'
        elif stock_code.startswith('30'):
            return '创业板'
        elif stock_code.startswith('20'):
            return '深市B股'
        else:
            return '其他'
    
    def get_stock_data_with_fallback(self, stock_code: str, start_date: str, end_date: str) -> str:
        """Acquisition of stock data (degrading mechanism)
This is the enhancement of the existing Get china stock data function.
"""
        logger.info(f"Access to stock data:{stock_code} ({start_date}Present.{end_date})")
        
        #First, make sure that basic stock information is available.
        stock_info = self.get_stock_basic_info(stock_code)
        if stock_info and 'error' in stock_info:
            return f"❌ 无法获取股票{stock_code}的基础信息: {stock_info.get('error', '未知错误')}"
        
        #Call for a unified Chinese stock data interface
        try:
            from .interface import get_china_stock_data_unified

            return get_china_stock_data_unified(stock_code, start_date, end_date)
        except Exception as e:
            return f"❌ 获取股票数据失败: {str(e)}\n\n💡 建议：\n1. 检查网络连接\n2. 确认股票代码格式正确\n3. 检查MongoDB配置"

#Examples of global services
_stock_data_service = None

def get_stock_data_service() -> StockDataService:
    """Examples of access to stock data services (single model)"""
    global _stock_data_service
    if _stock_data_service is None:
        _stock_data_service = StockDataService()
    return _stock_data_service