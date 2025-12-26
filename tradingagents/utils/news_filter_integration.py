"""News filter integration module
Integration of news filters into existing news acquisition processes
"""

import pandas as pd
import logging
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

def integrate_news_filtering(original_get_stock_news_em):
    """Decorators: Add news filter to the Get stock news em function

Args:
Original get stock news em: original Get stock news em function

Returns:
Packed Functions with News Filters
"""
    def filtered_get_stock_news_em(symbol: str, enable_filter: bool = True, min_score: float = 30, 
                                  use_semantic: bool = False, use_local_model: bool = False) -> pd.DataFrame:
        """Enhanced set stock news em, integrated news filtering

Args:
symbol: stock code
enabled file: Enable news filters
min score: Minimum relevance rating threshold
use semantic: whether semantic similarity is used to filter
use local model: using local classification models

Returns:
DataFrame: Filtered News Data
"""
        logger.info(f"[news filter integration] Start getting{symbol}News, filter switch:{enable_filter}")
        
        #Call original to get news
        start_time = datetime.now()
        try:
            news_df = original_get_stock_news_em(symbol)
            fetch_time = (datetime.now() - start_time).total_seconds()
            
            if news_df.empty:
                logger.warning(f"[New filter integration] Original function not accessed{symbol}Public information data")
                return news_df
            
            logger.info(f"[news filter integration] Original news access success:{len(news_df)}Article, time-consuming:{fetch_time:.2f}sec")
            
            #If filters are not enabled, return the original data directly
            if not enable_filter:
                logger.info(f"[new filter integration] Filter is disabled and returned to raw news data")
                return news_df
            
            #Enable news filtering
            filter_start_time = datetime.now()
            
            try:
                #Import Filter
                from tradingagents.utils.enhanced_news_filter import create_enhanced_news_filter
                
                #Create Filter
                news_filter = create_enhanced_news_filter(
                    symbol, 
                    use_semantic=use_semantic, 
                    use_local_model=use_local_model
                )
                
                #Execute Filter
                filtered_df = news_filter.filter_news_enhanced(news_df, min_score=min_score)
                
                filter_time = (datetime.now() - filter_start_time).total_seconds()
                
                #Record filter statistics
                original_count = len(news_df)
                filtered_count = len(filtered_df)
                filter_rate = (original_count - filtered_count) / original_count * 100 if original_count > 0 else 0
                
                logger.info(f"[news filter integration] News filter completed:")
                logger.info(f"- Raw news:{original_count}Article")
                logger.info(f"- Post-filter news:{filtered_count}Article")
                logger.info(f"- Filter rate:{filter_rate:.1f}%")
                logger.info(f"- Time-consuming filtering:{filter_time:.2f}sec")
                
                if not filtered_df.empty:
                    avg_score = filtered_df['final_score'].mean()
                    max_score = filtered_df['final_score'].max()
                    logger.info(f"- Average rating:{avg_score:.1f}")
                    logger.info(f"- Top rating:{max_score:.1f}")
                
                return filtered_df
                
            except Exception as filter_error:
                logger.error(f"[news filter integration] News filter failed:{filter_error}")
                logger.error(f"[news filter integration] Return raw news data as backup")
                return news_df
                
        except Exception as fetch_error:
            logger.error(f"[news filter integration] Original news access failed:{fetch_error}")
            return pd.DataFrame()  #Return empty DataFrame
    
    return filtered_get_stock_news_em


def patch_akshare_utils():
    """Add filter function for the Get stock news em function of the kshare utils module

 Abandoned: akshare utils module has been removed and this function is reserved for backward compatibility only
"""
    logger.warning("[newsfiltration integration]  Patch akshare utils > Wasted: akshare utils module has been removed")


def create_filtered_realtime_news_function():
    """Create an enhanced real-time news acquisition function
"""
    def get_filtered_realtime_stock_news(ticker: str, curr_date: str, hours_back: int = 6, 
                                       enable_filter: bool = True, min_score: float = 30) -> str:
        """Enhanced real-time news acquisition function, integrated news filter

Args:
ticker: Stock code
Curr date: Current date
Hours back: backtrace hours
enabled file: Enable news filters
min score: Minimum relevance rating threshold

Returns:
str: Formatted news reports
"""
        logger.info(f"[enhanced real-time news]{ticker}Other Organiser")
        
        try:
            #Import Original Functions
            from tradingagents.dataflows.news.realtime_news import get_realtime_stock_news

            #Call original to get news
            original_report = get_realtime_stock_news(ticker, curr_date, hours_back)
            
            if not enable_filter:
                logger.info(f"[Enhanced Real Time News] Filter disabled, returns original report")
                return original_report
            
            #Try retrieving and filtering if filtering and unit A is enabled
            if any(suffix in ticker for suffix in ['.SH', '.SZ', '.SS', '.XSHE', '.XSHG']) or \
               (not '.' in ticker and ticker.isdigit()):
                
                logger.info(f"[enhanced real-time news]")
                
                try:
                    #Note: kshare utils is abandoned, replaced with AKshareProvider
                    from tradingagents.dataflows.providers.china.akshare import get_akshare_provider

                    #Clear stock code
                    clean_ticker = ticker.replace('.SH', '').replace('.SZ', '').replace('.SS', '')\
                                    .replace('.XSHE', '').replace('.XSHG', '')

                    #Access to news using AKShareProvider (if available)
                    provider = get_akshare_provider()
                    #TODO: Need to get stock news method
                    # original_news_df = provider.get_stock_news(clean_ticker)
                    #Skip temporarily, return original report
                    logger.warning(f"[Enhanced real-time news] AKShare news functionality is not in place, returning to original report")
                    return original_report
                        
                except Exception as filter_error:
                    logger.error(f"News filtering failed:{filter_error}")
                    return original_report
            else:
                logger.info(f"[Enhanced real-time news] Non-A unit code, return original report")
                return original_report
                
        except Exception as e:
            logger.error(f"[Enhanced real-time news]{e}")
            return f"❌ 新闻获取失败: {str(e)}"
    
    return get_filtered_realtime_stock_news


#Automatically apply patches
def apply_news_filtering_patches():
    """Automatically apply news filter patches
"""
    logger.info("[news filter integration] Start applying news filter patches...")
    
    #1. Enhancement of kshare utils
    patch_akshare_utils()
    
    #2. Create enhanced real-time news functions
    enhanced_function = create_filtered_realtime_news_function()
    
    logger.info("[new filter integration] ✅ news filter patch complete")
    
    return enhanced_function


if __name__ == "__main__":
    #Test set successful.
    print("=== 测试新闻过滤集成 ===")
    
    #Apply Patch
    enhanced_news_function = apply_news_filtering_patches()
    
    #Test Enhancement Functions
    test_result = enhanced_news_function(
        ticker="600036",
        curr_date="2024-07-28",
        enable_filter=True,
        min_score=30
    )
    
    print(f"测试结果长度: {len(test_result)} 字符")
    print(f"测试结果预览: {test_result[:200]}...")