"""News data acquisition module
Integrated management of information sources
"""

#Import Google News
try:
    from .google_news import getNewsData
    GOOGLE_NEWS_AVAILABLE = True
except ImportError:
    getNewsData = None
    GOOGLE_NEWS_AVAILABLE = False

#Import Reddit
try:
    from .reddit import fetch_top_from_category
    REDDIT_AVAILABLE = True
except ImportError:
    fetch_top_from_category = None
    REDDIT_AVAILABLE = False

#Import Real Time News
try:
    from .realtime_news import (
        get_realtime_news,
        get_news_with_sentiment,
        search_news_by_keyword
    )
    REALTIME_NEWS_AVAILABLE = True
except ImportError:
    get_realtime_news = None
    get_news_with_sentiment = None
    search_news_by_keyword = None
    REALTIME_NEWS_AVAILABLE = False

#Import Chinese Financial Data Aggregator
try:
    from .chinese_finance import ChineseFinanceDataAggregator
    CHINESE_FINANCE_AVAILABLE = True
except ImportError:
    ChineseFinanceDataAggregator = None
    CHINESE_FINANCE_AVAILABLE = False

__all__ = [
    # Google News
    'getNewsData',
    'GOOGLE_NEWS_AVAILABLE',
    
    # Reddit
    'fetch_top_from_category',
    'REDDIT_AVAILABLE',
    
    # Realtime News
    'get_realtime_news',
    'get_news_with_sentiment',
    'search_news_by_keyword',
    'REALTIME_NEWS_AVAILABLE',

    # Chinese Finance
    'ChineseFinanceDataAggregator',
    'CHINESE_FINANCE_AVAILABLE',
]

