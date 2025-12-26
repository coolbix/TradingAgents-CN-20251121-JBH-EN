#!/usr/bin/env python3
"""Real-time news tool compatibility layer
Export function from news.realtime news module to maintain backward compatibility
"""

from tradingagents.dataflows.news.realtime_news import (
    get_realtime_stock_news,
    RealtimeNewsAggregator,
    NewsItem
)

__all__ = [
    'get_realtime_stock_news',
    'RealtimeNewsAggregator',
    'NewsItem'
]

