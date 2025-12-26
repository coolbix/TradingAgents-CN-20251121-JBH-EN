"""News Data Synchronization Service
Support for multi-data source news synchronization and emotional analysis
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field

from app.services.news_data_service import get_news_data_service
from tradingagents.dataflows.providers.china.tushare import get_tushare_provider
from tradingagents.dataflows.providers.china.akshare import get_akshare_provider
from tradingagents.dataflows.news.realtime_news import RealtimeNewsAggregator

logger = logging.getLogger(__name__)


@dataclass
class NewsSyncStats:
    """Synchronization of news statistics"""
    total_processed: int = 0
    successful_saves: int = 0
    failed_saves: int = 0
    duplicate_skipped: int = 0
    sources_used: List[str] = field(default_factory=list)
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None
    
    @property
    def duration_seconds(self) -> float:
        """Sync time (sec)"""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    @property
    def success_rate(self) -> float:
        """Success rate"""
        if self.total_processed == 0:
            return 0.0
        return (self.successful_saves / self.total_processed) * 100


class NewsDataSyncService:
    """News Data Synchronization Service"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._news_service = None
        self._tushare_provider = None
        self._akshare_provider = None
        self._realtime_aggregator = None
    
    async def _get_news_service(self):
        """Access to news data services"""
        if self._news_service is None:
            self._news_service = await get_news_data_service()
        return self._news_service
    
    async def _get_tushare_provider(self):
        """Access to Tushare providers"""
        if self._tushare_provider is None:
            self._tushare_provider = get_tushare_provider()
            await self._tushare_provider.connect()
        return self._tushare_provider
    
    async def _get_tushare_provider(self):
        """Access to Tushare providers"""
        if self._tushare_provider is None:
            from tradingagents.dataflows.providers.china.tushare import get_tushare_provider
            self._tushare_provider = get_tushare_provider()
            await self._tushare_provider.connect()
        return self._tushare_provider

    async def _get_akshare_provider(self):
        """Access to AKShare provider"""
        if self._akshare_provider is None:
            self._akshare_provider = get_akshare_provider()
            await self._akshare_provider.connect()
        return self._akshare_provider
    
    async def _get_realtime_aggregator(self):
        """Get real-time news aggregaters"""
        if self._realtime_aggregator is None:
            self._realtime_aggregator = RealtimeNewsAggregator()
        return self._realtime_aggregator
    
    async def sync_stock_news(
        self,
        symbol: str,
        data_sources: List[str] = None,
        hours_back: int = 24,
        max_news_per_source: int = 50
    ) -> NewsSyncStats:
        """Synchronization of single stock news data

Args:
symbol: stock code
data sources: list of data sources, default on all available sources
Hours back: backtrace hours
Max news per source: Maximum number of news per data source

Returns:
Sync Statistical Information
"""
        stats = NewsSyncStats()
        
        try:
            self.logger.info(f"@📰.{symbol}")
            
            if data_sources is None:
                data_sources = ["tushare", "akshare", "realtime"]
            
            news_service = await self._get_news_service()
            all_news = []
            
            #Tushare News
            if "tushare" in data_sources:
                try:
                    tushare_news = await self._sync_tushare_news(
                        symbol, hours_back, max_news_per_source
                    )
                    if tushare_news:
                        all_news.extend(tushare_news)
                        stats.sources_used.append("tushare")
                        self.logger.info(f"Tushare News Access Success:{len(tushare_news)}Article")
                except Exception as e:
                    self.logger.error(f"Tushare News Failed:{e}")
            
            #2. AKShare News
            if "akshare" in data_sources:
                try:
                    akshare_news = await self._sync_akshare_news(
                        symbol, hours_back, max_news_per_source
                    )
                    if akshare_news:
                        all_news.extend(akshare_news)
                        stats.sources_used.append("akshare")
                        self.logger.info(f"AKShare News Access Success:{len(akshare_news)}Article")
                except Exception as e:
                    self.logger.error(f"AKShare News Failed:{e}")
            
            #3. Real-time news aggregation
            if "realtime" in data_sources:
                try:
                    realtime_news = await self._sync_realtime_news(
                        symbol, hours_back, max_news_per_source
                    )
                    if realtime_news:
                        all_news.extend(realtime_news)
                        stats.sources_used.append("realtime")
                        self.logger.info(f"Live news access success:{len(realtime_news)}Article")
                except Exception as e:
                    self.logger.error(f"Real-time news access failed:{e}")
            
            #Preservation of news data
            if all_news:
                stats.total_processed = len(all_news)
                
                #To reprocess.
                unique_news = self._deduplicate_news(all_news)
                stats.duplicate_skipped = len(all_news) - len(unique_news)
                
                #Batch Save
                saved_count = await news_service.save_news_data(
                    unique_news, "multi_source", "CN"
                )
                stats.successful_saves = saved_count
                stats.failed_saves = len(unique_news) - saved_count
                
                self.logger.info(f"💾 {symbol}Synchronization of news:{saved_count}Article saved successfully")
            
            stats.end_time = datetime.utcnow()
            return stats
            
        except Exception as e:
            self.logger.error(f"Synchronized stock news failed{symbol}: {e}")
            stats.end_time = datetime.utcnow()
            return stats
    
    async def _sync_tushare_news(
        self,
        symbol: str,
        hours_back: int,
        max_news: int
    ) -> List[Dict[str, Any]]:
        """Sync Tushare News"""
        try:
            provider = await self._get_tushare_provider()

            if not provider.is_available():
                self.logger.warning("Tushare providers are not available")
                return []

            #Get news data and pass on hours back parameters
            news_data = await provider.get_stock_news(
                symbol=symbol,
                limit=max_news,
                hours_back=hours_back
            )

            if news_data:
                #Standardized public information data
                standardized_news = []
                for news in news_data:
                    standardized = self._standardize_tushare_news(news, symbol)
                    if standardized:
                        standardized_news.append(standardized)

                self.logger.info(f"Tushare News Access Success:{len(standardized_news)}Article")
                return standardized_news
            else:
                self.logger.debug("Tushare did not return the news data")
                return []

        except Exception as e:
            #Detailed error handling
            if any(keyword in str(e).lower() for keyword in ['权限', 'permission', 'unauthorized']):
                self.logger.warning(f"The Tushare news interface requires separate access:{e}")
            elif "积分" in str(e) or "point" in str(e).lower():
                self.logger.warning(f"There's not enough Tushare credit:{e}")
            else:
                self.logger.error(f"Tushare News Failed:{e}")
            return []
    
    async def _sync_akshare_news(
        self, 
        symbol: str, 
        hours_back: int, 
        max_news: int
    ) -> List[Dict[str, Any]]:
        """Sync AK Share News"""
        try:
            provider = await self._get_akshare_provider()
            
            if not provider.is_available():
                return []
            
            #Access to news data
            news_data = await provider.get_stock_news(symbol, limit=max_news)
            
            if news_data:
                #Standardized public information data
                standardized_news = []
                for news in news_data:
                    standardized = self._standardize_akshare_news(news, symbol)
                    if standardized:
                        standardized_news.append(standardized)
                
                return standardized_news
            
            return []
            
        except Exception as e:
            self.logger.error(f"AKshare News has failed:{e}")
            return []
    
    async def _sync_realtime_news(
        self, 
        symbol: str, 
        hours_back: int, 
        max_news: int
    ) -> List[Dict[str, Any]]:
        """Sync Real Time News"""
        try:
            aggregator = await self._get_realtime_aggregator()
            
            #Access to real-time news
            news_items = aggregator.get_realtime_stock_news(
                symbol, hours_back, max_news
            )
            
            if news_items:
                #Standardized public information data
                standardized_news = []
                for news_item in news_items:
                    standardized = self._standardize_realtime_news(news_item, symbol)
                    if standardized:
                        standardized_news.append(standardized)
                
                return standardized_news
            
            return []
            
        except Exception as e:
            self.logger.error(f"Live news sync failed:{e}")
            return []
    
    def _standardize_tushare_news(self, news: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        """Standardized Tushare news data"""
        try:
            return {
                "symbol": symbol,
                "title": news.get("title", ""),
                "content": news.get("content", ""),
                "summary": news.get("summary", ""),
                "url": news.get("url", ""),
                "source": news.get("source", "Tushare"),
                "author": news.get("author", ""),
                "publish_time": news.get("publish_time"),
                "category": self._classify_news_category(news.get("title", "")),
                "sentiment": self._analyze_sentiment(news.get("title", "") + " " + news.get("content", "")),
                "importance": self._assess_importance(news.get("title", "")),
                "keywords": self._extract_keywords(news.get("title", "") + " " + news.get("content", "")),
                "data_source": "tushare"
            }
        except Exception as e:
            self.logger.error(f"@❌> Standard Tushare News Failed:{e}")
            return None
    
    def _standardize_akshare_news(self, news: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        """Standardization of AKShare news data"""
        try:
            return {
                "symbol": symbol,
                "title": news.get("title", ""),
                "content": news.get("content", ""),
                "summary": news.get("summary", ""),
                "url": news.get("url", ""),
                "source": news.get("source", "AKShare"),
                "author": news.get("author", ""),
                "publish_time": news.get("publish_time"),
                "category": self._classify_news_category(news.get("title", "")),
                "sentiment": self._analyze_sentiment(news.get("title", "") + " " + news.get("content", "")),
                "importance": self._assess_importance(news.get("title", "")),
                "keywords": self._extract_keywords(news.get("title", "") + " " + news.get("content", "")),
                "data_source": "akshare"
            }
        except Exception as e:
            self.logger.error(f"@❌ #SystemAkshare News Failed:{e}")
            return None
    
    def _standardize_realtime_news(self, news_item, symbol: str) -> Optional[Dict[str, Any]]:
        """Standardized real-time news data"""
        try:
            return {
                "symbol": symbol,
                "title": news_item.title,
                "content": news_item.content,
                "summary": news_item.content[:200] + "..." if len(news_item.content) > 200 else news_item.content,
                "url": news_item.url,
                "source": news_item.source,
                "author": "",
                "publish_time": news_item.publish_time,
                "category": self._classify_news_category(news_item.title),
                "sentiment": self._analyze_sentiment(news_item.title + " " + news_item.content),
                "importance": self._assess_importance(news_item.title),
                "keywords": self._extract_keywords(news_item.title + " " + news_item.content),
                "data_source": "realtime"
            }
        except Exception as e:
            self.logger.error(f"Standardized real-time news failed:{e}")
            return None
    
    def _classify_news_category(self, title: str) -> str:
        """Categorized categories of information"""
        title_lower = title.lower()
        
        if any(word in title_lower for word in ["年报", "季报", "业绩", "财报", "公告"]):
            return "company_announcement"
        elif any(word in title_lower for word in ["政策", "央行", "监管", "法规"]):
            return "policy_news"
        elif any(word in title_lower for word in ["市场", "行情", "指数", "板块"]):
            return "market_news"
        elif any(word in title_lower for word in ["研报", "分析", "评级", "推荐"]):
            return "research_report"
        else:
            return "general"
    
    def _analyze_sentiment(self, text: str) -> str:
        """Analysis of emotions"""
        text_lower = text.lower()
        
        positive_words = ["增长", "上涨", "利好", "盈利", "成功", "突破", "创新", "优秀"]
        negative_words = ["下跌", "亏损", "风险", "问题", "困难", "下滑", "减少", "警告"]
        
        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)
        
        if positive_count > negative_count:
            return "positive"
        elif negative_count > positive_count:
            return "negative"
        else:
            return "neutral"
    
    def _assess_importance(self, title: str) -> str:
        """Assessment of importance"""
        title_lower = title.lower()
        
        high_importance_words = ["重大", "紧急", "突发", "年报", "业绩", "重组", "收购"]
        medium_importance_words = ["公告", "通知", "变更", "调整", "计划"]
        
        if any(word in title_lower for word in high_importance_words):
            return "high"
        elif any(word in title_lower for word in medium_importance_words):
            return "medium"
        else:
            return "low"
    
    def _extract_keywords(self, text: str) -> List[str]:
        """Extract Keywords"""
        #Simple keyword extraction, with more sophisticated NLP techniques available for practical application
        keywords = []
        
        common_keywords = [
            "业绩", "年报", "季报", "增长", "利润", "营收", "股价", "投资",
            "市场", "行业", "政策", "监管", "风险", "机会", "创新", "发展"
        ]
        
        for keyword in common_keywords:
            if keyword in text:
                keywords.append(keyword)
        
        return keywords[:10]  #Returns a maximum of 10 keywords
    
    def _deduplicate_news(self, news_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Go back to the news."""
        seen = set()
        unique_news = []
        
        for news in news_list:
            #Use title and URL as remark
            key = (news.get("title", ""), news.get("url", ""))
            if key not in seen:
                seen.add(key)
                unique_news.append(news)
        
        return unique_news
    
    async def sync_market_news(
        self,
        data_sources: List[str] = None,
        hours_back: int = 24,
        max_news_per_source: int = 100
    ) -> NewsSyncStats:
        """Sync market news

Args:
data sources: list of data sources
Hours back: backtrace hours
Max news per source: Maximum number of news per data source

Returns:
Sync Statistical Information
"""
        stats = NewsSyncStats()
        
        try:
            self.logger.info("Let's start synchronizing the market news...")
            
            if data_sources is None:
                data_sources = ["realtime"]
            
            news_service = await self._get_news_service()
            all_news = []
            
            #Real-time market news
            if "realtime" in data_sources:
                try:
                    aggregator = await self._get_realtime_aggregator()
                    
                    #Access to market news (no stock code specified)
                    news_items = aggregator.get_realtime_stock_news(
                        None, hours_back, max_news_per_source
                    )
                    
                    if news_items:
                        for news_item in news_items:
                            standardized = self._standardize_realtime_news(news_item, None)
                            if standardized:
                                all_news.append(standardized)
                        
                        stats.sources_used.append("realtime")
                        self.logger.info(f"Market news access success:{len(all_news)}Article")
                        
                except Exception as e:
                    self.logger.error(f"The market news has failed:{e}")
            
            #Preservation of news data
            if all_news:
                stats.total_processed = len(all_news)
                
                #To reprocess.
                unique_news = self._deduplicate_news(all_news)
                stats.duplicate_skipped = len(all_news) - len(unique_news)
                
                #Batch Save
                saved_count = await news_service.save_news_data(
                    unique_news, "market_news", "CN"
                )
                stats.successful_saves = saved_count
                stats.failed_saves = len(unique_news) - saved_count
                
                self.logger.info(f"The market news has been synchronized:{saved_count}Article saved successfully")
            
            stats.end_time = datetime.utcnow()
            return stats
            
        except Exception as e:
            self.logger.error(f"@❌ #Synthetic Market News Failed:{e}")
            stats.end_time = datetime.utcnow()
            return stats


#Examples of global services
_sync_service_instance = None

async def get_news_data_sync_service() -> NewsDataSyncService:
    """Examples of access to news data sync service"""
    global _sync_service_instance
    if _sync_service_instance is None:
        _sync_service_instance = NewsDataSyncService()
        logger.info("Successful initialization of news synchronisation service")
    return _sync_service_instance
