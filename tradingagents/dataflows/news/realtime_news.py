#!/usr/bin/env python3
"""Real-time news data acquisition tool
Addressing news lags
"""

import requests
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from typing import List, Dict, Optional
import time
import os
from dataclasses import dataclass

#Import Log Module
from tradingagents.config.runtime_settings import get_timezone_name

from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')



@dataclass
class NewsItem:
    """Public information project data structure"""
    title: str
    content: str
    source: str
    publish_time: datetime
    url: str
    urgency: str  # high, medium, low
    relevance_score: float


class RealtimeNewsAggregator:
    """Real-time news aggregater"""

    def __init__(self):
        self.headers = {
            'User-Agent': 'TradingAgents-CN/1.0'
        }

        #API Key Configuration
        self.finnhub_key = os.getenv('FINNHUB_API_KEY')
        self.alpha_vantage_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        self.newsapi_key = os.getenv('NEWSAPI_KEY')

    def get_realtime_stock_news(self, ticker: str, hours_back: int = 6, max_news: int = 10) -> List[NewsItem]:
        """Access to real-time stock news
Priority: Professional API > News API > search engine

Args:
ticker: Stock code
Hours back: backtrace hours
max news: Maximum number of news, default 10
"""
        logger.info(f"[News Aggregator]{ticker}Real time news, back in time:{hours_back}Hours")
        start_time = datetime.now(ZoneInfo(get_timezone_name()))
        all_news = []

        #1. FinnHub news in real time (highest priority)
        logger.info(f"[News Aggregator] Try to get from Finn Hub{ticker}Public information")
        finnhub_start = datetime.now(ZoneInfo(get_timezone_name()))
        finnhub_news = self._get_finnhub_realtime_news(ticker, hours_back)
        finnhub_time = (datetime.now(ZoneInfo(get_timezone_name())) - finnhub_start).total_seconds()

        if finnhub_news:
            logger.info(f"[News Aggregator] Successfully accessed from FinnHub{len(finnhub_news)}News, time-consuming:{finnhub_time:.2f}sec")
        else:
            logger.info(f"[news aggregator] FinnHub did not return the news, taking time:{finnhub_time:.2f}sec")

        all_news.extend(finnhub_news)

        #2. Alpha Vantage News
        logger.info(f"[News Aggregator] Try to get from Alpha Vantage{ticker}Public information")
        av_start = datetime.now(ZoneInfo(get_timezone_name()))
        av_news = self._get_alpha_vantage_news(ticker, hours_back)
        av_time = (datetime.now(ZoneInfo(get_timezone_name())) - av_start).total_seconds()

        if av_news:
            logger.info(f"[News Aggregator] Successfully accessed from Alpha Vantage{len(av_news)}News, time-consuming:{av_time:.2f}sec")
        else:
            logger.info(f"[news aggregator] Alpha Vantage did not return the news, taking time:{av_time:.2f}sec")

        all_news.extend(av_news)

        #3. NewsAPI (if configured)
        if self.newsapi_key:
            logger.info(f"[NewsAPI] Try to get it from NewsAPI{ticker}Public information")
            newsapi_start = datetime.now(ZoneInfo(get_timezone_name()))
            newsapi_news = self._get_newsapi_news(ticker, hours_back)
            newsapi_time = (datetime.now(ZoneInfo(get_timezone_name())) - newsapi_start).total_seconds()

            if newsapi_news:
                logger.info(f"[NewsAPI] Successfully accessed from NewsAPI{len(newsapi_news)}News, time-consuming:{newsapi_time:.2f}sec")
            else:
                logger.info(f"NewsAPI didn't return the news, timed:{newsapi_time:.2f}sec")

            all_news.extend(newsapi_news)
        else:
            logger.info(f"[NewsAPI] NewsAPI key is not configured, skipping this source")

        #4. Information sources in Chinese
        logger.info(f"[News Aggregator] Trying to get{ticker}Chinese News")
        chinese_start = datetime.now(ZoneInfo(get_timezone_name()))
        chinese_news = self._get_chinese_finance_news(ticker, hours_back)
        chinese_time = (datetime.now(ZoneInfo(get_timezone_name())) - chinese_start).total_seconds()

        if chinese_news:
            logger.info(f"[News Aggregator]{len(chinese_news)}Chinese financial news, time-consuming:{chinese_time:.2f}sec")
        else:
            logger.info(f"[news aggregater] No Chinese financial news obtained, time spent:{chinese_time:.2f}sec")

        all_news.extend(chinese_news)

        #To reorder and sort
        logger.info(f"[SINGING CONTINUES]{len(all_news)}News reordering and sorting")
        dedup_start = datetime.now(ZoneInfo(get_timezone_name()))
        unique_news = self._deduplicate_news(all_news)
        sorted_news = sorted(unique_news, key=lambda x: x.publish_time, reverse=True)
        dedup_time = (datetime.now(ZoneInfo(get_timezone_name())) - dedup_start).total_seconds()

        #Record the results.
        removed_count = len(all_news) - len(unique_news)
        logger.info(f"[News Syndication] News went back to finish, removed.{removed_count}Repeat the news.{len(sorted_news)}Article, time-consuming:{dedup_time:.2f}sec")

        #Recording of the overall situation
        total_time = (datetime.now(ZoneInfo(get_timezone_name())) - start_time).total_seconds()
        logger.info(f"[news aggregator]{ticker}The news aggregate is complete.{len(sorted_news)}News, total time:{total_time:.2f}sec")

        #Limiting the number of news to the latest max news
        if len(sorted_news) > max_news:
            original_count = len(sorted_news)
            sorted_news = sorted_news[:max_news]
            logger.info(f"[News polymer] 📰 News quantity limit: from{original_count}Limit to{max_news}It's an update.")

        #Record some examples of news titles
        if sorted_news:
            sample_titles = [item.title for item in sorted_news[:3]]
            logger.info(f"[news aggregater] Example of news title:{', '.join(sample_titles)}")

        return sorted_news

    def _get_finnhub_realtime_news(self, ticker: str, hours_back: int) -> List[NewsItem]:
        """Get Finn Hub real time news."""
        if not self.finnhub_key:
            return []

        try:
            #Calculate the time frame
            end_time = datetime.now(ZoneInfo(get_timezone_name()))
            start_time = end_time - timedelta(hours=hours_back)

            #FinnHub API Call
            url = "https://finnhub.io/api/v1/company-news"
            params = {
                'symbol': ticker,
                'from': start_time.strftime('%Y-%m-%d'),
                'to': end_time.strftime('%Y-%m-%d'),
                'token': self.finnhub_key
            }

            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()

            news_data = response.json()
            news_items = []

            for item in news_data:
                #Checking the timeliness of the news
                publish_time = datetime.fromtimestamp(item.get('datetime', 0), tz=ZoneInfo(get_timezone_name()))
                if publish_time < start_time:
                    continue

                #Assessment of urgency
                urgency = self._assess_news_urgency(item.get('headline', ''), item.get('summary', ''))

                news_items.append(NewsItem(
                    title=item.get('headline', ''),
                    content=item.get('summary', ''),
                    source=item.get('source', 'FinnHub'),
                    publish_time=publish_time,
                    url=item.get('url', ''),
                    urgency=urgency,
                    relevance_score=self._calculate_relevance(item.get('headline', ''), ticker)
                ))

            return news_items

        except Exception as e:
            logger.error(f"Finn Hub News Failed:{e}")
            return []

    def _get_alpha_vantage_news(self, ticker: str, hours_back: int) -> List[NewsItem]:
        """Get Alpha Vantage News"""
        if not self.alpha_vantage_key:
            return []

        try:
            url = "https://www.alphavantage.co/query"
            params = {
                'function': 'NEWS_SENTIMENT',
                'tickers': ticker,
                'apikey': self.alpha_vantage_key,
                'limit': 50
            }

            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()

            data = response.json()
            news_items = []

            if 'feed' in data:
                for item in data['feed']:
                    #Parsing Time
                    time_str = item.get('time_published', '')
                    try:
                        publish_time = datetime.strptime(time_str, '%Y%m%dT%H%M%S').replace(tzinfo=ZoneInfo(get_timezone_name()))
                    except:
                        continue

                    #Time limits for inspection
                    if publish_time < datetime.now(ZoneInfo(get_timezone_name())) - timedelta(hours=hours_back):
                        continue

                    urgency = self._assess_news_urgency(item.get('title', ''), item.get('summary', ''))

                    news_items.append(NewsItem(
                        title=item.get('title', ''),
                        content=item.get('summary', ''),
                        source=item.get('source', 'Alpha Vantage'),
                        publish_time=publish_time,
                        url=item.get('url', ''),
                        urgency=urgency,
                        relevance_score=self._calculate_relevance(item.get('title', ''), ticker)
                    ))

            return news_items

        except Exception as e:
            logger.error(f"Alpha Vantage News Failed:{e}")
            return []

    def _get_newsapi_news(self, ticker: str, hours_back: int) -> List[NewsItem]:
        """Get NewsAPI News"""
        try:
            #Build Search Query
            company_names = {
                'AAPL': 'Apple',
                'TSLA': 'Tesla',
                'NVDA': 'NVIDIA',
                'MSFT': 'Microsoft',
                'GOOGL': 'Google'
            }

            query = f"{ticker} OR {company_names.get(ticker, ticker)}"

            url = "https://newsapi.org/v2/everything"
            params = {
                'q': query,
                'language': 'en',
                'sortBy': 'publishedAt',
                'from': (datetime.now(ZoneInfo(get_timezone_name())) - timedelta(hours=hours_back)).isoformat(),
                'apiKey': self.newsapi_key
            }

            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()

            data = response.json()
            news_items = []

            for item in data.get('articles', []):
                #Parsing Time
                time_str = item.get('publishedAt', '')
                try:
                    publish_time = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                except:
                    continue

                urgency = self._assess_news_urgency(item.get('title', ''), item.get('description', ''))

                news_items.append(NewsItem(
                    title=item.get('title', ''),
                    content=item.get('description', ''),
                    source=item.get('source', {}).get('name', 'NewsAPI'),
                    publish_time=publish_time,
                    url=item.get('url', ''),
                    urgency=urgency,
                    relevance_score=self._calculate_relevance(item.get('title', ''), ticker)
                ))

            return news_items

        except Exception as e:
            logger.error(f"NewsAPI news access failed:{e}")
            return []

    def _get_chinese_finance_news(self, ticker: str, hours_back: int) -> List[NewsItem]:
        """Get Chinese Financial News"""
        #Integrated Chinese Finance News API: Financial Union, Eastern Wealth, etc.
        logger.info(f"[CHANTING IN CHINESE]{ticker}Chinese News, Retro:{hours_back}Hours")
        start_time = datetime.now(ZoneInfo(get_timezone_name()))

        try:
            news_items = []

            #1. Attempt to use AK-Share to access Eastern wealth stock news
            try:
                logger.info(f"[CHANTING IN CHINESE NEWS]")
                from tradingagents.dataflows.providers.china.akshare import AKShareProvider

                provider = AKShareProvider()

                #Processing stock code format
                #If it's a U.S. stock code, don't use East Wealth News.
                if '.' in ticker and any(suffix in ticker for suffix in ['.US', '.N', '.O', '.NYSE', '.NASDAQ']):
                    logger.info(f"[SPEAKING IN JAPANESE]{ticker}Skip the East Wealth News.")
                else:
                    #Processing of Unit A and Port Unit codes
                    clean_ticker = ticker.replace('.SH', '').replace('.SZ', '').replace('.SS', '')\
                                    .replace('.HK', '').replace('.XSHE', '').replace('.XSHG', '')

                    #Access to Eastern Wealth News
                    logger.info(f"[CHANTING IN CHINESE]{clean_ticker}The East Wealth News.")
                    em_start_time = datetime.now(ZoneInfo(get_timezone_name()))
                    news_df = provider.get_stock_news_sync(symbol=clean_ticker)

                    if not news_df.empty:
                        logger.info(f"[CHANTING IN CHINESE]{len(news_df)}Press data, start processing.")
                        processed_count = 0
                        skipped_count = 0
                        error_count = 0

                        #Convert to NewsItem format
                        for _, row in news_df.iterrows():
                            try:
                                #Parsing Time
                                time_str = row.get('时间', '')
                                if time_str:
                                    #Try to parse the time format, possibly '2023-01-01 12:34:56'
                                    try:
                                        publish_time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=ZoneInfo(get_timezone_name()))
                                    except:
                                        #Try other possible formats
                                        try:
                                            publish_time = datetime.strptime(time_str, '%Y-%m-%d').replace(tzinfo=ZoneInfo(get_timezone_name()))
                                        except:
                                            logger.warning(f"Can not parse time format:{time_str}Use current time")
                                            publish_time = datetime.now(ZoneInfo(get_timezone_name()))
                                else:
                                    logger.warning(f"[CHANTING IN CHINESE]")
                                    publish_time = datetime.now(ZoneInfo(get_timezone_name()))

                                #Time limits for inspection
                                if publish_time < datetime.now(ZoneInfo(get_timezone_name())) - timedelta(hours=hours_back):
                                    skipped_count += 1
                                    continue

                                #Assessment of urgency
                                title = row.get('标题', '')
                                content = row.get('内容', '')
                                urgency = self._assess_news_urgency(title, content)

                                news_items.append(NewsItem(
                                    title=title,
                                    content=content,
                                    source='东方财富',
                                    publish_time=publish_time,
                                    url=row.get('链接', ''),
                                    urgency=urgency,
                                    relevance_score=self._calculate_relevance(title, ticker)
                                ))
                                processed_count += 1
                            except Exception as item_e:
                                logger.error(f"The project to deal with East Wealth News failed:{item_e}")
                                error_count += 1
                                continue

                        em_time = (datetime.now(ZoneInfo(get_timezone_name())) - em_start_time).total_seconds()
                        logger.info(f"[CHANTING IN CHINESE]{processed_count}Bar, skip:{skipped_count}Article, error:{error_count}Article, time-consuming:{em_time:.2f}sec")
            except Exception as ak_e:
                logger.error(f"[CHANTING IN CHINESE]{ak_e}")

            #2. Federation RSS (if available)
            logger.info(f"[CHANTING IN CHINESE]")
            rss_start_time = datetime.now(ZoneInfo(get_timezone_name()))
            rss_sources = [
                "https://www.cls.cn/api/sw?app=CailianpressWeb&os=web&sv=7.7.5",
                #Add more RSS sources
            ]

            rss_success_count = 0
            rss_error_count = 0
            total_rss_items = 0

            for rss_url in rss_sources:
                try:
                    logger.info(f"[CHANTING IN CHINESE]{rss_url}")
                    rss_item_start = datetime.now(ZoneInfo(get_timezone_name()))
                    items = self._parse_rss_feed(rss_url, ticker, hours_back)
                    rss_item_time = (datetime.now(ZoneInfo(get_timezone_name())) - rss_item_start).total_seconds()

                    if items:
                        logger.info(f"[Culture News] Successfully accessed from RSS source{len(items)}News, time-consuming:{rss_item_time:.2f}sec")
                        news_items.extend(items)
                        total_rss_items += len(items)
                        rss_success_count += 1
                    else:
                        logger.info(f"The RSS source did not return the news, which took time:{rss_item_time:.2f}sec")
                except Exception as rss_e:
                    logger.error(f"[CHANTING IN CHINESE]{rss_e}")
                    rss_error_count += 1
                    continue

            #Record RSS Access Summary
            rss_total_time = (datetime.now(ZoneInfo(get_timezone_name())) - rss_start_time).total_seconds()
            logger.info(f"RSS News is finished.{rss_success_count}Man, failed source:{rss_error_count}Get the news:{total_rss_items}Article, total time-consuming:{rss_total_time:.2f}sec")

            #Recording summary of Chinese financial and economic news
            total_time = (datetime.now(ZoneInfo(get_timezone_name())) - start_time).total_seconds()
            logger.info(f"[SPEAKING IN CHINESE]{ticker}Chinese-language news is available, total{len(news_items)}News, total time:{total_time:.2f}sec")

            return news_items

        except Exception as e:
            logger.error(f"The Chinese news has failed:{e}")
            return []

    def _parse_rss_feed(self, rss_url: str, ticker: str, hours_back: int) -> List[NewsItem]:
        """Parsing RSS Source"""
        logger.info(f"[RSS parsing] Start parsing RSS source:{rss_url}Equities:{ticker}, trace time:{hours_back}Hours")
        start_time = datetime.now(ZoneInfo(get_timezone_name()))

        try:
            #Actual realization requires the use of feedparser library
            #This is a simplified realization, and the actual project should be replaced with a real RSS resolution logic.
            import feedparser

            logger.info(f"[RSS parsing] Try to get RSS source content")
            feed = feedparser.parse(rss_url)

            if not feed or not feed.entries:
                logger.warning(f"[RSS parsing] RSS source does not return valid content")
                return []

            logger.info(f"[RSS parsing] Successfully access RSS source, including{len(feed.entries)}Entry")
            news_items = []
            processed_count = 0
            skipped_count = 0

            for entry in feed.entries:
                try:
                    #Parsing Time
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        publish_time = datetime.fromtimestamp(time.mktime(entry.published_parsed), tz=ZoneInfo(get_timezone_name()))
                    else:
                        logger.warning(f"[RSS parsing] Entry missing release time, using current time")
                        publish_time = datetime.now(ZoneInfo(get_timezone_name()))

                    #Time limits for inspection
                    if publish_time < datetime.now(ZoneInfo(get_timezone_name())) - timedelta(hours=hours_back):
                        skipped_count += 1
                        continue

                    title = entry.title if hasattr(entry, 'title') else ''
                    content = entry.description if hasattr(entry, 'description') else ''

                    #Check for relevance
                    if ticker.lower() not in title.lower() and ticker.lower() not in content.lower():
                        skipped_count += 1
                        continue

                    #Assessment of urgency
                    urgency = self._assess_news_urgency(title, content)

                    news_items.append(NewsItem(
                        title=title,
                        content=content,
                        source='财联社',
                        publish_time=publish_time,
                        url=entry.link if hasattr(entry, 'link') else '',
                        urgency=urgency,
                        relevance_score=self._calculate_relevance(title, ticker)
                    ))
                    processed_count += 1
                except Exception as e:
                    logger.error(f"[RSS parsing] Failed to process RSS entry:{e}")
                    continue

            total_time = (datetime.now(ZoneInfo(get_timezone_name())) - start_time).total_seconds()
            logger.info(f"[RSS parsing] RSS source resolution completed. Success:{processed_count}Bar, skip:{skipped_count}Article, time-consuming:{total_time:.2f}sec")
            return news_items
        except ImportError:
            logger.error(f"[RSS parsing] feedparser library is not installed and cannot resolve RSS source")
            return []
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return []

    def _assess_news_urgency(self, title: str, content: str) -> str:
        """Assessment of the urgency of the news"""
        text = (title + ' ' + content).lower()

        #High emergency keyword
        high_urgency_keywords = [
            'breaking', 'urgent', 'alert', 'emergency', 'halt', 'suspend',
            '突发', '紧急', '暂停', '停牌', '重大'
        ]

        #Medium Emergency Keyword
        medium_urgency_keywords = [
            'earnings', 'report', 'announce', 'launch', 'merger', 'acquisition',
            '财报', '发布', '宣布', '并购', '收购'
        ]

        #Check high-emergency keywords
        for keyword in high_urgency_keywords:
            if keyword in text:
                logger.debug(f"[Emergency Assessment]{keyword}In the news:{title[:50]}...")
                return 'high'

        #Check medium emergency keywords
        for keyword in medium_urgency_keywords:
            if keyword in text:
                logger.debug(f"[Emergency Assessment]{keyword}In the news:{title[:50]}...")
                return 'medium'

        logger.debug(f"[Emergency assessment] No emergency keyword was detected and assessed as low emergency:{title[:50]}...")
        return 'low'

    def _calculate_relevance(self, title: str, ticker: str) -> float:
        """Calculate news relevance scores"""
        text = title.lower()
        ticker_lower = ticker.lower()

        #Basic Relevance - Stock Code appears directly in the title Medium
        if ticker_lower in text:
            logger.debug(f"[Relevance calculation] Stock code{ticker}Directly in the title, relevance rating: 1.0, title:{title[:50]}...")
            return 1.0

        #Company name matches
        company_names = {
            'aapl': ['apple', 'iphone', 'ipad', 'mac'],
            'tsla': ['tesla', 'elon musk', 'electric vehicle'],
            'nvda': ['nvidia', 'gpu', 'ai chip'],
            'msft': ['microsoft', 'windows', 'azure'],
            'googl': ['google', 'alphabet', 'search']
        }

        #Examination of company-related keywords
        if ticker_lower in company_names:
            for name in company_names[ticker_lower]:
                if name in text:
                    logger.debug(f"[Relevance calculations]{name}' In title, relevance rating: 0.8, title:{title[:50]}...")
                    return 0.8

        #Quantities for extracting stock codes (for Chinese equities)
        pure_code = ''.join(filter(str.isdigit, ticker))
        if pure_code and pure_code in text:
            logger.debug(f"[Relevance calculation] Stock code numerical component{pure_code}In the title, relevance rating: 0.9, title:{title[:50]}...")
            return 0.9

        logger.debug(f"[Relevance calculation] No explicit relevance detected, using default rating: 0.3, heading:{title[:50]}...")
        return 0.3  #Default Relevance

    def _deduplicate_news(self, news_items: List[NewsItem]) -> List[NewsItem]:
        """Go back to the news."""
        logger.info(f"[news go heavy]{len(news_items)}We'll reprocess the news.")
        start_time = datetime.now(ZoneInfo(get_timezone_name()))

        seen_titles = set()
        unique_news = []
        duplicate_count = 0
        short_title_count = 0

        for item in news_items:
            #Simple title to heavy
            title_key = item.title.lower().strip()

            #Checking Title Length
            if len(title_key) <= 10:
                logger.debug(f"[news over] Skip short headlines: '{item.title}' , source:{item.source}")
                short_title_count += 1
                continue

            #Check for repetition
            if title_key in seen_titles:
                logger.debug(f"[news rewrite]{item.title[:50]}...' from:{item.source}")
                duplicate_count += 1
                continue

            #Add to Results Set
            seen_titles.add(title_key)
            unique_news.append(item)

        #Record the results.
        time_taken = (datetime.now(ZoneInfo(get_timezone_name())) - start_time).total_seconds()
        logger.info(f"[news rumbling] Go back to the original news:{len(news_items)}Article, after heavy:{len(unique_news)}Article,")
        logger.info(f"[news off] Remove repetition:{duplicate_count}Article, short title:{short_title_count}Article, time-consuming:{time_taken:.2f}sec")

        return unique_news

    def format_news_report(self, news_items: List[NewsItem], ticker: str) -> str:
        """Formatting news reports"""
        logger.info(f"[Press Report]{ticker}Generate news reports")
        start_time = datetime.now(ZoneInfo(get_timezone_name()))

        if not news_items:
            logger.warning(f"[press report] Not obtained{ticker}Real time news data")
            return f"未获取到{ticker}的实时新闻数据。"

        #Group by emergency
        high_urgency = [n for n in news_items if n.urgency == 'high']
        medium_urgency = [n for n in news_items if n.urgency == 'medium']
        low_urgency = [n for n in news_items if n.urgency == 'low']

        #Recording of news classification
        logger.info(f"[Press Report]{ticker}News classification statistics: high emergency{len(high_urgency)}Article, Medium Emergency{len(medium_urgency)}Article, Low Emergency{len(low_urgency)}Article")

        #Record the distribution of news sources
        news_sources = {}
        for item in news_items:
            source = item.source
            if source in news_sources:
                news_sources[source] += 1
            else:
                news_sources[source] = 1

        sources_info = ", ".join([f"{source}: {count}条" for source, count in news_sources.items()])
        logger.info(f"[Press Report]{ticker}Distribution of sources of information:{sources_info}")

        report = f"# {ticker} 实时新闻分析报告\n\n"
        report += f"📅 生成时间: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y-%m-%d %H:%M:%S')}\n"
        report += f"📊 新闻总数: {len(news_items)}条\n\n"

        if high_urgency:
            report += "## 🚨 紧急新闻\n\n"
            for news in high_urgency[:3]:  #Show a maximum of 3
                report += f"### {news.title}\n"
                report += f"**来源**: {news.source} | **时间**: {news.publish_time.strftime('%H:%M')}\n"
                report += f"{news.content}\n\n"

        if medium_urgency:
            report += "## 📢 重要新闻\n\n"
            for news in medium_urgency[:5]:  #Show maximum 5
                report += f"### {news.title}\n"
                report += f"**来源**: {news.source} | **时间**: {news.publish_time.strftime('%H:%M')}\n"
                report += f"{news.content}\n\n"

        #Add a time-bound statement
        latest_news = max(news_items, key=lambda x: x.publish_time)
        time_diff = datetime.now(ZoneInfo(get_timezone_name())) - latest_news.publish_time

        report += f"\n## ⏰ 数据时效性\n"
        report += f"最新新闻发布于: {time_diff.total_seconds() / 60:.0f}分钟前\n"

        if time_diff.total_seconds() < 1800:  #Thirty minutes. Internal
            report += "🟢 数据时效性: 优秀 (30分钟内)\n"
        elif time_diff.total_seconds() < 3600:  #One hour.
            report += "🟡 数据时效性: 良好 (1小时内)\n"
        else:
            report += "🔴 数据时效性: 一般 (超过1小时)\n"

        #Recording reports to generate completed information
        end_time = datetime.now(ZoneInfo(get_timezone_name()))
        time_taken = (end_time - start_time).total_seconds()
        report_length = len(report)

        logger.info(f"[Press Report]{ticker}The production of public information reports is complete and takes time:{time_taken:.2f}seconds, report length:{report_length}Character")

        #Record time-sensitive information
        time_diff_minutes = time_diff.total_seconds() / 60
        logger.info(f"[Press Report]{ticker}The latest news is published on{time_diff_minutes:.1f}A minute ago.")

        return report


def get_realtime_stock_news(ticker: str, curr_date: str, hours_back: int = 6) -> str:
    """Main interface function to access real-time stock news
"""
    logger.info(f"== sync, corrected by elderman ==")
    logger.info(f"Function: get realtime stock news")
    logger.info(f"[Press analysis] Parameter: ticker={ticker}, curr_date={curr_date}, hours_back={hours_back}")
    logger.info(f"[Press Analysis] Start getting{ticker}The real-time news, date:{curr_date},Trace time:{hours_back}Hours")
    start_total_time = datetime.now(ZoneInfo(get_timezone_name()))
    logger.info(f"[Press Analysis] Starts at:{start_total_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")

    #Assessing stock types
    logger.info(f"== sync, corrected by elderman == @elder man")
    stock_type = "未知"
    is_china_stock = False
    logger.info(f"[Press Analysis] Original picker:{ticker}")

    if '.' in ticker:
        logger.info(f"[newsanalyzing]")
        if any(suffix in ticker for suffix in ['.SH', '.SZ', '.SS', '.XSHE', '.XSHG']):
            stock_type = "A股"
            is_china_stock = True
            logger.info(f"[News Analysis] Match to A stock suffix, stock type:{stock_type}")
        elif '.HK' in ticker:
            stock_type = "港股"
            logger.info(f"[Press Analysis] Matching to Hong Kong stock suffix, stock type:{stock_type}")
        elif any(suffix in ticker for suffix in ['.US', '.N', '.O', '.NYSE', '.NASDAQ']):
            stock_type = "美股"
            logger.info(f"[News Analysis] Matches the American stock suffix, stock type:{stock_type}")
        else:
            logger.info(f"[News Analysis] Not matched to known suffix")
    else:
        logger.info(f"[Press Analysis] ticker doesn't contain dots and tries to use StockUtils to judge.")
        #Try StockUtils to judge stock types
        try:
            from tradingagents.utils.stock_utils import StockUtils
            logger.info(f"[News Analysis] Successfully imported StockUtils to start judging stock types")
            market_info = StockUtils.get_market_info(ticker)
            logger.info(f"[Press Analysis] StockUtils returns to market information:{market_info}")
            if market_info['is_china']:
                stock_type = "A股"
                is_china_stock = True
                logger.info(f"[Press Analysis] StockUtils judged A.")
            elif market_info['is_hk']:
                stock_type = "港股"
                logger.info(f"[Press Analysis] StockUtils judges the Port Unit")
            elif market_info['is_us']:
                stock_type = "美股"
                logger.info(f"[Press Analysis] StockUtils judged it to be America's share.")
        except Exception as e:
            logger.warning(f"[Press Analysis] Using StockUtils to judge stock types failed:{e}")

    logger.info(f"[News Analysis] Final findings - stocks{ticker}Type:{stock_type}, Whether Unit A:{is_china_stock}")

    #For Unit A, priority is given to Eastern Wealth News.
    if is_china_stock:
        logger.info(f"== sync, corrected by elderman == @elder man")
        logger.info(f"[Press Analysis]{ticker}Try using the East Wealth News as a priority")
        try:
            logger.info(f"[Press Analysis] Trying to get news through Akshare Provider")
            from tradingagents.dataflows.providers.china.akshare import AKShareProvider

            provider = AKShareProvider()
            logger.info(f"[Press Analysis] Successfully created Akshare Provider instance")

            #Processing Unit A code
            clean_ticker = ticker.replace('.SH', '').replace('.SZ', '').replace('.SS', '')\
                            .replace('.XSHE', '').replace('.XSHG', '')
            logger.info(f"[Press Analysis] Original picker:{ticker}- ♪ Clean-up picker:{clean_ticker}")

            logger.info(f"[Press Analysis] Prepare to call provider.get stock news sync(){clean_ticker})")
            logger.info(f"[News Analysis] Starting to take wealth from the East.{clean_ticker}Public information data")
            start_time = datetime.now(ZoneInfo(get_timezone_name()))
            logger.info(f"[News Analysis] Eastern Wealth API calls at:{start_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")

            news_df = provider.get_stock_news_sync(symbol=clean_ticker, limit=10)

            end_time = datetime.now(ZoneInfo(get_timezone_name()))
            time_taken = (end_time - start_time).total_seconds()
            logger.info(f"[Press Analysis] Eastern Wealth API calls to end:{end_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
            logger.info(f"[news analysis] Eastern wealth API calls time:{time_taken:.2f}sec")
            logger.info(f"[News Analysis] East Wealth API returns data type:{type(news_df)}")

            if hasattr(news_df, 'empty'):
                logger.info(f"Whether the Eastern Wealth API returned to DataFrame is empty:{news_df.empty}")
                if not news_df.empty:
                    logger.info(f"[Press Analysis] East Wealth API returns to DataFrame shape:{news_df.shape}")
                    logger.info(f"[Press Analysis] Eastern Wealth API returns to DataFrame's listing:{list(news_df.columns) if hasattr(news_df, 'columns') else 'No listing'}")
            else:
                logger.info(f"[Press Analysis] Eastern Wealth API returns data:{news_df}")

            if not news_df.empty:
                #Build simple news reports
                news_count = len(news_df)
                logger.info(f"[Press Analysis] Successful access{news_count}East Wealth News, time-consuming.{time_taken:.2f}sec")

                report = f"# {ticker} 东方财富新闻报告\n\n"
                report += f"📅 生成时间: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y-%m-%d %H:%M:%S')}\n"
                report += f"📊 新闻总数: {news_count}条\n"
                report += f"🕒 获取耗时: {time_taken:.2f}秒\n\n"

                #Record some examples of news titles
                sample_titles = [row.get('新闻标题', '无标题') for _, row in news_df.head(3).iterrows()]
                logger.info(f"[Press Analysis] Example of press title:{', '.join(sample_titles)}")

                logger.info(f"[Press Analysis] Start building news reports")
                for idx, (_, row) in enumerate(news_df.iterrows()):
                    if idx < 3:  #Only the details of the first 3 articles are recorded
                        logger.info(f"[Press Analysis]{idx+1}News: Title ={row.get('Press title', 'Untitled')}, Time{row.get('Release time', 'No time')}")
                    report += f"### {row.get('新闻标题', '')}\n"
                    report += f"📅 {row.get('发布时间', '')}\n"
                    report += f"🔗 {row.get('新闻链接', '')}\n\n"
                    report += f"{row.get('新闻内容', '无内容')}\n\n"

                total_time_taken = (datetime.now(ZoneInfo(get_timezone_name())) - start_total_time).total_seconds()
                logger.info(f"[Press Analysis] Successfully generated{ticker}News reports, total time.{total_time_taken:.2f}Second, news source:")
                logger.info(f"[Press analysis] Report length:{len(report)}Character")
                logger.info(f"== sync, corrected by elderman == @elder man")
                return report
            else:
                logger.warning(f"[News Analysis] East wealth is missing.{ticker}News, time-consuming.{time_taken:.2f}Seconds, try to use other news sources")
        except Exception as e:
            logger.error(f"[Press Analysis] Eastern Wealth News Failed:{e}, will try other news sources")
            logger.error(f"[Press Analysis]{type(e).__name__}: {str(e)}")
            import traceback
            logger.error(f"[News Analysis] Unusual stack:{traceback.format_exc()}")
    else:
        logger.info(f"== sync, corrected by elderman == @elder man")
        logger.info(f"[Press Analysis] Stock type is{stock_type}It's not Unit A.")

    #If Unit A or Unit A news access fails, use real-time news aggregaters
    logger.info(f"== sync, corrected by elderman == @elder man")
    aggregator = RealtimeNewsAggregator()
    logger.info(f"[Press Analysis] Examples of successful creation of real-time news aggregaters")
    try:
        logger.info(f"[News Analysis] Attempted to use real-time news aggregaters{ticker}Public information")
        start_time = datetime.now(ZoneInfo(get_timezone_name()))
        logger.info(f"[Press Analysis] The polymer calls at the start time:{start_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")

        #Access to real-time news
        news_items = aggregator.get_realtime_stock_news(ticker, hours_back, max_news=10)

        end_time = datetime.now(ZoneInfo(get_timezone_name()))
        time_taken = (end_time - start_time).total_seconds()
        logger.info(f"[Press Analysis]{end_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        logger.info(f"[Press Analysis]{time_taken:.2f}sec")
        logger.info(f"[Press Analysis] The polymer returns data type:{type(news_items)}")
        logger.info(f"[Press Analysis] The polymer returns data:{news_items}")

        #If you succeed in getting news
        if news_items and len(news_items) > 0:
            news_count = len(news_items)
            logger.info(f"[News Analysis] Real-time news aggregater successfully accessed{news_count}Article{ticker}News, time-consuming.{time_taken:.2f}sec")

            #Record some examples of news titles
            sample_titles = [item.title for item in news_items[:3]]
            logger.info(f"[Press Analysis] Example of press title:{', '.join(sample_titles)}")

            #Formatting Reports
            logger.info(f"[Press Analysis] Start formatting news reports")
            report = aggregator.format_news_report(news_items, ticker)
            logger.info(f"[Press Analysis] Formatted report, length:{len(report)}Character")

            total_time_taken = (datetime.now(ZoneInfo(get_timezone_name())) - start_total_time).total_seconds()
            logger.info(f"[Press Analysis] Successfully generated{ticker}News reports, total time.{total_time_taken:.2f}Seconds, news source: Real-time news aggregater")
            logger.info(f"== sync, corrected by elderman == @elder man")
            return report
        else:
            logger.warning(f"[News Analysis] Real-time news polymer not available{ticker}News, time-consuming.{time_taken:.2f}Seconds, try to use a backup source.")
            #If you don't get the news, try the backup.
    except Exception as e:
        logger.error(f"[News Analysis] Real-time news aggregater acquisition failed:{e}I'll try a backup source.")
        logger.error(f"[Press Analysis]{type(e).__name__}: {str(e)}")
        import traceback
        logger.error(f"[News Analysis] Unusual stack:{traceback.format_exc()}")
        #Continue to try a backup when there is an anomaly.

    #Alternative 1: For the Port Unit, priority is given to the use of Eastern Wealth News (Unit A has been processed earlier)
    if not is_china_stock and '.HK' in ticker:
        logger.info(f"[Press Analysis]{ticker}Try using East Wealth News.")
        try:
            from tradingagents.dataflows.providers.china.akshare import AKShareProvider

            provider = AKShareProvider()

            #Processing of port unit codes
            clean_ticker = ticker.replace('.HK', '')

            logger.info(f"[News Analysis] Started the Eastern Wealth Acquisition Port Unit.{clean_ticker}Public information data")
            start_time = datetime.now(ZoneInfo(get_timezone_name()))
            news_df = provider.get_stock_news_sync(symbol=clean_ticker, limit=10)
            end_time = datetime.now(ZoneInfo(get_timezone_name()))
            time_taken = (end_time - start_time).total_seconds()

            if not news_df.empty:
                #Build simple news reports
                news_count = len(news_df)
                logger.info(f"[Press Analysis] Successful access{news_count}East Fortune Port News, time-consuming{time_taken:.2f}sec")

                report = f"# {ticker} 东方财富新闻报告\n\n"
                report += f"📅 生成时间: {datetime.now(ZoneInfo(get_timezone_name())).strftime('%Y-%m-%d %H:%M:%S')}\n"
                report += f"📊 新闻总数: {news_count}条\n"
                report += f"🕒 获取耗时: {time_taken:.2f}秒\n\n"

                #Record some examples of news titles
                sample_titles = [row.get('新闻标题', '无标题') for _, row in news_df.head(3).iterrows()]
                logger.info(f"[Press Analysis] Example of press title:{', '.join(sample_titles)}")

                for _, row in news_df.iterrows():
                    report += f"### {row.get('新闻标题', '')}\n"
                    report += f"📅 {row.get('发布时间', '')}\n"
                    report += f"🔗 {row.get('新闻链接', '')}\n\n"
                    report += f"{row.get('新闻内容', '无内容')}\n\n"

                logger.info(f"[Press Analysis] Successfully produced East Wealth News, news source: East Wealth.")
                return report
            else:
                logger.warning(f"[News Analysis] East wealth is missing.{clean_ticker}News data, time-consuming{time_taken:.2f}Seconds, try the next backup.")
        except Exception as e:
            logger.error(f"[Press Analysis] Eastern Wealth News Failed:{e}, will try the next backup.")

    #Alternative 2: Try Google News
    try:
        from tradingagents.dataflows.interface import get_google_news

        #Build a search query by stock type
        if stock_type == "A股":
            #Use of Chinese keyword in Unit A
            clean_ticker = ticker.replace('.SH', '').replace('.SZ', '').replace('.SS', '')\
                           .replace('.XSHE', '').replace('.XSHG', '')
            search_query = f"{clean_ticker} 股票 公司 财报 新闻"
            logger.info(f"[Press Analysis] Start getting Unit A from Google.{clean_ticker}Queries:{search_query}")
        elif stock_type == "港股":
            #The Hong Kong Unit uses Chinese keywords
            clean_ticker = ticker.replace('.HK', '')
            search_query = f"{clean_ticker} 港股 公司"
            logger.info(f"[Press analysis] Start getting port units from Google{clean_ticker}Queries:{search_query}")
        else:
            #USU uses English keywords
            search_query = f"{ticker} stock news"
            logger.info(f"[Press Analysis] Start getting it from Google.{ticker}Queries:{search_query}")

        start_time = datetime.now(ZoneInfo(get_timezone_name()))
        google_news = get_google_news(search_query, curr_date, 1)
        end_time = datetime.now(ZoneInfo(get_timezone_name()))
        time_taken = (end_time - start_time).total_seconds()

        if google_news and len(google_news.strip()) > 0:
            #Estimated number of news received
            news_lines = google_news.strip().split('\n')
            news_count = sum(1 for line in news_lines if line.startswith('###'))

            logger.info(f"[Press Analysis] Successful access to Google News, estimated{news_count}News. Time.{time_taken:.2f}sec")

            #Record some examples of news titles
            sample_titles = [line.replace('### ', '') for line in news_lines if line.startswith('### ')][:3]
            if sample_titles:
                logger.info(f"[Press Analysis] Example of press title:{', '.join(sample_titles)}")

            logger.info(f"[Press Analysis] Successfully produced Google News Report, source: Google")
            return google_news
        else:
            logger.warning(f"[Press Analysis] Google News is not available{ticker}News data, time-consuming{time_taken:.2f}sec")
    except Exception as e:
        logger.error(f"[Press Analysis] Google News Access Failed:{e}All options have been tried.")

    #All methods fail.
    total_time_taken = (datetime.now(ZoneInfo(get_timezone_name())) - start_total_time).total_seconds()
    logger.error(f"[Press Analysis]{ticker}All the methods of access to information have failed, always taking time.{total_time_taken:.2f}sec")

    #Record detailed failure information
    failure_details = {
        "股票代码": ticker,
        "股票类型": stock_type,
        "分析日期": curr_date,
        "回溯时间": f"{hours_back}小时",
        "总耗时": f"{total_time_taken:.2f}秒"
    }
    logger.error(f"[Press Analysis] Details of news access failures:{failure_details}")

    return f"""
实时新闻获取失败 - {ticker}
分析日期: {curr_date}

❌ 错误信息: 所有可用的新闻源都未能获取到相关新闻

💡 备用建议:
1. 检查网络连接和API密钥配置
2. 使用基础新闻分析作为备选
3. 关注官方财经媒体的最新报道
4. 考虑使用专业金融终端获取实时新闻

注: 实时新闻获取依赖外部API服务的可用性。
"""
