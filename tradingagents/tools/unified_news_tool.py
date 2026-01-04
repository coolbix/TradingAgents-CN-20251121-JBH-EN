#!/usr/bin/env python3
"""Harmonization of information analysis tools
Integration of information acquisition logic into a tool function in different markets such as Unit A, the Port Unit, the United States Unit Medium
Let the big model have only one tool to access all types of stock.
"""

import logging
from datetime import datetime
import re

logger = logging.getLogger(__name__)

class UnifiedNewsAnalyzer:
    """Harmonization of news analysts and integration of all news access logic"""
    
    def __init__(self, toolkit):
        """Initialize unified news analyst

        Args:
            Toolkit: Toolkit containing various news access tools
        """
        self.toolkit = toolkit
        
    def get_stock_news_unified(self, stock_code: str, max_news: int = 10, model_info: str = "") -> str:
        """Unified news access interface
        Automatically identify stock types according to stock code and obtain corresponding news

        Args:
            Stock code: Stock code
            Max news: Maximum number of news
            Model info: Model information currently in use for special handling

        Returns:
            st: Formatted news content
        """
        logger.info(f"[Unified News Tool] Start accessing{stock_code}News, models:{model_info}")
        logger.info(f"[Uniform News Tool]  Current model information:{model_info}")
        
        #Identification of stock types
        stock_type = self._identify_stock_type(stock_code)
        logger.info(f"[Uniform News Tool] Types of stocks:{stock_type}")
        
        #Call for appropriate acquisition methods by stock type
        if stock_type == "A股":
            result = self._get_a_share_news(stock_code, max_news, model_info)
        elif stock_type == "港股":
            result = self._get_hk_share_news(stock_code, max_news, model_info)
        elif stock_type == "美股":
            result = self._get_us_share_news(stock_code, max_news, model_info)
        else:
            #Default use of Unit A logic
            result = self._get_a_share_news(stock_code, max_news, model_info)
        
        #Add a detailed debugging log
        logger.info(f"[Uniform News Tool] 📊 News acquisition completed, resulting in length:{len(result)}Character")
        logger.info(f"[Uniform News Tool] 📋 returns the results preview (front 1000 characters):{result[:1000]}")
        
        #Record warning if result is empty or too short
        if not result or len(result.strip()) < 50:
            logger.warning(f"[Unified News Tool] ⚠️ returns abnormally short or empty!")
            logger.warning(f"[Uniform News Tool] 📝 Full result content: '{result}'")
        
        return result
    
    def _identify_stock_type(self, stock_code: str) -> str:
        """Identification of stock types"""
        stock_code = stock_code.upper().strip()
        
        #Unit A judgement
        if re.match(r'^(00|30|60|68)\d{4}$', stock_code):
            return "A股"
        elif re.match(r'^(SZ|SH)\d{6}$', stock_code):
            return "A股"
        
        #Hong Kong Unit judgement
        elif re.match(r'^\d{4,5}\.HK$', stock_code):
            return "港股"
        elif re.match(r'^\d{4,5}$', stock_code) and len(stock_code) <= 5:
            return "港股"
        
        #America's judgement.
        elif re.match(r'^[A-Z]{1,5}$', stock_code):
            return "美股"
        elif '.' in stock_code and not stock_code.endswith('.HK'):
            return "美股"
        
        #Defaultly by unit A
        else:
            return "A股"

    def _get_news_from_database(self, stock_code: str, max_news: int = 10) -> str:
        """Get news from the database

        Args:
            Stock code: Stock code
            Max news: Maximum number of news

        Returns:
            str: formatted news content, return empty string if no news
        """
        try:
            from tradingagents.dataflows.cache.app_adapter import get_mongodb_client
            from datetime import timedelta

            #Make sure max news is the integer.
            max_news = int(max_news)

            client = get_mongodb_client()
            if not client:
                logger.warning(f"[Uniform News Tool] Unable to connect to MongoDB")
                return ""

            db = client.get_database('tradingagents')
            collection = db.stock_news

            #Standardised stock code (elimination of suffix)
            clean_code = stock_code.replace('.SH', '').replace('.SZ', '').replace('.SS', '')\
                                   .replace('.XSHE', '').replace('.XSHG', '').replace('.HK', '')

            #Queries for the last 30 days (expanded time frame)
            thirty_days_ago = datetime.now() - timedelta(days=30)

            #Try multiple queries (using symbol fields)
            query_list = [
                {'symbol': clean_code, 'publish_time': {'$gte': thirty_days_ago}},
                {'symbol': stock_code, 'publish_time': {'$gte': thirty_days_ago}},
                {'symbols': clean_code, 'publish_time': {'$gte': thirty_days_ago}},
                #If there is no news for the last 30 days, check all news (open-ended)
                {'symbol': clean_code},
                {'symbols': clean_code},
            ]

            news_items = []
            for query in query_list:
                cursor = collection.find(query).sort('publish_time', -1).limit(max_news)
                news_items = list(cursor)
                if news_items:
                    logger.info(f"[Uniform News Tool] 📊{query}Found it.{len(news_items)}News")
                    break

            if not news_items:
                logger.info(f"[Unified News Tool] Not found in database{stock_code}Public information")
                return ""

            #Format News
            report = f"# {stock_code} 最新新闻 (数据库缓存)\n\n"
            report += f"📅 查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            report += f"📊 新闻数量: {len(news_items)} 条\n\n"

            for i, news in enumerate(news_items, 1):
                title = news.get('title', '无标题')
                content = news.get('content', '') or news.get('summary', '')
                source = news.get('source', '未知来源')
                publish_time = news.get('publish_time', datetime.now())
                sentiment = news.get('sentiment', 'neutral')

                #Emotion Icon
                sentiment_icon = {
                    'positive': '📈',
                    'negative': '📉',
                    'neutral': '➖'
                }.get(sentiment, '➖')

                report += f"## {i}. {sentiment_icon} {title}\n\n"
                report += f"**来源**: {source} | **时间**: {publish_time.strftime('%Y-%m-%d %H:%M') if isinstance(publish_time, datetime) else publish_time}\n"
                report += f"**情绪**: {sentiment}\n\n"

                if content:
                    #Limit Content Length
                    content_preview = content[:500] + '...' if len(content) > 500 else content
                    report += f"{content_preview}\n\n"

                report += "---\n\n"

            logger.info(f"[Universal News Tool] ✅ successfully retrieved and formatted from the database{len(news_items)}News")
            return report

        except Exception as e:
            logger.error(f"[Uniform News Tool] Failed to get news from the database:{e}")
            import traceback
            logger.error(traceback.format_exc())
            return ""

    def _sync_news_from_akshare(self, stock_code: str, max_news: int = 10) -> bool:
        """Sync News from AKShare to Database (Sync Method)
        Use synchronized database client and event cycle in new threads to avoid incident cycle conflicts

        Args:
            Stock code: Stock code
            Max news: Maximum number of news

        Returns:
            Bool: Successfully synchronized
        """
        try:
            import asyncio
            import concurrent.futures

            #Standardised stock code (elimination of suffix)
            clean_code = stock_code.replace('.SH', '').replace('.SZ', '').replace('.SS', '')\
                                   .replace('.XSHE', '').replace('.XSHG', '').replace('.HK', '')

            logger.info(f"[Uniform News Tool] 🔄{clean_code}News...")

            #🔥 Run in a new thread using the sync database client
            def run_sync_in_new_thread():
                """Create a new cycle of events and run sync jobs in a new thread"""
                #Create New Event Cycle
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)

                try:
                    #Define a walk to get news
                    async def get_news_task():
                        try:
                            #Dynamic import AKShare program (right import path)
                            from tradingagents.dataflows.providers.china.akshare import AKShareProvider

                            #Create example of provider
                            provider = AKShareProvider()

                            #Call provider for news
                            news_data = await provider.get_stock_news(
                                symbol=clean_code,
                                limit=max_news
                            )

                            return news_data

                        except Exception as e:
                            logger.error(f"[Unified News Tool] ❌ Access to news failed:{e}")
                            import traceback
                            logger.error(traceback.format_exc())
                            return None

                    #Get news in a new cycle
                    news_data = new_loop.run_until_complete(get_news_task())

                    if not news_data:
                        logger.warning(f"[Uniform News Tool] ⚠️ Not available for news data")
                        return False

                    logger.info(f"[Unified News Tool] 📥{len(news_data)}News")

                    #🔥 Save to database using sync (not dependent on event cycle)
                    from app.services.news_data_service import NewsDataService

                    news_service = NewsDataService()
                    saved_count = news_service.save_news_data_sync(
                        news_data=news_data,
                        data_source="akshare",
                        market="CN"
                    )

                    logger.info(f"[Unified News Tool] ✅{saved_count}News")
                    return saved_count > 0

                finally:
                    #Clear the cycle of events
                    new_loop.close()

            #Execution in online pool
            logger.info(f"[Uniform News Tool] Run synchronized missions in new threads to avoid circular conflict of events")
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(run_sync_in_new_thread)
                result = future.result(timeout=30)  #30 seconds past time.
                return result

        except concurrent.futures.TimeoutError:
            logger.error(f"[Uniform News Tool] ❌ Sync News Timeout (30 seconds)")
            return False
        except Exception as e:
            logger.error(f"[Unified News Tool] ❌ Synchronising News Failed:{e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def _get_a_share_news(self, stock_code: str, max_news: int, model_info: str = "") -> str:
        """Access to Unit A information"""
        logger.info(f"Access to Unit A{stock_code}Public information")

        #Fetch the current date
        curr_date = datetime.now().strftime("%Y-%m-%d")

        #Priority 0: Access to information from databases (highest priority)
        try:
            logger.info(f"[Uniform public information tool] 🔍{stock_code}News...")
            db_news = self._get_news_from_database(stock_code, max_news)
            if db_news:
                logger.info(f"[Uniform News Tool] ✅ Database news acquisition success:{len(db_news)}Character")
                return self._format_news_result(db_news, "数据库缓存", model_info)
            else:
                logger.info(f"[Uniform News Tool] Not available in database ⚠️{stock_code}News, trying to synchronize...")

                #🔥 <SyncSyncSync News when data is not available
                try:
                    logger.info(f"[Uniform News Tool] 📡 Calling Synchronization Service{stock_code}News...")
                    synced_news = self._sync_news_from_akshare(stock_code, max_news)

                    if synced_news:
                        logger.info(f"[Uniform News Tool] ✅ Synchronize successfully and re-access the database...")
                        #Retake From Database
                        db_news = self._get_news_from_database(stock_code, max_news)
                        if db_news:
                            logger.info(f"[Unified News Tool] ✅ Synced database news acquisition success:{len(db_news)}Character")
                            return self._format_news_result(db_news, "数据库缓存(新同步)", model_info)
                    else:
                        logger.warning(f"[Uniform News Tool] ⚠️ Synchronization service does not return news data")

                except Exception as sync_error:
                    logger.warning(f"[Uniform News Tool] ⚠️ Synchronization service call failed:{sync_error}")

                logger.info(f"[Uniform News Tool] ⚠️ Synchronize without data and try other data sources...")
        except Exception as e:
            logger.warning(f"[Unified News Tool] Database news acquisition failed:{e}")

        #Priority 1: Real-time East Wealth News
        try:
            if hasattr(self.toolkit, 'get_realtime_stock_news'):
                logger.info(f"[Uniform News Tool] Try Eastern Wealth Real Time News...")
                #Correct call method using the LangChain tool:.invoke() method and dictionary parameters
                result = self.toolkit.get_realtime_stock_news.invoke({"ticker": stock_code, "curr_date": curr_date})
                
                #A detailed record of the return of Eastern wealth.
                logger.info(f"[Uniform News Tool] 📊 The East Wealth Returns Content Length:{len(result) if result else 0}Character")
                logger.info(f"[Unional News Tool] 📋 East Wealth Returning Content Preview (prefix 500 characters):{result[:500] if result else 'None'}")
                
                if result and len(result.strip()) > 100:
                    logger.info(f"[Unional News Tool] ✅ Eastern Wealth News Success:{len(result)}Character")
                    return self._format_news_result(result, "东方财富实时新闻", model_info)
                else:
                    logger.warning(f"[Uniform News Tool] ⚠️ East Wealth News is too short or empty")
        except Exception as e:
            logger.warning(f"[Unional News Tool] Eastern Wealth News Failed:{e}")
        
        #Priority 2: Google News
        try:
            if hasattr(self.toolkit, 'get_google_news'):
                logger.info(f"[Union News Tool] Try Google News...")
                query = f"{stock_code} 股票 新闻 财报 业绩"
                #Correct call method using the LangChain tool:.invoke() method and dictionary parameters
                result = self.toolkit.get_google_news.invoke({"query": query, "curr_date": curr_date})
                if result and len(result.strip()) > 50:
                    logger.info(f"[Unified News Tool] ✅ Google News Access Success:{len(result)}Character")
                    return self._format_news_result(result, "Google新闻", model_info)
        except Exception as e:
            logger.warning(f"Google News Failed:{e}")
        
        #Priority 3: OpenAI Global News
        try:
            if hasattr(self.toolkit, 'get_global_news_openai'):
                logger.info(f"[Union News Tool] Try OpenAI Global News...")
                #Correct call method using the LangChain tool:.invoke() method and dictionary parameters
                result = self.toolkit.get_global_news_openai.invoke({"curr_date": curr_date})
                if result and len(result.strip()) > 50:
                    logger.info(f"[Uniform News Tool] ✅OpenAI News Access Success:{len(result)}Character")
                    return self._format_news_result(result, "OpenAI全球新闻", model_info)
        except Exception as e:
            logger.warning(f"OpenAI news access failed:{e}")
        
        return "❌ 无法获取A股新闻数据，所有新闻源均不可用"
    
    def _get_hk_share_news(self, stock_code: str, max_news: int, model_info: str = "") -> str:
        """Access to information in the Port Unit"""
        logger.info(f"[Uniform information tool] Access Port Unit{stock_code}Public information")
        
        #Fetch the current date
        curr_date = datetime.now().strftime("%Y-%m-%d")
        
        #Priority 1: Google News (Hong Kong Unit search)
        try:
            if hasattr(self.toolkit, 'get_google_news'):
                logger.info(f"[Unified News Tool] Try Google Port News...")
                query = f"{stock_code} 港股 香港股票 新闻"
                #Correct call method using the LangChain tool:.invoke() method and dictionary parameters
                result = self.toolkit.get_google_news.invoke({"query": query, "curr_date": curr_date})
                if result and len(result.strip()) > 50:
                    logger.info(f"[Uniform News Tool] ✅{len(result)}Character")
                    return self._format_news_result(result, "Google港股新闻", model_info)
        except Exception as e:
            logger.warning(f"[Uniform News Tool] Google Port Unit news access failed:{e}")
        
        #Priority 2: OpenAI Global News
        try:
            if hasattr(self.toolkit, 'get_global_news_openai'):
                logger.info(f"[Unified News Tool] Try OpenAI Port News...")
                #Correct call method using the LangChain tool:.invoke() method and dictionary parameters
                result = self.toolkit.get_global_news_openai.invoke({"curr_date": curr_date})
                if result and len(result.strip()) > 50:
                    logger.info(f"[Uniform News Tool] ✅{len(result)}Character")
                    return self._format_news_result(result, "OpenAI港股新闻", model_info)
        except Exception as e:
            logger.warning(f"[Uniform News Tool] Failed to get news from OpenAI Port Unit:{e}")
        
        #Priority 3: Real-time news (if supporting the Port Unit)
        try:
            if hasattr(self.toolkit, 'get_realtime_stock_news'):
                logger.info(f"[Unified News Tool] Try Real-Time Port News...")
                #Correct call method using the LangChain tool:.invoke() method and dictionary parameters
                result = self.toolkit.get_realtime_stock_news.invoke({"ticker": stock_code, "curr_date": curr_date})
                if result and len(result.strip()) > 100:
                    logger.info(f"[Unified News Tool] ✅ Real-Time Port Unit news acquisition success:{len(result)}Character")
                    return self._format_news_result(result, "实时港股新闻", model_info)
        except Exception as e:
            logger.warning(f"[Unified News Tool] The Real-Time Port Unit News Failed:{e}")
        
        return "❌ 无法获取港股新闻数据，所有新闻源均不可用"
    
    def _get_us_share_news(self, stock_code: str, max_news: int, model_info: str = "") -> str:
        """Access to American News"""
        logger.info(f"[Uniform public information tool]{stock_code}Public information")
        
        #Fetch the current date
        curr_date = datetime.now().strftime("%Y-%m-%d")
        
        #Priority 1: OpenAI Global News
        try:
            if hasattr(self.toolkit, 'get_global_news_openai'):
                logger.info(f"[Unional News Tool] Try OpenAI America News...")
                #Correct call method using the LangChain tool:.invoke() method and dictionary parameters
                result = self.toolkit.get_global_news_openai.invoke({"curr_date": curr_date})
                if result and len(result.strip()) > 50:
                    logger.info(f"[Uniform News Tool] ✅ OpenAI US News Access Success:{len(result)}Character")
                    return self._format_news_result(result, "OpenAI美股新闻", model_info)
        except Exception as e:
            logger.warning(f"[Unional News Tool] OpenAI US News Failed:{e}")
        
        #Priority 2: Google News
        try:
            if hasattr(self.toolkit, 'get_google_news'):
                logger.info(f"[Unified News Tool] Try Google America News...")
                query = f"{stock_code} stock news earnings financial"
                #Correct call method using the LangChain tool:.invoke() method and dictionary parameters
                result = self.toolkit.get_google_news.invoke({"query": query, "curr_date": curr_date})
                if result and len(result.strip()) > 50:
                    logger.info(f"[Uniform News Tool] ✅United States News Access Success:{len(result)}Character")
                    return self._format_news_result(result, "Google美股新闻", model_info)
        except Exception as e:
            logger.warning(f"The Google U.S. News has failed:{e}")
        
        #Priority 3: FinnHub News (if available)
        try:
            if hasattr(self.toolkit, 'get_finnhub_news'):
                logger.info(f"[Unified News Tool] Try Finn Hub News...")
                #Correct call method using the LangChain tool:.invoke() method and dictionary parameters
                result = self.toolkit.get_finnhub_news.invoke({"symbol": stock_code, "max_results": min(max_news, 50)})
                if result and len(result.strip()) > 50:
                    logger.info(f"[Unional News Tool] ✅FinnHub's News Access Success:{len(result)}Character")
                    return self._format_news_result(result, "FinnHub美股新闻", model_info)
        except Exception as e:
            logger.warning(f"[Unified News Tool] FinnHub's US News Access Failed:{e}")
        
        return "❌ 无法获取美股新闻数据，所有新闻源均不可用"
    
    def _format_news_result(self, news_content: str, source: str, model_info: str = "") -> str:
        """Format news results"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        #Add debugging log: Print original news content
        logger.info(f"[Universal News Tool] 📋 Original News Content Preview (pre-500 characters):{news_content[:500]}")
        logger.info(f"[Universal News Tool] 📊 Original content length:{len(news_content)}Character")
        
        #Test for Google/Gemini model
        is_google_model = any(keyword in model_info.lower() for keyword in ['google', 'gemini', 'gemma'])
        original_length = len(news_content)
        google_control_applied = False
        
        #Add Google Model Test Log 🔍
        if is_google_model:
            logger.info(f"[Uniform News Tool] 🤖 detected Google model and enabled special handling")
        
        #Special length control of Google models
        if is_google_model and len(news_content) > 5000:  #Lower threshold to 5,000 characters
            logger.warning(f"[Uniform News Tool] 🔧 detected Google model with too much news content.{len(news_content)}Character) for length control...")
            
            #Stricter Length Control Policy
            lines = news_content.split('\n')
            important_lines = []
            char_count = 0
            target_length = 3000  #Set target length at 3,000 words Arguments
            
            #First round: Precedence retention of key words Okay.
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                    
                #Check to include key words
                important_keywords = ['股票', '公司', '财报', '业绩', '涨跌', '价格', '市值', '营收', '利润', 
                                    '增长', '下跌', '上涨', '盈利', '亏损', '投资', '分析', '预期', '公告']
                
                is_important = any(keyword in line for keyword in important_keywords)
                
                if is_important and char_count + len(line) < target_length:
                    important_lines.append(line)
                    char_count += len(line)
                elif not is_important and char_count + len(line) < target_length * 0.7:  #It's not important.
                    important_lines.append(line)
                    char_count += len(line)
                
                #Stop adding if the target length is reached
                if char_count >= target_length:
                    break
            
            #If the material extracted is still too long, further cut.
            if important_lines:
                processed_content = '\n'.join(important_lines)
                if len(processed_content) > target_length:
                    processed_content = processed_content[:target_length] + "...(内容已智能截断)"
                
                news_content = processed_content
                google_control_applied = True
                logger.info(f"[Uniform News Tool] ✅ Google model smart length control completed from{original_length}Character compression to{len(news_content)}Character")
            else:
                #If there is no major line, cut directly to target length
                news_content = news_content[:target_length] + "...(内容已强制截断)"
                google_control_applied = True
                logger.info(f"[Uniform News Tool] Forced break to the Google model ⚠️{target_length}Character")
        
        #Calculate the length of the final formatting results to ensure that the total length is reasonable
        base_format_length = 300  #The approximate length of the format template
        if is_google_model and (len(news_content) + base_format_length) > 4000:
            #If the format is still too long, then the news content will be further compressed.
            max_content_length = 3500
            if len(news_content) > max_content_length:
                news_content = news_content[:max_content_length] + "...(已优化长度)"
                google_control_applied = True
                logger.info(f"[Uniform public information tool] 🔧 Google model final length optimized, content length:{len(news_content)}Character")
        
        formatted_result = f"""
=== 📰 新闻数据来源: {source} ===
获取时间: {timestamp}
数据长度: {len(news_content)} 字符
{f"模型类型: {model_info}" if model_info else ""}
{f"🔧 Google模型长度控制已应用 (原长度: {original_length} 字符)" if google_control_applied else ""}

=== 📋 新闻内容 ===
{news_content}

=== ✅ 数据状态 ===
状态: 成功获取
来源: {source}
时间戳: {timestamp}
"""
        return formatted_result.strip()


def create_unified_news_tool(toolkit):
    """Create unified news tool function"""
    analyzer = UnifiedNewsAnalyzer(toolkit)
    
    def get_stock_news_unified(stock_code: str, max_news: int = 100, model_info: str = ""):
        """Unified news access tool

        Args:
            Stock code(str): Equities code (support A shares like 000001, Hong Kong shares like 0.700.HK, United States shares like AAPL)
            max news(int): Maximum number of news, default 100
            Model info(str): Model information currently in use for special handling

        Returns:
            st: Formatted news content
        """
        if not stock_code:
            return "❌ 错误: 未提供股票代码"
        
        return analyzer.get_stock_news_unified(stock_code, max_news, model_info)
    
    #Set Tool Properties
    get_stock_news_unified.name = "get_stock_news_unified"
    get_stock_news_unified.description = """
统一新闻获取工具 - 根据股票代码自动获取相应市场的新闻

功能:
- 自动识别股票类型（A股/港股/美股）
- 根据股票类型选择最佳新闻源
- A股: 优先东方财富 -> Google中文 -> OpenAI
- 港股: 优先Google -> OpenAI -> 实时新闻
- 美股: 优先OpenAI -> Google英文 -> FinnHub
- 返回格式化的新闻内容
- 支持Google模型的特殊长度控制
"""
    
    return get_stock_news_unified