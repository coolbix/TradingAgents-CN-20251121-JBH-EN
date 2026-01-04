from langchain_core.messages import BaseMessage, HumanMessage, ToolMessage, AIMessage
from typing import List
from typing import Annotated
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import RemoveMessage
from langchain_core.tools import tool
from datetime import date, timedelta, datetime
import functools
import pandas as pd
import os
from dateutil.relativedelta import relativedelta
from langchain_openai import ChatOpenAI
import tradingagents.dataflows.interface as interface
from tradingagents.default_config import DEFAULT_CONFIG
from langchain_core.messages import HumanMessage

#Import Unified Log System and Tool Log Decorator
from tradingagents.utils.logging_init import get_logger
from tradingagents.utils.tool_logging import log_tool_call, log_analysis_step

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')


def create_msg_delete():
    def delete_messages(state):
        """Clear messages and add placeholder for Anthropic compatibility"""
        messages = state["messages"]
        
        # Remove all messages
        removal_operations = [RemoveMessage(id=m.id) for m in messages]
        
        # Add a minimal placeholder message
        placeholder = HumanMessage(content="Continue")
        
        return {"messages": removal_operations + [placeholder]}
    
    return delete_messages


class Toolkit:
    _config = DEFAULT_CONFIG.copy()

    @classmethod
    def update_config(cls, config):
        """Update the class-level configuration."""
        cls._config.update(config)

    @property
    def config(self):
        """Access the configuration."""
        return self._config

    def __init__(self, config=None):
        if config:
            self.update_config(config)

    @staticmethod
    @tool
    def get_reddit_news(
        curr_date: Annotated[str, "Date you want to get news for in yyyy-mm-dd format"],
    ) -> str:
        """
        Retrieve global news from Reddit within a specified time frame.
        Args:
            curr_date (str): Date you want to get news for in yyyy-mm-dd format
        Returns:
            str: A formatted dataframe containing the latest global news from Reddit in the specified time frame.
        """
        
        global_news_result = interface.get_reddit_global_news(curr_date, 7, 5)

        return global_news_result

    @staticmethod
    @tool
    def get_finnhub_news(
        ticker: Annotated[
            str,
            "Search query of a company, e.g. 'AAPL, TSM, etc.",
        ],
        start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
        end_date: Annotated[str, "End date in yyyy-mm-dd format"],
    ):
        """
        Retrieve the latest news about a given stock from Finnhub within a date range
        Args:
            ticker (str): Ticker of a company. e.g. AAPL, TSM
            start_date (str): Start date in yyyy-mm-dd format
            end_date (str): End date in yyyy-mm-dd format
        Returns:
            str: A formatted dataframe containing news about the company within the date range from start_date to end_date
        """

        end_date_str = end_date

        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        look_back_days = (end_date - start_date).days

        finnhub_news_result = interface.get_finnhub_news(
            ticker, end_date_str, look_back_days
        )

        return finnhub_news_result

    @staticmethod
    @tool
    def get_reddit_stock_info(
        ticker: Annotated[
            str,
            "Ticker of a company. e.g. AAPL, TSM",
        ],
        curr_date: Annotated[str, "Current date you want to get news for"],
    ) -> str:
        """
        Retrieve the latest news about a given stock from Reddit, given the current date.
        Args:
            ticker (str): Ticker of a company. e.g. AAPL, TSM
            curr_date (str): current date in yyyy-mm-dd format to get news for
        Returns:
            str: A formatted dataframe containing the latest news about the company on the given date
        """

        stock_news_results = interface.get_reddit_company_news(ticker, curr_date, 7, 5)

        return stock_news_results

    @staticmethod
    @tool
    def get_chinese_social_sentiment(
        ticker: Annotated[str, "Ticker of a company. e.g. AAPL, TSM"],
        curr_date: Annotated[str, "Current date in yyyy-mm-dd format"],
    ) -> str:
        """Access to emotional analysis and discussion on selected stocks on social media and financial platforms in China.
        China's local platforms, such as snowballs, Eastern Wealth Bars, and New Waves.
        Args:
            ticker (str): Stock codes such as AAPL, TSM
            Curr date(str): Current date, format is yyyy-mm-dd
        Returns:
            str: Formatted report containing analysis of Chinese investors ' emotions, discussion of heat, key views
        """
        try:
            #This is where data from multiple Chinese platforms can be integrated.
            chinese_sentiment_results = interface.get_chinese_social_sentiment(ticker, curr_date)
            return chinese_sentiment_results
        except Exception as e:
            #If Chinese platform data acquisition fails, back to old Reddit data
            return interface.get_reddit_company_news(ticker, curr_date, 7, 5)

    @staticmethod
    #@tool # removed: please use get stock fundamentals unified or get stock mark data unified
    def get_china_stock_data(
        stock_code: Annotated[str, "中国股票代码，如 000001(平安银行), 600519(贵州茅台)"],
        start_date: Annotated[str, "开始日期，格式 yyyy-mm-dd"],
        end_date: Annotated[str, "结束日期，格式 yyyy-mm-dd"],
    ) -> str:
        """Obtain real-time and historical data from China A and provide professional stock data through high-quality data sources such as Tushare.
        Support for comprehensive data, such as real-time patterns, historical K-lines, technical indicators, and automatic use of best data sources.
        Args:
            Stock code(str): Chinese stock code, e.g. 000001 (Peace Bank), 600519 (Guizhou Shao Tai)
            Start date(str): Start date, format yyyy-mm-dd
            End date(str): End date, format yyyy-mm-dd
        Returns:
            str: Complete stock analysis with real-time performance, historical data, technical indicators
        """
        try:
            logger.debug(f"== sync, corrected by elderman == @elder man")
            logger.debug(f"[DBUG] Parameter: stock code={stock_code}, start_date={start_date}, end_date={end_date}")

            from tradingagents.dataflows.interface import get_china_stock_data_unified
            logger.debug(f"📊 [DBUG] Successfully imported UDI interface")

            logger.debug(f"📊 [DEBUG] is calling the UDI...")
            result = get_china_stock_data_unified(stock_code, start_date, end_date)

            logger.debug(f"📊 [DBUG] Unified data source interface call complete")
            logger.debug(f"[DBUG] returns the result type:{type(result)}")
            logger.debug(f"[DEBUG] Return result length:{len(result) if result else 0}")
            logger.debug(f"[DEBUG] returns 200 characters before the result:{str(result)[:200]}...")
            logger.debug(f"== sync, corrected by elderman == @elder man")

            return result
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            logger.error(f"== sync, corrected by elderman == @elder man")
            logger.error(f"[DBUG] Error type:{type(e).__name__}")
            logger.error(f"[DEBUG] Error message:{str(e)}")
            logger.error(f"[DEBUG] Detailed stacks:")
            print(error_details)
            logger.error(f"== sync, corrected by elderman == @elder man")
            return f"中国股票数据获取失败: {str(e)}。请检查网络连接或稍后重试。"

    @staticmethod
    @tool
    def get_china_market_overview(
        curr_date: Annotated[str, "当前日期，格式 yyyy-mm-dd"],
    ) -> str:
        """Get an overview of the Chinese stock market as a whole, including real-time performance of key indicators.
        It covers key indicators such as the above-documented index, the in-depth evidence index, the entrepreneurship index and the 50-year-old.
        Args:
            Curr date(str): Current date, format yyyy-mm-dd
        Returns:
            str: Market overview report with real-time information on key indicators
        """
        try:
            #Use Tushare to obtain key index data
            from tradingagents.dataflows.providers.china.tushare import get_tushare_adapter

            adapter = get_tushare_adapter()


            #Use Tushare to access key index information
            #It can be expanded to capture specific index data.
            return f"""# 中国股市概览 - {curr_date}

## 📊 主要指数
- 上证指数: 数据获取中...
- 深证成指: 数据获取中...
- 创业板指: 数据获取中...
- 科创50: 数据获取中...

## 💡 说明
市场概览功能正在从TDX迁移到Tushare，完整功能即将推出。
当前可以使用股票数据获取功能分析个股。

数据来源: Tushare专业数据源
更新时间: {curr_date}
"""

        except Exception as e:
            return f"中国市场概览获取失败: {str(e)}。正在从TDX迁移到Tushare数据源。"

    @staticmethod
    @tool
    def get_YFin_data(
        symbol: Annotated[str, "ticker symbol of the company"],
        start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
        end_date: Annotated[str, "End date in yyyy-mm-dd format"],
    ) -> str:
        """
        Retrieve the stock price data for a given ticker symbol from Yahoo Finance.
        Args:
            symbol (str): Ticker symbol of the company, e.g. AAPL, TSM
            start_date (str): Start date in yyyy-mm-dd format
            end_date (str): End date in yyyy-mm-dd format
        Returns:
            str: A formatted dataframe containing the stock price data for the specified ticker symbol in the specified date range.
        """

        result_data = interface.get_YFin_data(symbol, start_date, end_date)

        return result_data

    @staticmethod
    @tool
    def get_YFin_data_online(
        symbol: Annotated[str, "ticker symbol of the company"],
        start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
        end_date: Annotated[str, "End date in yyyy-mm-dd format"],
    ) -> str:
        """
        Retrieve the stock price data for a given ticker symbol from Yahoo Finance.
        Args:
            symbol (str): Ticker symbol of the company, e.g. AAPL, TSM
            start_date (str): Start date in yyyy-mm-dd format
            end_date (str): End date in yyyy-mm-dd format
        Returns:
            str: A formatted dataframe containing the stock price data for the specified ticker symbol in the specified date range.
        """

        result_data = interface.get_YFin_data_online(symbol, start_date, end_date)

        return result_data

    @staticmethod
    @tool
    def get_stockstats_indicators_report(
        symbol: Annotated[str, "ticker symbol of the company"],
        indicator: Annotated[
            str, "technical indicator to get the analysis and report of"
        ],
        curr_date: Annotated[
            str, "The current trading date you are trading on, YYYY-mm-dd"
        ],
        look_back_days: Annotated[int, "how many days to look back"] = 30,
    ) -> str:
        """
        Retrieve stock stats indicators for a given ticker symbol and indicator.
        Args:
            symbol (str): Ticker symbol of the company, e.g. AAPL, TSM
            indicator (str): Technical indicator to get the analysis and report of
            curr_date (str): The current trading date you are trading on, YYYY-mm-dd
            look_back_days (int): How many days to look back, default is 30
        Returns:
            str: A formatted dataframe containing the stock stats indicators for the specified ticker symbol and indicator.
        """

        result_stockstats = interface.get_stock_stats_indicators_window(
            symbol, indicator, curr_date, look_back_days, False
        )

        return result_stockstats

    @staticmethod
    @tool
    def get_stockstats_indicators_report_online(
        symbol: Annotated[str, "ticker symbol of the company"],
        indicator: Annotated[
            str, "technical indicator to get the analysis and report of"
        ],
        curr_date: Annotated[
            str, "The current trading date you are trading on, YYYY-mm-dd"
        ],
        look_back_days: Annotated[int, "how many days to look back"] = 30,
    ) -> str:
        """
        Retrieve stock stats indicators for a given ticker symbol and indicator.
        Args:
            symbol (str): Ticker symbol of the company, e.g. AAPL, TSM
            indicator (str): Technical indicator to get the analysis and report of
            curr_date (str): The current trading date you are trading on, YYYY-mm-dd
            look_back_days (int): How many days to look back, default is 30
        Returns:
            str: A formatted dataframe containing the stock stats indicators for the specified ticker symbol and indicator.
        """

        result_stockstats = interface.get_stock_stats_indicators_window(
            symbol, indicator, curr_date, look_back_days, True
        )

        return result_stockstats

    @staticmethod
    @tool
    def get_finnhub_company_insider_sentiment(
        ticker: Annotated[str, "ticker symbol for the company"],
        curr_date: Annotated[
            str,
            "current date of you are trading at, yyyy-mm-dd",
        ],
    ):
        """
        Retrieve insider sentiment information about a company (retrieved from public SEC information) for the past 30 days
        Args:
            ticker (str): ticker symbol of the company
            curr_date (str): current date you are trading at, yyyy-mm-dd
        Returns:
            str: a report of the sentiment in the past 30 days starting at curr_date
        """

        data_sentiment = interface.get_finnhub_company_insider_sentiment(
            ticker, curr_date, 30
        )

        return data_sentiment

    @staticmethod
    @tool
    def get_finnhub_company_insider_transactions(
        ticker: Annotated[str, "ticker symbol"],
        curr_date: Annotated[
            str,
            "current date you are trading at, yyyy-mm-dd",
        ],
    ):
        """
        Retrieve insider transaction information about a company (retrieved from public SEC information) for the past 30 days
        Args:
            ticker (str): ticker symbol of the company
            curr_date (str): current date you are trading at, yyyy-mm-dd
        Returns:
            str: a report of the company's insider transactions/trading information in the past 30 days
        """

        data_trans = interface.get_finnhub_company_insider_transactions(
            ticker, curr_date, 30
        )

        return data_trans

    @staticmethod
    @tool
    def get_simfin_balance_sheet(
        ticker: Annotated[str, "ticker symbol"],
        freq: Annotated[
            str,
            "reporting frequency of the company's financial history: annual/quarterly",
        ],
        curr_date: Annotated[str, "current date you are trading at, yyyy-mm-dd"],
    ):
        """
        Retrieve the most recent balance sheet of a company
        Args:
            ticker (str): ticker symbol of the company
            freq (str): reporting frequency of the company's financial history: annual / quarterly
            curr_date (str): current date you are trading at, yyyy-mm-dd
        Returns:
            str: a report of the company's most recent balance sheet
        """

        data_balance_sheet = interface.get_simfin_balance_sheet(ticker, freq, curr_date)

        return data_balance_sheet

    @staticmethod
    @tool
    def get_simfin_cashflow(
        ticker: Annotated[str, "ticker symbol"],
        freq: Annotated[
            str,
            "reporting frequency of the company's financial history: annual/quarterly",
        ],
        curr_date: Annotated[str, "current date you are trading at, yyyy-mm-dd"],
    ):
        """
        Retrieve the most recent cash flow statement of a company
        Args:
            ticker (str): ticker symbol of the company
            freq (str): reporting frequency of the company's financial history: annual / quarterly
            curr_date (str): current date you are trading at, yyyy-mm-dd
        Returns:
            str: a report of the company's most recent cash flow statement
        """

        data_cashflow = interface.get_simfin_cashflow(ticker, freq, curr_date)

        return data_cashflow

    @staticmethod
    @tool
    def get_simfin_income_stmt(
        ticker: Annotated[str, "ticker symbol"],
        freq: Annotated[
            str,
            "reporting frequency of the company's financial history: annual/quarterly",
        ],
        curr_date: Annotated[str, "current date you are trading at, yyyy-mm-dd"],
    ):
        """
        Retrieve the most recent income statement of a company
        Args:
            ticker (str): ticker symbol of the company
            freq (str): reporting frequency of the company's financial history: annual / quarterly
            curr_date (str): current date you are trading at, yyyy-mm-dd
        Returns:
            str: a report of the company's most recent income statement
        """

        data_income_stmt = interface.get_simfin_income_statements(
            ticker, freq, curr_date
        )

        return data_income_stmt

    @staticmethod
    @tool
    def get_google_news(
        query: Annotated[str, "Query to search with"],
        curr_date: Annotated[str, "Curr date in yyyy-mm-dd format"],
    ):
        """
        Retrieve the latest news from Google News based on a query and date range.
        Args:
            query (str): Query to search with
            curr_date (str): Current date in yyyy-mm-dd format
            look_back_days (int): How many days to look back
        Returns:
            str: A formatted string containing the latest news from Google News based on the query and date range.
        """

        google_news_results = interface.get_google_news(query, curr_date, 7)

        return google_news_results

    @staticmethod
    @tool
    def get_realtime_stock_news(
        ticker: Annotated[str, "Ticker of a company. e.g. AAPL, TSM"],
        curr_date: Annotated[str, "Current date in yyyy-mm-dd format"],
    ) -> str:
        """Access to real-time news analysis of equities to address the lag in traditional news sources.
        Integration of a number of professional financial services API, providing updates in 15-30 minutes.
        Support for multi-source query mechanisms, giving priority to the use of real-time news aggregaters and the automatic attempt of back-up sources in case of failure.
        For both Unit A and the Port Unit, preference is given to Chinese-language financial and economic news sources (e.g. Eastern Wealth).

        Args:
            ticker (str): Stock codes such as AAPL, TSM, 600036.SH
            Curr date(str): Current date, format is yyyy-mm-dd
        Returns:
            str: Formatted reports containing real-time news analysis, emergency assessments, time-bound statements
        """
        from tradingagents.dataflows.realtime_news_utils import get_realtime_stock_news
        return get_realtime_stock_news(ticker, curr_date, hours_back=6)

    @staticmethod
    @tool
    def get_stock_news_openai(
        ticker: Annotated[str, "the company's ticker"],
        curr_date: Annotated[str, "Current date in yyyy-mm-dd format"],
    ):
        """
        Retrieve the latest news about a given stock by using OpenAI's news API.
        Args:
            ticker (str): Ticker of a company. e.g. AAPL, TSM
            curr_date (str): Current date in yyyy-mm-dd format
        Returns:
            str: A formatted string containing the latest news about the company on the given date.
        """

        openai_news_results = interface.get_stock_news_openai(ticker, curr_date)

        return openai_news_results

    @staticmethod
    @tool
    def get_global_news_openai(
        curr_date: Annotated[str, "Current date in yyyy-mm-dd format"],
    ):
        """
        Retrieve the latest macroeconomics news on a given date using OpenAI's macroeconomics news API.
        Args:
            curr_date (str): Current date in yyyy-mm-dd format
        Returns:
            str: A formatted string containing the latest macroeconomic news on the given date.
        """

        openai_news_results = interface.get_global_news_openai(curr_date)

        return openai_news_results

    @staticmethod
    #@tool# removed: get stock fundamentals unified
    def get_fundamentals_openai(
        ticker: Annotated[str, "the company's ticker"],
        curr_date: Annotated[str, "Current date in yyyy-mm-dd format"],
    ):
        """
        Retrieve the latest fundamental information about a given stock on a given date by using OpenAI's news API.
        Args:
            ticker (str): Ticker of a company. e.g. AAPL, TSM
            curr_date (str): Current date in yyyy-mm-dd format
        Returns:
            str: A formatted string containing the latest fundamental information about the company on the given date.
        """
        logger.debug(f"[DBUG] get fundamentals openai called: ticker={ticker}, date={curr_date}")

        #Check for Chinese stocks.
        import re
        if re.match(r'^\d{6}$', str(ticker)):
            logger.debug(f"[DEBUG]{ticker}")
            #Acquisition of Chinese stock names using a single interface
            try:
                from tradingagents.dataflows.interface import get_china_stock_info_unified
                stock_info = get_china_stock_info_unified(ticker)

                #Parsing stock name
                if "股票名称:" in stock_info:
                    company_name = stock_info.split("股票名称:")[1].split("\n")[0].strip()
                else:
                    company_name = f"股票代码{ticker}"

                logger.debug(f"[DBUG] Chinese stock name map:{ticker} -> {company_name}")
            except Exception as e:
                logger.error(f"⚠️ [DBUG] Failed to retrieve stock names from a unified interface:{e}")
                company_name = f"股票代码{ticker}"

            #Modify query to contain the correct corporate name
            modified_query = f"{company_name}({ticker})"
            logger.debug(f"📊 [DBUG]{modified_query}")
        else:
            logger.debug(f"[DBUG]{ticker}")
            modified_query = ticker

        try:
            openai_fundamentals_results = interface.get_fundamentals_openai(
                modified_query, curr_date
            )
            logger.debug(f"[DBUG] OpenAI Basic Analysis Length:{len(openai_fundamentals_results) if openai_fundamentals_results else 0}")
            return openai_fundamentals_results
        except Exception as e:
            logger.error(f"[DBUG] OpenAI Basic Analysis failed:{str(e)}")
            return f"基本面分析失败: {str(e)}"

    @staticmethod
    #@tool# removed: get stock fundamentals unified
    def get_china_fundamentals(
        ticker: Annotated[str, "中国A股股票代码，如600036"],
        curr_date: Annotated[str, "当前日期，格式为yyyy-mm-dd"],
    ):
        """Access to basic face-to-face information on Chinese stock A, using Chinese stock data sources.
        Args:
            ticker (str): Chinese stock code A, e.g. 6,00036 000001
            curr date(str): Current date in yyyy-mm-dd
        Returns:
            st: Formatted string with basic face information on shares
        """
        logger.debug(f"[DEBUG] get china fundamentals called: ticker={ticker}, date={curr_date}")

        #Check for Chinese stocks.
        import re
        if not re.match(r'^\d{6}$', str(ticker)):
            return f"错误：{ticker} 不是有效的中国A股代码格式"

        try:
            #Access to stock data using the unified data source interface (default Tushare to support backup data sources)
            from tradingagents.dataflows.interface import get_china_stock_data_unified
            logger.debug(f"[DBUG]{ticker}Stock data...")

            #Access to the most recent 30-day data for basic face analysis
            from datetime import datetime, timedelta
            end_date = datetime.strptime(curr_date, '%Y-%m-%d')
            start_date = end_date - timedelta(days=30)

            stock_data = get_china_stock_data_unified(
                ticker,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )

            logger.debug(f"[DBUG] Stock data acquisition complete, length:{len(stock_data) if stock_data else 0}")

            if not stock_data or "获取失败" in stock_data or "❌" in stock_data:
                return f"无法获取股票 {ticker} 的基本面数据：{stock_data}"

            #Call a real fundamental analysis.
            from tradingagents.dataflows.optimized_china_data import OptimizedChinaDataProvider

            #Create analyser instance
            analyzer = OptimizedChinaDataProvider()

            #Generate real basic analysis
            fundamentals_report = analyzer._generate_fundamentals_report(ticker, stock_data)

            logger.debug(f"[DBUG] Production of basic face analysis for China is complete.")
            logger.debug(f"[DBUG] get china fundamentals:{len(fundamentals_report)}")

            return fundamentals_report

        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            logger.error(f"[DBUG] get china fundamentals failed:")
            logger.error(f"[DEBUG] Error:{str(e)}")
            logger.error(f"[DEBUG] Stack:{error_details}")
            return f"中国股票基本面分析失败: {str(e)}"

    @staticmethod
    #@tool # removed: please use get stock fundamentals unified or get stock mark data unified
    def get_hk_stock_data_unified(
        symbol: Annotated[str, "港股代码，如：0700.HK、9988.HK等"],
        start_date: Annotated[str, "开始日期，格式：YYYY-MM-DD"],
        end_date: Annotated[str, "结束日期，格式：YYYY-MM-DD"]
    ) -> str:
        """Harmonization of access to port unit data, priority use of AKShare data source, backup Yahoo Finance

        Args:
            Symbol: Port Unit Code (e.g. 0700.HK)
            Start date: Start date (YYYYY-MM-DD)
            End date: End Date (YYYYY-MM-DD)

        Returns:
            str: Formatted Port Unit data
        """
        logger.debug(f"[DBUG] get hk stock data unified: symbol={symbol}, start_date={start_date}, end_date={end_date}")

        try:
            from tradingagents.dataflows.interface import get_hk_stock_data_unified

            result = get_hk_stock_data_unified(symbol, start_date, end_date)

            logger.debug(f"[DBUG] Port Unit data acquisition completed, length:{len(result) if result else 0}")

            return result

        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            logger.error(f"[DEBUG] get hk stock data unified failed:")
            logger.error(f"[DEBUG] Error:{str(e)}")
            logger.error(f"[DEBUG] Stack:{error_details}")
            return f"港股数据获取失败: {str(e)}"

    @staticmethod
    @tool
    @log_tool_call(tool_name="get_stock_fundamentals_unified", log_args=True)
    def get_stock_fundamentals_unified(
        ticker: Annotated[str, "股票代码（支持A股、港股、美股）"],
        start_date: Annotated[str, "开始日期，格式：YYYY-MM-DD"] = None,
        end_date: Annotated[str, "结束日期，格式：YYYY-MM-DD"] = None,
        curr_date: Annotated[str, "当前日期，格式：YYYY-MM-DD"] = None
    ) -> str:
        """A uniform stock fundamental analysis tool
        Automatically identify stock types (A, port, US) and call the corresponding data Source
        Supporting analytical level-based data acquisition strategies

        Args:
            ticker: Stock code (e.g. 000001, 0700.HK, AAPL)
            Start date: Start date (optional, format: YYYY-MM-DD)
            End date: End date (optional, format: YYYY-MM-DD)
            Curr date: Current date (optional, format: YYYY-MM-DD)

        Returns:
            str: Basic analysis of data and reports
        """
        logger.info(f"Analysis of stocks:{ticker}")

        # Get an analytical level configuration to support a level-based data acquisition strategy
        research_depth = Toolkit._config.get('research_depth', '标准')
        logger.info(f"Current level of analysis:{research_depth}")
        
        #Map of Numerical to Chinese Level
        numeric_to_chinese = {
            1: "快速",
            2: "基础", 
            3: "标准",
            4: "深度",
            5: "全面"
        }
        
        #Standardized research depth: supporting digital input
        if isinstance(research_depth, (int, float)):
            research_depth = int(research_depth)
            if research_depth in numeric_to_chinese:
                chinese_depth = numeric_to_chinese[research_depth]
                logger.info(f"🔢 [level conversion] Numerical grade{research_depth}→ Chinese rank '{chinese_depth}'")
                research_depth = chinese_depth
            else:
                logger.warning(f"Invalid numerical grade:{research_depth}, use default standard analysis")
                research_depth = "标准"
        elif isinstance(research_depth, str):
            #If a number is in string form, convert to integer
            if research_depth.isdigit():
                numeric_level = int(research_depth)
                if numeric_level in numeric_to_chinese:
                    chinese_depth = numeric_to_chinese[numeric_level]
                    logger.info(f"🔢 [class transformation] String numbers '{research_depth}' → Chinese rank '{chinese_depth}'")
                    research_depth = chinese_depth
                else:
                    logger.warning(f"Invalid string numerical level:{research_depth}, use default standard analysis")
                    research_depth = "标准"
            #If it's already Chinese, use it directly.
            elif research_depth in ["快速", "基础", "标准", "深度", "全面"]:
                logger.info(f"📝 [level confirmation] For Chinese: '{research_depth}'")
            else:
                logger.warning(f"Unknown depth of study:{research_depth}, use default standard analysis")
                research_depth = "标准"
        else:
            logger.warning(f"Invalid study depth type:{type(research_depth)}, use default standard analysis")
            research_depth = "标准"
        
        #Adjusting data acquisition strategies to analytical levels
        #🔧 Amending Map Relationship: Data depth should be consistent with research depth
        if research_depth == "快速":
            #Rapid analysis: acquisition of basic data and reduction of data source calls
            data_depth = "basic"
            logger.info(f"🔧 [analytical level] rapid analysis model: access to basic data")
        elif research_depth == "基础":
            #Basic analysis: access to standard data
            data_depth = "standard"
            logger.info(f"Basic analysis model: access to standard data")
        elif research_depth == "标准":
            #Standard analysis: access to standard data (not full!)
            data_depth = "standard"
            logger.info(f"🔧 [analytical level] Standard analytical model: acquisition of standard data")
        elif research_depth == "深度":
            #Depth analysis: capture complete data
            data_depth = "full"
            logger.info(f"🔧 [analytical level] depth analysis mode: capture complete data")
        elif research_depth == "全面":
            #Comprehensive analysis: obtaining the most comprehensive data, including all available data Source
            data_depth = "comprehensive"
            logger.info(f"🔧 [analytical level] Comprehensive analysis model: obtaining the most comprehensive data")
        else:
            #Default use of standard analysis
            data_depth = "standard"
            logger.info(f"Unknown level, using standard analytical mode")

        #Add detailed stock code tracking log
        logger.info(f"[Equal code tracking]{ticker}' (type:{type(ticker)})")
        logger.info(f"[Equal code tracking]{len(str(ticker))}")
        logger.info(f"[Equal code tracking]{list(str(ticker))}")

        #Save originalticker for comparison
        original_ticker = ticker

        try:
            from tradingagents.utils.stock_utils import StockUtils
            from datetime import datetime, timedelta

            #Automatically recognize stock types
            market_info = StockUtils.get_market_info(ticker)
            is_china = market_info['is_china']
            is_hk = market_info['is_hk']
            is_us = market_info['is_us']

            logger.info(f"[StockUtils.get market info]{market_info}")
            logger.info(f"📊 [Uniform Basic Tool] Stock types:{market_info['market_name']}")
            logger.info(f"Currency:{market_info['currency_name']} ({market_info['currency_symbol']})")

            #Check if ticker has changed in the process.
            if str(ticker) != str(original_ticker):
                logger.warning(f"Warning: The stock code has changed! Original: '{original_ticker}Now:{ticker}'")

            #Set Default Date
            if not curr_date:
                curr_date = datetime.now().strftime('%Y-%m-%d')
        
            #Optimization of basic analysis: no significant historical data required, only current price and financial data
            #Set the number of different analysis modules based on the depth level of the data, rather than historical data ranges
            #🔧Correct map relationship: anallysis modules should be consistent with data depth
            if data_depth == "basic":  #Quick analysis: basic module
                analysis_modules = "basic"
                logger.info(f"📊 [basic policy] Quick analysis model: access to basic financial indicators")
            elif data_depth == "standard":  #Basic/standard analysis: standard modules
                analysis_modules = "standard"
                logger.info(f"📊 [Basic Policy] Standard Analysis Model: Access to Standard Financial Analysis")
            elif data_depth == "full":  #Depth analysis: complete module
                analysis_modules = "full"
                logger.info(f"📊 [basic policy] Depth analysis model: capture complete fundamental analysis")
            elif data_depth == "comprehensive":  #Comprehensive analysis: integrated modules
                analysis_modules = "comprehensive"
                logger.info(f"📊 [Basic policy] Comprehensive analysis model: access to comprehensive fundamental analysis")
            else:
                analysis_modules = "standard"  #Default Standard Analysis
                logger.info(f"[Basic policy]")
            
            #Basic analysis strategy:
            #1. Access to data for 10 days (ensuring access to data, processing weekends/ holidays)
            #2. Participation in analysis using only the most recent two-day data (current prices only)
            days_to_fetch = 10  #Fixed access to data for 10 days
            days_to_analyze = 2  #Only for the last two days.

            logger.info(f"[Basic Policy]{days_to_fetch}Day data, analysis of recent{days_to_analyze}days")

            if not start_date:
                start_date = (datetime.now() - timedelta(days=days_to_fetch)).strftime('%Y-%m-%d')

            if not end_date:
                end_date = curr_date

            result_data = []

            if is_china:
                #China A unit: basic face analysis optimization strategy - obtain only the necessary current price and fundamental face data
                logger.info(f"Processing Unit A data, data depth:{data_depth}...")
                logger.info(f"[Equal code tracking]{ticker}'")
                logger.info(f"💡 [optimizing strategy] Basic analysis captures only current prices and financial data, not historical dayline data")

                #Optimizing strategy: Basic face analysis does not require a large amount of historical dayline data
                #Access only to current equity information (the latest 1-2 days) and basic financial data
                try:
                    #Access to up-to-date stock price information (data for the latest 1-2 days only)
                    from datetime import datetime, timedelta
                    recent_end_date = curr_date
                    recent_start_date = (datetime.strptime(curr_date, '%Y-%m-%d') - timedelta(days=2)).strftime('%Y-%m-%d')

                    from tradingagents.dataflows.interface import get_china_stock_data_unified
                    logger.info(f"🔍 [Securities Code Tracking] call get china stock data unified (requires only the latest prices), input parameter: ticker='{ticker}', start_date='{recent_start_date}', end_date='{recent_end_date}'")
                    current_price_data = get_china_stock_data_unified(ticker, recent_start_date, recent_end_date)

                    #Debugging: print the 500 words before returning data Arguments
                    logger.info(f"A share price data back in length:{len(current_price_data)}")
                    logger.info(f"🔍 [basic tool debugging] A share price data top 500 characters:\n{current_price_data[:500]}")

                    result_data.append(f"## A股当前价格信息\n{current_price_data}")
                except Exception as e:
                    logger.error(f"❌ [Basic Tool debugging] A share price data has failed:{e}")
                    result_data.append(f"## A股当前价格信息\n获取失败: {e}")
                    current_price_data = ""

                try:
                    #Access to basic financial data (which is central to the fundamental analysis)
                    from tradingagents.dataflows.optimized_china_data import OptimizedChinaDataProvider
                    analyzer = OptimizedChinaDataProvider()
                    logger.info(f"[Equal code tracking]{ticker}', analysis_modules='{analysis_modules}'")

                    #Transfer analysis module parameters to basic face analysis methods
                    fundamentals_data = analyzer._generate_fundamentals_report(ticker, current_price_data, analysis_modules)

                    #Debugging: print the 500 words before returning data Arguments
                    logger.info(f"🔍 [basic tool debugs] Basic data back in length for Unit A:{len(fundamentals_data)}")
                    logger.info(f"🔍 [basic tool debugging] 500 characters in front of the basic face data of unit A:\n{fundamentals_data[:500]}")

                    result_data.append(f"## A股基本面财务数据\n{fundamentals_data}")
                except Exception as e:
                    logger.error(f"❌ [basic tool debugging] Unit A fundamental data acquisition failed:{e}")
                    result_data.append(f"## A股基本面财务数据\n获取失败: {e}")

            elif is_hk:
                #Port Unit: use of AKShare data source to support multiple standby programmes
                logger.info(f"🇭🇰 [UCP] Processing Port Unit data, data depth:{data_depth}...")

                hk_data_success = False

                #Unified policy: complete data for all levels
                #Reason: The hints are uniform, if incomplete data lead to LLM analysis based on non-existent data (the illusion)
                logger.info(f"🔍 [Hong Kong Stock Base] Unified policy: capture complete data (overlooking data depth parameters)")

                #Main data source: AKShare
                try:
                    from tradingagents.dataflows.interface import get_hk_stock_data_unified
                    hk_data = get_hk_stock_data_unified(ticker, start_date, end_date)

                    #Debugging: print the 500 words before returning data Arguments
                    logger.info(f"🔍 [basic tool debugging] Port Unit data back in length:{len(hk_data)}")
                    logger.info(f"🔍 [basic tool debugging] Hong Kong Unit data pre-500 characters:\n{hk_data[:500]}")

                    #Check data quality
                    if hk_data and len(hk_data) > 100 and "❌" not in hk_data:
                        result_data.append(f"## 港股数据\n{hk_data}")
                        hk_data_success = True
                        logger.info(f"✅ [Uniform Basic Tool] Major data sources for the Hong Kong Unit were successful")
                    else:
                        logger.warning(f"⚠️ [Uniform Basic Tool] The Hong Kong Unit ' s main data sources are of poor quality")

                except Exception as e:
                    logger.error(f"[Basic tool debugging]{e}")

                #Alternative: Basic Port Unit information
                if not hk_data_success:
                    try:
                        from tradingagents.dataflows.interface import get_hk_stock_info_unified
                        hk_info = get_hk_stock_info_unified(ticker)

                        basic_info = f"""## 港股基础信息

**股票代码**: {ticker}
**股票名称**: {hk_info.get('name', f'港股{ticker}')}
**交易货币**: 港币 (HK$)
**交易所**: 香港交易所 (HKG)
**数据源**: {hk_info.get('source', '基础信息')}

⚠️ 注意：详细的价格和财务数据暂时无法获取，建议稍后重试或使用其他数据源。

**基本面分析建议**：
- 建议查看公司最新财报
- 关注港股市场整体走势
- 考虑汇率因素对投资的影响
"""
                        result_data.append(basic_info)
                        logger.info(f"✅ [Uniform Basic Tool] Port Unit backup information successfully")

                    except Exception as e2:
                        #Final standby option
                        fallback_info = f"""## 港股信息（备用）

**股票代码**: {ticker}
**股票类型**: 港股
**交易货币**: 港币 (HK$)
**交易所**: 香港交易所 (HKG)

❌ 数据获取遇到问题: {str(e2)}

**建议**：
- 请稍后重试
- 或使用其他数据源
- 检查股票代码格式是否正确
"""
                        result_data.append(fallback_info)
                        logger.error(f"All data sources in the Hong Kong Unit failed:{e2}")

            else:
                #United States share: using OpenAI/Finnhub data source
                logger.info(f"[Unanimous Basic Tool]")

                #Unified policy: complete data for all levels
                #Reason: The hints are uniform, if incomplete data lead to LLM analysis based on non-existent data (the illusion)
                logger.info(f"🔍 [U.S.E. Basics] Unified policy: capture complete data (overlooking data depth parameters)")

                try:
                    from tradingagents.dataflows.interface import get_fundamentals_openai
                    us_data = get_fundamentals_openai(ticker, curr_date)
                    result_data.append(f"## 美股基本面数据\n{us_data}")
                    logger.info(f"✅ [Unified Basic Tool]")
                except Exception as e:
                    result_data.append(f"## 美股基本面数据\n获取失败: {e}")
                    logger.error(f"❌ [Uniform Basic Tool] Data acquisition failed:{e}")

            #Group All Data
            combined_result = f"""# {ticker} 基本面分析数据

**股票类型**: {market_info['market_name']}
**货币**: {market_info['currency_name']} ({market_info['currency_symbol']})
**分析日期**: {curr_date}
**数据深度级别**: {data_depth}

{chr(10).join(result_data)}

---
*数据来源: 根据股票类型自动选择最适合的数据源*
"""

            #Add detailed data acquisition log
            logger.info(f"📊 [Uniform Basic Tool] = = = = data access completed summary = = = = =")
            logger.info(f"[Unique Basic Tool] Stock code:{ticker}")
            logger.info(f"📊 [Uniform Basic Tool] Stock types:{market_info['market_name']}")
            logger.info(f"Data depth level:{data_depth}")
            logger.info(f"Number of data modules obtained:{len(result_data)}")
            logger.info(f"Total data length:{len(combined_result)}Character")
            
            #Record details of each data module
            for i, data_section in enumerate(result_data, 1):
                section_lines = data_section.split('\n')
                section_title = section_lines[0] if section_lines else "未知模块"
                section_length = len(data_section)
                logger.info(f"📊 [UCP] Data module{i}: {section_title} ({section_length}Character)")
                
                #Special tag if data contains error information
                if "获取失败" in data_section or "❌" in data_section:
                    logger.warning(f"⚠️ [UCP] Data module{i}Contains error information")
                else:
                    logger.info(f"✅ [UCP] Data module{i}Success")
            
            #Documenting specific acquisition strategies based on data depth level
            if data_depth in ["basic", "standard"]:
                logger.info(f"📊 [Uniform Basic Tool] Basic/Standard Level Strategy: Access to core price data and basic information only")
            elif data_depth in ["full", "detailed", "comprehensive"]:
                logger.info(f"📊 [Universal Basic Tool] Complete/detailed/comprehensive level strategy: access to price data + fundamental data")
            else:
                logger.info(f"Default policy: Getting complete data")
            
            logger.info(f"📊 [Unified Basic Tool] = = = = end of summary data acquisition = = = =")
            
            return combined_result

        except Exception as e:
            error_msg = f"统一基本面分析工具执行失败: {str(e)}"
            logger.error(f"❌ [Uniform Basic Tool]{error_msg}")
            return error_msg

    @staticmethod
    @tool
    @log_tool_call(tool_name="get_stock_market_data_unified", log_args=True)
    def get_stock_market_data_unified(
        ticker: Annotated[str, "股票代码（支持A股、港股、美股）"],
        start_date: Annotated[str, "开始日期，格式：YYYY-MM-DD。注意：系统会自动扩展到配置的回溯天数（通常为365天），你只需要传递分析日期即可"],
        end_date: Annotated[str, "结束日期，格式：YYYY-MM-DD。通常与start_date相同，传递当前分析日期即可"]
    ) -> str:
        """Common stock market data tool
        Automatically identify stock types (A, Port, USA) and use the corresponding data sources to obtain price and technical indicator data

        Important: The system automatically expands the date range to the number of days (usually 365 days) of the configuration to ensure that technical indicators are calculated with sufficient historical data.
        All you need to do is pass the current analysis date as start date and end date, without manually calculating the historical date range.

        Args:
            ticker: Stock code (e.g. 000001, 0700.HK, AAPL)
            Start date: Start date (format: YYYY-MM-DD). Just pass the current analysis date. The system will expand automatically.
            End date: End date (format: YYYY-MM-DD). Just pass the current analysis date.

        Returns:
            str: Analysis of market data and technology

        Example:
        If the date of analysis is 2025-11-09, pass:
        -Ticker: "00700.HK"
        - Start date: "2025-11-09"
        -end date: "2025-11-09"
        The system automatically captures 365 days of historical data from 2024-11-09 to 2025-11-09.
        """
        logger.info(f"Analysis of stocks:{ticker}")

        try:
            from tradingagents.utils.stock_utils import StockUtils

            #Automatically recognize stock types
            market_info = StockUtils.get_market_info(ticker)
            is_china = market_info['is_china']
            is_hk = market_info['is_hk']
            is_us = market_info['is_us']

            logger.info(f"[Unified Market Tool]{market_info['market_name']}")
            logger.info(f"[Unified Market Instrument] Currency:{market_info['currency_name']} ({market_info['currency_symbol']}")

            result_data = []

            if is_china:
                #China A Unit: use of Chinese stock data sources
                logger.info(f"[Unified Market Tool] Processing A share market data...")

                try:
                    from tradingagents.dataflows.interface import get_china_stock_data_unified
                    stock_data = get_china_stock_data_unified(ticker, start_date, end_date)

                    #Debugging: print the 500 words before returning data Arguments
                    logger.info(f"🔍 [Market tool debugging] Unit A data return length:{len(stock_data)}")
                    logger.info(f"🔍 [Market tool debugging] A shares pre- 500 characters:\n{stock_data[:500]}")

                    result_data.append(f"## A股市场数据\n{stock_data}")
                except Exception as e:
                    logger.error(f"[Market tool debugging]{e}")
                    result_data.append(f"## A股市场数据\n获取失败: {e}")

            elif is_hk:
                #Port Unit: use of AKShare data source
                logger.info(f"[Uniform Market Tool]")

                try:
                    from tradingagents.dataflows.interface import get_hk_stock_data_unified
                    hk_data = get_hk_stock_data_unified(ticker, start_date, end_date)

                    #Debugging: print the 500 words before returning data Arguments
                    logger.info(f"🔍 [Market tool debugging] Port Unit data back in length:{len(hk_data)}")
                    logger.info(f"🔍 [Market tool debugging] Port stock data pre-500 characters:\n{hk_data[:500]}")

                    result_data.append(f"## 港股市场数据\n{hk_data}")
                except Exception as e:
                    logger.error(f"[Market tool debugging]{e}")
                    result_data.append(f"## 港股市场数据\n获取失败: {e}")

            else:
                #US share: Prioritize FINNHUB API data source
                logger.info(f"[Unified Market Tool]")

                try:
                    from tradingagents.dataflows.providers.us.optimized import get_us_stock_data_cached
                    us_data = get_us_stock_data_cached(ticker, start_date, end_date)
                    result_data.append(f"## 美股市场数据\n{us_data}")
                except Exception as e:
                    result_data.append(f"## 美股市场数据\n获取失败: {e}")

            #Group All Data
            combined_result = f"""# {ticker} 市场数据分析

**股票类型**: {market_info['market_name']}
**货币**: {market_info['currency_name']} ({market_info['currency_symbol']})
**分析期间**: {start_date} 至 {end_date}

{chr(10).join(result_data)}

---
*数据来源: 根据股票类型自动选择最适合的数据源*
"""

            logger.info(f"Data acquisition completed, total length:{len(combined_result)}")
            return combined_result

        except Exception as e:
            error_msg = f"统一市场数据工具执行失败: {str(e)}"
            logger.error(f"[Unified Market Tool]{error_msg}")
            return error_msg

    @staticmethod
    @tool
    @log_tool_call(tool_name="get_stock_news_unified", log_args=True)
    def get_stock_news_unified(
        ticker: Annotated[str, "股票代码（支持A股、港股、美股）"],
        curr_date: Annotated[str, "当前日期，格式：YYYY-MM-DD"]
    ) -> str:
        """Unified stock news tool
        Automatically identify stock types (A, Hong Kong, United States) and call corresponding news data Source

        Args:
            ticker: Stock code (e.g. 000001, 0700.HK, AAPL)
            Curr date: Current date (format: YYYY-MM-DD)

        Returns:
            str: Public information analysis reports
        """
        logger.info(f"Analysis of stocks:{ticker}")

        try:
            from tradingagents.utils.stock_utils import StockUtils
            from datetime import datetime, timedelta

            #Automatically recognize stock types
            market_info = StockUtils.get_market_info(ticker)
            is_china = market_info['is_china']
            is_hk = market_info['is_hk']
            is_us = market_info['is_us']

            logger.info(f"[Unified News Tool] Stock types:{market_info['market_name']}")

            #Calculates the date range of the news query
            end_date = datetime.strptime(curr_date, '%Y-%m-%d')
            start_date = end_date - timedelta(days=7)
            start_date_str = start_date.strftime('%Y-%m-%d')

            result_data = []

            if is_china or is_hk:
                #Chinese Unit A and Port Unit: Using Akshare East Wealth News and Google News (search in Chinese)
                logger.info(f"[Unified News Tool]")

                #1. Attempted access to Akshare East Wealth News
                try:
                    #Processing stock code
                    clean_ticker = ticker.replace('.SH', '').replace('.SZ', '').replace('.SS', '')\
                                   .replace('.HK', '').replace('.XSHE', '').replace('.XSHG', '')
                    
                    logger.info(f"[Unified News Tool]{clean_ticker}")

                    #Access to news via AKShareProvider
                    from tradingagents.dataflows.providers.china.akshare import AKShareProvider

                    provider = AKShareProvider()

                    #Access to Eastern Wealth News
                    news_df = provider.get_stock_news_sync(symbol=clean_ticker)

                    if news_df is not None and not news_df.empty:
                        #Format East Wealth News
                        em_news_items = []
                        for _, row in news_df.iterrows():
                            #AKShare returned fields First Name
                            news_title = row.get('新闻标题', '') or row.get('标题', '')
                            news_time = row.get('发布时间', '') or row.get('时间', '')
                            news_url = row.get('新闻链接', '') or row.get('链接', '')

                            news_item = f"- **{news_title}** [{news_time}]({news_url})"
                            em_news_items.append(news_item)
                        
                        #Add to result
                        if em_news_items:
                            em_news_text = "\n".join(em_news_items)
                            result_data.append(f"## 东方财富新闻\n{em_news_text}")
                            logger.info(f"[Universal News Tool]{len(em_news_items)}East Wealth News")
                except Exception as em_e:
                    logger.error(f"[Unional News Tool] The East Wealth News has failed:{em_e}")
                    result_data.append(f"## 东方财富新闻\n获取失败: {em_e}")

                #2. Access to Google news as a complement
                try:
                    #Can not open message
                    if is_china:
                        #Unit A uses stock code to search and add more Chinese keywords
                        clean_ticker = ticker.replace('.SH', '').replace('.SZ', '').replace('.SS', '')\
                                       .replace('.XSHE', '').replace('.XSHG', '')
                        search_query = f"{clean_ticker} 股票 公司 财报 新闻"
                        logger.info(f"Google News Search Key:{search_query}")
                    else:
                        #Port Unit search using code
                        search_query = f"{ticker} 港股"
                        logger.info(f"🇭🇰 [Unified News Tool] Hong Kong News Search Key:{search_query}")

                    from tradingagents.dataflows.interface import get_google_news
                    news_data = get_google_news(search_query, curr_date)
                    result_data.append(f"## Google新闻\n{news_data}")
                    logger.info(f"[Unique News Tool]")
                except Exception as google_e:
                    logger.error(f"Google News Failed:{google_e}")
                    result_data.append(f"## Google新闻\n获取失败: {google_e}")

            else:
                #United States shares: use of Finnhub news
                logger.info(f"[Unified News Tool]")

                try:
                    from tradingagents.dataflows.interface import get_finnhub_news
                    news_data = get_finnhub_news(ticker, start_date_str, curr_date)
                    result_data.append(f"## 美股新闻\n{news_data}")
                except Exception as e:
                    result_data.append(f"## 美股新闻\n获取失败: {e}")

            #Group All Data
            combined_result = f"""# {ticker} 新闻分析

**股票类型**: {market_info['market_name']}
**分析日期**: {curr_date}
**新闻时间范围**: {start_date_str} 至 {curr_date}

{chr(10).join(result_data)}

---
*数据来源: 根据股票类型自动选择最适合的新闻源*
"""

            logger.info(f"Data acquisition complete, total length:{len(combined_result)}")
            return combined_result

        except Exception as e:
            error_msg = f"统一新闻工具执行失败: {str(e)}"
            logger.error(f"[Unified News Tool]{error_msg}")
            return error_msg

    @staticmethod
    @tool
    @log_tool_call(tool_name="get_stock_sentiment_unified", log_args=True)
    def get_stock_sentiment_unified(
        ticker: Annotated[str, "股票代码（支持A股、港股、美股）"],
        curr_date: Annotated[str, "当前日期，格式：YYYY-MM-DD"]
    ) -> str:
        """Common stock mood analysis tool
        Automatically identify stock types (A, port, US) and call corresponding emotional data Source

        Args:
            ticker: Stock code (e.g. 000001, 0700.HK, AAPL)
            Curr date: Current date (format: YYYY-MM-DD)

        Returns:
            str: Emotional analysis
        """
        logger.info(f"Analysis of stocks:{ticker}")

        try:
            from tradingagents.utils.stock_utils import StockUtils

            #Automatically recognize stock types
            market_info = StockUtils.get_market_info(ticker)
            is_china = market_info['is_china']
            is_hk = market_info['is_hk']
            is_us = market_info['is_us']

            logger.info(f"[Universal Emotional Tool]{market_info['market_name']}")

            result_data = []

            if is_china or is_hk:
                #China Unit A and Port Unit: Social Media Emotional Analysis
                logger.info(f"[Unanimous Emotional Tool]")

                try:
                    #You can form Chinese social media emotions like microblogging, snowballs, Eastern wealth.
                    #Current use of base emotional analysis
                    sentiment_summary = f"""
## 中文市场情绪分析

**股票**: {ticker} ({market_info['market_name']})
**分析日期**: {curr_date}

### 市场情绪概况
- 由于中文社交媒体情绪数据源暂未完全集成，当前提供基础分析
- 建议关注雪球、东方财富、同花顺等平台的讨论热度
- 港股市场还需关注香港本地财经媒体情绪

### 情绪指标
- 整体情绪: 中性
- 讨论热度: 待分析
- 投资者信心: 待评估

*注：完整的中文社交媒体情绪分析功能正在开发中*
"""
                    result_data.append(sentiment_summary)
                except Exception as e:
                    result_data.append(f"## 中文市场情绪\n获取失败: {e}")

            else:
                #United States share: use of Reddit emotional analysis
                logger.info(f"♪ 🇺🇸 ♪")

                try:
                    from tradingagents.dataflows.interface import get_reddit_sentiment

                    sentiment_data = get_reddit_sentiment(ticker, curr_date)
                    result_data.append(f"## 美股Reddit情绪\n{sentiment_data}")
                except Exception as e:
                    result_data.append(f"## 美股Reddit情绪\n获取失败: {e}")

            #Group All Data
            combined_result = f"""# {ticker} 情绪分析

**股票类型**: {market_info['market_name']}
**分析日期**: {curr_date}

{chr(10).join(result_data)}

---
*数据来源: 根据股票类型自动选择最适合的情绪数据源*
"""

            logger.info(f"Data acquisition complete, total length:{len(combined_result)}")
            return combined_result

        except Exception as e:
            error_msg = f"统一情绪分析工具执行失败: {str(e)}"
            logger.error(f"[Unanimous Emotional Tool]{error_msg}")
            return error_msg
