"""Alpha Vantage News Data Provider

Provision of quality market news and emotional analysis data

Reference original TradingAgents Achieved
"""

from typing import Annotated, Dict, Any
import json
from datetime import datetime

from .alpha_vantage_common import _make_api_request, format_datetime_for_api, format_response_as_string

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')


def get_news(
    ticker: Annotated[str, "Stock symbol for news articles"],
    start_date: Annotated[str, "Start date for news search, YYYY-MM-DD"],
    end_date: Annotated[str, "End date for news search, YYYY-MM-DD"]
) -> str:
    """Access to stock-related news and emotional analysis data

    Return real-time and historical market news and emotional data from major news outlets around the world.
    It covers the topics of stocks, encrypted currency, foreign exchange and fiscal policy, mergers and acquisitions, and IPO.

    Args:
        ticker: Stock code
        Start date: Start date, format YYYY-MM-DD
        End date: End date, format YYYY-MM-DD

    Returns:
        Formatted news data string (JSON format)

    Example:
    News = get news
    """
    try:
        logger.info(f"[Alpha Vantage]{ticker}, {start_date}to{end_date}")
        
        #Build Request Parameters
        params = {
            "tickers": ticker.upper(),
            "time_from": format_datetime_for_api(start_date),
            "time_to": format_datetime_for_api(end_date),
            "sort": "LATEST",
            "limit": "50",  #Up to 50 news items returned
        }
        
        #Launch API Request
        data = _make_api_request("NEWS_SENTIMENT", params)
        
        #Format Response
        if isinstance(data, dict):
            #Can not open message
            feed = data.get("feed", [])
            
            if not feed:
                return f"# No news found for {ticker} between {start_date} and {end_date}\n"
            
            #Build Formatting Output
            result = f"# News and Sentiment for {ticker.upper()}\n"
            result += f"# Period: {start_date} to {end_date}\n"
            result += f"# Total articles: {len(feed)}\n"
            result += f"# Retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            
            #Add every news
            for idx, article in enumerate(feed, 1):
                result += f"## Article {idx}\n"
                result += f"**Title**: {article.get('title', 'N/A')}\n"
                result += f"**Source**: {article.get('source', 'N/A')}\n"
                result += f"**Published**: {article.get('time_published', 'N/A')}\n"
                result += f"**URL**: {article.get('url', 'N/A')}\n"
                
                #Emotional analysis
                sentiment = article.get('overall_sentiment_label', 'N/A')
                sentiment_score = article.get('overall_sentiment_score', 'N/A')
                result += f"**Sentiment**: {sentiment} (Score: {sentiment_score})\n"
                
                #Summary
                summary = article.get('summary', 'N/A')
                if len(summary) > 200:
                    summary = summary[:200] + "..."
                result += f"**Summary**: {summary}\n"
                
                #The emotion of the stock.
                ticker_sentiment = article.get('ticker_sentiment', [])
                for ts in ticker_sentiment:
                    if ts.get('ticker', '').upper() == ticker.upper():
                        result += f"**Ticker Sentiment**: {ts.get('ticker_sentiment_label', 'N/A')} "
                        result += f"(Score: {ts.get('ticker_sentiment_score', 'N/A')}, "
                        result += f"Relevance: {ts.get('relevance_score', 'N/A')})\n"
                        break
                
                result += "\n---\n\n"
            
            logger.info(f"[Alpha Vantage]{len(feed)}News")
            return result
        else:
            return format_response_as_string(data, f"News for {ticker}")
            
    except Exception as e:
        logger.error(f"[Alpha Vantage]{ticker}: {e}")
        return f"Error retrieving news for {ticker}: {str(e)}"


def get_insider_transactions(
    symbol: Annotated[str, "Ticker symbol, e.g., IBM"]
) -> str:
    """Get Inner Transaction Data

    Return to the latest and historical in-person transaction data of key stakeholders (founders, executives, board members, etc.).

    Args:
        symbol: stock code

    Returns:
        Formatted Inner Person Transactions Data String (JSON format)

    Example:
    > transports = get insider transactions ("AAPL")
    """
    try:
        logger.info(f"[Alpha Vantage]{symbol}")
        
        #Build Request Parameters
        params = {
            "symbol": symbol.upper(),
        }
        
        #Launch API Request
        data = _make_api_request("INSIDER_TRANSACTIONS", params)
        
        #Format Response
        if isinstance(data, dict):
            transactions = data.get("data", [])
            
            if not transactions:
                return f"# No insider transactions found for {symbol}\n"
            
            #Build Formatting Output
            result = f"# Insider Transactions for {symbol.upper()}\n"
            result += f"# Total transactions: {len(transactions)}\n"
            result += f"# Retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            
            #Add every transaction
            for idx, txn in enumerate(transactions[:20], 1):  #Limit top 20
                result += f"## Transaction {idx}\n"
                result += f"**Insider**: {txn.get('insider_name', 'N/A')}\n"
                result += f"**Title**: {txn.get('insider_title', 'N/A')}\n"
                result += f"**Transaction Type**: {txn.get('transaction_type', 'N/A')}\n"
                result += f"**Date**: {txn.get('transaction_date', 'N/A')}\n"
                result += f"**Shares**: {txn.get('shares_traded', 'N/A')}\n"
                result += f"**Price**: ${txn.get('price_per_share', 'N/A')}\n"
                result += f"**Value**: ${txn.get('transaction_value', 'N/A')}\n"
                result += f"**Shares Owned After**: {txn.get('shares_owned_after_transaction', 'N/A')}\n"
                result += "\n---\n\n"
            
            logger.info(f"[Alpha Vantage]{len(transactions)}An insider.")
            return result
        else:
            return format_response_as_string(data, f"Insider Transactions for {symbol}")
            
    except Exception as e:
        logger.error(f"[Alpha Vantage]{symbol}: {e}")
        return f"Error retrieving insider transactions for {symbol}: {str(e)}"


def get_market_news(
    topics: Annotated[str, "News topics, e.g., 'technology,earnings'"] = None,
    start_date: Annotated[str, "Start date, YYYY-MM-DD"] = None,
    end_date: Annotated[str, "End date, YYYY-MM-DD"] = None,
    limit: Annotated[int, "Number of articles to return"] = 50
) -> str:
    """Access to market-wide news (without qualification of specific shares)

    Args:
        Topics: News themes, multiple themes separated by commas (optional)
        Start date: Start date (optional)
        End date: End Date (optional)
        Limited: returns the number of articles, default 50

    Returns:
        Formatted news data string

    Example:
    {\\bord0\\shad0\\alphaH3D}news = get market news
    """
    try:
        logger.info(f"[Alpha Vantage]{topics}")
        
        #Build Request Parameters
        params = {
            "sort": "LATEST",
            "limit": str(limit),
        }
        
        if topics:
            params["topics"] = topics
        
        if start_date:
            params["time_from"] = format_datetime_for_api(start_date)
        
        if end_date:
            params["time_to"] = format_datetime_for_api(end_date)
        
        #Launch API Request
        data = _make_api_request("NEWS_SENTIMENT", params)
        
        #Format Response (like get news)
        if isinstance(data, dict):
            feed = data.get("feed", [])
            
            if not feed:
                return "# No market news found\n"
            
            result = f"# Market News\n"
            if topics:
                result += f"# Topics: {topics}\n"
            if start_date and end_date:
                result += f"# Period: {start_date} to {end_date}\n"
            result += f"# Total articles: {len(feed)}\n"
            result += f"# Retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            
            for idx, article in enumerate(feed, 1):
                result += f"## Article {idx}\n"
                result += f"**Title**: {article.get('title', 'N/A')}\n"
                result += f"**Source**: {article.get('source', 'N/A')}\n"
                result += f"**Published**: {article.get('time_published', 'N/A')}\n"
                result += f"**Sentiment**: {article.get('overall_sentiment_label', 'N/A')} "
                result += f"(Score: {article.get('overall_sentiment_score', 'N/A')})\n"
                
                summary = article.get('summary', 'N/A')
                if len(summary) > 200:
                    summary = summary[:200] + "..."
                result += f"**Summary**: {summary}\n\n"
                result += "---\n\n"
            
            logger.info(f"[Alpha Vantage]{len(feed)}Market News")
            return result
        else:
            return format_response_as_string(data, "Market News")
            
    except Exception as e:
        logger.error(f"[Alpha Vantage]{e}")
        return f"Error retrieving market news: {str(e)}"

