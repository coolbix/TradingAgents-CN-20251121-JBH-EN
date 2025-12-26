"""Alpha Vantage Basic Data Provider

Provide basic corporate data, including:
- Corporate profile
- Financial statements (balance sheet, cash flow statement, profit statement)
- Valuation indicators

Reference original TradingAgents Achieved
"""

from typing import Annotated
import json
from datetime import datetime

from .alpha_vantage_common import _make_api_request, format_response_as_string

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')


def get_fundamentals(
    ticker: Annotated[str, "Ticker symbol of the company"],
    curr_date: Annotated[str, "Current date (not used for Alpha Vantage)"] = None
) -> str:
    """Access to comprehensive corporate fundamentals

Includes financial ratios and key indicators such as:
- Valuation indicators such as market value, PE, PB, ROE
- Financial indicators such as income, profits, EPS, etc.
- Company information, etc.

Args:
ticker: Stock code
Curr date: Current date (Alpha Vantage does not use this parameter)

Returns:
Formatted Corporate Profile Data String

Example:
=fundamentals = get fundamentals
"""
    try:
        logger.info(f"[Alpha Vantage]{ticker}")
        
        #Build Request Parameters
        params = {
            "symbol": ticker.upper(),
        }
        
        #Launch API Request
        data = _make_api_request("OVERVIEW", params)
        
        #Format Response
        if isinstance(data, dict) and data:
            #Extract key indicators
            result = f"# Company Overview: {ticker.upper()}\n"
            result += f"# Retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            
            #Basic information
            result += "## Basic Information\n"
            result += f"**Name**: {data.get('Name', 'N/A')}\n"
            result += f"**Symbol**: {data.get('Symbol', 'N/A')}\n"
            result += f"**Exchange**: {data.get('Exchange', 'N/A')}\n"
            result += f"**Currency**: {data.get('Currency', 'N/A')}\n"
            result += f"**Country**: {data.get('Country', 'N/A')}\n"
            result += f"**Sector**: {data.get('Sector', 'N/A')}\n"
            result += f"**Industry**: {data.get('Industry', 'N/A')}\n\n"
            
            #Company description
            description = data.get('Description', 'N/A')
            if len(description) > 500:
                description = description[:500] + "..."
            result += f"**Description**: {description}\n\n"
            
            #Valuation indicators
            result += "## Valuation Metrics\n"
            result += f"**Market Cap**: ${data.get('MarketCapitalization', 'N/A')}\n"
            result += f"**PE Ratio**: {data.get('PERatio', 'N/A')}\n"
            result += f"**PEG Ratio**: {data.get('PEGRatio', 'N/A')}\n"
            result += f"**Price to Book**: {data.get('PriceToBookRatio', 'N/A')}\n"
            result += f"**Price to Sales**: {data.get('PriceToSalesRatioTTM', 'N/A')}\n"
            result += f"**EV to Revenue**: {data.get('EVToRevenue', 'N/A')}\n"
            result += f"**EV to EBITDA**: {data.get('EVToEBITDA', 'N/A')}\n\n"
            
            #Financial indicators
            result += "## Financial Metrics\n"
            result += f"**Revenue TTM**: ${data.get('RevenueTTM', 'N/A')}\n"
            result += f"**Gross Profit TTM**: ${data.get('GrossProfitTTM', 'N/A')}\n"
            result += f"**EBITDA**: ${data.get('EBITDA', 'N/A')}\n"
            result += f"**Net Income TTM**: ${data.get('NetIncomeTTM', 'N/A')}\n"
            result += f"**EPS**: ${data.get('EPS', 'N/A')}\n"
            result += f"**Diluted EPS TTM**: ${data.get('DilutedEPSTTM', 'N/A')}\n\n"
            
            #Profitability
            result += "## Profitability\n"
            result += f"**Profit Margin**: {data.get('ProfitMargin', 'N/A')}\n"
            result += f"**Operating Margin TTM**: {data.get('OperatingMarginTTM', 'N/A')}\n"
            result += f"**Return on Assets TTM**: {data.get('ReturnOnAssetsTTM', 'N/A')}\n"
            result += f"**Return on Equity TTM**: {data.get('ReturnOnEquityTTM', 'N/A')}\n\n"
            
            #Divide Information
            result += "## Dividend Information\n"
            result += f"**Dividend Per Share**: ${data.get('DividendPerShare', 'N/A')}\n"
            result += f"**Dividend Yield**: {data.get('DividendYield', 'N/A')}\n"
            result += f"**Dividend Date**: {data.get('DividendDate', 'N/A')}\n"
            result += f"**Ex-Dividend Date**: {data.get('ExDividendDate', 'N/A')}\n\n"
            
            #Equities Information
            result += "## Stock Information\n"
            result += f"**52 Week High**: ${data.get('52WeekHigh', 'N/A')}\n"
            result += f"**52 Week Low**: ${data.get('52WeekLow', 'N/A')}\n"
            result += f"**50 Day MA**: ${data.get('50DayMovingAverage', 'N/A')}\n"
            result += f"**200 Day MA**: ${data.get('200DayMovingAverage', 'N/A')}\n"
            result += f"**Shares Outstanding**: {data.get('SharesOutstanding', 'N/A')}\n"
            result += f"**Beta**: {data.get('Beta', 'N/A')}\n\n"
            
            #Financial health
            result += "## Financial Health\n"
            result += f"**Book Value**: ${data.get('BookValue', 'N/A')}\n"
            result += f"**Debt to Equity**: {data.get('DebtToEquity', 'N/A')}\n"
            result += f"**Current Ratio**: {data.get('CurrentRatio', 'N/A')}\n"
            result += f"**Quick Ratio**: {data.get('QuickRatio', 'N/A')}\n\n"
            
            #Analyst's target price.
            result += "## Analyst Targets\n"
            result += f"**Analyst Target Price**: ${data.get('AnalystTargetPrice', 'N/A')}\n"
            result += f"**Analyst Rating Strong Buy**: {data.get('AnalystRatingStrongBuy', 'N/A')}\n"
            result += f"**Analyst Rating Buy**: {data.get('AnalystRatingBuy', 'N/A')}\n"
            result += f"**Analyst Rating Hold**: {data.get('AnalystRatingHold', 'N/A')}\n"
            result += f"**Analyst Rating Sell**: {data.get('AnalystRatingSell', 'N/A')}\n"
            result += f"**Analyst Rating Strong Sell**: {data.get('AnalystRatingStrongSell', 'N/A')}\n\n"
            
            logger.info(f"[Alpha Vantage]{ticker}")
            return result
        else:
            return format_response_as_string(data, f"Fundamentals for {ticker}")
            
    except Exception as e:
        logger.error(f"[Alpha Vantage]{ticker}: {e}")
        return f"Error retrieving fundamentals for {ticker}: {str(e)}"


def get_balance_sheet(
    ticker: Annotated[str, "Ticker symbol of the company"],
    freq: Annotated[str, "Reporting frequency: annual/quarterly (not used)"] = "quarterly",
    curr_date: Annotated[str, "Current date (not used)"] = None
) -> str:
    """Get balance sheet data

Args:
ticker: Stock code
freq: Report frequency (Alpha Vantage returns all data)
Curr date: Current date (not used)

Returns:
Formatted balance sheet data string
"""
    try:
        logger.info(f"[Alpha Vantage]{ticker}")
        
        params = {"symbol": ticker.upper()}
        data = _make_api_request("BALANCE_SHEET", params)
        
        return format_response_as_string(data, f"Balance Sheet for {ticker}")
        
    except Exception as e:
        logger.error(f"[Alpha Vantage]{ticker}: {e}")
        return f"Error retrieving balance sheet for {ticker}: {str(e)}"


def get_cashflow(
    ticker: Annotated[str, "Ticker symbol of the company"],
    freq: Annotated[str, "Reporting frequency: annual/quarterly (not used)"] = "quarterly",
    curr_date: Annotated[str, "Current date (not used)"] = None
) -> str:
    """Access to cash flow statement data

Args:
ticker: Stock code
freq: Report frequency (Alpha Vantage returns all data)
Curr date: Current date (not used)

Returns:
Formatted cash flow table data string
"""
    try:
        logger.info(f"[Alpha Vantage]{ticker}")
        
        params = {"symbol": ticker.upper()}
        data = _make_api_request("CASH_FLOW", params)
        
        return format_response_as_string(data, f"Cash Flow for {ticker}")
        
    except Exception as e:
        logger.error(f"[Alpha Vantage]{ticker}: {e}")
        return f"Error retrieving cash flow for {ticker}: {str(e)}"


def get_income_statement(
    ticker: Annotated[str, "Ticker symbol of the company"],
    freq: Annotated[str, "Reporting frequency: annual/quarterly (not used)"] = "quarterly",
    curr_date: Annotated[str, "Current date (not used)"] = None
) -> str:
    """Access to profit statement data

Args:
ticker: Stock code
freq: Report frequency (Alpha Vantage returns all data)
Curr date: Current date (not used)

Returns:
Formatted profit table data string
"""
    try:
        logger.info(f"[Alpha Vantage]{ticker}")
        
        params = {"symbol": ticker.upper()}
        data = _make_api_request("INCOME_STATEMENT", params)
        
        return format_response_as_string(data, f"Income Statement for {ticker}")
        
    except Exception as e:
        logger.error(f"[Alpha Vantage]{ticker}: {e}")
        return f"Error retrieving income statement for {ticker}: {str(e)}"

