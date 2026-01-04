#!/usr/bin/env python3
"""Data integrity checker
To check the completeness of historical data, to include the latest transaction date, and to automatically retake it if needed
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple, List
import pandas as pd

logger = logging.getLogger(__name__)


class DataCompletenessChecker:
    """Data integrity checker"""
    
    def __init__(self):
        self.logger = logger
    
    def check_data_completeness(
        self,
        symbol: str,
        data: str,
        start_date: str,
        end_date: str,
        market: str = "CN"
    ) -> Tuple[bool, str, dict]:
        """Check data integrity

        Args:
            symbol: stock code
            Data: Data string
            Start date: Start date (YYYYY-MM-DD)
            End date: End Date (YYYYY-MM-DD)
            Market type (CN/HK/US)

        Returns:
            I'm sorry.
            -is complete: Data completeness
            - message: Check the results
            -details: detailed dictionary
        """
        details = {
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "market": market,
            "data_rows": 0,
            "expected_rows": 0,
            "missing_days": 0,
            "has_latest_trade_date": False,
            "latest_date_in_data": None,
            "latest_trade_date": None,
            "completeness_ratio": 0.0
        }
        
        #1. Check for empty or erroneous data
        if not data or "❌" in data or "错误" in data or "获取失败" in data:
            return False, "数据为空或包含错误", details
        
        #2. Attempt to interpret data
        try:
            df = self._parse_data_to_dataframe(data)
            if df is None or df.empty:
                return False, "无法解析数据或数据为空", details
            
            details["data_rows"] = len(df)
            
            #3. Date range in data acquisition
            if 'date' in df.columns:
                date_col = 'date'
            elif 'trade_date' in df.columns:
                date_col = 'trade_date'
            else:
                #Try to find the date bar
                date_col = None
                for col in df.columns:
                    if 'date' in col.lower() or '日期' in col:
                        date_col = col
                        break
                
                if not date_col:
                    self.logger.warning(f"Could not close temporary folder: %s{symbol}")
                    return False, "无法找到日期列", details
            
            #Convert date as datetime
            df[date_col] = pd.to_datetime(df[date_col])
            df = df.sort_values(date_col)
            
            data_start_date = df[date_col].min()
            data_end_date = df[date_col].max()
            details["latest_date_in_data"] = data_end_date.strftime('%Y-%m-%d')
            
            #4. Access to the latest trading date
            latest_trade_date = self._get_latest_trade_date(market)
            details["latest_trade_date"] = latest_trade_date
            
            #5. Check to include the latest transaction date
            if latest_trade_date:
                latest_trade_dt = datetime.strptime(latest_trade_date, '%Y-%m-%d')
                details["has_latest_trade_date"] = data_end_date.date() >= latest_trade_dt.date()
            
            #6. Calculate the number of expected trading days (broad estimate)
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            total_days = (end_dt - start_dt).days + 1
            
            #Assuming that the trading day is about 70% of the total number of days.
            expected_trade_days = int(total_days * 0.7)
            details["expected_rows"] = expected_trade_days
            
            #Calculation of completeness ratio
            if expected_trade_days > 0:
                completeness_ratio = len(df) / expected_trade_days
                details["completeness_ratio"] = completeness_ratio
            
            #8. Checking data gaps
            missing_days = self._check_data_gaps(df, date_col)
            details["missing_days"] = len(missing_days)
            
            #9. Comprehensive judgement
            is_complete = True
            messages = []
            
            #Check 1: Data sufficiency
            if len(df) < expected_trade_days * 0.5:  #Less than 50 per cent
                is_complete = False
                messages.append(f"数据量不足（{len(df)}条，预期约{expected_trade_days}条）")
            
            #Check 2: Include the latest trading date
            if not details["has_latest_trade_date"]:
                is_complete = False
                messages.append(f"缺少最新交易日数据（最新: {details['latest_date_in_data']}, 应为: {latest_trade_date}）")
            
            #Check 3: Any more gaps
            if len(missing_days) > expected_trade_days * 0.1:  #Over 10 per cent gap
                is_complete = False
                messages.append(f"数据缺口较多（{len(missing_days)}个缺口）")
            
            if is_complete:
                message = f"✅ 数据完整（{len(df)}条记录，完整性{completeness_ratio:.1%}）"
            else:
                message = "⚠️ 数据不完整: " + "; ".join(messages)
            
            return is_complete, message, details
            
        except Exception as e:
            self.logger.error(f"Checking data integrity failed:{e}")
            return False, f"检查失败: {str(e)}", details
    
    def _parse_data_to_dataframe(self, data: str) -> Optional[pd.DataFrame]:
        """Resolve data string to DataFrame"""
        try:
            #Try multiple resolution methods
            
            #Mode 1: Assumptions are CSV formats
            from io import StringIO
            try:
                df = pd.read_csv(StringIO(data))
                if not df.empty:
                    return df
            except Exception:
                pass
            
            #Mode 2: Assuming TSV format
            try:
                df = pd.read_csv(StringIO(data), sep='\t')
                if not df.empty:
                    return df
            except Exception:
                pass
            
            #Mode 3: Assuming space separation
            try:
                df = pd.read_csv(StringIO(data), sep=r'\s+')
                if not df.empty:
                    return df
            except Exception:
                pass
            
            return None
            
        except Exception as e:
            self.logger.error(f"Could not close temporary folder: %s{e}")
            return None
    
    def _get_latest_trade_date(self, market: str = "CN") -> Optional[str]:
        """Get the latest transaction date"""
        try:
            if market == "CN":
                #Unit A: use Tushare to find the latest transaction date
                from tradingagents.dataflows.providers.china.tushare import TushareProvider
                import asyncio
                
                provider = TushareProvider()
                if provider.is_available():
                    loop = asyncio.get_event_loop()
                    if loop.is_closed():
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                    
                    latest_date = loop.run_until_complete(provider.find_latest_trade_date())
                    if latest_date:
                        return latest_date
            
            #Alternative scenario: assuming the latest trading date is today or yesterday (in the case of the weekend, push forward)
            today = datetime.now()
            for delta in range(0, 5):  #Five days at most.
                check_date = today - timedelta(days=delta)
                #Skip weekend
                if check_date.weekday() < 5:  #0-4 is Monday to Friday.
                    return check_date.strftime('%Y-%m-%d')
            
            return None
            
        except Exception as e:
            self.logger.error(f"The latest trading day failed:{e}")
            return None
    
    def _check_data_gaps(self, df: pd.DataFrame, date_col: str) -> List[str]:
        """Check data gaps"""
        try:
            df = df.sort_values(date_col)
            dates = df[date_col].tolist()
            
            missing_dates = []
            for i in range(len(dates) - 1):
                current_date = dates[i]
                next_date = dates[i + 1]
                
                #Calculate Date Difference
                delta = (next_date - current_date).days
                
                #If the gap is greater than 3 days (considering weekends), there may be gaps
                if delta > 3:
                    missing_dates.append(f"{current_date.strftime('%Y-%m-%d')} 到 {next_date.strftime('%Y-%m-%d')}")
            
            return missing_dates
            
        except Exception as e:
            self.logger.error(f"Checking data gaps failed:{e}")
            return []


#Global Examples
_checker = None

def get_data_completeness_checker() -> DataCompletenessChecker:
    """Example of obtaining data integrity checker"""
    global _checker
    if _checker is None:
        _checker = DataCompletenessChecker()
    return _checker

