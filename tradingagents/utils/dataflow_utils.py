"""Data Circulation Tool Functions

Move from TradingAGents/dataworks/utils.py
"""
import os
import json
import pandas as pd
from datetime import date, timedelta, datetime
from typing import Annotated

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')


SavePathType = Annotated[str, "File path to save data. If None, data is not saved."]

def save_output(data: pd.DataFrame, tag: str, save_path: SavePathType = None) -> None:
    """Save DataFrame to CSV files

Args:
Data: DataFrame to save
tag: Label (for logs)
Save Path: Save path, not None
"""
    if save_path:
        data.to_csv(save_path)
        logger.info(f"{tag} saved to {save_path}")


def get_current_date():
    """Get Current Date (YYYYY-MM-DD format)

Returns:
str: Current Date String
"""
    return date.today().strftime("%Y-%m-%d")


def decorate_all_methods(decorator):
    """Decorator type: Apply specified decorator for all methods of the class

Args:
Decorator: Decorator function to apply

Returns:
Function: Decoder function

Example:
@decorate all methods (my decorator)
Well, that's it.
>def method1(self):
♪ Pass ♪
"""
    def class_decorator(cls):
        for attr_name, attr_value in cls.__dict__.items():
            if callable(attr_value):
                setattr(cls, attr_name, decorator(attr_value))
        return cls

    return class_decorator


def get_next_weekday(date_input):
    """Get the next working day (jumping the weekend)

Args:
date input: date object or date string (YYYYY-MM-DD)

Returns:
datetime: the date of the next working day

Example:
# Saturday
♪ Back Monday
"""
    if not isinstance(date_input, datetime):
        date_input = datetime.strptime(date_input, "%Y-%m-%d")

    if date_input.weekday() >= 5:  #Saturday (5) or Sunday (6)
        days_to_add = 7 - date_input.weekday()
        next_weekday = date_input + timedelta(days=days_to_add)
        return next_weekday
    else:
        return date_input


def get_trading_date_range(target_date=None, lookback_days=10):
    """Date range for accessing transaction data

Policy: Obtain the latest N-day data to ensure that data from the last transaction date are available
This will automatically address weekends, holidays and data delays.

Args:
target date: Target date (datetime object or string YYY-MM-DD), default today
Lookback days: Number of days to search forward, default 10 days (coverable weekend + small leave)

Returns:
tuple: (start date, end date) two strings, format YYYY-MM-DD

Example:
Get trading date range
(2025-10-03), 2025-10-13)

# Sunday
(2025-10-02), 2025-10-12)
"""
    from datetime import datetime, timedelta

    #Process Input Date
    if target_date is None:
        target_date = datetime.now()
    elif isinstance(target_date, str):
        target_date = datetime.strptime(target_date, "%Y-%m-%d")

    #For future dates, use today
    today = datetime.now()
    if target_date.date() > today.date():
        target_date = today

    #Calculating Start Date (N Days Forward)
    start_date = target_date - timedelta(days=lookback_days)

    #Return Date Range
    return start_date.strftime("%Y-%m-%d"), target_date.strftime("%Y-%m-%d")

