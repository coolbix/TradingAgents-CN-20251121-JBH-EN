"""Transaction time judgement tool module

Provides a uniform transaction timing logic to be used to determine whether or not the current time is within Unit A trading time.
"""

from datetime import datetime, time as dtime
from typing import Optional
from zoneinfo import ZoneInfo

from app.core.config import settings


def is_trading_time(now: Optional[datetime] = None) -> bool:
    """Judge whether or not the buffer period occurs at the time of A-stock trading or after closing

Transaction time:
- 9.30-11.30 a.m.
- 15:00 to 15:00
- Post-disclose buffer period: 15:00-15:30 (ensure that closing prices are obtained)

Description of the buffer period after closing:
- 30 minutes after the transaction.
- Assuming six minutes at a time, we can add five opportunities for synchronization.
- significantly reduce the risk of missing closing prices

Args:
Now: Specify the time, default to the current time (use the configured time zone)

Returns:
Bool: Is it within the trading time
"""
    tz = ZoneInfo(settings.TIMEZONE)
    now = now or datetime.now(tz)
    
    #Chile
    if now.weekday() > 4:
        return False
    
    t = now.time()
    
    #Regular period of transactions at the point of surrender/deep exchange
    morning = dtime(9, 30)
    noon = dtime(11, 30)
    afternoon_start = dtime(13, 0)
    #Post-disclose buffer period (extended 30 minutes to 15:30)
    buffer_end = dtime(15, 30)
    
    return (morning <= t <= noon) or (afternoon_start <= t <= buffer_end)


def is_strict_trading_time(now: Optional[datetime] = None) -> bool:
    """Determination of whether or not to be within strict A stock trading time (not including buffer period)

Transaction time:
- 9.30-11.30 a.m.
- 15:00 to 15:00

Args:
Now: Specify the time, default to the current time (use the configured time zone)

Returns:
Bool: In strict trading time Internal
"""
    tz = ZoneInfo(settings.TIMEZONE)
    now = now or datetime.now(tz)
    
    #Chile
    if now.weekday() > 4:
        return False
    
    t = now.time()
    
    #Regular period of transactions at the point of surrender/deep exchange
    morning = dtime(9, 30)
    noon = dtime(11, 30)
    afternoon_start = dtime(13, 0)
    afternoon_end = dtime(15, 0)
    
    return (morning <= t <= noon) or (afternoon_start <= t <= afternoon_end)


def is_pre_market_time(now: Optional[datetime] = None) -> bool:
    """Adjudication of pre-discretion time (9-9:30)

Args:
Now: Specify the time, default to the current time (use the configured time zone)

Returns:
Bool: Whether the time is before the disk
"""
    tz = ZoneInfo(settings.TIMEZONE)
    now = now or datetime.now(tz)
    
    #Chile
    if now.weekday() > 4:
        return False
    
    t = now.time()
    pre_market_start = dtime(9, 0)
    pre_market_end = dtime(9, 30)
    
    return pre_market_start <= t < pre_market_end


def is_after_market_time(now: Optional[datetime] = None) -> bool:
    """Post-disbursement time (15-5:30)

Args:
Now: Specify the time, default to the current time (use the configured time zone)

Returns:
Bool: Is it after schedule time?
"""
    tz = ZoneInfo(settings.TIMEZONE)
    now = now or datetime.now(tz)
    
    #Chile
    if now.weekday() > 4:
        return False
    
    t = now.time()
    after_market_start = dtime(15, 0)
    after_market_end = dtime(15, 30)
    
    return after_market_start <= t <= after_market_end


def get_trading_status(now: Optional[datetime] = None) -> str:
    """Get Current Transaction Status

Args:
Now: Specify the time, default to the current time (use the configured time zone)

Returns:
str: Transaction status
- "pre market":
- "Morning session" :
- "noon break":
- "afternoon session":
- "after market": Post-drive buffer period
- "Closed":
"""
    tz = ZoneInfo(settings.TIMEZONE)
    now = now or datetime.now(tz)
    
    #Weekend
    if now.weekday() > 4:
        return "closed"
    
    t = now.time()
    
    #Define Time Points
    pre_market_start = dtime(9, 0)
    morning_start = dtime(9, 30)
    noon = dtime(11, 30)
    afternoon_start = dtime(13, 0)
    afternoon_end = dtime(15, 0)
    after_market_end = dtime(15, 30)
    
    #Decision Status
    if pre_market_start <= t < morning_start:
        return "pre_market"
    elif morning_start <= t <= noon:
        return "morning_session"
    elif noon < t < afternoon_start:
        return "noon_break"
    elif afternoon_start <= t <= afternoon_end:
        return "afternoon_session"
    elif afternoon_end < t <= after_market_end:
        return "after_market"
    else:
        return "closed"

