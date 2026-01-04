"""Speed Limiter
To control the API call frequency to avoid going beyond the limit of the data source
"""
import asyncio
import time
import logging
from collections import deque
from typing import Optional

logger = logging.getLogger(__name__)


class RateLimiter:
    """Slide window speed limiter

    Use slide window algorithm to accurately control API call frequency
    """
    
    def __init__(self, max_calls: int, time_window: float, name: str = "RateLimiter"):
        """Initialization speed limiter

        Args:
            max calls: Maximum number of calls within the time window
            Time window: Time window size (sec)
            Name: Limiter name (for logs)
        """
        self.max_calls = max_calls
        self.time_window = time_window
        self.name = name
        self.calls = deque()  #Storage Call Timestamp
        self.lock = asyncio.Lock()  #Make sure it's clear.
        
        #Statistical information
        self.total_calls = 0
        self.total_waits = 0
        self.total_wait_time = 0.0
        
        logger.info(f"🔧 {self.name}Initialization:{max_calls}Minor/{time_window}sec")
    
    async def acquire(self):
        """Access to call permission
        If you exceed the speed limit, you wait until you can call.
        """
        async with self.lock:
            now = time.time()
            
            #Remove old call records outside the time window
            while self.calls and self.calls[0] <= now - self.time_window:
                self.calls.popleft()
            
            #Wait if the maximum number of calls within the current window is reached
            if len(self.calls) >= self.max_calls:
                #Calculate the time required to wait
                oldest_call = self.calls[0]
                wait_time = oldest_call + self.time_window - now + 0.01  #A little buffer.
                
                if wait_time > 0:
                    self.total_waits += 1
                    self.total_wait_time += wait_time
                    
                    logger.debug(f"⏳ {self.name}Speed limit, wait{wait_time:.2f}sec")
                    await asyncio.sleep(wait_time)
                    
                    #Retrieving current time
                    now = time.time()
                    
                    #Clean up old records again.
                    while self.calls and self.calls[0] <= now - self.time_window:
                        self.calls.popleft()
            
            #Record this call
            self.calls.append(now)
            self.total_calls += 1
    
    def get_stats(self) -> dict:
        """Access to statistical information"""
        return {
            "name": self.name,
            "max_calls": self.max_calls,
            "time_window": self.time_window,
            "current_calls": len(self.calls),
            "total_calls": self.total_calls,
            "total_waits": self.total_waits,
            "total_wait_time": self.total_wait_time,
            "avg_wait_time": self.total_wait_time / self.total_waits if self.total_waits > 0 else 0
        }
    
    def reset_stats(self):
        """Reset Statistical Information"""
        self.total_calls = 0
        self.total_waits = 0
        self.total_wait_time = 0.0
        logger.info(f"🔄 {self.name}Statistical information reset")


class TushareRateLimiter(RateLimiter):
    """Tushare special speed limitr

    Automatically adjust the flow limit policy to Tushare's grade
    """
    
    #Tushare Accumulation Level Limit Configuration
    TIER_LIMITS = {
        "free": {"max_calls": 100, "time_window": 60},      #Free users: 100 times/minute
        "basic": {"max_calls": 200, "time_window": 60},     #Basic users: 200 per minute
        "standard": {"max_calls": 400, "time_window": 60},  #Standard users: 400 per minute
        "premium": {"max_calls": 600, "time_window": 60},   #Advanced users: 600 per minute
        "vip": {"max_calls": 800, "time_window": 60},       #VIP users: 800 times/minute
    }
    
    def __init__(self, tier: str = "standard", safety_margin: float = 0.8):
        """Initialize Tushare Speed Limiter

        Args:
            tier: Score (free/basic/standard/premium/vip)
            Safe margin: security margin (0-1), actual limit as a percentage of theoretical limitation That's right.
        """
        if tier not in self.TIER_LIMITS:
            logger.warning(f"Unknown Tushare score:{tier}, use default 'standard '")
            tier = "standard"
        
        limits = self.TIER_LIMITS[tier]
        
        #Apply Safe Margins
        max_calls = int(limits["max_calls"] * safety_margin)
        time_window = limits["time_window"]
        
        super().__init__(
            max_calls=max_calls,
            time_window=time_window,
            name=f"TushareRateLimiter({tier})"
        )
        
        self.tier = tier
        self.safety_margin = safety_margin
        
        logger.info(f"Tushare speed limitr configured:{tier}Level,"
                   f"{max_calls}Minor/{time_window}sec{safety_margin*100:.0f}%)")


class AKShareRateLimiter(RateLimiter):
    """AKShare Special Speed Limiter

    AKShare has no clear limit on flow and uses conservative restriction tactics.
    """
    
    def __init__(self, max_calls: int = 60, time_window: float = 60):
        """Initialize AKShare Speed Limiter

        Args:
            max calls: maximum number of calls within the time window (default 60 times/minute)
            Time window: Time window size (sec)
        """
        super().__init__(
            max_calls=max_calls,
            time_window=time_window,
            name="AKShareRateLimiter"
        )


class BaoStockRateLimiter(RateLimiter):
    """BaoStock special speed limiter

    BaoStock does not have a clear limit on flow and uses conservative restriction tactics
    """
    
    def __init__(self, max_calls: int = 100, time_window: float = 60):
        """Initializing BaoStock Rate Limiter

        Args:
            max calls: maximum number of calls within the time window (default 100 times/minute)
            Time window: Time window size (sec)
        """
        super().__init__(
            max_calls=max_calls,
            time_window=time_window,
            name="BaoStockRateLimiter"
        )


#Examples of global speed limitrs
_tushare_limiter: Optional[TushareRateLimiter] = None
_akshare_limiter: Optional[AKShareRateLimiter] = None
_baostock_limiter: Optional[BaoStockRateLimiter] = None


def get_tushare_rate_limiter(tier: str = "standard", safety_margin: float = 0.8) -> TushareRateLimiter:
    """Get a Tushare speed limiter (single case)"""
    global _tushare_limiter
    if _tushare_limiter is None:
        _tushare_limiter = TushareRateLimiter(tier=tier, safety_margin=safety_margin)
    return _tushare_limiter


def get_akshare_rate_limiter() -> AKShareRateLimiter:
    """Get AKShare Speed Limiter (single)"""
    global _akshare_limiter
    if _akshare_limiter is None:
        _akshare_limiter = AKShareRateLimiter()
    return _akshare_limiter


def get_baostock_rate_limiter() -> BaoStockRateLimiter:
    """Get the BaoStock Speed Limiter (single case)"""
    global _baostock_limiter
    if _baostock_limiter is None:
        _baostock_limiter = BaoStockRateLimiter()
    return _baostock_limiter


def reset_all_limiters():
    """Reset All Rate Limiters"""
    global _tushare_limiter, _akshare_limiter, _baostock_limiter
    _tushare_limiter = None
    _akshare_limiter = None
    _baostock_limiter = None
    logger.info("All speed limitrs have been reset")

