"""QuotesService: Provides a batch of real-time snapshots (AKShare East Wealth spot interface) with memory of TTL caches.
- Do not use Tux as a background data source.
- Be used only for the collection of items before filtering back.
"""
from __future__ import annotations

import asyncio
import time
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def _safe_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        #Comma/percentage/space in processing string
        if isinstance(v, str):
            s = v.strip().replace(",", "")
            if s.endswith("%"):
                s = s[:-1]
            if s == "-" or s == "":
                return None
            return float(s)
        #Process pandas/numpy values
        return float(v)
    except Exception:
        return None


class QuotesService:
    def __init__(self, ttl_seconds: int = 30) -> None:
        self._ttl = ttl_seconds
        self._cache_ts: float = 0.0
        self._cache: Dict[str, Dict[str, Optional[float]]] = {}
        self._lock = asyncio.Lock()

    async def get_quotes(self, codes: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
        """Obtain near real-time snapshots of a group of equities (latest prices, drops, turnover).
- Priority is given to the use of the cache; the cache is overtime or empty and a full-market snapshot is updated.
- returns the code that only contains the request.
"""
        codes = [c.strip() for c in codes if c]
        now = time.time()
        async with self._lock:
            if self._cache and (now - self._cache_ts) < self._ttl:
                return {c: q for c, q in self._cache.items() if c in codes and q}
            #Refresh Cache
            data = await asyncio.to_thread(self._fetch_spot_akshare)
            self._cache = data
            self._cache_ts = time.time()
            return {c: q for c, q in self._cache.items() if c in codes and q}

    def _fetch_spot_akshare(self) -> Dict[str, Dict[str, Optional[float]]]:
        """(c) Draw and standardize the dictionaries through the AKShare All-Professional Rapids interface.
Expected column (common): codes, names, latest prices, drops, turnover.
Different versions may vary and multiple listings are compatible.
"""
        try:
            import akshare as ak  #Used in project without additional installation
            df = ak.stock_zh_a_spot_em()
            if df is None or getattr(df, "empty", True):
                logger.warning("AKShare spot returns empty data")
                return {}
            #Compatible common listings
            code_col = next((c for c in ["代码", "代码code", "symbol", "股票代码"] if c in df.columns), None)
            price_col = next((c for c in ["最新价", "现价", "最新价(元)", "price", "最新"] if c in df.columns), None)
            pct_col = next((c for c in ["涨跌幅", "涨跌幅(%)", "涨幅", "pct_chg"] if c in df.columns), None)
            amount_col = next((c for c in ["成交额", "成交额(元)", "amount", "成交额(万元)"] if c in df.columns), None)

            if not code_col or not price_col:
                logger.error(f"AKShare spot missing necessary column: code={code_col}, price={price_col}")
                return {}

            result: Dict[str, Dict[str, Optional[float]]] = {}
            for _, row in df.iterrows():  # type: ignore
                code_raw = row.get(code_col)
                if not code_raw:
                    continue
                #Standardised stock code: remove lead 0 and complete it to 6-bit
                code_str = str(code_raw).strip()
                #If it's a pure number, remove the lead zero and then replace it with six places.
                if code_str.isdigit():
                    code_clean = code_str.lstrip('0') or '0'  #Remove pilot 0, and if all zeros, keep a zero
                    code = code_clean.zfill(6)  #We got six.
                else:
                    code = code_str.zfill(6)
                close = _safe_float(row.get(price_col))
                pct = _safe_float(row.get(pct_col)) if pct_col else None
                amt = _safe_float(row.get(amount_col)) if amount_col else None
                #If the amount of the transaction is in 10,000 dollars, convert it to one dollar (part of the interface is in thousands dollars, not to twist it, keep it as it is and display it from the front end)
                result[code] = {"close": close, "pct_chg": pct, "amount": amt}
            logger.info(f"AKShare spot withdrawal completed:{len(result)}Article")
            return result
        except Exception as e:
            logger.error(f"This post is part of our special coverage Libya Protests 2011.{e}")
            return {}


_quotes_service: Optional[QuotesService] = None


def get_quotes_service() -> QuotesService:
    global _quotes_service
    if _quotes_service is None:
        _quotes_service = QuotesService(ttl_seconds=30)
    return _quotes_service

