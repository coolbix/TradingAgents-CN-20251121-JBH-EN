#!/usr/bin/env python3
"""App Cache Read Adapter
- Basic information collection: stock basic info
- Roundup: market quotes

Priority data source when ta use app cache is enabled; the unhit part continues to retreat from the upper to the straight-link data source.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime

import pandas as pd
import logging

_logger = logging.getLogger('dataflows')

try:
    from tradingagents.config.database_manager import get_mongodb_client
except Exception:  #Pragma: no cover - weak dependence
    get_mongodb_client = None  # type: ignore


BASICS_COLLECTION = "stock_basic_info"
QUOTES_COLLECTION = "market_quotes"


def get_basics_from_cache(stock_code: Optional[str] = None) -> Optional[Dict[str, Any] | List[Dict[str, Any]]]:
    """Read basic information from app."""
    if get_mongodb_client is None:
        return None
    client = get_mongodb_client()
    if not client:
        return None
    try:
        #Database name from DatacaseManager Internal Configuration
        db_name = None
        try:
            #Access to DatabaseManager exposed configuration
            from tradingagents.config.database_manager import get_database_manager  # type: ignore
            db_name = get_database_manager().mongodb_config.get("database", "tradingagents")
        except Exception:
            db_name = "tradingagents"
        db = client[db_name]
        coll = db[BASICS_COLLECTION]
        if stock_code:
            code6 = str(stock_code).zfill(6)
            try:
                _logger.debug(f"[app cache] Search for basic information{db_name} coll={BASICS_COLLECTION} code={code6}")
            except Exception:
                pass
            #Also query symbol and code fields to ensure compatibility with old and new data formats
            doc = coll.find_one({"$or": [{"symbol": code6}, {"code": code6}]})
            if not doc:
                try:
                    _logger.debug(f"[app cache] Basic information missed{db_name} coll={BASICS_COLLECTION} code={code6}")
                except Exception:
                    pass
            return doc or None
        else:
            cursor = coll.find({})
            docs = list(cursor)
            return docs or None
    except Exception as e:
        try:
            _logger.debug(f"[app cache] Basic information reading anomaly (neglect):{e}")
        except Exception:
            pass
        return None


def get_market_quote_dataframe(symbol: str) -> Optional[pd.DataFrame]:
    """Read the latest snapshot of a single stock from app to DataFrame."""
    if get_mongodb_client is None:
        return None
    client = get_mongodb_client()
    if not client:
        return None
    try:
        #Access to a database
        from tradingagents.config.database_manager import get_database_manager  # type: ignore
        db_name = get_database_manager().mongodb_config.get("database", "tradingagents")
        db = client[db_name]
        coll = db[QUOTES_COLLECTION]
        code = str(symbol).zfill(6)
        try:
            _logger.debug(f"[app cache] Query line{db_name} coll={QUOTES_COLLECTION} code={code}")
        except Exception:
            pass
        doc = coll.find_one({"code": code})
        if not doc:
            try:
                _logger.debug(f"[app cache]{db_name} coll={QUOTES_COLLECTION} code={code}")
            except Exception:
                pass
            return None
        #Construct DataFrame, Field Alignment Tushare Standard Map
        row = {
            "code": code,
            "date": doc.get("trade_date"),  # YYYYMMDD
            "open": doc.get("open"),
            "high": doc.get("high"),
            "low": doc.get("low"),
            "close": doc.get("close"),
            "volume": doc.get("volume"),
            "amount": doc.get("amount"),
            "pct_chg": doc.get("pct_chg"),
            "change": None,
        }
        df = pd.DataFrame([row])
        return df
    except Exception as e:
        try:
            _logger.debug(f"[app cache] Line reading anomaly (neglect):{e}")
        except Exception:
            pass
        return None

