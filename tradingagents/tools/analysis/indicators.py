from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class IndicatorSpec:
    name: str
    params: Optional[Dict[str, Any]] = None


SUPPORTED = {"ma", "ema", "macd", "rsi", "boll", "atr", "kdj"}


def _require_cols(df: pd.DataFrame, cols: Iterable[str]):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame缺少必要列: {missing}, 现有列: {list(df.columns)[:10]}...")


def ma(close: pd.Series, n: int, min_periods: int = None) -> pd.Series:
    """Calculate moving average line (Moving Age)

Args:
close: close price sequence
n: Cycle
Min periods: Minimal number of cycles, default 1 (allowing calculation of previous period data when insufficient)

Returns:
Move the average line sequence
"""
    if min_periods is None:
        min_periods = 1  #Default is 1, consistent with existing code
    return close.rolling(window=int(n), min_periods=min_periods).mean()


def ema(close: pd.Series, n: int) -> pd.Series:
    """Compute index moving average line (Exponential Moving Age)

Args:
close: close price sequence
n: Cycle

Returns:
Index moving average line sequence
"""
    return close.ewm(span=int(n), adjust=False).mean()


def macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """Calculating MACD Indicators

Args:
close: close price sequence
Fast-line cycle, default 12
slow: slow-line cycle, default 26
Signal: signal line cycle, default 9

Returns:
DataFrame with dif, dea, macd hist
-dif: Difference between fast and slow lines (DIF)
- dea: DIF signal lines (DEA)
- Macd hist: MACD column map (DIF-DEA)
"""
    dif = ema(close, fast) - ema(close, slow)
    dea = dif.ewm(span=int(signal), adjust=False).mean()
    hist = dif - dea
    return pd.DataFrame({"dif": dif, "dea": dea, "macd_hist": hist})


def rsi(close: pd.Series, n: int = 14, method: str = 'ema') -> pd.Series:
    """Calculating RSI Indicators

Args:
close: close price sequence
n: Cycle, default 14
method: Calculator
- 'ema': index movement average (international standard, Wilder's method)
Simple moving average
- 'china': Chinese SMA.

Returns:
RSI sequence (0-100)

Note:
- 'ema': use ewm (alpha=1/n, adjust=False) for international markets
- 'sma': simple moving average using rolling (window=n).
- 'china': using ewm(com=n-1, adjust=True) in line with Hansu/Tunta
"""
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    if method == 'ema':
        #International standard: average movement of Wilder's index
        avg_gain = gain.ewm(alpha=1 / float(n), adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / float(n), adjust=False).mean()
    elif method == 'sma':
        #Simple moving average
        avg_gain = gain.rolling(window=int(n), min_periods=1).mean()
        avg_loss = loss.rolling(window=int(n), min_periods=1).mean()
    elif method == 'china':
        #Chinese-style SMA: Same-fast/Tun-Touch style
        # SMA(X, N, 1) = ewm(com=N-1, adjust=True).mean()
        #Reference: https://blog.csdn.net/u011218867/articles/117427927
        avg_gain = gain.ewm(com=int(n) - 1, adjust=True).mean()
        avg_loss = loss.ewm(com=int(n) - 1, adjust=True).mean()
    else:
        raise ValueError(f"不支持的RSI计算方法: {method}，支持的方法: 'ema', 'sma', 'china'")

    rs = avg_gain / (avg_loss.replace(0, np.nan))
    rsi_val = 100 - (100 / (1 + rs))
    return rsi_val


def boll(close: pd.Series, n: int = 20, k: float = 2.0, min_periods: int = None) -> pd.DataFrame:
    """Calculating the Bollinger Bands

Args:
close: close price sequence
n: Cycle, default 20
k: Standard difference, default 2.0
Min periods: Minimal number of cycles, default 1 (allowing calculation of previous period data when insufficient)

Returns:
DataFrame with boll mid, boll upper, boll lower
- boll mid: medium track (n-day moving average)
- boll upper: on-orbit (middle track + k times standard deviation)
-Boll lower: De-orbit (medium track - k times standard deviation)
"""
    if min_periods is None:
        min_periods = 1  #Default is 1, consistent with existing code
    mid = close.rolling(window=int(n), min_periods=min_periods).mean()
    std = close.rolling(window=int(n), min_periods=min_periods).std()
    upper = mid + k * std
    lower = mid - k * std
    return pd.DataFrame({"boll_mid": mid, "boll_upper": upper, "boll_lower": lower})


def atr(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low).abs(),
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(window=int(n), min_periods=int(n)).mean()


def kdj(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 9, m1: int = 3, m2: int = 3) -> pd.DataFrame:
    lowest_low = low.rolling(window=int(n), min_periods=int(n)).min()
    highest_high = high.rolling(window=int(n), min_periods=int(n)).max()
    rsv = (close - lowest_low) / (highest_high - lowest_low) * 100
    #Process Zero and Start NAN
    rsv = rsv.replace([np.inf, -np.inf], np.nan)

    #Increment by classic formula (initialization 50)
    k = pd.Series(np.nan, index=close.index)
    d = pd.Series(np.nan, index=close.index)
    alpha_k = 1 / float(m1)
    alpha_d = 1 / float(m2)
    last_k = 50.0
    last_d = 50.0
    for i in range(len(close)):
        rv = rsv.iloc[i]
        if np.isnan(rv):
            k.iloc[i] = np.nan
            d.iloc[i] = np.nan
            continue
        curr_k = (1 - alpha_k) * last_k + alpha_k * rv
        curr_d = (1 - alpha_d) * last_d + alpha_d * curr_k
        k.iloc[i] = curr_k
        d.iloc[i] = curr_d
        last_k, last_d = curr_k, curr_d
    j = 3 * k - 2 * d
    return pd.DataFrame({"kdj_k": k, "kdj_d": d, "kdj_j": j})


def compute_indicator(df: pd.DataFrame, spec: IndicatorSpec) -> pd.DataFrame:
    name = spec.name.lower()
    params = spec.params or {}
    out = df.copy()

    if name == "ma":
        _require_cols(df, ["close"])
        n = int(params.get("n", params.get("period", 20)))
        out[f"ma{n}"] = ma(df["close"], n)
        return out

    if name == "ema":
        _require_cols(df, ["close"])
        n = int(params.get("n", params.get("period", 20)))
        out[f"ema{n}"] = ema(df["close"], n)
        return out

    if name == "macd":
        _require_cols(df, ["close"])
        fast = int(params.get("fast", 12))
        slow = int(params.get("slow", 26))
        signal = int(params.get("signal", 9))
        macd_df = macd(df["close"], fast=fast, slow=slow, signal=signal)
        for c in macd_df.columns:
            out[c] = macd_df[c]
        return out

    if name == "rsi":
        _require_cols(df, ["close"])
        n = int(params.get("n", params.get("period", 14)))
        out[f"rsi{n}"] = rsi(df["close"], n)
        return out

    if name == "boll":
        _require_cols(df, ["close"])
        n = int(params.get("n", 20))
        k = float(params.get("k", 2.0))
        boll_df = boll(df["close"], n=n, k=k)
        for c in boll_df.columns:
            out[c] = boll_df[c]
        return out

    if name == "atr":
        _require_cols(df, ["high", "low", "close"])
        n = int(params.get("n", 14))
        out[f"atr{n}"] = atr(df["high"], df["low"], df["close"], n=n)
        return out

    if name == "kdj":
        _require_cols(df, ["high", "low", "close"])
        n = int(params.get("n", 9))
        m1 = int(params.get("m1", 3))
        m2 = int(params.get("m2", 3))
        kdj_df = kdj(df["high"], df["low"], df["close"], n=n, m1=m1, m2=m2)
        for c in kdj_df.columns:
            out[c] = kdj_df[c]
        return out

    raise ValueError(f"不支持的指标: {name}")


def compute_many(df: pd.DataFrame, specs: List[IndicatorSpec]) -> pd.DataFrame:
    if not specs:
        return df.copy()
    #Roughly heavy (by name+sorted (params))
    def key(s: IndicatorSpec):
        p = s.params or {}
        items = tuple(sorted(p.items()))
        return (s.name.lower(), items)

    unique_specs: List[IndicatorSpec] = []
    seen = set()
    for s in specs:
        k = key(s)
        if k not in seen:
            seen.add(k)
            unique_specs.append(s)

    out = df.copy()
    for s in unique_specs:
        out = compute_indicator(out, s)
    return out


def last_values(df: pd.DataFrame, columns: List[str]) -> Dict[str, Any]:
    if df.empty:
        return {c: None for c in columns}
    last = df.iloc[-1]
    return {c: (None if c not in df.columns else (None if pd.isna(last.get(c)) else last.get(c))) for c in columns}


def add_all_indicators(df: pd.DataFrame, close_col: str = 'close',
                       high_col: str = 'high', low_col: str = 'low',
                       rsi_style: str = 'international') -> pd.DataFrame:
    """Add all commonly used technical indicators to DataFrame

This is a unified technical indicator calculation function that replaces duplicated computing codes in each data source module.

Args:
df: DataFrame with price data
close col: close price listing, default 'close'
High col: Best price listing, default 'high ' (reserved, not used)
Low col: Minimum price listing, default 'low' (set aside, not used)
rsi style: RSI computing style
- 'international': international standards (RSI14, use EMA)
- 'china': Chinese style (RSI 6/12/24 + RSI14, using Chinese-style SMA)

Returns:
DataFrame (modified in situ) of the technical indicator column added

Add indicator columns:
- Ma5, ma10, ma20, ma60: Move the average line
- RSI indicator (14 days, international standards)
- rsi6, rsi12, rsi24: RSI indicator (Chinese style only when rsi style='china')
-rsi14: RSI indicator (14 days, simple moving average, only when rsi style='china')
- Macd dif, Macd dea, Macd: MACD indicators
- boll mid, boll upper, boll lower:

Example:
> df = pd. DataFrame(  FMT 0 )
> df = add all indicators(df)
>print(df[['close', 'ma5', 'rsi'].tail())
I'm sorry.
♪ Chinese style
Df = all indicators
>print(df(['close', 'rsi6', 'rsi12', 'rsi24']]. tail()
"""
    #Check necessary columns
    if close_col not in df.columns:
        raise ValueError(f"DataFrame缺少收盘价列: {close_col}")

    #Calculate moving average lines (MA5, MA10, MA20, MA60)
    df['ma5'] = ma(df[close_col], 5, min_periods=1)
    df['ma10'] = ma(df[close_col], 10, min_periods=1)
    df['ma20'] = ma(df[close_col], 20, min_periods=1)
    df['ma60'] = ma(df[close_col], 60, min_periods=1)

    #Calculating RSI Indicators
    if rsi_style == 'china':
        #Chinese style: RSI6, RSI12, RSI24
        df['rsi6'] = rsi(df[close_col], 6, method='china')
        df['rsi12'] = rsi(df[close_col], 12, method='china')
        df['rsi24'] = rsi(df[close_col], 24, method='china')
        #Retain RSI14 as reference for international standards (use simple moving average)
        df['rsi14'] = rsi(df[close_col], 14, method='sma')
        #Add also 'rsi ' column for compatibility (arguing rsi12)
        df['rsi'] = df['rsi12']
    else:
        #International standard: RSI14 (use EMA)
        df['rsi'] = rsi(df[close_col], 14, method='ema')

    #Compute MCD
    macd_df = macd(df[close_col], fast=12, slow=26, signal=9)
    df['macd_dif'] = macd_df['dif']
    df['macd_dea'] = macd_df['dea']
    df['macd'] = macd_df['macd_hist'] * 2  #Note: Multiply here by two to align with Tunnel/Sun.

    #Calculating Brynches (20 days, double standard deviation)
    boll_df = boll(df[close_col], n=20, k=2.0, min_periods=1)
    df['boll_mid'] = boll_df['boll_mid']
    df['boll_upper'] = boll_df['boll_upper']
    df['boll_lower'] = boll_df['boll_lower']

    return df

