#!/usr/bin/env python3
"""TradingAgents Run-time Configure Adapter (weakly dependent)

- Try to read dynamic system settings (if available) from the backend app.services.config provider
- Back to the environment variable and default if it is not available or cannot be synchronized in the walk event cycle
- Maintaining the independence of the Trading Agencies package: retreating silently when not available without introducing hard reliance
"""

from __future__ import annotations

import os
import asyncio
from typing import Any, Optional, Callable

import logging
_logger = logging.getLogger("tradingagents.config")


def _get_event_loop_running() -> bool:
    """Check for event cycle running"""
    try:
        #Get running loop throws RuntimeError when there's no event cycle.
        loop = asyncio.get_running_loop()
        return loop is not None and loop.is_running()
    except RuntimeError:
        return False
    except Exception:
        #Other anomalies also say there is no cycle of events.
        return False


def _get_system_settings_sync() -> dict:
    """Optimal effort to get backend dynamics system settings.
- If the backend is not available/not installed, return empty dict
- If the current cycle of events is running, return empty dict to avoid a dead lock/dip

Note: In order to avoid a cycle of events and conflicts, the current reality is always to return to an empty dictionary.
Use environment variables and defaults for configuration.
"""
    #Temporary solution: completely disable dynamic configuration acquisition to avoid cycle conflict
    #TODO: In the future, the use of thread pools or other means to secure dynamic configurations can be considered
    _logger.debug("Dynamic configuration capture disabled, using environment variables and defaults")
    return {}

    #The following code is a temporary comment to avoid circular conflict of events
    ## First check
    # if _get_event_loop_running():
    #logger.debug("incident cycle running, skip dynamic configuration acquisition")
    #     return {}

    # try:
    ## Delay import to avoid hard dependence
    #     from app.services.config_provider import provider as config_provider  # type: ignore

    ## Second check: make sure the cycle of events is not started during import
    #     if _get_event_loop_running():
    #logger.debug("Ex import detected event cycle, skip dynamic configuration acquisition")
    #         return {}

    ## Third check: confirm before calling asyncio.run
    #     try:
    ## Try to get the current event cycle, if successful to show that the cycle is running
    #         current_loop = asyncio.get_running_loop()
    #         if current_loop and current_loop.is_running():
    #logger.debug("asyncio.run detected active cycle before call, skip)
    #             return {}
    #     except RuntimeError:
    ## There's no running cycle of events, asyncio.run
    #         pass

    ## One-time sync call with asyncio.run
    #     return asyncio.run(config_provider.get_effective_system_settings()) or {}

    # except RuntimeError as e:
    #     error_msg = str(e).lower()
    #     if any(keyword in error_msg for keyword in [
    #         "cannot be called from a running event loop",
    #         "got future attached to a different loop",
    #         "task was destroyed but it is pending"
    #     ]):
    #logger.debug(f) "Expected event cycle conflicts, skip dynamic configuration acquisition:   FT 0 ")
    #         return {}
    #logger.debug(f) "Retrieving dynamic configuration failed" (RuntimeError):   FMT 0 ")
    #     return {}
    # except Exception as e:
    #logger.debug(f) "Retrieving dynamic configuration failed:   FT 0 ")
    #     return {}


def _coerce(value: Any, caster: Callable[[Any], Any], default: Any) -> Any:
    try:
        if value is None:
            return default
        return caster(value)
    except Exception:
        return default


def get_number(env_var: str, system_key: Optional[str], default: float | int, caster: Callable[[Any], Any]) -> float | int:
    """Acquiring numerical configuration by priority: DB(system settings) > ENV > default
- env var: Environmental variables such as "TA US MIN API INTERVAL SECONDS"
-system key: Dynamic system setting keys such as "ta us min api interval seconds" (for None)
- default:
-caster: Type conversion functions such as fload or int
"""
    #1) DB Dynamic Settings
    if system_key:
        eff = _get_system_settings_sync()
        if isinstance(eff, dict) and system_key in eff:
            return _coerce(eff.get(system_key), caster, default)

    #2) Environmental variables
    env_val = os.getenv(env_var)
    if env_val is not None and str(env_val).strip() != "":
        return _coerce(env_val, caster, default)

    #3) Code Default
    return default


def get_float(env_var: str, system_key: Optional[str], default: float) -> float:
    return get_number(env_var, system_key, default, float)  # type: ignore[arg-type]


def get_int(env_var: str, system_key: Optional[str], default: int) -> int:
    return get_number(env_var, system_key, default, int)  # type: ignore[arg-type]


# --- Boolean access helper ---------------------------------------------------

def get_bool(env_var: str, system_key: Optional[str], default: bool) -> bool:
    """Get Boolean Configuration by Priority: DB(system settings) > ENV > default"""
    #1) DB Dynamic Settings
    if system_key:
        eff = _get_system_settings_sync()
        if isinstance(eff, dict) and system_key in eff:
            v = eff.get(system_key)
            if isinstance(v, bool):
                return v
            if isinstance(v, (int, float)):
                return bool(v)
            if isinstance(v, str):
                return str(v).strip().lower() in ("1", "true", "yes", "on")
    #2) Environmental variables
    env_val = os.getenv(env_var)
    if env_val is not None and str(env_val).strip() != "":
        return str(env_val).strip().lower() in ("1", "true", "yes", "on")
    #3) Code Default
    return default


def is_use_app_cache_enabled(default: bool = False) -> bool:
    """Whether to enable priority reading from the app cache (Mongo collection). ENV: TA USE APP CACHE; DB: ta use app cache
    An assessment log will be recorded, containing the source and the original ENV values, to facilitate the sorting of the effective path.
    """
    #Infer source (DB/ENV/DEFAULT)
    src = "default"
    env_val = os.getenv("TA_USE_APP_CACHE")
    try:
        eff = _get_system_settings_sync()
    except Exception:
        eff = {}
    if isinstance(eff, dict) and "ta_use_app_cache" in eff:
        src = "db"
    elif env_val is not None and str(env_val).strip() != "":
        src = "env"

    #Final value (following DB > ENV > DEFAULT)
    val = get_bool("TA_USE_APP_CACHE", "ta_use_app_cache", default)

    try:
        _logger.info(f"[runtime_settings] TA_USE_APP_CACHE evaluated -> {val} (source={src}, env={env_val})")
    except Exception:
        pass
    return val


# --- Timezone access helpers -------------------------------------------------
from typing import Optional as _Optional
from zoneinfo import ZoneInfo as _ZoneInfo


def get_timezone_name(default: str = "Asia/Shanghai") -> str:
    """Return configured timezone name with priority: DB(system_settings) > ENV > default.
    - DB key: app_timezone (preferred) or APP_TIMEZONE
    - ENV vars checked in order: APP_TIMEZONE, TIMEZONE, TA_TIMEZONE
    """
    try:
        eff = _get_system_settings_sync()
        if isinstance(eff, dict):
            tz = eff.get("app_timezone") or eff.get("APP_TIMEZONE")
            if isinstance(tz, str) and tz.strip():
                return tz.strip()
    except Exception:
        pass

    for env_key in ("APP_TIMEZONE", "TIMEZONE", "TA_TIMEZONE"):
        val = os.getenv(env_key)
        if isinstance(val, str) and val.strip():
            return val.strip()

    return default


def get_zoneinfo(default: str = "Asia/Shanghai") -> _ZoneInfo:
    """Convenience: return ZoneInfo for the configured timezone name."""
    name = get_timezone_name(default)
    try:
        return _ZoneInfo(name)
    except Exception:
        # Fallback to UTC if invalid
        return _ZoneInfo("UTC")


__all__ = [
    "get_float",
    "get_int",
    "get_bool",
    "is_use_app_cache_enabled",
    "get_timezone_name",
    "get_zoneinfo",
]
