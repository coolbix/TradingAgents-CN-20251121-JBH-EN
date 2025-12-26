"""Harmonize the API response format tool
"""
from datetime import datetime
from typing import Any, Optional, Dict
from app.utils.timezone import now_tz


def ok(data: Any = None, message: str = "ok") -> Dict[str, Any]:
    """Standard Successful Response
Return structure:   FT 0 
"""
    return {
        "success": True,
        "data": data,
        "message": message,
        "timestamp": now_tz().isoformat()
    }


def fail(message: str = "error", code: int = 500, data: Any = None) -> Dict[str, Any]:
    """Standard failed response (general error still recommends HTTPException, a function used for business failure scenes)"""
    return {
        "success": False,
        "data": data,
        "message": message,
        "code": code,
        "timestamp": now_tz().isoformat()
    }

