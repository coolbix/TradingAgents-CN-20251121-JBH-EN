from fastapi import APIRouter
import time
from pathlib import Path

router = APIRouter()


def get_version() -> str:
    """Read version numbers from Version files"""
    try:
        version_file = Path(__file__).parent.parent.parent / "VERSION"
        if version_file.exists():
            return version_file.read_text(encoding='utf-8').strip()
    except Exception:
        pass
    return "0.1.16"  #Default Version Number


@router.get("/health")
async def health():
    """Health check interface - front-end use"""
    return {
        "success": True,
        "data": {
            "status": "ok",
            "version": get_version(),
            "timestamp": int(time.time()),
            "service": "TradingAgents-CN API"
        },
        "message": "服务运行正常"
    }

@router.get("/healthz")
async def healthz():
    """Kubernetes Health Examination"""
    return {"status": "ok"}

@router.get("/readyz")
async def readyz():
    """Kubernetes readiness check"""
    return {"ready": True}