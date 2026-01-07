import asyncio
import os
import sys

# Ensure repository root is on sys.path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(CURRENT_DIR)
sys.path.insert(0, REPO_ROOT)

from app.core.database import init_database_async, close_database_async  # noqa: E402
from app.services.quotes_ingestion_service import QuotesIngestionService  # noqa: E402


async def main():
    await init_database_async()
    svc = QuotesIngestionService()
    await svc.ensure_indexes()
    await svc.backfill_last_close_snapshot()
    await close_database_async()


if __name__ == "__main__":
    asyncio.run(main())

