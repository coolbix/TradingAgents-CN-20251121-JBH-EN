"""Database management services
"""

import json
import os
import csv
import gzip
import shutil
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from bson import ObjectId
import motor.motor_asyncio
import redis.asyncio as redis
from pymongo.errors import ServerSelectionTimeoutError

from app.core.database import get_mongo_db, get_redis_client, db_manager
from app.core.config import settings

from app.services.database import status_checks as _db_status
from app.services.database import cleanup as _db_cleanup
from app.services.database import backups as _db_backups
from app.services.database.serialization import serialize_document as _serialize_doc

logger = logging.getLogger(__name__)


class DatabaseService:
    """Database management services"""

    def __init__(self):
        self.backup_dir = os.path.join(settings.TRADINGAGENTS_DATA_DIR, "backups")
        self.export_dir = os.path.join(settings.TRADINGAGENTS_DATA_DIR, "exports")

        #Ensure directory exists
        os.makedirs(self.backup_dir, exist_ok=True)
        os.makedirs(self.export_dir, exist_ok=True)

    async def get_database_status(self) -> Dict[str, Any]:
        """Obtain database connectivity (commission submodule)"""
        return await _db_status.get_database_status()

    async def _get_mongodb_status(self) -> Dict[str, Any]:
        """Get MongoDB status (commissioned submodule)"""
        return await _db_status.get_mongodb_status()

    async def _get_redis_status(self) -> Dict[str, Any]:
        """Get Redis status (commission submodule)"""
        return await _db_status.get_redis_status()

    async def get_database_stats(self) -> Dict[str, Any]:
        """Access to database statistics"""
        try:
            db = get_mongo_db()

            #Get all the pools.
            collection_names = await db.list_collection_names()

            collections_info = []
            total_documents = 0
            total_size = 0

            #Parallel access to all aggregate statistical information
            import asyncio

            async def get_collection_stats(collection_name: str):
                """Obtain statistical information from individual pools"""
                try:
                    stats = await db.command("collStats", collection_name)
                    #Use the count field in CollStats to avoid additional count documents queries
                    doc_count = stats.get('count', 0)

                    return {
                        "name": collection_name,
                        "documents": doc_count,
                        "size": stats.get('size', 0),
                        "storage_size": stats.get('storageSize', 0),
                        "indexes": stats.get('nindexes', 0),
                        "index_size": stats.get('totalIndexSize', 0)
                    }
                except Exception as e:
                    logger.error(f"Get a set.{collection_name}Statistics failed:{e}")
                    return {
                        "name": collection_name,
                        "documents": 0,
                        "size": 0,
                        "storage_size": 0,
                        "indexes": 0,
                        "index_size": 0
                    }

            #Get all the aggregate statistics in parallel
            collections_info = await asyncio.gather(
                *[get_collection_stats(name) for name in collection_names]
            )

            #Total calculated
            for collection_info in collections_info:
                total_documents += collection_info['documents']
                total_size += collection_info['storage_size']

            return {
                "total_collections": len(collection_names),
                "total_documents": total_documents,
                "total_size": total_size,
                "collections": collections_info
            }
        except Exception as e:
            raise Exception(f"获取数据库统计失败: {str(e)}")

    async def test_connections(self) -> Dict[str, Any]:
        """Test database connection (commission submodule)"""
        return await _db_status.test_connections()

    async def _test_mongodb_connection(self) -> Dict[str, Any]:
        """Test MongoDB connection (commissioned submodule)"""
        return await _db_status.test_mongodb_connection()

    async def _test_redis_connection(self) -> Dict[str, Any]:
        """Test Redis connection (commissioned submodule)"""
        return await _db_status.test_redis_connection()

    async def create_backup(self, name: str, collections: List[str] = None, user_id: str = None) -> Dict[str, Any]:
        """Create database backup (auto-select best method)

        - If mongodump is available, use original backup (quick)
        - Otherwise use Python.
        """
        #Check if mongodump is available
        if _db_backups._check_mongodump_available():
            logger.info("✅ with original backup from mongodump (recommended)")
            return await _db_backups.create_backup_native(
                name=name,
                backup_dir=self.backup_dir,
                collections=collections,
                user_id=user_id
            )
        else:
            logger.warning("⚠️mongodump is not available, using Python backup (slower)")
            logger.warning("Suggested installation of MongoDB Data Tools to obtain faster backup speed")
            return await _db_backups.create_backup(
                name=name,
                backup_dir=self.backup_dir,
                collections=collections,
                user_id=user_id
            )

    async def list_backups(self) -> List[Dict[str, Any]]:
        """Get Backup List (Commissioner Submodule)"""
        return await _db_backups.list_backups()

    async def delete_backup(self, backup_id: str) -> None:
        """Remove Backup (commission submodule)"""
        await _db_backups.delete_backup(backup_id)

    async def cleanup_old_data(self, days: int) -> Dict[str, Any]:
        """Clear old data (commission submodule)"""
        return await _db_cleanup.cleanup_old_data(days)

    async def cleanup_analysis_results(self, days: int) -> Dict[str, Any]:
        """Clean up outdated analysis results (commissioned submodule)"""
        return await _db_cleanup.cleanup_analysis_results(days)

    async def cleanup_operation_logs(self, days: int) -> Dict[str, Any]:
        """Clear Operations Log (commissioned submodule)"""
        return await _db_cleanup.cleanup_operation_logs(days)

    async def import_data(self, content: bytes, collection: str, format: str = "json",
                         overwrite: bool = False, filename: str = None) -> Dict[str, Any]:
        """Import data (commissioned submodule)"""
        return await _db_backups.import_data(content, collection, format=format, overwrite=overwrite, filename=filename)

    async def export_data(self, collections: List[str] = None, format: str = "json", sanitize: bool = False) -> str:
        """Export data (commissioned submodule)"""
        return await _db_backups.export_data(collections, export_dir=self.export_dir, format=format, sanitize=sanitize)

    def _serialize_document(self, doc: dict) -> dict:
        """Sequencing documents, processing special types (commissioned submodules)"""
        return _serialize_doc(doc)
