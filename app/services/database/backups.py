"""
Backup, import, and export routines extracted from DatabaseService.
"""
from __future__ import annotations

import json
import os
import gzip
import asyncio
import subprocess
import shutil
from datetime import datetime
from typing import Any, Dict, List, Optional
import logging

from bson import ObjectId

from app.core.database import get_mongo_db
from app.core.config import SETTINGS
from .serialization import serialize_document

logger = logging.getLogger(__name__)


def _check_mongodump_available() -> bool:
    """Check if the mongodump command is available"""
    return shutil.which("mongodump") is not None


async def create_backup_native(name: str, backup_dir: str, collections: Optional[List[str]] = None, user_id: str | None = None) -> Dict[str, Any]:
    """Create a backup using the MongoDB Native Mongodump command (recommended, fast)

    Strengths:
    - Speed.
    - Compressive efficiency.
    - Support large data volumes
    - Multiple collections in parallel.

    Requests:
    - The system needs to install MongoDB Data Tools
    - Mongodump command available in PATH
    """
    if not _check_mongodump_available():
        raise Exception("mongodump 命令不可用，请安装 MongoDB Database Tools 或使用 create_backup() 方法")

    db = get_mongo_db()

    backup_id = str(ObjectId())
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_dirname = f"backup_{name}_{timestamp}"
    backup_path = os.path.join(backup_dir, backup_dirname)

    os.makedirs(backup_dir, exist_ok=True)

    #Build Mongodump command
    cmd = [
        "mongodump",
        "--uri", SETTINGS.MONGO_URI,
        "--out", backup_path,
        "--gzip"  #Enable compression
    ]

    #If you have specified a collection, only these pools will be backed up.
    if collections:
        for collection_name in collections:
            cmd.extend(["--collection", collection_name])

    logger.info(f"Start the mongodump backup:{name}")

    #Use asyncio.to theread to execute blocked subprocess call in the online pool
    def _run_mongodump():
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600  #One hour out of time.
        )
        if result.returncode != 0:
            raise Exception(f"mongodump 执行失败: {result.stderr}")
        return result

    try:
        await asyncio.to_thread(_run_mongodump)
        logger.info(f"Backup is complete:{name}")
    except subprocess.TimeoutExpired:
        raise Exception("备份超时（超过1小时）")
    except Exception as e:
        logger.error(f"Backup failed:{e}")
        #Clear failed backup directory
        if os.path.exists(backup_path):
            await asyncio.to_thread(shutil.rmtree, backup_path)
        raise

    #Calculate Backup Size
    def _get_dir_size(path):
        total = 0
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                total += os.path.getsize(filepath)
        return total

    file_size = await asyncio.to_thread(_get_dir_size, backup_path)

    #Retrieving a collection list of actual backups
    if not collections:
        collections = await db.list_collection_names()
        collections = [c for c in collections if not c.startswith("system.")]

    backup_meta = {
        "_id": ObjectId(backup_id),
        "name": name,
        "filename": backup_dirname,
        "file_path": backup_path,
        "size": file_size,
        "collections": collections,
        "created_at": datetime.utcnow(),
        "created_by": user_id,
        "backup_type": "mongodump",  #Tag Backup Type
    }

    await db.database_backups.insert_one(backup_meta)

    return {
        "id": backup_id,
        "name": name,
        "filename": backup_dirname,
        "file_path": backup_path,
        "size": file_size,
        "collections": collections,
        "created_at": backup_meta["created_at"].isoformat(),
        "backup_type": "mongodump",
    }


async def create_backup(name: str, backup_dir: str, collections: Optional[List[str]] = None, user_id: str | None = None) -> Dict[str, Any]:
    """Create database backup (Python achieved, compatible but slow)

    For large data volume (>100MB), it is recommended to use file backup native() method
    """
    db = get_mongo_db()

    backup_id = str(ObjectId())
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"backup_{name}_{timestamp}.json.gz"
    backup_path = os.path.join(backup_dir, backup_filename)

    if not collections:
        collections = await db.list_collection_names()

    backup_data: Dict[str, Any] = {
        "backup_id": backup_id,
        "name": name,
        "created_at": datetime.utcnow().isoformat(),
        "created_by": user_id,
        "collections": collections,
        "data": {},
    }

    for collection_name in collections:
        collection = db[collection_name]
        documents: List[dict] = []
        async for doc in collection.find():
            documents.append(serialize_document(doc))
        backup_data["data"][collection_name] = documents

    os.makedirs(backup_dir, exist_ok=True)

    #Use asyncio.to theread to put the blocking I/O operation in the thread pool
    def _write_backup():
        with gzip.open(backup_path, "wt", encoding="utf-8") as f:
            json.dump(backup_data, f, ensure_ascii=False, indent=2)
        return os.path.getsize(backup_path)

    file_size = await asyncio.to_thread(_write_backup)

    backup_meta = {
        "_id": ObjectId(backup_id),
        "name": name,
        "filename": backup_filename,
        "file_path": backup_path,
        "size": file_size,
        "collections": collections,
        "created_at": datetime.utcnow(),
        "created_by": user_id,
    }

    await db.database_backups.insert_one(backup_meta)

    return {
        "id": backup_id,
        "name": name,
        "filename": backup_filename,
        "file_path": backup_path,
        "size": file_size,
        "collections": collections,
        "created_at": backup_meta["created_at"].isoformat(),
    }


async def list_backups() -> List[Dict[str, Any]]:
    db = get_mongo_db()
    backups: List[Dict[str, Any]] = []
    async for backup in db.database_backups.find().sort("created_at", -1):
        backups.append({
            "id": str(backup["_id"]),
            "name": backup["name"],
            "filename": backup["filename"],
            "size": backup["size"],
            "collections": backup["collections"],
            "created_at": backup["created_at"].isoformat(),
            "created_by": backup.get("created_by"),
        })
    return backups


async def delete_backup(backup_id: str) -> None:
    db = get_mongo_db()
    backup = await db.database_backups.find_one({"_id": ObjectId(backup_id)})
    if not backup:
        raise Exception("备份不存在")
    if os.path.exists(backup["file_path"]):
        #Use asyncio.to thread to remove the blocked file from the thread pool
        backup_type = backup.get("backup_type", "python")
        if backup_type == "mongodump":
            #Mongodump Backup is a directory, which needs to be deleted over time
            await asyncio.to_thread(shutil.rmtree, backup["file_path"])
        else:
            #Python backup is a single file
            await asyncio.to_thread(os.remove, backup["file_path"])
    await db.database_backups.delete_one({"_id": ObjectId(backup_id)})


def _convert_date_fields(doc: dict) -> dict:
    """Convert date fields in documents (string - > datetime)

    Common date fields:
    - Creatured at, upted at, completed at.
    - Started at, finished at
    - analysis date (maintain string format because it is a date rather than a time stamp)
    """
    from dateutil import parser

    date_fields = [
        "created_at", "updated_at", "completed_at",
        "started_at", "finished_at", "deleted_at",
        "last_login", "last_modified", "timestamp"
    ]

    for field in date_fields:
        if field in doc and isinstance(doc[field], str):
            try:
                #Try Parsing Date String
                doc[field] = parser.parse(doc[field])
                logger.debug(f"Convert date fields{field}: {doc[field]}")
            except Exception as e:
                logger.warning(f"Could not close temporary folder: %s{field}: {doc[field]}, Error:{e}")

    return doc


async def import_data(content: bytes, collection: str, *, format: str = "json", overwrite: bool = False, filename: str | None = None) -> Dict[str, Any]:
    """Import Data to Database

    Two import modes are supported:
    Single-pool mode: Import data to specified pool
    Multi-pool mode: Import export files containing multiple pools (automated detection)
    """
    db = get_mongo_db()

    if format.lower() == "json":
        #Use asyncio.to thread to place the blocked JSON resolution in the thread pool for execution
        def _parse_json():
            return json.loads(content.decode("utf-8"))

        data = await asyncio.to_thread(_parse_json)
    else:
        raise Exception(f"不支持的格式: {format}")

    #Tests for export formats for multiple pools
    logger.info(f"[import detection] Data type:{type(data)}")

    #New format: dictionaries containing export info and data
    if isinstance(data, dict) and "export_info" in data and "data" in data:
        logger.info(f"📦 new version of multipool export file (includes export info)")
        export_info = data.get("export_info", {})
        logger.info(f"Export information: Created ={export_info.get('created_at')}, set ={len(export_info.get('collections', []))}")

        #Drawing actual data
        data = data["data"]
        logger.info(f"Including{len(data)}Pool:{list(data.keys())}")

    #🔥 Old format: a map directly from the grouping to the document list
    if isinstance(data, dict):
        logger.info(f"🔍 [import detection]{len(data)}Key")
        logger.info(f"List of keys:{list(data.keys())[:10]}")  #Show top 10 only

        #Check the type of each key pair
        for k, v in list(data.items())[:5]:  #Only the first five.
            logger.info(f"🔍 [import detection] key '{k}': Value type={type(v)}, is the list ={isinstance(v, list)}")
            if isinstance(v, list):
                logger.info(f"🔍 [import detection] key '{k}': list length={len(v)}")

    if isinstance(data, dict) and all(isinstance(k, str) and isinstance(v, list) for k, v in data.items()):
        #Multipool Mode
        logger.info(f"Confirm as multi-pool import mode{len(data)}Round up.")

        total_inserted = 0
        imported_collections = []

        for coll_name, documents in data.items():
            if not documents:  #Skip empty collections
                logger.info(f"Jumping through empty pools:{coll_name}")
                continue

            collection_obj = db[coll_name]

            if overwrite:
                deleted_count = await collection_obj.delete_many({})
                logger.info(f"All right.{coll_name}: Delete{deleted_count.deleted_count}a document")

            #Process  id fields and date fields
            for doc in documents:
                #Convert  id
                if "_id" in doc and isinstance(doc["_id"], str):
                    try:
                        doc["_id"] = ObjectId(doc["_id"])
                    except Exception:
                        del doc["_id"]

                #🔥 Convert date fields (string - > datetime)
                _convert_date_fields(doc)

            #Insert Data
            if documents:
                res = await collection_obj.insert_many(documents)
                inserted_count = len(res.inserted_ids)
                total_inserted += inserted_count
                imported_collections.append(coll_name)
                logger.info(f"Import set{coll_name}：{inserted_count}a document")

        return {
            "mode": "multi_collection",
            "collections": imported_collections,
            "total_collections": len(imported_collections),
            "total_inserted": total_inserted,
            "filename": filename,
            "format": format,
            "overwrite": overwrite,
        }
    else:
        #Single-pool mode (old version compatible)
        logger.info(f"Single group import mode, target group:{collection}")
        logger.info(f"Data types:{type(data)}")

        if isinstance(data, dict):
            logger.info(f"The dictionary contains:{len(data)}Key")
            logger.info(f"List of keys:{list(data.keys())[:10]}")

        collection_obj = db[collection]

        if not isinstance(data, list):
            logger.info(f"🔍 [single-pool mode] Data is not a list, converted to a list")
            data = [data]

        logger.info(f"🔍 [single assembly mode]{len(data)}a document")

        if overwrite:
            deleted_count = await collection_obj.delete_many({})
            logger.info(f"All right.{collection}: Delete{deleted_count.deleted_count}a document")

        for doc in data:
            #Convert  id
            if "_id" in doc and isinstance(doc["_id"], str):
                try:
                    doc["_id"] = ObjectId(doc["_id"])
                except Exception:
                    del doc["_id"]

            #🔥 Convert date fields (string - > datetime)
            _convert_date_fields(doc)

        inserted_count = 0
        if data:
            res = await collection_obj.insert_many(data)
            inserted_count = len(res.inserted_ids)

        return {
            "mode": "single_collection",
            "collection": collection,
            "inserted_count": inserted_count,
            "filename": filename,
            "format": format,
            "overwrite": overwrite,
        }


def _sanitize_document(doc: Any) -> Any:
    """Recursively empty sensitive fields of the document

    Sensitive field keywords: api key, api secret, secret, token, password,
    You're not going to be able to do that.

    Excluded fields: max tokens, timeout, configuration times etc. (not sensitive information)
    """
    SENSITIVE_KEYWORDS = [
        "api_key", "api_secret", "secret", "token", "password",
        "client_secret", "webhook_secret", "private_key"
    ]

    #Excluded fields (although containing sensitive keywords but not sensitive information)
    EXCLUDED_FIELDS = [
        "max_tokens",      #LLM configuration: max token
        "timeout",         #Timeout
        "retry_times",     #Number of retries
        "context_length",  #Context Length
    ]

    if isinstance(doc, dict):
        sanitized = {}
        for k, v in doc.items():
            #Check if in the excluded list
            if k.lower() in [f.lower() for f in EXCLUDED_FIELDS]:
                #Keep the field
                if isinstance(v, (dict, list)):
                    sanitized[k] = _sanitize_document(v)
                else:
                    sanitized[k] = v
            #Check if field names contain sensitive keywords (neglect case)
            elif any(keyword in k.lower() for keyword in SENSITIVE_KEYWORDS):
                sanitized[k] = ""  #Clear Sensitive Fields
            elif isinstance(v, (dict, list)):
                sanitized[k] = _sanitize_document(v)  #Recursive processing
            else:
                sanitized[k] = v
        return sanitized
    elif isinstance(doc, list):
        return [_sanitize_document(item) for item in doc]
    else:
        return doc


async def export_data(collections: Optional[List[str]] = None, *, export_dir: str, format: str = "json", sanitize: bool = False) -> str:
    import pandas as pd

    #🔥 with a walk-in database connection
    db = get_mongo_db()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    if not collections:
        #🔥 list collection names()
        collections = await db.list_collection_names()
        collections = [c for c in collections if not c.startswith("system.")]

    os.makedirs(export_dir, exist_ok=True)

    all_data: Dict[str, List[dict]] = {}
    for collection_name in collections:
        collection = db[collection_name]
        docs: List[dict] = []

        #users group to export only empty arrays in desensitization mode (maintain structure, do not export actual user data)
        if sanitize and collection_name == "users":
            all_data[collection_name] = []
            continue

        #🔥Diverse Query Results
        async for doc in collection.find():
            docs.append(serialize_document(doc))
        all_data[collection_name] = docs

    #If dissensitivity is enabled, clear all sensitive fields
    if sanitize:
        all_data = _sanitize_document(all_data)

    if format.lower() == "json":
        filename = f"export_{timestamp}.json"
        file_path = os.path.join(export_dir, filename)
        export_data_dict = {
            "export_info": {
                "created_at": datetime.utcnow().isoformat(),
                "collections": collections,
                "format": format,
            },
            "data": all_data,
        }

        #Use asyncio.to theread to put the blocking I/O operation in the thread pool
        def _write_json():
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(export_data_dict, f, ensure_ascii=False, indent=2)

        await asyncio.to_thread(_write_json)
        return file_path

    if format.lower() == "csv":
        filename = f"export_{timestamp}.csv"
        file_path = os.path.join(export_dir, filename)
        rows: List[dict] = []
        for collection_name, documents in all_data.items():
            for doc in documents:
                row = {**doc}
                row["_collection"] = collection_name
                rows.append(row)

        #Use asyncio.to theread to put the blocking I/O operation in the thread pool
        def _write_csv():
            if rows:
                pd.DataFrame(rows).to_csv(file_path, index=False, encoding="utf-8-sig")
            else:
                pd.DataFrame().to_csv(file_path, index=False, encoding="utf-8-sig")

        await asyncio.to_thread(_write_csv)
        return file_path

    if format.lower() in ["xlsx", "excel"]:
        filename = f"export_{timestamp}.xlsx"
        file_path = os.path.join(export_dir, filename)

        #Use asyncio.to theread to put the blocking I/O operation in the thread pool
        def _write_excel():
            with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
                for collection_name, documents in all_data.items():
                    df = pd.DataFrame(documents) if documents else pd.DataFrame()
                    sheet = collection_name[:31]
                    df.to_excel(writer, sheet_name=sheet, index=False)

        await asyncio.to_thread(_write_excel)
        return file_path

    raise Exception(f"不支持的导出格式: {format}")

