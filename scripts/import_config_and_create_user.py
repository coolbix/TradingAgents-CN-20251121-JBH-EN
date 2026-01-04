#!/usr/bin/env python3
"""
Import configuration data and create a default user

Features:
1. Import configuration data from an exported JSON file into MongoDB
2. Create a default administrator user (admin/admin123)
3. Support selective collection import
4. Support overwriting or skipping existing data

Usage:
python scripts/import_config_and_create_user.py <export_file.json>
python scripts/import_config_and_create_user.py <export_file.json> --overwrite
python scripts/import_config_and_create_user.py <export_file.json> --collections system_configs users
"""

import json
import sys
import hashlib
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import argparse

# Add project root directory to sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pymongo import MongoClient
from bson import ObjectId


def running_inside_docker() -> bool:
    """Best-effort detection of whether the script runs inside a Docker container."""
    if Path("/.dockerenv").exists():
        return True

    cgroup = Path("/proc/1/cgroup")
    try:
        if cgroup.exists() and "docker" in cgroup.read_text():
            return True
    except Exception:
        # Ignore any failure and fall back to host mode
        pass

    return False


def load_env_config(script_dir: Path) -> dict:
    """Load configuration from the .env file

    Args:
        script_dir: Directory where the script is located

    Returns:
        Configuration dictionary including mongodb_port, etc.
    """
    # Locate .env file (in project root directory)
    env_file = script_dir.parent / '.env'

    config = {
        'mongodb_port': 27017,  # Default port
        'mongodb_host': 'localhost',
        'mongodb_username': 'admin',
        'mongodb_password': 'tradingagents123',
        'mongodb_database': 'tradingagents'
    }

    if env_file.exists():
        try:
            with open(env_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()

                        if key == 'MONGODB_PORT':
                            config['mongodb_port'] = int(value)
                        elif key == 'MONGODB_HOST':
                            config['mongodb_host'] = value
                        elif key == 'MONGODB_USERNAME':
                            config['mongodb_username'] = value
                        elif key == 'MONGODB_PASSWORD':
                            config['mongodb_password'] = value
        except Exception as e:
            print(f"⚠️  Warning: Failed to read .env file: {e}")
            print(f"   Using default configuration")
    else:
        print(f"⚠️  Warning: .env file not found: {env_file}")
        print(f"   Using default configuration")

    return config


# MongoDB configuration
# Use service name "mongodb" when running inside Docker
# Use "localhost" when running on the host machine
DB_NAME = "tradingagents"

# Default administrator user
DEFAULT_ADMIN = {
    "username": "admin",
    "password": "admin123",
    "email": "admin@tradingagents.cn"
}

# Configuration collections
CONFIG_COLLECTIONS = [
    "system_configs",
    "users",
    "llm_providers",
    "market_categories",
    "user_tags",
    "datasource_groupings",
    "platform_configs",
    "user_configs",
    "model_catalog"
]


def hash_password(password: str) -> str:
    """Hash password using SHA256 (consistent with system)"""
    return hashlib.sha256(password.encode()).hexdigest()


def convert_to_bson(data: Any) -> Any:
    """Convert JSON data to BSON-compatible format"""
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            # Handle ObjectId
            if key == "_id" or key.endswith("_id"):
                if isinstance(value, str) and len(value) == 24:
                    try:
                        result[key] = ObjectId(value)
                        continue
                    except Exception:
                        pass

            # Handle datetime fields
            if key.endswith("_at") or key in ["created_at", "updated_at", "last_login", "added_at"]:
                if isinstance(value, str):
                    try:
                        result[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        continue
                    except Exception:
                        pass

            result[key] = convert_to_bson(value)
        return result

    elif isinstance(data, list):
        return [convert_to_bson(item) for item in data]

    else:
        return data


def load_export_file(file_path: str) -> Dict[str, Any]:
    """Load exported JSON file"""
    print(f"\n📂 Loading export file: {file_path}")

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if "export_info" not in data or "data" not in data:
            print("❌ Error: Invalid file format, missing export_info or data fields")
            sys.exit(1)

        export_info = data["export_info"]
        print(f"✅ File loaded successfully")
        print(f"   Export time: {export_info.get('created_at', 'Unknown')}")
        print(f"   Export format: {export_info.get('format', 'Unknown')}")
        print(f"   Number of collections: {len(export_info.get('collections', []))}")

        return data

    except FileNotFoundError:
        print(f"❌ Error: File not found: {file_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Error: JSON parsing failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: Failed to load file: {e}")
        sys.exit(1)


def connect_mongodb(use_docker: bool = True, config: dict = None) -> MongoClient:
    """Connect to MongoDB

    Args:
        use_docker: True = running inside Docker (use service name 'mongodb')
        False = running on host machine (use 'localhost')
        config: Configuration dictionary
    """
    if config is None:
        config = {
            'mongodb_port': 27017,
            'mongodb_host': 'localhost',
            'mongodb_username': 'admin',
            'mongodb_password': 'tradingagents123',
            'mongodb_database': 'tradingagents'
        }

    # Build MongoDB URI candidates to handle both container and host execution
    configured_host = config['mongodb_host']
    port = config['mongodb_port']
    username = config['mongodb_username']
    password = config['mongodb_password']
    database = config['mongodb_database']

    host_candidates = []
    if use_docker:
        host_candidates.append("mongodb")
    if configured_host not in host_candidates:
        host_candidates.append(configured_host)
    # When running on the host, try common loopback addresses as fallback
    if not use_docker:
        for fallback in ("localhost", "127.0.0.1"):
            if fallback not in host_candidates:
                host_candidates.append(fallback)

    last_error: Optional[Exception] = None

    for candidate in host_candidates:
        mongo_uri = f"mongodb://{username}:{password}@{candidate}:{port}/{database}?authSource=admin"
        env_name = "Docker container service 'mongodb'" if candidate == "mongodb" else f"host '{candidate}'"

        print(f"\n🔌 Connecting to MongoDB ({env_name})...")
        print(f"   URI: mongodb://{username}:***@{candidate}:{port}/{database}?authSource=admin")

        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            print(f"✅ MongoDB connected successfully via {candidate}:{port}")
            return client
        except Exception as e:
            last_error = e
            print(f"   ❌ Failed to connect using host '{candidate}': {e}")
            if candidate != host_candidates[-1]:
                print("   Trying next candidate...")

    print(f"\n❌ Error: MongoDB connection failed after trying: {', '.join(host_candidates)}")
    if last_error:
        print(f"   Last error: {last_error}")
    if use_docker:
        print(f"   Ensure you are running inside Docker or use --host for host mode")
        print(f"   Check containers: docker ps | grep mongodb")
    else:
        print(f"   Ensure MongoDB is running and listening on port {port}")
        print(f"   Check port: netstat -an | findstr {port}")
        print(f"   If MongoDB runs in Docker with port mapping, you can also try --mongodb-host localhost")
    sys.exit(1)


def import_collection(
    db: Any,
    collection_name: str,
    documents: List[Dict[str, Any]],
    overwrite: bool = False
) -> Dict[str, int]:
    """Import a single collection"""
    collection = db[collection_name]

    # Convert documents to BSON-compatible format
    converted_docs = [convert_to_bson(doc) for doc in documents]

    if overwrite:
        # Overwrite mode: delete existing data
        result = collection.delete_many({})
        deleted_count = result.deleted_count

        if converted_docs:
            result = collection.insert_many(converted_docs)
            inserted_count = len(result.inserted_ids)
        else:
            inserted_count = 0

        return {"deleted": deleted_count, "inserted": inserted_count, "skipped": 0}
    else:
        # Incremental mode: skip existing documents
        inserted_count = 0
        skipped_count = 0

        for doc in converted_docs:
            # Check existence by _id, username, or name
            query = {}
            if "_id" in doc:
                query["_id"] = doc["_id"]
            elif "username" in doc:
                query["username"] = doc["username"]
            elif "name" in doc:
                query["name"] = doc["name"]
            else:
                collection.insert_one(doc)
                inserted_count += 1
                continue

            if collection.find_one(query):
                skipped_count += 1
            else:
                collection.insert_one(doc)
                inserted_count += 1

        return {"deleted": 0, "inserted": inserted_count, "skipped": skipped_count}


def create_default_admin(db: Any, overwrite: bool = False) -> bool:
    """Create default administrator user"""
    print(f"\n👤 Creating default administrator user...")

    users_collection = db.users
    existing_user = users_collection.find_one({"username": DEFAULT_ADMIN["username"]})

    if existing_user:
        if not overwrite:
            print(f"⚠️  User '{DEFAULT_ADMIN['username']}' already exists, skipping creation")
            return False
        else:
            print(f"⚠️  User '{DEFAULT_ADMIN['username']}' already exists, overwriting")
            users_collection.delete_one({"username": DEFAULT_ADMIN["username"]})

    user_doc = {
        "username": DEFAULT_ADMIN["username"],
        "email": DEFAULT_ADMIN["email"],
        "hashed_password": hash_password(DEFAULT_ADMIN["password"]),
        "is_active": True,
        "is_verified": True,
        "is_admin": True,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
        "last_login": None,
        "preferences": {
            "default_market": "A-shares",
            "default_depth": "Deep",
            "ui_theme": "light",
            "language": "zh-CN",
            "notifications_enabled": True,
            "email_notifications": False
        },
        "daily_quota": 10000,
        "concurrent_limit": 10,
        "total_analyses": 0,
        "successful_analyses": 0,
        "failed_analyses": 0,
        "favorite_stocks": []
    }

    users_collection.insert_one(user_doc)

    print(f"✅ Default administrator user created successfully")
    print(f"   Username: {DEFAULT_ADMIN['username']}")
    print(f"   Password: {DEFAULT_ADMIN['password']}")
    print(f"   Email: {DEFAULT_ADMIN['email']}")
    print(f"   Role: Administrator")

    return True


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Import configuration data and create a default user",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run inside Docker container (default)
  python scripts/import_config_and_create_user.py

  # Run on host machine (connect to localhost:27017)
  python scripts/import_config_and_create_user.py --host

  # Import from specified file (default overwrite mode)
  python scripts/import_config_and_create_user.py export.json

  # Incremental mode: skip existing data
  python scripts/import_config_and_create_user.py --incremental

  # Import only specified collections
  python scripts/import_config_and_create_user.py --collections system_configs users

  # Only create default user, do not import data
  python scripts/import_config_and_create_user.py --create-user-only
        """
    )

    parser.add_argument(
        "export_file",
        nargs="?",
        help="Path to exported JSON file (default: install/database_export_config_*.json)"
    )
    parser.add_argument(
        "--host",
        action="store_true",
        help="Run on host machine (connect to localhost:27017). Default is Docker (mongodb:27017)"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=True,
        help="Overwrite existing data (default: overwrite)"
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Incremental mode: skip existing data"
    )
    parser.add_argument(
        "--collections",
        nargs="+",
        help="Collections to import (default: all configuration collections)"
    )
    parser.add_argument(
        "--create-user-only",
        action="store_true",
        help="Only create default user, do not import data"
    )
    parser.add_argument(
        "--skip-user",
        action="store_true",
        help="Skip creating default user"
    )
    parser.add_argument(
        "--mongodb-port",
        type=int,
        help="MongoDB port (override .env configuration)"
    )
    parser.add_argument(
        "--mongodb-host",
        type=str,
        help="MongoDB host (override .env configuration)"
    )

    args = parser.parse_args()

    if args.incremental:
        args.overwrite = False

    if not args.create_user_only and not args.export_file:
        install_dir = project_root / "install"
        if install_dir.exists():
            config_files = list(install_dir.glob("database_export_config_*.json"))
            if config_files:
                args.export_file = str(sorted(config_files)[-1])
                print(f"💡 No file specified, using default: {args.export_file}")
            else:
                parser.error("No configuration file found in install directory")
        else:
            parser.error("Export file is required unless --create-user-only is used")

    print("=" * 80)
    print("📦 Import Configuration Data and Create Default User")
    print("=" * 80)

    script_dir = Path(__file__).parent
    env_config = load_env_config(script_dir)

    if args.mongodb_port:
        env_config['mongodb_port'] = args.mongodb_port
        print(f"💡 Using MongoDB port from CLI: {args.mongodb_port}")
    if args.mongodb_host:
        env_config['mongodb_host'] = args.mongodb_host
        print(f"💡 Using MongoDB host from CLI: {args.mongodb_host}")

    if args.host:
        use_docker = False
    else:
        use_docker = running_inside_docker()
        if use_docker:
            print("💡 Detected Docker container environment; will use service name 'mongodb'.")
        else:
            print("💡 Detected host environment; will try configured host/localhost before Docker service name.")

    client = connect_mongodb(use_docker=use_docker, config=env_config)
    db = client[DB_NAME]

    if not args.create_user_only:
        export_data = load_export_file(args.export_file)
        data = export_data["data"]

        if args.collections:
            collections_to_import = args.collections
        else:
            collections_to_import = [c for c in CONFIG_COLLECTIONS if c in data]

        print(f"\n📋 Preparing to import {len(collections_to_import)} collections:")
        for col in collections_to_import:
            print(f"   - {col}: {len(data.get(col, []))} documents")

        print(f"\n🚀 Starting import...")
        print(f"   Mode: {'Overwrite' if args.overwrite else 'Incremental'}")

        total_stats = {"deleted": 0, "inserted": 0, "skipped": 0}

        for collection_name in collections_to_import:
            if collection_name not in data:
                print(f"⚠️  Skipping {collection_name}: not found in export file")
                continue

            print(f"\n   Importing {collection_name}...")
            stats = import_collection(db, collection_name, data[collection_name], args.overwrite)

            total_stats["deleted"] += stats["deleted"]
            total_stats["inserted"] += stats["inserted"]
            total_stats["skipped"] += stats["skipped"]

            if args.overwrite:
                print(f"      ✅ Deleted {stats['deleted']}, inserted {stats['inserted']}")
            else:
                print(f"      ✅ Inserted {stats['inserted']}, skipped {stats['skipped']}")

        print(f"\n📊 Import Summary:")
        if args.overwrite:
            print(f"   Deleted: {total_stats['deleted']} documents")
        print(f"   Inserted: {total_stats['inserted']} documents")
        if not args.overwrite:
            print(f"   Skipped: {total_stats['skipped']} documents")

    if not args.skip_user:
        create_default_admin(db, args.overwrite)

    client.close()

    print("\n" + "=" * 80)
    print("✅ Operation completed!")
    print("=" * 80)

    if not args.skip_user:
        print("\n🔐 Login Information:")
        print(f"   Username: {DEFAULT_ADMIN['username']}")
        print(f"   Password: {DEFAULT_ADMIN['password']}")

    print("\n📝 Next Steps:")
    print("   1. Restart backend service: docker restart tradingagents-backend")
    print("   2. Open the frontend and log in using the default account")
    print("   3. Verify that system configuration is loaded correctly")


if __name__ == "__main__":
    main()
