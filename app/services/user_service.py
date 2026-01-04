"""User services - database-based user management
"""

import hashlib
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
from pymongo import MongoClient
from bson import ObjectId

from app.core.config import SETTINGS
from app.models.user import User, UserCreate, UserUpdate, UserResponse

#Try Import Log Manager
try:
    from tradingagents.utils.logging_manager import get_logger
except ImportError:
    #Use standard log if import failed
    import logging
    def get_logger(name: str) -> logging.Logger:
        return logging.getLogger(name)

logger = get_logger('user_service')


class UserService:
    """User service category"""

    def __init__(self):
        self.client = MongoClient(SETTINGS.MONGO_URI)
        self.db = self.client[SETTINGS.MONGO_DB]
        self.users_collection = self.db.users

    def close(self):
        """Close database connection"""
        if hasattr(self, 'client') and self.client:
            self.client.close()
            logger.info("UserService MongoDB connection closed")

    def __del__(self):
        """Parsing function to ensure that the connection is closed"""
        self.close()
    
    @staticmethod
    def hash_password(password: str) -> str:
        """Password Hash."""
        #It's safer to use bcrypt, but first for compatibility SHA-256
        return hashlib.sha256(password.encode()).hexdigest()
    
    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        """Authentication password"""
        return UserService.hash_password(plain_password) == hashed_password
    
    async def create_user(self, user_data: UserCreate) -> Optional[User]:
        """Create User"""
        try:
            #Check if username exists
            existing_user = self.users_collection.find_one({"username": user_data.username})
            if existing_user:
                logger.warning(f"Username already exists:{user_data.username}")
                return None
            
            #Check if the mailbox exists
            existing_email = self.users_collection.find_one({"email": user_data.email})
            if existing_email:
                logger.warning(f"Mailbox already exists:{user_data.email}")
                return None
            
            #Create User Document
            user_doc = {
                "username": user_data.username,
                "email": user_data.email,
                "hashed_password": self.hash_password(user_data.password),
                "is_active": True,
                "is_verified": False,
                "is_admin": False,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "last_login": None,
                "preferences": {
                    #Analysis preferences
                    "default_market": "A股",
                    "default_depth": "3",  #Level 1-5, level 3, standard analysis (recommended)
                    "default_analysts": ["市场分析师", "基本面分析师"],
                    "auto_refresh": True,
                    "refresh_interval": 30,
                    #Appearance Settings
                    "ui_theme": "light",
                    "sidebar_width": 240,
                    #Languages and regions
                    "language": "zh-CN",
                    #Notification Settings
                    "notifications_enabled": True,
                    "email_notifications": False,
                    "desktop_notifications": True,
                    "analysis_complete_notification": True,
                    "system_maintenance_notification": True
                },
                "daily_quota": 1000,
                "concurrent_limit": 3,
                "total_analyses": 0,
                "successful_analyses": 0,
                "failed_analyses": 0,
                "favorite_stocks": []
            }
            
            result = self.users_collection.insert_one(user_doc)
            user_doc["_id"] = result.inserted_id
            
            logger.info(f"Other Organiser{user_data.username}")
            return User(**user_doc)
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return None
    
    async def authenticate_user(self, username: str, password: str) -> Optional[User]:
        """User Authentication"""
        try:
            logger.info(f"[Arabicate user]{username}")

            #Find Users
            user_doc = self.users_collection.find_one({"username": username})
            logger.info(f"[Arabicate user]{'Found User' if user_doc else 'User does not exist'}")

            if not user_doc:
                logger.warning(f"The user does not exist:{username}")
                return None

            logger.info(f"Other Organiser{user_doc.get('username')}, email={user_doc.get('email')}, is_active={user_doc.get('is_active')}")

            #Authentication password
            input_password_hash = self.hash_password(password)
            stored_password_hash = user_doc["hashed_password"]
            logger.info(f"[AG/CC BY-NC-SA 2.0]")
            logger.info(f"Enter password Hash:{input_password_hash[:20]}...")
            logger.info(f"Store password Hash:{stored_password_hash[:20]}...")
            logger.info(f"Hash matches:{input_password_hash == stored_password_hash}")

            if not self.verify_password(password, user_doc["hashed_password"]):
                logger.warning(f"[AG/CC] Password error:{username}")
                return None

            #Check if user activated
            if not user_doc.get("is_active", True):
                logger.warning(f"[Arabicate user] User disabled:{username}")
                return None

            #Update final login time
            self.users_collection.update_one(
                {"_id": user_doc["_id"]},
                {"$set": {"last_login": datetime.utcnow()}}
            )

            logger.info(f"Could not close temporary folder: %s{username}")
            return User(**user_doc)
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return None
    
    async def get_user_by_username(self, username: str) -> Optional[User]:
        """Get users by user name"""
        try:
            user_doc = self.users_collection.find_one({"username": username})
            if user_doc:
                return User(**user_doc)
            return None
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return None
    
    async def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Retrieving from user ID"""
        try:
            if not ObjectId.is_valid(user_id):
                return None
            
            user_doc = self.users_collection.find_one({"_id": ObjectId(user_id)})
            if user_doc:
                return User(**user_doc)
            return None
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return None
    
    async def update_user(self, username: str, user_data: UserUpdate) -> Optional[User]:
        """Update user information"""
        try:
            update_data = {"updated_at": datetime.utcnow()}
            
            #Update provided fields only
            if user_data.email:
                #Check if the mailbox is already used by other users
                existing_email = self.users_collection.find_one({
                    "email": user_data.email,
                    "username": {"$ne": username}
                })
                if existing_email:
                    logger.warning(f"Mailbox has been used:{user_data.email}")
                    return None
                update_data["email"] = user_data.email
            
            if user_data.preferences:
                update_data["preferences"] = user_data.preferences.model_dump()
            
            if user_data.daily_quota is not None:
                update_data["daily_quota"] = user_data.daily_quota
            
            if user_data.concurrent_limit is not None:
                update_data["concurrent_limit"] = user_data.concurrent_limit
            
            result = self.users_collection.update_one(
                {"username": username},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                logger.info(f"User update successful:{username}")
                return await self.get_user_by_username(username)
            else:
                logger.warning(f"User does not exist or does not need to be updated:{username}")
                return None
                
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return None
    
    async def change_password(self, username: str, old_password: str, new_password: str) -> bool:
        """Change Password"""
        try:
            #Authenticate old password
            user = await self.authenticate_user(username, old_password)
            if not user:
                logger.warning(f"Synchronising {username}")
                return False
            
            #Update Password
            new_hashed_password = self.hash_password(new_password)
            result = self.users_collection.update_one(
                {"username": username},
                {
                    "$set": {
                        "hashed_password": new_hashed_password,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            if result.modified_count > 0:
                logger.info(f"The password was changed successfully:{username}")
                return True
            else:
                logger.error(f"Could not close temporary folder: %s{username}")
                return False
                
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return False
    
    async def reset_password(self, username: str, new_password: str) -> bool:
        """Reset password (administrator operation)"""
        try:
            new_hashed_password = self.hash_password(new_password)
            result = self.users_collection.update_one(
                {"username": username},
                {
                    "$set": {
                        "hashed_password": new_hashed_password,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            if result.modified_count > 0:
                logger.info(f"Successfully reset password:{username}")
                return True
            else:
                logger.error(f"Could not close temporary folder: %s{username}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to reset password:{e}")
            return False
    
    async def create_admin_user(self, username: str = "admin", password: str = "admin123", email: str = "admin@tradingagents.cn") -> Optional[User]:
        """Create administrator user"""
        try:
            #Check if an administrator exists
            existing_admin = self.users_collection.find_one({"username": username})
            if existing_admin:
                logger.info(f"Other Organiser{username}")
                return User(**existing_admin)
            
            #Create administrator user document
            admin_doc = {
                "username": username,
                "email": email,
                "hashed_password": self.hash_password(password),
                "is_active": True,
                "is_verified": True,
                "is_admin": True,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "last_login": None,
                "preferences": {
                    "default_market": "A股",
                    "default_depth": "深度",
                    "ui_theme": "light",
                    "language": "zh-CN",
                    "notifications_enabled": True,
                    "email_notifications": False
                },
                "daily_quota": 10000,  #Higher quota for administrators
                "concurrent_limit": 10,
                "total_analyses": 0,
                "successful_analyses": 0,
                "failed_analyses": 0,
                "favorite_stocks": []
            }
            
            result = self.users_collection.insert_one(admin_doc)
            admin_doc["_id"] = result.inserted_id
            
            logger.info(f"Could not close temporary folder: %s{username}")
            logger.info(f"Password:{password}")
            logger.info("Please change the default password immediately!")
            
            return User(**admin_doc)
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return None
    
    async def list_users(self, skip: int = 0, limit: int = 100) -> List[UserResponse]:
        """Get User List"""
        try:
            cursor = self.users_collection.find().skip(skip).limit(limit)
            users = []
            
            for user_doc in cursor:
                user = User(**user_doc)
                users.append(UserResponse(
                    id=str(user.id),
                    username=user.username,
                    email=user.email,
                    is_active=user.is_active,
                    is_verified=user.is_verified,
                    created_at=user.created_at,
                    last_login=user.last_login,
                    preferences=user.preferences,
                    daily_quota=user.daily_quota,
                    concurrent_limit=user.concurrent_limit,
                    total_analyses=user.total_analyses,
                    successful_analyses=user.successful_analyses,
                    failed_analyses=user.failed_analyses
                ))
            
            return users
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return []
    
    async def deactivate_user(self, username: str) -> bool:
        """Disable User"""
        try:
            result = self.users_collection.update_one(
                {"username": username},
                {
                    "$set": {
                        "is_active": False,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            if result.modified_count > 0:
                logger.info(f"User disabled:{username}")
                return True
            else:
                logger.warning(f"Other Organiser{username}")
                return False
                
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return False
    
    async def activate_user(self, username: str) -> bool:
        """Activate User"""
        try:
            result = self.users_collection.update_one(
                {"username": username},
                {
                    "$set": {
                        "is_active": True,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            if result.modified_count > 0:
                logger.info(f"The user has activated:{username}")
                return True
            else:
                logger.warning(f"Other Organiser{username}")
                return False
                
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return False


#Examples of global user services
user_service = UserService()
