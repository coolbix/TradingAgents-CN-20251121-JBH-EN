#!/usr/bin/env python3
"""Database Configuration Management Module
Harmonized management of MongoDB and Redis connection configuration
"""

import os 
from typing import Dict ,Any ,Optional 


class DatabaseConfig :
    """Database Configuration Management Category"""

    @staticmethod 
    def get_mongodb_config ()->Dict [str ,Any ]:
        """Get MongoDB Configuration

Returns:
Dict [str, Any]: MongoDB configuration dictionary

Rices:
ValueError: When the necessary configuration is not set Time
"""
        connection_string =os .getenv ('MONGODB_CONNECTION_STRING')
        if not connection_string :
            raise ValueError (
            "MongoDB连接字符串未配置。请设置环境变量 MONGODB_CONNECTION_STRING\n"
            "例如: MONGODB_CONNECTION_STRING=mongodb://localhost:27017/"
            )

        return {
        'connection_string':connection_string ,
        'database':os .getenv ('MONGODB_DATABASE','tradingagents'),
        'auth_source':os .getenv ('MONGODB_AUTH_SOURCE','admin')
        }

    @staticmethod 
    def get_redis_config ()->Dict [str ,Any ]:
        """Get Redis Configuration

Returns:
Dict [str, Any]: Redis configuration dictionary

Rices:
ValueError: When the necessary configuration is not set Time
"""
        # Prefer connecting string
        connection_string =os .getenv ('REDIS_CONNECTION_STRING')
        if connection_string :
            return {
            'connection_string':connection_string ,
            'database':int (os .getenv ('REDIS_DATABASE',0 ))
            }

            # Use separated configuration parameters
        host =os .getenv ('REDIS_HOST')
        port =os .getenv ('REDIS_PORT')

        if not host or not port :
            raise ValueError (
            "Redis连接配置未完整设置。请设置以下环境变量之一：\n"
            "1. REDIS_CONNECTION_STRING=redis://localhost:6379/0\n"
            "2. REDIS_HOST + REDIS_PORT (例如: REDIS_HOST=localhost, REDIS_PORT=6379)"
            )

        return {
        'host':host ,
        'port':int (port ),
        'password':os .getenv ('REDIS_PASSWORD'),
        'database':int (os .getenv ('REDIS_DATABASE',0 ))
        }

    @staticmethod 
    def validate_config ()->Dict [str ,bool ]:
        """Verify whether the database configuration is complete

Returns:
Dict[str, bool]: Verify results
"""
        result ={
        'mongodb_valid':False ,
        'redis_valid':False 
        }

        try :
            DatabaseConfig .get_mongodb_config ()
            result ['mongodb_valid']=True 
        except ValueError:
            pass 

        try :
            DatabaseConfig .get_redis_config ()
            result ['redis_valid']=True 
        except ValueError:
            pass 

        return result 

    @staticmethod 
    def get_config_status ()->str :
        """Fetch a friendly description of the configuration status

Returns:
str: Configure Status Description
"""
        validation =DatabaseConfig .validate_config ()

        if validation ['mongodb_valid']and validation ['redis_valid']:
            return "✅ 所有数据库配置正常"
        elif validation ['mongodb_valid']:
            return "⚠️ MongoDB配置正常，Redis配置缺失"
        elif validation ['redis_valid']:
            return "⚠️ Redis配置正常，MongoDB配置缺失"
        else :
            return "❌ 数据库配置缺失，请检查环境变量"