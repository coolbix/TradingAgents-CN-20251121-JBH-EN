from fastapi import APIRouter, Depends, HTTPException, status
from typing import Any, Dict
import re
import logging

from app.core.config import SETTINGS
from app.routers.auth_db import get_current_user

router = APIRouter()
logger = logging.getLogger("webapi")

SENSITIVE_KEYS = {
    "MONGODB_PASSWORD",
    "REDIS_PASSWORD",
    "JWT_SECRET",
    "CSRF_SECRET",
    "STOCK_DATA_API_KEY",
    "REFRESH_TOKEN_EXPIRE_DAYS",  # not sensitive itself, but keep for completeness
}

MASK = "***"


def _mask_value(key: str, value: Any) -> Any:
    if value is None:
        return None
    if key in SENSITIVE_KEYS:
        return MASK
    # Mask URLs that may contain credentials
    if key in {"MONGO_URI", "REDIS_URL"} and isinstance(value, str):
        v = value
        # mongodb://user:pass@host:port/db?...
        v = re.sub(r"(mongodb://[^:/?#]+):([^@/]+)@", r"\1:***@", v)
        # redis://:pass@host:port/db
        v = re.sub(r"(redis://:)[^@/]+@", r"\1***@", v)
        return v
    return value


def _build_summary() -> Dict[str, Any]:
    raw = SETTINGS.model_dump()
    # Attach derived URLs
    raw["MONGO_URI"] = SETTINGS.MONGO_URI
    raw["REDIS_URL"] = SETTINGS.REDIS_URL

    summary: Dict[str, Any] = {}
    for k, v in raw.items():
        summary[k] = _mask_value(k, v)
    return summary


@router.get("/config/summary", tags=["system"], summary="配置概要（已屏蔽敏感项，需管理员）")
async def get_config_summary(current_user: dict = Depends(get_current_user)) -> Dict[str, Any]:
    """Returns the summary of settings that are currently in effect. Sensitive fields will be displayed in *** mask.
    Access control: Administrator status required.
    """
    if not current_user.get("is_admin", False):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required")
    return {"settings": _build_summary()}


@router.get("/config/validate", tags=["system"], summary="验证配置完整性")
async def validate_config():
    """Validation of the integrity and effectiveness of system configuration.
    Returns the validation results, including missing profiles and invalid configurations.

    Authentication content:
    1. Environmental Variable Configuration (.env file)
    2. Configuration stored in MongoDB (large models, data sources, etc.)

    Note: This interface is first configured from MongoDB to the environment variable before validation.
    """
    from app.core.startup_validator import StartupConfigValidator
    from app.core.config_bridge import consolidate_configs_to_osenviron
    from app.services.config_service import CONFIG_SERVICE

    try:
        #Step 1: Reload Configuration - Read configuration from MongoDB and receive environmental variables
        try:
            consolidate_configs_to_osenviron()
            logger.info("✅ Configuration Reloads from MongoDB to environment variable")
        except Exception as e:
            logger.warning(f"Reloading failed:{e}, the configuration of the .env file will be verified")

        #Step 2: Verify Environmental Variable Configuration
        validator = StartupConfigValidator()
        env_result = validator.validate()

        #Step 3: Validation of the configuration in MongoDB (plant level)
        mongodb_validation = {
            "llm_providers": [],
            "data_source_configs": [],
            "warnings": []
        }

        try:
            from app.utils.api_key_utils import (
                is_valid_api_key,
                get_env_api_key_for_provider
            )

            #🔥 Changes: Read raw data directly from the database and avoid using modified data returned by getting llm providers()
            #Get llm providers() gives the environment variable Key value to provider.api key, making it impossible to distinguish between sources
            from pymongo import MongoClient
            from app.core.config import SETTINGS
            from app.models.config_models import LLMProvider

            #Create a simultaneous MongoDB client
            client = MongoClient(SETTINGS.MONGO_URI)
            db = client[SETTINGS.MONGO_DB_NAME]
            providers_collection = db.llm_providers

            #Query all plant configurations (original data)
            providers_data = list(providers_collection.find())
            llm_providers = [LLMProvider(**data) for data in providers_data]

            #Close Sync Client
            client.close()

            logger.info(f"Other Organiser{len(llm_providers)}A big modeler.")

            for provider in llm_providers:
                #Validation of only active plants
                if not provider.is_active:
                    continue

                validation_item = {
                    "name": provider.name,
                    "display_name": provider.display_name,
                    "is_active": provider.is_active,
                    "has_api_key": False,
                    "status": "未配置",
                    "source": None,  #Identification configuration source (database/environment)
                    "mongodb_configured": False,  #MongoDB Configuration
                    "env_configured": False  #Environmental variable configuration
                }

                #Key: Check the validity of the original API Key in the database
                db_key_valid = is_valid_api_key(provider.api_key)
                validation_item["mongodb_configured"] = db_key_valid

                #Checks whether the API Key of the environment variable is valid
                env_key = get_env_api_key_for_provider(provider.name)
                env_key_valid = env_key is not None
                validation_item["env_configured"] = env_key_valid

                if db_key_valid:
                    #valid API Key in MongoDB (highest priority)
                    validation_item["has_api_key"] = True
                    validation_item["status"] = "已配置"
                    validation_item["source"] = "database"
                elif env_key_valid:
                    #Not available in MongoDB, but there are valid API Key in environment variables
                    validation_item["has_api_key"] = True
                    validation_item["status"] = "已配置（环境变量）"
                    validation_item["source"] = "environment"
                    #Use yellow alert to configure in the database
                    mongodb_validation["warnings"].append(
                        f"大模型厂家 {provider.display_name} 使用环境变量配置，建议在数据库中配置以便统一管理"
                    )
                else:
                    #MongoDB has no valid API Key
                    validation_item["status"] = "未配置"
                    mongodb_validation["warnings"].append(
                        f"大模型厂家 {provider.display_name} 已启用但未配置有效的 API Key（数据库和环境变量中都未找到）"
                    )

                mongodb_validation["llm_providers"].append(validation_item)

            #Verify data source configuration
            from app.utils.api_key_utils import (
                is_valid_api_key,
                get_env_api_key_for_datasource
            )

            system_config = await CONFIG_SERVICE.get_system_config_from_database()
            if system_config and system_config.data_source_configs:
                logger.info(f"Other Organiser{len(system_config.data_source_configs)}Data source configuration")

                for ds_config in system_config.data_source_configs:
                    #Verify only enabled data sources
                    if not ds_config.enabled:
                        continue

                    validation_item = {
                        "name": ds_config.name,
                        "type": ds_config.type,
                        "enabled": ds_config.enabled,
                        "has_api_key": False,
                        "status": "未配置",
                        "source": None,  #Identification configuration source (database/environment/biltin)
                        "mongodb_configured": False,  #Add: MongoDB Configure
                        "env_configured": False  #Add: Environmental variables configured
                    }

                    #Some data sources do not require API Key (e.g. AKShare)
                    if ds_config.type in ["akshare", "yahoo"]:
                        validation_item["has_api_key"] = True
                        validation_item["status"] = "已配置（无需密钥）"
                        validation_item["source"] = "builtin"
                        validation_item["mongodb_configured"] = True
                        validation_item["env_configured"] = True
                    else:
                        #Check if API Key in database is valid
                        db_key_valid = is_valid_api_key(ds_config.api_key)
                        validation_item["mongodb_configured"] = db_key_valid

                        #Checks whether the API Key of the environment variable is valid
                        ds_type = ds_config.type.value if hasattr(ds_config.type, 'value') else ds_config.type
                        env_key = get_env_api_key_for_datasource(ds_type)
                        env_key_valid = env_key is not None
                        validation_item["env_configured"] = env_key_valid

                        if db_key_valid:
                            #valid API Key in MongoDB (highest priority)
                            validation_item["has_api_key"] = True
                            validation_item["status"] = "已配置"
                            validation_item["source"] = "database"
                        elif env_key_valid:
                            #Not available in MongoDB, but there are valid API Key in environment variables
                            validation_item["has_api_key"] = True
                            validation_item["status"] = "已配置（环境变量）"
                            validation_item["source"] = "environment"
                            #Use yellow alert to configure in the database
                            mongodb_validation["warnings"].append(
                                f"数据源 {ds_config.name} 使用环境变量配置，建议在数据库中配置以便统一管理"
                            )
                        else:
                            #MongoDB has no valid API Key
                            validation_item["status"] = "未配置"
                            mongodb_validation["warnings"].append(
                                f"数据源 {ds_config.name} 已启用但未配置有效的 API Key（数据库和环境变量中都未找到）"
                            )

                    mongodb_validation["data_source_configs"].append(validation_item)

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}", exc_info=True)
            mongodb_validation["warnings"].append(f"MongoDB 配置验证失败: {str(e)}")

        #Merge Authentication Results
        logger.info(f"MongoDB validation results:{len(mongodb_validation['llm_providers'])}A big modeler.{len(mongodb_validation['data_source_configs'])}Data sources,{len(mongodb_validation['warnings'])}A warning.")

        #Change: Validation is considered unsuccessful only if there is a problem with configuration
        #MongoDB Configuration Warning (recommended configuration) does not affect overall validation results
        #Red errors are only shown when the necessary configuration in the environment variable is missing or invalid
        overall_success = env_result.success

        return {
            "success": True,
            "data": {
                #Validation results for environmental variables
                "env_validation": {
                    "success": env_result.success,
                    "missing_required": [
                        {"key": config.key, "description": config.description}
                        for config in env_result.missing_required
                    ],
                    "missing_recommended": [
                        {"key": config.key, "description": config.description}
                        for config in env_result.missing_recommended
                    ],
                    "invalid_configs": [
                        {"key": config.key, "error": config.description}
                        for config in env_result.invalid_configs
                    ],
                    "warnings": env_result.warnings
                },
                #MongoDB Configuration Validation Results
                "mongodb_validation": mongodb_validation,
                #Overall validation results (consider only necessary configurations)
                "success": overall_success
            },
            "message": "配置验证完成"
        }
    except Exception as e:
        logger.error(f"Configure authentication failed:{e}", exc_info=True)
        return {
            "success": False,
            "data": None,
            "message": f"配置验证失败: {str(e)}"
        }
