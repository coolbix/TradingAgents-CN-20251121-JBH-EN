"""Configure to manage API route
"""

import logging
from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.routers.auth_db import get_current_user
from app.models.user import User
from app.models.config_models import (
    SystemConfigResponse, LLMConfigRequest, DataSourceConfigRequest,
    DatabaseConfigRequest, ConfigTestRequest, ConfigTestResponse,
    LLMConfig, DataSourceConfig, DatabaseConfig,
    LLMProvider, LLMProviderRequest, LLMProviderResponse,
    MarketCategory, MarketCategoryRequest, DataSourceGrouping,
    DataSourceGroupingRequest, DataSourceOrderRequest,
    ModelCatalog, ModelInfo
)
from app.services.config_service import CONFIG_SERVICE
from datetime import datetime
from app.utils.timezone import now_tz

from app.services.operation_log_service import log_operation
from app.models.operationlog_models import ActionType
from app.services.config_provider import CONFIG_PROVIDER as config_provider



router = APIRouter(prefix="/config", tags=["配置管理"])
logger = logging.getLogger("webapi")


#== sync, corrected by elderman == @elder man

@router.post("/reload", summary="重新加载配置")
async def reload_config(current_user: dict = Depends(get_current_user)):
    """Reload configuration and bridge to environment variable

    Effective immediately after configuration update without restarting service
    """
    try:
        from app.core.config_bridge import reload_bridged_config

        success = reload_bridged_config()

        if success:
            await log_operation(
                user_id=str(current_user.get("user_id", "")),
                username=current_user.get("username", "unknown"),
                action_type=ActionType.CONFIG_MANAGEMENT,
                action="重载配置",
                details={"action": "reload_config"},
                ip_address="",
                user_agent=""
            )

            return {
                "success": True,
                "message": "配置重载成功",
                "data": {
                    "reloaded_at": now_tz().isoformat()
                }
            }
        else:
            return {
                "success": False,
                "message": "配置重载失败，请查看日志"
            }
    except Exception as e:
        logger.error(f"Cannot initialise Evolution's mail component.{e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"配置重载失败: {str(e)}"
        )


#== sync, corrected by elderman == @elder man
from copy import deepcopy

def _sanitize_llm_configs(items):
    try:
        return [LLMConfig(**{**i.model_dump(), "api_key": None}) for i in items]
    except Exception:
        return items

def _sanitize_datasource_configs(items):
    """Desensitive data source configuration, return abbreviated API Key

    Logical:
    1. Return abbreviated version if there is a valid API Key in the database
    2. If not available in the database, try to read from the environment variable and return the abbreviated version
    3. If none, return
    """
    try:
        from app.utils.api_key_utils import (
            is_valid_api_key,
            truncate_api_key,
            get_env_api_key_for_datasource
        )

        result = []
        for item in items:
            data = item.model_dump()

            #Process API Key
            db_key = data.get("api_key")
            if is_valid_api_key(db_key):
                #API Key in database, return abbreviated version
                data["api_key"] = truncate_api_key(db_key)
            else:
                #There is no valid API Key in the database, trying to read from the environment variable
                ds_type = data.get("type")
                if isinstance(ds_type, str):
                    env_key = get_env_api_key_for_datasource(ds_type)
                    if env_key:
                        #API Key, return abbreviated version of the environment variable
                        data["api_key"] = truncate_api_key(env_key)
                    else:
                        data["api_key"] = None
                else:
                    data["api_key"] = None

            #Process API Secret
            db_secret = data.get("api_secret")
            if is_valid_api_key(db_secret):
                data["api_secret"] = truncate_api_key(db_secret)
            else:
                data["api_secret"] = None

            result.append(DataSourceConfig(**data))

        return result
    except Exception as e:
        print(f"⚠️ 脱敏数据源配置失败: {e}")
        return items

def _sanitize_database_configs(items):
    try:
        return [DatabaseConfig(**{**i.model_dump(), "password": None}) for i in items]
    except Exception:
        return items

def _sanitize_kv(d: Dict[str, Any]) -> Dict[str, Any]:
    """Sensitization of potentially sensitive keys in the dictionary (for response only)."""
    try:
        if not isinstance(d, dict):
            return d
        sens_patterns = ("key", "secret", "password", "token", "client_secret")
        redacted = {}
        for k, v in d.items():
            if isinstance(k, str) and any(p in k.lower() for p in sens_patterns):
                redacted[k] = None
            else:
                redacted[k] = v
        return redacted
    except Exception:
        return d




class SetDefaultRequest(BaseModel):
    """Set default configuration request"""
    name: str


@router.get("/system", response_model=SystemConfigResponse)
async def get_system_config(
    current_user: User = Depends(get_current_user)
):
    """Get System Configuration"""
    try:
        config = await CONFIG_SERVICE.get_system_config_from_database()
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="系统配置不存在"
            )

        return SystemConfigResponse(
            config_name=config.config_name,
            config_type=config.config_type,
            llm_configs=_sanitize_llm_configs(config.llm_configs),
            default_llm=config.default_llm,
            data_source_configs=_sanitize_datasource_configs(config.data_source_configs),
            default_data_source=config.default_data_source,
            database_configs=_sanitize_database_configs(config.database_configs),
            system_settings=_sanitize_kv(config.system_settings),
            created_at=config.created_at,
            updated_at=config.updated_at,
            version=config.version,
            is_active=config.is_active
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取系统配置失败: {str(e)}"
        )


#== sync, corrected by elderman ==

@router.get("/llm/providers", response_model=List[LLMProviderResponse])
async def get_llm_providers(
    current_user: User = Depends(get_current_user)
):
    """Get all the big models."""
    try:
        from app.utils.api_key_utils import (
            is_valid_api_key,
            truncate_api_key,
            get_env_api_key_for_provider
        )

        providers = await CONFIG_SERVICE.get_llm_providers()
        result = []

        for provider in providers:
            #Process API Key: Prioritize database configuration and check environment variables if the database is not available
            db_key_valid = is_valid_api_key(provider.api_key)
            if db_key_valid:
                #API Key in database, return abbreviated version
                api_key_display = truncate_api_key(provider.api_key)
            else:
                #There is no valid API Key in the database, trying to read from the environment variable
                env_key = get_env_api_key_for_provider(provider.name)
                if env_key:
                    #API Key, return abbreviated version of the environment variable
                    api_key_display = truncate_api_key(env_key)
                else:
                    api_key_display = None

            #Process API Secret
            db_secret_valid = is_valid_api_key(provider.api_secret)
            if db_secret_valid:
                api_secret_display = truncate_api_key(provider.api_secret)
            else:
                #Note: API Secret is not usually among environment variables, so only data are checked here Library
                api_secret_display = None

            result.append(
                LLMProviderResponse(
                    id=str(provider.id),
                    name=provider.name,
                    display_name=provider.display_name,
                    description=provider.description,
                    website=provider.website,
                    api_doc_url=provider.api_doc_url,
                    logo_url=provider.logo_url,
                    is_active=provider.is_active,
                    supported_features=provider.supported_features,
                    default_base_url=provider.default_base_url,
                    #Returns the abbreviated API Key (6th + "..." + 6th)
                    api_key=api_key_display,
                    api_secret=api_secret_display,
                    extra_config={
                        **provider.extra_config,
                        "has_api_key": bool(api_key_display),
                        "has_api_secret": bool(api_secret_display)
                    },
                    created_at=provider.created_at,
                    updated_at=provider.updated_at
                )
            )

        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取厂家列表失败: {str(e)}"
        )


@router.post("/llm/providers", response_model=dict)
async def add_llm_provider(
    request: LLMProviderRequest,
    current_user: User = Depends(get_current_user)
):
    """Add a large modeler (option A:REST does not accept keys, forced cleaning)"""
    try:
        sanitized = request.model_dump()
        if 'api_key' in sanitized:
            sanitized['api_key'] = ""
        provider = LLMProvider(**sanitized)
        provider_id = await CONFIG_SERVICE.add_llm_provider(provider)

        #Audit log (overlooking anomalies)
        try:
            await log_operation(
                user_id=str(getattr(current_user, "id", "")),
                username=getattr(current_user, "username", "unknown"),
                action_type=ActionType.CONFIG_MANAGEMENT,
                action="add_llm_provider",
                details={"provider_id": str(provider_id), "name": request.name},
                success=True,
            )
        except Exception:
            pass
        return {
            "success": True,
            "message": "厂家添加成功",
            "data": {"id": str(provider_id)}
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"添加厂家失败: {str(e)}"
        )


@router.put("/llm/providers/{provider_id}", response_model=dict)
async def update_llm_provider(
    provider_id: str,
    request: LLMProviderRequest,
    current_user: User = Depends(get_current_user)
):
    """Update the big modeler."""
    try:
        from app.utils.api_key_utils import should_skip_api_key_update

        update_data = request.model_dump(exclude_unset=True)

        #Changes: updated logic for processing API Key
        #1. If API Key is an empty string, this means that the user wants to empty the key → Save an empty string
        #2. If API Key is a placeholder or cut key (e.g. "sk-99054..."), delete the field (not updated)
        #Update if API Key is a valid complete key
        if 'api_key' in update_data:
            api_key = update_data.get('api_key', '')
            #Delete the field if updates (placeholder or cut key) should be skipped
            if should_skip_api_key_update(api_key):
                del update_data['api_key']
            #If empty string, keep (means empty)
            #If a valid complete key is maintained (indicating updates)

        if 'api_secret' in update_data:
            api_secret = update_data.get('api_secret', '')
            #Same logic API Secret
            if should_skip_api_key_update(api_secret):
                del update_data['api_secret']

        success = await CONFIG_SERVICE.update_llm_provider(provider_id, update_data)

        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="update_llm_provider",
                    details={"provider_id": provider_id, "changed_keys": list(request.model_dump().keys())},
                    success=True,
                )
            except Exception:
                pass
            return {
                "success": True,
                "message": "厂家更新成功",
                "data": {}
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="厂家不存在"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新厂家失败: {str(e)}"
        )


@router.delete("/llm/providers/{provider_id}", response_model=dict)
async def delete_llm_provider(
    provider_id: str,
    current_user: User = Depends(get_current_user)
):
    """Remove Large Modeler"""
    try:
        success = await CONFIG_SERVICE.delete_llm_provider(provider_id)

        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="delete_llm_provider",
                    details={"provider_id": provider_id},
                    success=True,
                )
            except Exception:
                pass
            return {
                "success": True,
                "message": "厂家删除成功",
                "data": {}
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="厂家不存在"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除厂家失败: {str(e)}"
        )


@router.patch("/llm/providers/{provider_id}/toggle", response_model=dict)
async def toggle_llm_provider(
    provider_id: str,
    request: dict,
    current_user: User = Depends(get_current_user)
):
    """Toggle large modeler state"""
    try:
        is_active = request.get("is_active", True)
        success = await CONFIG_SERVICE.toggle_llm_provider(provider_id, is_active)

        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="toggle_llm_provider",
                    details={"provider_id": provider_id, "is_active": bool(is_active)},
                    success=True,
                )
            except Exception:
                pass
            return {
                "success": True,
                "message": f"厂家已{'启用' if is_active else '禁用'}",
                "data": {}
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="厂家不存在"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"切换厂家状态失败: {str(e)}"
        )


@router.post("/llm/providers/{provider_id}/fetch-models", response_model=dict)
async def fetch_provider_models(
    provider_id: str,
    current_user: User = Depends(get_current_user)
):
    """Fetch model list from the manufacturer API"""
    try:
        result = await CONFIG_SERVICE.fetch_provider_models(provider_id)
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"获取模型列表失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取模型列表失败: {str(e)}"
        )


@router.post("/llm/providers/migrate-env", response_model=dict)
async def migrate_env_to_providers(
    current_user: User = Depends(get_current_user)
):
    """Migration of environmental variables to plant management"""
    try:
        result = await CONFIG_SERVICE.migrate_env_to_providers()
        #Audit log (overlooking anomalies)
        try:
            await log_operation(
                user_id=str(getattr(current_user, "id", "")),
                username=getattr(current_user, "username", "unknown"),
                action_type=ActionType.CONFIG_MANAGEMENT,
                action="migrate_env_to_providers",
                details={
                    "migrated_count": result.get("migrated_count", 0),
                    "skipped_count": result.get("skipped_count", 0)
                },
                success=bool(result.get("success", False)),
            )
        except Exception:
            pass

        return {
            "success": result["success"],
            "message": result["message"],
            "data": {
                "migrated_count": result.get("migrated_count", 0),
                "skipped_count": result.get("skipped_count", 0)
            }
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"环境变量迁移失败: {str(e)}"
        )


@router.post("/llm/providers/init-aggregators", response_model=dict)
async def init_aggregator_providers(
    current_user: User = Depends(get_current_user)
):
    """Initialized polymerization channel plant configuration (302.AI, OpenRouter, etc.)"""
    try:
        result = await CONFIG_SERVICE.init_aggregator_providers()

        #Audit log (overlooking anomalies)
        try:
            await log_operation(
                user_id=str(getattr(current_user, "id", "")),
                username=getattr(current_user, "username", "unknown"),
                action_type=ActionType.CONFIG_MANAGEMENT,
                action="init_aggregator_providers",
                details={
                    "added_count": result.get("added", 0),
                    "skipped_count": result.get("skipped", 0)
                },
                success=bool(result.get("success", False)),
            )
        except Exception:
            pass

        return {
            "success": result["success"],
            "message": result["message"],
            "data": {
                "added_count": result.get("added", 0),
                "skipped_count": result.get("skipped", 0)
            }
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"初始化聚合渠道失败: {str(e)}"
        )


@router.post("/llm/providers/{provider_id}/test", response_model=dict)
async def test_provider_api(
    provider_id: str,
    current_user: User = Depends(get_current_user)
):
    """Tester API Key"""
    try:
        logger.info(f"- Request for API testing.{provider_id}")
        result = await CONFIG_SERVICE.test_provider_api(provider_id)
        logger.info(f"API test results:{result}")
        return result
    except Exception as e:
        logger.error(f"Test factory API failed:{e}")
        raise HTTPException(
            status_code=500,
            detail=f"测试厂家API失败: {str(e)}"
        )


#== sync, corrected by elderman == @elder man

@router.post("/llm", response_model=dict)
async def add_llm_config(
    request: LLMConfigRequest,
    current_user: User = Depends(get_current_user)
):
    """Add or update large model configuration"""
    try:
        logger.info(f"🔧 Add/update large model configuration start")
        logger.info(f"Data requested:{request.model_dump()}")
        logger.info(f"The plant:{request.provider}, Model:{request.model_name}")

        #Create LLM Configuration
        llm_config_data = request.model_dump()
        logger.info(f"Original configuration data:{llm_config_data}")

        #If API keys are not provided, get from the plant configuration
        if not llm_config_data.get('api_key'):
            logger.info(f"The API key is empty and obtained from the plant configuration:{request.provider}")

            #Get Plant Configuration
            providers = await CONFIG_SERVICE.get_llm_providers()
            logger.info(f"Found it.{len(providers)}Plant Configuration")

            for p in providers:
                logger.info(f"- Vendor:{p.name}, with an API key:{bool(p.api_key)}")

            provider_config = next((p for p in providers if p.name == request.provider), None)

            if provider_config:
                logger.info(f"We found the plant configuration:{provider_config.name}")
                if provider_config.api_key:
                    llm_config_data['api_key'] = provider_config.api_key
                    logger.info(f"✅ successfully obtained the manufacturer's API key (long:{len(provider_config.api_key)})")
                else:
                    logger.warning(f"The manufacturer.{request.provider}No API key configured")
                    llm_config_data['api_key'] = ""
            else:
                logger.warning(f"No factory found.{request.provider}Configure")
                llm_config_data['api_key'] = ""
        else:
            logger.info(f"Use the available API keys (long:{len(llm_config_data.get('api_key', ''))})")

        logger.info(f"Final configuration data:{llm_config_data}")
        #🔥 Changes: Allows writing through RST, but emptys if an invalid key
        #Invalid key: Empty string, placeholder (your xx), insufficient length
        if 'api_key' in llm_config_data:
            api_key = llm_config_data.get('api_key', '')
            #If Key is invalid, empty (use system environment variable)
            if not api_key or api_key.startswith('your_') or api_key.startswith('your-') or len(api_key) <= 10:
                llm_config_data['api_key'] = ""


        #Try creating LLMConfig objects
        try:
            llm_config = LLMConfig(**llm_config_data)
            logger.info(f"LLMConfig object created successfully")
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            logger.error(f"Data for failure:{llm_config_data}")
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"配置数据验证失败: {str(e)}"
            )

        #Save Configuration
        success = await CONFIG_SERVICE.update_llm_config(llm_config)

        if success:
            logger.info(f"✅ Large model configuration update successful:{llm_config.provider}/{llm_config.model_name}")

            #Sync Pricing Configuration to TradingAGents
            try:
                from app.core.config_bridge import sync_pricing_config_now
                sync_pricing_config_now()
                logger.info(f"✅ Pricing configuration synchronized to trafficagents")
            except Exception as e:
                logger.warning(f"Synchronized pricing configuration failed:{e}")

            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="update_llm_config",
                    details={"provider": llm_config.provider, "model_name": llm_config.model_name},
                    success=True,
                )
            except Exception:
                pass
            return {"message": "大模型配置更新成功", "model_name": llm_config.model_name}
        else:
            logger.error(f"Large model configuration failed")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="大模型配置更新失败"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Add large model configuration anomalies:{e}")
        import traceback
        logger.error(f"Anomalous stack:{traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"添加大模型配置失败: {str(e)}"
        )


@router.post("/datasource", response_model=dict)
async def add_data_source_config(
    request: DataSourceConfigRequest,
    current_user: User = Depends(get_current_user)
):
    """Add Data Source Configuration"""
    try:
        #Open source version: All users can modify configuration

        #Get Current Configuration
        config = await CONFIG_SERVICE.get_system_config_from_database()
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="系统配置不存在"
            )

        #Add new data source configuration
        #🔥 Modification: Support the preservation of API Key (consistent with large modeler management logic)
        from app.utils.api_key_utils import should_skip_api_key_update, is_valid_api_key

        _req = request.model_dump()

        #Process API Key
        if 'api_key' in _req:
            api_key = _req.get('api_key', '')
            #If a placeholder or cut key, empty the field
            if should_skip_api_key_update(api_key):
                _req['api_key'] = ""
            #If empty string, keep (means using environment variable)
            elif api_key == '':
                _req['api_key'] = ''
            #If a new key is entered, the validity must be verified
            elif not is_valid_api_key(api_key):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="API Key 无效：长度必须大于 10 个字符，且不能是占位符"
                )
            #Valid Full Key, Save

        #Process API Secret
        if 'api_secret' in _req:
            api_secret = _req.get('api_secret', '')
            if should_skip_api_key_update(api_secret):
                _req['api_secret'] = ""
            #If an empty string, keep
            elif api_secret == '':
                _req['api_secret'] = ''
            #If a new key is entered, the validity must be verified
            elif not is_valid_api_key(api_secret):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="API Secret 无效：长度必须大于 10 个字符，且不能是占位符"
                )

        ds_config = DataSourceConfig(**_req)
        config.data_source_configs.append(ds_config)

        success = await CONFIG_SERVICE.save_system_config(config)
        if success:
            #Automatically create a data source group relationship 🆕
            market_categories = _req.get('market_categories', [])
            if market_categories:
                for category_id in market_categories:
                    try:
                        grouping = DataSourceGrouping(
                            data_source_name=ds_config.name,
                            market_category_id=category_id,
                            priority=ds_config.priority,
                            enabled=ds_config.enabled
                        )
                        await CONFIG_SERVICE.add_datasource_to_category(grouping)
                    except Exception as e:
                        #If there is a group or other error, record without affecting the main process
                        logger.warning(f"Could not close temporary folder: %s{str(e)}")

            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="add_data_source_config",
                    details={"name": ds_config.name, "market_categories": market_categories},
                    success=True,
                )
            except Exception:
                pass
            return {"message": "数据源配置添加成功", "name": ds_config.name}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="数据源配置添加失败"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"添加数据源配置失败: {str(e)}"
        )


@router.post("/database", response_model=dict)
async def add_database_config(
    request: DatabaseConfigRequest,
    current_user: User = Depends(get_current_user)
):
    """Add Database Configuration"""
    try:
        #Open source version: All users can modify configuration

        #Get Current Configuration
        config = await CONFIG_SERVICE.get_system_config_from_database()
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="系统配置不存在"
            )

        #Add a new database configuration (option A: Clean sensitive fields)
        _req = request.model_dump()
        _req['password'] = ""
        db_config = DatabaseConfig(**_req)
        config.database_configs.append(db_config)

        success = await CONFIG_SERVICE.save_system_config(config)
        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="add_database_config",
                    details={"name": db_config.name},
                    success=True,
                )
            except Exception:
                pass
            return {"message": "数据库配置添加成功", "name": db_config.name}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="数据库配置添加失败"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"添加数据库配置失败: {str(e)}"
        )


@router.post("/test", response_model=ConfigTestResponse)
async def test_config(
    request: ConfigTestRequest,
    current_user: User = Depends(get_current_user)
):
    """Test Configuration Connection"""
    try:
        if request.config_type == "llm":
            llm_config = LLMConfig(**request.config_data)
            result = await CONFIG_SERVICE.test_llm_config(llm_config)
        elif request.config_type == "datasource":
            ds_config = DataSourceConfig(**request.config_data)
            result = await CONFIG_SERVICE.test_data_source_config(ds_config)
        elif request.config_type == "database":
            db_config = DatabaseConfig(**request.config_data)
            result = await CONFIG_SERVICE.test_database_config(db_config)
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="不支持的配置类型"
            )

        return ConfigTestResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"测试配置失败: {str(e)}"
        )


@router.post("/database/{db_name}/test", response_model=ConfigTestResponse)
async def test_saved_database_config(
    db_name: str,
    current_user: dict = Depends(get_current_user)
):
    """Test saved database configuration (take full configuration from database, including password)"""
    try:
        logger.info(f"Test the saved database configuration:{db_name}")

        #Get full system configuration from the database
        config = await CONFIG_SERVICE.get_system_config_from_database()
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="系统配置不存在"
            )

        #Find specified database configuration
        db_config = None
        for db in config.database_configs:
            if db.name == db_name:
                db_config = db
                break

        if not db_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"数据库配置 '{db_name}' 不存在"
            )

        logger.info(f"Cannot initialise Evolution's mail component.{db_config.name} ({db_config.type})")
        logger.info(f"Connect information:{db_config.host}:{db_config.port}")
        logger.info(f"User name:{db_config.username or '(none)'}")
        logger.info(f"Password:{'***' if db_config.password else '(none)'}")

        #Test using full configuration
        result = await CONFIG_SERVICE.test_database_config(db_config)

        return ConfigTestResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Test database configuration failed:{e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"测试数据库配置失败: {str(e)}"
        )


@router.get("/llm", response_model=List[LLMConfig])
async def get_llm_configs(
    current_user: User = Depends(get_current_user)
):
    """Fetch all large model configurations"""
    try:
        logger.info("Start accessing large models...")
        config = await CONFIG_SERVICE.get_system_config_from_database()

        if not config:
            logger.warning("⚠️ System configuration is empty, return empty list")
            return []

        logger.info(f"📊 System configuration exists, large model configuration number:{len(config.llm_configs)}")

        #Create some examples if no large model configuration
        if not config.llm_configs:
            logger.info("No large model configuration, create example configuration...")
            #This can create an example configuration based on the existing manufacturer
            #Return empty list for the time being so that the frontend can show "no configuration"

        #Access to all vendor information to filter models of banned vendors
        providers = await CONFIG_SERVICE.get_llm_providers()
        active_provider_names = {p.name for p in providers if p.is_active}

        #Filter: returns only the active model and the model is also enabled by the supplier
        filtered_configs = [
            llm_config for llm_config in config.llm_configs
            if llm_config.enabled and llm_config.provider in active_provider_names
        ]

        logger.info(f"✅ Number of large filtered models configured:{len(filtered_configs)}(Original:{len(config.llm_configs)})")

        return _sanitize_llm_configs(filtered_configs)
    except Exception as e:
        logger.error(f"Can not get folder: %s: %s{e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取大模型配置失败: {str(e)}"
        )


@router.delete("/llm/{provider}/{model_name}")
async def delete_llm_config(
    provider: str,
    model_name: str,
    current_user: User = Depends(get_current_user)
):
    """Remove Large Model Configuration"""
    try:
        logger.info(f"Remove large model configuration request-provider:{provider}, model_name: {model_name}")
        success = await CONFIG_SERVICE.delete_llm_config(provider, model_name)

        if success:
            logger.info(f"The large model configuration was successfully deleted -{provider}/{model_name}")

            #Sync Pricing Configuration to TradingAGents
            try:
                from app.core.config_bridge import sync_pricing_config_now
                sync_pricing_config_now()
                logger.info(f"✅ Pricing configuration synchronized to trafficagents")
            except Exception as e:
                logger.warning(f"Synchronized pricing configuration failed:{e}")

            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="delete_llm_config",
                    details={"provider": provider, "model_name": model_name},
                    success=True,
                )
            except Exception:
                pass
            return {"message": "大模型配置删除成功"}
        else:
            logger.warning(f"No large model configuration found -{provider}/{model_name}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="大模型配置不存在"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Remove large model configuration anomalies -{provider}/{model_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除大模型配置失败: {str(e)}"
        )


@router.post("/llm/set-default")
async def set_default_llm(
    request: SetDefaultRequest,
    current_user: User = Depends(get_current_user)
):
    """Set Default Large Model"""
    try:
        success = await CONFIG_SERVICE.set_default_llm(request.name)
        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="set_default_llm",
                    details={"name": request.name},
                    success=True,
                )
            except Exception:
                pass
            return {"message": "默认大模型设置成功", "default_llm": request.name}
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="指定的大模型不存在"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"设置默认大模型失败: {str(e)}"
        )


@router.get("/datasource", response_model=List[DataSourceConfig])
async def get_data_source_configs(
    current_user: User = Depends(get_current_user)
):
    """Get All Data Source Configurations"""
    try:
        config = await CONFIG_SERVICE.get_system_config_from_database()
        if not config:
            return []
        return _sanitize_datasource_configs(config.data_source_configs)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取数据源配置失败: {str(e)}"
        )


@router.put("/datasource/{name}", response_model=dict)
async def update_data_source_config(
    name: str,
    request: DataSourceConfigRequest,
    current_user: User = Depends(get_current_user)
):
    """Update data source configuration"""
    try:
        #Get Current Configuration
        config = await CONFIG_SERVICE.get_system_config_from_database()
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="系统配置不存在"
            )

        #Find and update the data source configuration
        from app.utils.api_key_utils import should_skip_api_key_update, is_valid_api_key

        def _truncate_api_key(api_key: str, prefix_len: int = 6, suffix_len: int = 6) -> str:
            """Cut API Key for display"""
            if not api_key or len(api_key) <= prefix_len + suffix_len:
                return api_key
            return f"{api_key[:prefix_len]}...{api_key[-suffix_len:]}"

        for i, ds_config in enumerate(config.data_source_configs):
            if ds_config.name == name:
                #Update Configuration
                #🔥 Modification: updated logic for processing API Key (consistent with large modeler management)
                _req = request.model_dump()

                #Process API Key
                if 'api_key' in _req:
                    api_key = _req.get('api_key')
                    logger.info(f"API Key:{repr(api_key)}(Types:{type(api_key).__name__}, length:{len(api_key) if api_key else 0})")

                    #If None or empty string, preserve original value (not updated)
                    if api_key is None or api_key == '':
                        logger.info(f"None or empty string, retain original value")
                        _req['api_key'] = ds_config.api_key or ""
                    #🔥 If it contains "..." (cut marks), verify whether it is the original unmodified value
                    elif api_key and "..." in api_key:
                        logger.info(f"🔍 [API Key Validation] detects cut marks and verifys whether they match the original database values")

                        #Same cut-off process for complete API Key in database
                        if ds_config.api_key:
                            truncated_db_key = _truncate_api_key(ds_config.api_key)
                            logger.info(f"[API Key Validation]{truncated_db_key}")
                            logger.info(f"[API Key Verifyes]{api_key}")

                            #Compare post-cut values
                            if api_key == truncated_db_key:
                                #Same, indicating that the user has not changed and maintains the full value in the database
                                logger.info(f"[API Key Validation]")
                                _req['api_key'] = ds_config.api_key
                            else:
                                #Different, indicating that the user has modified but incompletely
                                logger.error(f"The cut-off value does not match and the user may modify the incomplete key")
                                raise HTTPException(
                                    status_code=status.HTTP_400_BAD_REQUEST,
                                    detail=f"API Key 格式错误：检测到截断标记但与数据库中的值不匹配，请输入完整的 API Key"
                                )
                        else:
                            #There is no original value in the database, but it is unreasonable that the front end sends a cut-off value
                            logger.error(f"No original value in the database, but cut-off value received")
                            raise HTTPException(
                                status_code=status.HTTP_400_BAD_REQUEST,
                                detail=f"API Key 格式错误：请输入完整的 API Key"
                            )
                    #Do not update if placeholder (main value retained)
                    elif should_skip_api_key_update(api_key):
                        logger.info(f"Skip update (placeholder) and retain original value")
                        _req['api_key'] = ds_config.api_key or ""
                    #If a new key is entered, the validity must be verified
                    elif not is_valid_api_key(api_key):
                        logger.error(f"Validation failed: '{api_key}'(Longer:{len(api_key)})")
                        logger.error(f"- Length check:{len(api_key)} > 10? {len(api_key) > 10}")
                        logger.error(f"- Placeholder prefix check: Startswith ('your ')?{api_key.startswith('your_')}, startswith('your-')? {api_key.startswith('your-')}")
                        logger.error(f"- Placeholder suffix check: endswith ('here')?{api_key.endswith('_here')}, endswith('-here')? {api_key.endswith('-here')}")
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"API Key 无效：长度必须大于 10 个字符，且不能是占位符（当前长度: {len(api_key)}）"
                        )
                    else:
                        logger.info(f"The key will be updated (long:{len(api_key)})")
                    #valid full key, keep (means update)

                #Process API Secret
                if 'api_secret' in _req:
                    api_secret = _req.get('api_secret')
                    logger.info(f"API Secret:{repr(api_secret)}(Types:{type(api_secret).__name__}, length:{len(api_secret) if api_secret else 0})")

                    #If None or empty string, preserve original value (not updated)
                    if api_secret is None or api_secret == '':
                        logger.info(f"None or empty string, maintain original value")
                        _req['api_secret'] = ds_config.api_secret or ""
                    #🔥 If it contains "..." (cut marks), verify whether it is the original unmodified value
                    elif api_secret and "..." in api_secret:
                        logger.info(f"🔍 [API Secret Validation] Detects cut marks to verify whether they match the original database values")

                        #Same cut-off process for complete API Secret in the database
                        if ds_config.api_secret:
                            truncated_db_secret = _truncate_api_key(ds_config.api_secret)
                            logger.info(f"[API Secret Validation]{truncated_db_secret}")
                            logger.info(f"Value received:{api_secret}")

                            #Compare post-cut values
                            if api_secret == truncated_db_secret:
                                #Same, indicating that the user has not changed and maintains the full value in the database
                                logger.info(f"✅ [API Secret Validation] cut-off values match, maintain original database values")
                                _req['api_secret'] = ds_config.api_secret
                            else:
                                #Different, indicating that the user has modified but incompletely
                                logger.error(f"The cut-off value does not match and the user may have modified an incomplete key")
                                raise HTTPException(
                                    status_code=status.HTTP_400_BAD_REQUEST,
                                    detail=f"API Secret 格式错误：检测到截断标记但与数据库中的值不匹配，请输入完整的 API Secret"
                                )
                        else:
                            #There is no original value in the database, but it is unreasonable that the front end sends a cut-off value
                            logger.error(f"No original value in the database, but cut-off value received")
                            raise HTTPException(
                                status_code=status.HTTP_400_BAD_REQUEST,
                                detail=f"API Secret 格式错误：请输入完整的 API Secret"
                            )
                    #Do not update if placeholder (main value retained)
                    elif should_skip_api_key_update(api_secret):
                        logger.info(f"Skip update (placeholder) and retain original value")
                        _req['api_secret'] = ds_config.api_secret or ""
                    #If a new key is entered, the validity must be verified
                    elif not is_valid_api_key(api_secret):
                        logger.error(f"Verification failed: '{api_secret}'(Longer:{len(api_secret)})")
                        logger.error(f"- Length check:{len(api_secret)} > 10? {len(api_secret) > 10}")
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"API Secret 无效：长度必须大于 10 个字符，且不能是占位符（当前长度: {len(api_secret)}）"
                        )
                    else:
                        logger.info(f"Validation pass will update key (long:{len(api_secret)})")

                updated_config = DataSourceConfig(**_req)
                config.data_source_configs[i] = updated_config

                success = await CONFIG_SERVICE.save_system_config(config)
                if success:
                    #Synchronize market classification relationships
                    new_categories = set(_req.get('market_categories', []))

                    #Get the current group relationship
                    current_groupings = await CONFIG_SERVICE.get_datasource_groupings()
                    current_categories = set(
                        g.market_category_id
                        for g in current_groupings
                        if g.data_source_name == name
                    )

                    #Category to add
                    to_add = new_categories - current_categories
                    for category_id in to_add:
                        try:
                            grouping = DataSourceGrouping(
                                data_source_name=name,
                                market_category_id=category_id,
                                priority=updated_config.priority,
                                enabled=updated_config.enabled
                            )
                            await CONFIG_SERVICE.add_datasource_to_category(grouping)
                        except Exception as e:
                            logger.warning(f"Could not close temporary folder: %s{str(e)}")

                    #Category to delete
                    to_remove = current_categories - new_categories
                    for category_id in to_remove:
                        try:
                            await CONFIG_SERVICE.remove_datasource_from_category(name, category_id)
                        except Exception as e:
                            logger.warning(f"Could not close temporary folder: %s{str(e)}")

                    #Audit log (overlooking anomalies)
                    try:
                        await log_operation(
                            user_id=str(getattr(current_user, "id", "")),
                            username=getattr(current_user, "username", "unknown"),
                            action_type=ActionType.CONFIG_MANAGEMENT,
                            action="update_data_source_config",
                            details={"name": name, "market_categories": list(new_categories)},
                            success=True,
                        )
                    except Exception:
                        pass
                    return {"message": "数据源配置更新成功"}
                else:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="数据源配置更新失败"
                    )

        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="数据源配置不存在"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新数据源配置失败: {str(e)}"
        )


@router.delete("/datasource/{name}", response_model=dict)
async def delete_data_source_config(
    name: str,
    current_user: User = Depends(get_current_user)
):
    """Delete Data Source Configuration"""
    try:
        #Get Current Configuration
        config = await CONFIG_SERVICE.get_system_config_from_database()
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="系统配置不存在"
            )

        #Find and remove the data source configuration
        for i, ds_config in enumerate(config.data_source_configs):
            if ds_config.name == name:
                config.data_source_configs.pop(i)

                success = await CONFIG_SERVICE.save_system_config(config)
                if success:
                    #Audit log (overlooking anomalies)
                    try:
                        await log_operation(
                            user_id=str(getattr(current_user, "id", "")),
                            username=getattr(current_user, "username", "unknown"),
                            action_type=ActionType.CONFIG_MANAGEMENT,
                            action="delete_data_source_config",
                            details={"name": name},
                            success=True,
                        )
                    except Exception:
                        pass
                    return {"message": "数据源配置删除成功"}
                else:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="数据源配置删除失败"
                    )

        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="数据源配置不存在"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除数据源配置失败: {str(e)}"
        )


#== sync, corrected by elderman == @elder man

@router.get("/market-categories", response_model=List[MarketCategory])
async def get_market_categories(
    current_user: User = Depends(get_current_user)
):
    """Access to all market classifications"""
    try:
        categories = await CONFIG_SERVICE.get_market_categories()
        return categories
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取市场分类失败: {str(e)}"
        )


@router.post("/market-categories", response_model=dict)
async def add_market_category(
    request: MarketCategoryRequest,
    current_user: User = Depends(get_current_user)
):
    """Add Market Classification"""
    try:
        category = MarketCategory(**request.model_dump())
        success = await CONFIG_SERVICE.add_market_category(category)

        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="add_market_category",
                    details={"id": str(getattr(category, 'id', ''))},
                    success=True,
                )
            except Exception:
                pass
            return {"message": "市场分类添加成功", "id": category.id}
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="市场分类ID已存在"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"添加市场分类失败: {str(e)}"
        )


@router.put("/market-categories/{category_id}", response_model=dict)
async def update_market_category(
    category_id: str,
    request: Dict[str, Any],
    current_user: User = Depends(get_current_user)
):
    """Updating market classifications"""
    try:
        success = await CONFIG_SERVICE.update_market_category(category_id, request)

        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="update_market_category",
                    details={"category_id": category_id, "changed_keys": list(request.keys())},
                    success=True,
                )
            except Exception:
                pass
            return {"message": "市场分类更新成功"}
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="市场分类不存在"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新市场分类失败: {str(e)}"
        )


@router.delete("/market-categories/{category_id}", response_model=dict)
async def delete_market_category(
    category_id: str,
    current_user: User = Depends(get_current_user)
):
    """Remove Market Classification"""
    try:
        success = await CONFIG_SERVICE.delete_market_category(category_id)

        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="delete_market_category",
                    details={"category_id": category_id},
                    success=True,
                )
            except Exception:
                pass
            return {"message": "市场分类删除成功"}
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="无法删除分类，可能还有数据源使用此分类"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除市场分类失败: {str(e)}"
        )


#== sync, corrected by elderman == @elder man

@router.get("/datasource-groupings", response_model=List[DataSourceGrouping])
async def get_datasource_groupings(
    current_user: User = Depends(get_current_user)
):
    """Get All Data Source Group Relationships"""
    try:
        groupings = await CONFIG_SERVICE.get_datasource_groupings()
        return groupings
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取数据源分组关系失败: {str(e)}"
        )


@router.post("/datasource-groupings", response_model=dict)
async def add_datasource_to_category(
    request: DataSourceGroupingRequest,
    current_user: User = Depends(get_current_user)
):
    """Add data sources to classification"""
    try:
        grouping = DataSourceGrouping(**request.model_dump())
        success = await CONFIG_SERVICE.add_datasource_to_category(grouping)

        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="add_datasource_to_category",
                    details={"data_source_name": request.data_source_name, "category_id": request.category_id},
                    success=True,
                )
            except Exception:
                pass
            return {"message": "数据源添加到分类成功"}
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="数据源已在该分类中"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"添加数据源到分类失败: {str(e)}"
        )


@router.delete("/datasource-groupings/{data_source_name}/{category_id}", response_model=dict)
async def remove_datasource_from_category(
    data_source_name: str,
    category_id: str,
    current_user: User = Depends(get_current_user)
):
    """Remove data source from classification"""
    try:
        success = await CONFIG_SERVICE.remove_datasource_from_category(data_source_name, category_id)

        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="remove_datasource_from_category",
                    details={"data_source_name": data_source_name, "category_id": category_id},
                    success=True,
                )
            except Exception:
                pass
            return {"message": "数据源从分类中移除成功"}
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="数据源分组关系不存在"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"从分类中移除数据源失败: {str(e)}"
        )


@router.put("/datasource-groupings/{data_source_name}/{category_id}", response_model=dict)
async def update_datasource_grouping(
    data_source_name: str,
    category_id: str,
    request: Dict[str, Any],
    current_user: User = Depends(get_current_user)
):
    """Update data source group relationships"""
    try:
        success = await CONFIG_SERVICE.update_datasource_grouping(data_source_name, category_id, request)

        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="update_datasource_grouping",
                    details={"data_source_name": data_source_name, "category_id": category_id, "changed_keys": list(request.keys())},
                    success=True,
                )
            except Exception:
                pass
            return {"message": "数据源分组关系更新成功"}
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="数据源分组关系不存在"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新数据源分组关系失败: {str(e)}"
        )


@router.put("/market-categories/{category_id}/datasource-order", response_model=dict)
async def update_category_datasource_order(
    category_id: str,
    request: DataSourceOrderRequest,
    current_user: User = Depends(get_current_user)
):
    """Update the sorting of data sources in the classification"""
    try:
        success = await CONFIG_SERVICE.update_category_datasource_order(category_id, request.data_sources)

        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="update_category_datasource_order",
                    details={"category_id": category_id, "data_sources": request.data_sources},
                    success=True,
                )
            except Exception:
                pass
            return {"message": "数据源排序更新成功"}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="数据源排序更新失败"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新数据源排序失败: {str(e)}"
        )


@router.post("/datasource/set-default")
async def set_default_data_source(
    request: SetDefaultRequest,
    current_user: User = Depends(get_current_user)
):
    """Set Default Data Source"""
    try:
        success = await CONFIG_SERVICE.set_default_data_source(request.name)
        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="set_default_datasource",
                    details={"name": request.name},
                    success=True,
                )
            except Exception:
                pass
            return {"message": "默认数据源设置成功", "default_data_source": request.name}
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="指定的数据源不存在"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"设置默认数据源失败: {str(e)}"
        )


@router.get("/settings", response_model=Dict[str, Any])
async def get_system_settings(
    current_user: User = Depends(get_current_user)
):
    """Get System Settings"""
    try:
        effective = await config_provider.get_effective_system_settings()
        return _sanitize_kv(effective)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取系统设置失败: {str(e)}"
        )


@router.get("/settings/meta", response_model=dict)
async def get_system_settings_meta(
    current_user: User = Depends(get_current_user)
):
    """(c) Obtaining metadata (sensitivity, redactability, source, availability of values) from the system settings.
    Return structure:   FT 0 }, message}
    """
    try:
        meta_map = await config_provider.get_system_settings_meta()
        items = [
            {"key": k, **v} for k, v in meta_map.items()
        ]
        return {"success": True, "data": {"items": items}, "message": ""}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取系统设置元数据失败: {str(e)}"
        )


@router.put("/settings", response_model=dict)
async def update_system_settings(
    settings: Dict[str, Any],
    current_user: User = Depends(get_current_user)
):
    """Update System Settings"""
    try:
        #Print receiving settings (for debugging)
        logger.info(f"📝Receives system settings update requests, including{len(settings)}Item")
        if 'quick_analysis_model' in settings:
            logger.info(f"  ✓ quick_analysis_model: {settings['quick_analysis_model']}")
        else:
            logger.warning(f"Quick analysis model")
        if 'deep_analysis_model' in settings:
            logger.info(f"  ✓ deep_analysis_model: {settings['deep_analysis_model']}")
        else:
            logger.warning(f"Not containing")

        success = await CONFIG_SERVICE.update_system_settings(settings)
        if success:
            #Audit log (overlooking log anomalies without affecting the main process)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="update_system_settings",
                    details={"changed_keys": list(settings.keys())},
                    success=True,
                )
            except Exception:
                pass
            #Invalid Cache
            try:
                config_provider.invalidate()
            except Exception:
                pass
            return {"message": "系统设置更新成功"}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="系统设置更新失败"
            )
    except HTTPException:
        raise
    except Exception as e:
        #Audit failure record (overlooking log anomaly)
        try:
            await log_operation(
                user_id=str(getattr(current_user, "id", "")),
                username=getattr(current_user, "username", "unknown"),
                action_type=ActionType.CONFIG_MANAGEMENT,
                action="update_system_settings",
                details={"changed_keys": list(settings.keys())},
                success=False,
                error_message=str(e),
            )
        except Exception:
            pass
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新系统设置失败: {str(e)}"
        )


@router.post("/export", response_model=dict)
async def export_config(
    current_user: User = Depends(get_current_user)
):
    """Export Configuration"""
    try:
        config_data = await CONFIG_SERVICE.export_config()
        #Audit log (overlooking anomalies)
        try:
            await log_operation(
                user_id=str(getattr(current_user, "id", "")),
                username=getattr(current_user, "username", "unknown"),
                action_type=ActionType.DATA_EXPORT,
                action="export_config",
                details={"size": len(str(config_data))},
                success=True,
            )
        except Exception:
            pass
        return {
            "message": "配置导出成功",
            "data": config_data,
            "exported_at": now_tz().isoformat()
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"导出配置失败: {str(e)}"
        )


@router.post("/import", response_model=dict)
async def import_config(
    config_data: Dict[str, Any],
    current_user: User = Depends(get_current_user)
):
    """Import Configuration"""
    try:
        success = await CONFIG_SERVICE.import_config(config_data)
        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.DATA_IMPORT,
                    action="import_config",
                    details={"keys": list(config_data.keys())[:10]},
                    success=True,
                )
            except Exception:
                pass
            return {"message": "配置导入成功"}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="配置导入失败"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"导入配置失败: {str(e)}"
        )


@router.post("/migrate-legacy", response_model=dict)
async def migrate_legacy_config(
    current_user: User = Depends(get_current_user)
):
    """Move traditional configuration"""
    try:
        success = await CONFIG_SERVICE.migrate_legacy_config()
        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="migrate_legacy_config",
                    details={},
                    success=True,
                )
            except Exception:
                pass
            return {"message": "传统配置迁移成功"}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="传统配置迁移失败"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"迁移传统配置失败: {str(e)}"
        )


@router.post("/default/llm", response_model=dict)
async def set_default_llm(
    request: SetDefaultRequest,
    current_user: User = Depends(get_current_user)
):
    """Set Default Large Model"""
    try:
        #Open source version: All users can modify configuration

        success = await CONFIG_SERVICE.set_default_llm(request.name)
        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="set_default_llm",
                    details={"name": request.name},
                    success=True,
                )
            except Exception:
                pass
            return {"message": f"默认大模型已设置为: {request.name}"}
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="设置默认大模型失败，请检查模型名称是否正确"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"设置默认大模型失败: {str(e)}"
        )


@router.post("/default/datasource", response_model=dict)
async def set_default_data_source(
    request: SetDefaultRequest,
    current_user: User = Depends(get_current_user)
):
    """Set Default Data Source"""
    try:
        #Open source version: All users can modify configuration

        success = await CONFIG_SERVICE.set_default_data_source(request.name)
        if success:
            #Audit log (overlooking anomalies)
            try:
                await log_operation(
                    user_id=str(getattr(current_user, "id", "")),
                    username=getattr(current_user, "username", "unknown"),
                    action_type=ActionType.CONFIG_MANAGEMENT,
                    action="set_default_datasource",
                    details={"name": request.name},
                    success=True,
                )
            except Exception:
                pass
            return {"message": f"默认数据源已设置为: {request.name}"}
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="设置默认数据源失败，请检查数据源名称是否正确"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"设置默认数据源失败: {str(e)}"
        )


@router.get("/models", response_model=List[Dict[str, Any]])
async def get_available_models(
    current_user: User = Depends(get_current_user)
):
    """Get a list of available models"""
    try:
        models = await CONFIG_SERVICE.get_available_models()
        return models
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取模型列表失败: {str(e)}"
        )


#== sync, corrected by elderman == @elder man

@router.get("/model-catalog", response_model=List[Dict[str, Any]])
async def get_model_catalog(
    current_user: User = Depends(get_current_user)
):
    """Fetch all model directories"""
    try:
        catalogs = await CONFIG_SERVICE.get_model_catalog()
        return [catalog.model_dump(by_alias=False) for catalog in catalogs]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取模型目录失败: {str(e)}"
        )


@router.get("/model-catalog/{provider}", response_model=Dict[str, Any])
async def get_provider_model_catalog(
    provider: str,
    current_user: User = Depends(get_current_user)
):
    """Retrieving a model directory of specified manufacturers"""
    try:
        catalog = await CONFIG_SERVICE.get_provider_models(provider)
        if not catalog:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到厂家 {provider} 的模型目录"
            )
        return catalog.model_dump(by_alias=False)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取模型目录失败: {str(e)}"
        )


class ModelCatalogRequest(BaseModel):
    """Model Directory Request"""
    provider: str
    provider_name: str
    models: List[Dict[str, Any]]


@router.post("/model-catalog", response_model=dict)
async def save_model_catalog(
    request: ModelCatalogRequest,
    current_user: User = Depends(get_current_user)
):
    """Save or update the model directory"""
    try:
        logger.info(f"📝Received a request to save the model directory: protocol={request.provider}, models ={len(request.models)}")
        logger.info(f"Data requested:{request.model_dump()}")

        #Convert to ModelInfo List
        models = [ModelInfo(**m) for m in request.models]
        logger.info(f"Successful conversion{len(models)}Model")

        catalog = ModelCatalog(
            provider=request.provider,
            provider_name=request.provider_name,
            models=models
        )
        logger.info(f"Could not close temporary folder: %s")

        success = await CONFIG_SERVICE.save_model_catalog(catalog)
        logger.info(f"Save results:{success}")

        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="保存模型目录失败"
            )

        #Log Operations Log
        await log_operation(
            user_id=str(current_user["id"]),
            username=current_user.get("username", "unknown"),
            action_type=ActionType.CONFIG_MANAGEMENT,
            action="update_model_catalog",
            details={"provider": request.provider, "provider_name": request.provider_name, "models_count": len(request.models)}
        )

        return {"success": True, "message": "模型目录保存成功"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"保存模型目录失败: {str(e)}"
        )


@router.delete("/model-catalog/{provider}", response_model=dict)
async def delete_model_catalog(
    provider: str,
    current_user: User = Depends(get_current_user)
):
    """Remove model directory"""
    try:
        success = await CONFIG_SERVICE.delete_model_catalog(provider)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到厂家 {provider} 的模型目录"
            )

        #Log Operations Log
        await log_operation(
            user_id=str(current_user["id"]),
            username=current_user.get("username", "unknown"),
            action_type=ActionType.CONFIG_MANAGEMENT,
            action="delete_model_catalog",
            details={"provider": provider}
        )

        return {"success": True, "message": "模型目录删除成功"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除模型目录失败: {str(e)}"
        )


@router.post("/model-catalog/init", response_model=dict)
async def init_model_catalog(
    current_user: User = Depends(get_current_user)
):
    """Initialize the default model directory"""
    try:
        success = await CONFIG_SERVICE.init_default_model_catalog()
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="初始化模型目录失败"
            )

        return {"success": True, "message": "模型目录初始化成功"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"初始化模型目录失败: {str(e)}"
        )


#== sync, corrected by elderman == @elder man

@router.get("/database", response_model=List[DatabaseConfig])
async def get_database_configs(
    current_user: dict = Depends(get_current_user)
):
    """Get All Database Configurations"""
    try:
        logger.info("Can not open message")
        configs = await CONFIG_SERVICE.get_database_configs()
        logger.info(f"Other Organiser{len(configs)}Database Configuration")
        return configs
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取数据库配置失败: {str(e)}"
        )


@router.get("/database/{db_name}", response_model=DatabaseConfig)
async def get_database_config(
    db_name: str,
    current_user: dict = Depends(get_current_user)
):
    """Get specified database configuration"""
    try:
        logger.info(f"Access to database configurations:{db_name}")
        config = await CONFIG_SERVICE.get_database_config(db_name)

        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"数据库配置 '{db_name}' 不存在"
            )

        return config
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取数据库配置失败: {str(e)}"
        )


@router.post("/database", response_model=dict)
async def add_database_config(
    request: DatabaseConfigRequest,
    current_user: dict = Depends(get_current_user)
):
    """Add Database Configuration"""
    try:
        logger.info(f"Add database configuration:{request.name}")

        #Convert to DataConfig Object
        db_config = DatabaseConfig(**request.model_dump())

        #Add Profile
        success = await CONFIG_SERVICE.add_database_config(db_config)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="添加数据库配置失败，可能已存在同名配置"
            )

        #Log Operations Log
        await log_operation(
            user_id=current_user["id"],
            username=current_user.get("username", "unknown"),
            action_type=ActionType.CONFIG_MANAGEMENT,
            action=f"添加数据库配置: {request.name}",
            details={"name": request.name, "type": request.type, "host": request.host, "port": request.port}
        )

        return {"success": True, "message": "数据库配置添加成功"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"添加数据库配置失败: {str(e)}"
        )


@router.put("/database/{db_name}", response_model=dict)
async def update_database_config(
    db_name: str,
    request: DatabaseConfigRequest,
    current_user: dict = Depends(get_current_user)
):
    """Update Database Configuration"""
    try:
        logger.info(f"Update the database configuration:{db_name}")

        #Check if the name matches
        if db_name != request.name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="URL中的名称与请求体中的名称不匹配"
            )

        #Convert to DataConfig Object
        db_config = DatabaseConfig(**request.model_dump())

        #Update Configuration
        success = await CONFIG_SERVICE.update_database_config(db_config)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"数据库配置 '{db_name}' 不存在"
            )

        #Log Operations Log
        await log_operation(
            user_id=current_user["id"],
            username=current_user.get("username", "unknown"),
            action_type=ActionType.CONFIG_MANAGEMENT,
            action=f"更新数据库配置: {db_name}",
            details={"name": request.name, "type": request.type, "host": request.host, "port": request.port}
        )

        return {"success": True, "message": "数据库配置更新成功"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update of database configuration failed:{e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新数据库配置失败: {str(e)}"
        )


@router.delete("/database/{db_name}", response_model=dict)
async def delete_database_config(
    db_name: str,
    current_user: dict = Depends(get_current_user)
):
    """Delete Database Configuration"""
    try:
        logger.info(f"Delete database configuration:{db_name}")

        #Remove Configuration
        success = await CONFIG_SERVICE.delete_database_config(db_name)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"数据库配置 '{db_name}' 不存在"
            )

        #Log Operations Log
        await log_operation(
            user_id=current_user["id"],
            username=current_user.get("username", "unknown"),
            action_type=ActionType.CONFIG_MANAGEMENT,
            action=f"删除数据库配置: {db_name}",
            details={"name": db_name}
        )

        return {"success": True, "message": "数据库配置删除成功"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete database configuration failed:{e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除数据库配置失败: {str(e)}"
        )
