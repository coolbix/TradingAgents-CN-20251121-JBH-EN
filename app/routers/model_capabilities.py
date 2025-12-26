"""Modelling capacity to manage API routers
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

from app.services.model_capability_service import get_model_capability_service
from app.constants.model_capabilities import (
    DEFAULT_MODEL_CAPABILITIES,
    ANALYSIS_DEPTH_REQUIREMENTS,
    CAPABILITY_DESCRIPTIONS,
    ModelRole,
    ModelFeature,
    get_model_capability_badge,
    get_role_badge,
    get_feature_badge
)
from app.core.unified_config import unified_config
from app.core.response import ok, fail
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/model-capabilities", tags=["模型能力管理"])


#== sync, corrected by elderman == @elder man

class ModelCapabilityInfo(BaseModel):
    """Model capabilities information"""
    model_name: str
    capability_level: int
    suitable_roles: List[str]
    features: List[str]
    recommended_depths: List[str]
    performance_metrics: Optional[Dict[str, Any]] = None
    description: Optional[str] = None


class ModelRecommendationRequest(BaseModel):
    """Model recommendation request"""
    research_depth: str = Field(..., description="研究深度：快速/基础/标准/深度/全面")


class ModelRecommendationResponse(BaseModel):
    """Model Recommended Response"""
    quick_model: str
    deep_model: str
    quick_model_info: ModelCapabilityInfo
    deep_model_info: ModelCapabilityInfo
    reason: str


class ModelValidationRequest(BaseModel):
    """Model validation request"""
    quick_model: str
    deep_model: str
    research_depth: str


class ModelValidationResponse(BaseModel):
    """Model validation response"""
    valid: bool
    warnings: List[str]
    recommendations: List[str]


class BatchInitRequest(BaseModel):
    """Batch request for initialization"""
    overwrite: bool = Field(default=False, description="是否覆盖已有配置")


#== sync, corrected by elderman == @elder man

@router.get("/default-configs")
async def get_default_model_configs():
    """Get all default model capability configurations

Returns the predefined common model capability configuration for reference and initialization.
"""
    try:
        #Convert to Sequencable Format
        configs = {}
        for model_name, config in DEFAULT_MODEL_CAPABILITIES.items():
            configs[model_name] = {
                "model_name": model_name,
                "capability_level": config["capability_level"],
                "suitable_roles": [str(role) for role in config["suitable_roles"]],
                "features": [str(feature) for feature in config["features"]],
                "recommended_depths": config["recommended_depths"],
                "performance_metrics": config.get("performance_metrics"),
                "description": config.get("description")
            }

        return {
            "success": True,
            "data": configs,
            "message": "获取默认模型配置成功"
        }
    except Exception as e:
        logger.error(f"Fetching default model configuration failed:{e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/depth-requirements", response_model=dict)
async def get_depth_requirements():
    """Obtain analysis depth requirements

Returns the minimum requirements for the model for each analytical depth.
"""
    try:
        #Convert to Sequencable Format
        requirements = {}
        for depth, req in ANALYSIS_DEPTH_REQUIREMENTS.items():
            requirements[depth] = {
                "min_capability": req["min_capability"],
                "quick_model_min": req["quick_model_min"],
                "deep_model_min": req["deep_model_min"],
                "required_features": [str(f) for f in req["required_features"]],
                "description": req["description"]
            }

        return ok(requirements, "获取分析深度要求成功")
    except Exception as e:
        logger.error(f"Failed to get analysis depth:{e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/capability-descriptions", response_model=dict)
async def get_capability_descriptions():
    """Capability Level Description"""
    try:
        return ok(CAPABILITY_DESCRIPTIONS, "获取能力等级描述成功")
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/badges", response_model=dict)
async def get_all_badges():
    """Get all badge styles

Returns the insignia style configuration of the level of ability, role, character.
"""
    try:
        badges = {
            "capability_levels": {
                str(level): get_model_capability_badge(level)
                for level in range(1, 6)
            },
            "roles": {
                str(role): get_role_badge(role)
                for role in ModelRole
            },
            "features": {
                str(feature): get_feature_badge(feature)
                for feature in ModelFeature
            }
        }

        return ok(badges, "获取徽章样式成功")
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/recommend", response_model=dict)
async def recommend_models(request: ModelRecommendationRequest):
    """Recommended Model

The most appropriate model pairs are recommended based on the depth of the analysis.
"""
    try:
        capability_service = get_model_capability_service()

        #Get recommended models
        quick_model, deep_model = capability_service.recommend_models_for_depth(
            request.research_depth
        )

        logger.info(f"RECOMMENDED MODEL: Quick={quick_model}, deep={deep_model}")

        #Get Model Details
        quick_info = capability_service.get_model_config(quick_model)
        deep_info = capability_service.get_model_config(deep_model)

        logger.info(f"Model details: Quick info={quick_info}, deep_info={deep_info}")

        #Generate a reason for recommendation
        depth_req = ANALYSIS_DEPTH_REQUIREMENTS.get(
            request.research_depth,
            ANALYSIS_DEPTH_REQUIREMENTS["标准"]
        )

        #Capability Level Description
        capability_desc = {
            1: "基础级",
            2: "标准级",
            3: "高级",
            4: "专业级",
            5: "旗舰级"
        }

        quick_level_desc = capability_desc.get(quick_info['capability_level'], "标准级")
        deep_level_desc = capability_desc.get(deep_info['capability_level'], "标准级")

        reason = (
            f"• 快速模型：{quick_level_desc}，注重速度和成本，适合数据收集\n"
            f"• 深度模型：{deep_level_desc}，注重质量和推理，适合分析决策"
        )

        response_data = {
            "quick_model": quick_model,
            "deep_model": deep_model,
            "quick_model_info": quick_info,
            "deep_model_info": deep_info,
            "reason": reason
        }

        logger.info(f"Response data returned:{response_data}")

        return ok(response_data, "模型推荐成功")
    except Exception as e:
        logger.error(f"Model recommendation failed:{e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/validate", response_model=dict)
async def validate_models(request: ModelValidationRequest):
    """Validate model pairs

Verifys whether the selected model is suitable for the specified depth of analysis.
"""
    try:
        capability_service = get_model_capability_service()

        #Validate model pairs
        validation = capability_service.validate_model_pair(
            request.quick_model,
            request.deep_model,
            request.research_depth
        )

        return ok(validation, "模型验证完成")
    except Exception as e:
        logger.error(f"Model validation failed:{e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch-init", response_model=dict)
async def batch_init_capabilities(request: BatchInitRequest):
    """Batch initialization model capability

Configures automatic filling capacity parameters for models in databases.
"""
    try:
        #Get All LLM Configurations
        llm_configs = unified_config.get_llm_configs()

        updated_count = 0
        skipped_count = 0

        for config in llm_configs:
            model_name = config.model_name

            #Check if you can configure
            has_capability = hasattr(config, 'capability_level') and config.capability_level is not None

            if has_capability and not request.overwrite:
                skipped_count += 1
                continue

            #Capability parameters from default configuration
            if model_name in DEFAULT_MODEL_CAPABILITIES:
                default_config = DEFAULT_MODEL_CAPABILITIES[model_name]

                #Update Configuration
                config.capability_level = default_config["capability_level"]
                config.suitable_roles = [str(role) for role in default_config["suitable_roles"]]
                config.features = [str(feature) for feature in default_config["features"]]
                config.recommended_depths = default_config["recommended_depths"]
                config.performance_metrics = default_config.get("performance_metrics")

                #Save to Database
                #TODO: Achieve saving logic
                updated_count += 1
                logger.info(f"Initialized Model{model_name}Capability parameters")
            else:
                logger.warning(f"Model{model_name}No default configuration, Skip")
                skipped_count += 1

        return ok(
            {
                "updated_count": updated_count,
                "skipped_count": skipped_count,
                "total_count": len(llm_configs)
            },
            f"批量初始化完成：更新{updated_count}个，跳过{skipped_count}个"
        )
    except Exception as e:
        logger.error(f"Batch initialization failed:{e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/model/{model_name}", response_model=dict)
async def get_model_capability(model_name: str):
    """Capability information for acquiring specified models

Args:
Model name: Model name
"""
    try:
        capability_service = get_model_capability_service()
        config = capability_service.get_model_config(model_name)

        return ok(config, f"获取模型 {model_name} 能力信息成功")
    except Exception as e:
        logger.error(f"Failed to access model capability information:{e}")
        raise HTTPException(status_code=500, detail=str(e))

