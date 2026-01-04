"""Modelling capacity management services

Provide model capacity assessment, validation and referral functions.
"""

from typing import Tuple, Dict, Optional, List, Any
from app.constants.model_capabilities import (
    ANALYSIS_DEPTH_REQUIREMENTS,
    DEFAULT_MODEL_CAPABILITIES,
    CAPABILITY_DESCRIPTIONS,
    ModelRole,
    ModelFeature
)
from app.core.unified_config import unified_config
import logging
import re

logger = logging.getLogger(__name__)


class ModelCapabilityService:
    """Modelling capacity management services"""

    def _parse_aggregator_model_name(self, model_name: str) -> Tuple[Optional[str], str]:
        """The model name for the solver channel

        Args:
            Model name: Model name, which may contain prefixes (e. g. openai/gpt-4, anthropic/claude-3-sonnet)

        Returns:
            (original manufacturer, original model name)
        """
        #Common polymer channel model name format:
        # - openai/gpt-4
        # - anthropic/claude-3-sonnet
        # - google/gemini-pro

        if "/" in model_name:
            parts = model_name.split("/", 1)
            if len(parts) == 2:
                provider_hint = parts[0].lower()
                original_model = parts[1]

                #Map provider hints to standard name
                provider_map = {
                    "openai": "openai",
                    "anthropic": "anthropic",
                    "google": "google",
                    "deepseek": "deepseek",
                    "alibaba": "qwen",
                    "qwen": "qwen",
                    "zhipu": "zhipu",
                    "baidu": "baidu",
                    "moonshot": "moonshot"
                }

                provider = provider_map.get(provider_hint)
                return provider, original_model

        return None, model_name

    def _get_model_capability_with_mapping(self, model_name: str) -> Tuple[int, Optional[str]]:
        """Acquisition of model capability level (support for aggregate channel mapping)

        Returns:
            (Capacity level, map original model name)
        """
        #1. First attempt direct matching
        if model_name in DEFAULT_MODEL_CAPABILITIES:
            return DEFAULT_MODEL_CAPABILITIES[model_name]["capability_level"], None

        #2. Attempt to interpret the polymer channel model First Name
        provider, original_model = self._parse_aggregator_model_name(model_name)

        if original_model and original_model != model_name:
            #Try using the original model name
            if original_model in DEFAULT_MODEL_CAPABILITIES:
                logger.info(f"Syndication channel model map:{model_name} -> {original_model}")
                return DEFAULT_MODEL_CAPABILITIES[original_model]["capability_level"], original_model

        #Return Default
        return 2, None

    def get_model_capability(self, model_name: str) -> int:
        """Level of capability to acquire the model (support the polymer channel model mapping)

        Args:
            model name: Model name (possibly containing prefixes to aggregate channels, e.g. openai/gpt-4)

        Returns:
            Capacity level (1-5)
        """
        #1. Prioritize reading from database configuration
        try:
            llm_configs = unified_config.get_llm_configs()
            for config in llm_configs:
                if config.model_name == model_name:
                    return getattr(config, 'capability_level', 2)
        except Exception as e:
            logger.warning(f"Failed to read model capability from configuration:{e}")

        #2. Read from the default map (support of polymer channel mapping)
        capability, mapped_model = self._get_model_capability_with_mapping(model_name)
        if mapped_model:
            logger.info(f"Use map model{mapped_model}Capacity level:{capability}")

        return capability
    
    def get_model_config(self, model_name: str) -> Dict[str, Any]:
        """Get complete configuration information for the model (support for polymer channel model mapping)

        Args:
            Model name: Model name (possibly containing prefixes to aggregate channels)

        Returns:
            Model Configuration Dictionary
        """
        #1. Prioritize reading from MongoDB database configuration (using sync client)
        try:
            from pymongo import MongoClient
            from app.core.config import SETTINGS
            from app.models.config import SystemConfig

            #Use sync MongoDB client
            client = MongoClient(SETTINGS.MONGO_URI)
            db = client[SETTINGS.MONGO_DB_NAME]
            collection = db.system_configs  #NOTE: The collective name is a complex number

            #Query system configuration (consistent with config service)
            doc = collection.find_one({"is_active": True}, sort=[("version", -1)])

            logger.info(f"[MongoDB] Queries: doc={'Existence' if doc else 'does not exist'}")
            if doc:
                logger.info(f"[MongoDB] Document version:{doc.get('version')}, is_active: {doc.get('is_active')}")

            if doc and "llm_configs" in doc:
                llm_configs = doc["llm_configs"]
                logger.info(f"[MongoDB] lm configs:{len(llm_configs)}")

                for config_dict in llm_configs:
                    if config_dict.get("model_name") == model_name:
                        logger.info(f"[MongoDB]{model_name}")
                        #🔧 Converts the string list to the list of names
                        features_str = config_dict.get('features', [])
                        features_enum = []
                        for feature_str in features_str:
                            try:
                                #Convert string to ModelFeature
                                features_enum.append(ModelFeature(feature_str))
                            except ValueError:
                                logger.warning(f"Unknown characteristic value:{feature_str}")

                        #🔧 Converts the string list to the list of names
                        roles_str = config_dict.get('suitable_roles', ["both"])
                        roles_enum = []
                        for role_str in roles_str:
                            try:
                                #Convert string to ModelRole count
                                roles_enum.append(ModelRole(role_str))
                            except ValueError:
                                logger.warning(f"Unknown character value:{role_str}")

                        #If no character, default to both
                        if not roles_enum:
                            roles_enum = [ModelRole.BOTH]

                        logger.info(f"[MongoDB Configuration]{model_name}: features={features_enum}, roles={roles_enum}")

                        #Close Connection
                        client.close()

                        return {
                            "model_name": config_dict.get("model_name"),
                            "capability_level": config_dict.get('capability_level', 2),
                            "suitable_roles": roles_enum,
                            "features": features_enum,
                            "recommended_depths": config_dict.get('recommended_depths', ["快速", "基础", "标准"]),
                            "performance_metrics": config_dict.get('performance_metrics', None)
                        }

            #Close Connection
            client.close()

        except Exception as e:
            logger.warning(f"Failed to read model information from MongoDB:{e}", exc_info=True)

        #Read from default map (direct matching)
        if model_name in DEFAULT_MODEL_CAPABILITIES:
            return DEFAULT_MODEL_CAPABILITIES[model_name]

        #3. Experimental assembly channel model mapping
        provider, original_model = self._parse_aggregator_model_name(model_name)
        if original_model and original_model != model_name:
            if original_model in DEFAULT_MODEL_CAPABILITIES:
                logger.info(f"Syndication channel model map:{model_name} -> {original_model}")
                config = DEFAULT_MODEL_CAPABILITIES[original_model].copy()
                config["model_name"] = model_name  #Keep original model name
                config["_mapped_from"] = original_model  #Record map source
                return config

        #4. Return Default Configuration
        logger.warning(f"No model found{model_name}, using the default configuration")
        return {
            "model_name": model_name,
            "capability_level": 2,
            "suitable_roles": [ModelRole.BOTH],
            "features": [ModelFeature.TOOL_CALLING],
            "recommended_depths": ["快速", "基础", "标准"],
            "performance_metrics": {"speed": 3, "cost": 3, "quality": 3}
        }
    
    def validate_model_pair(
        self,
        quick_model: str,
        deep_model: str,
        research_depth: str
    ) -> Dict[str, Any]:
        """Verify whether the model is suitable for the current analysis depth

        Args:
            Quick model: Quick Analysis Model Name
            Deep model: Depth Analysis Model Name
            Research depth: Research depth (quick/basis/standard/deep/full)

        Returns:
            Valid, warnings, verifications
        """
        logger.info(f"Starting to verify model pairs: Quick={quick_model}, deep={deep_model}, depth={research_depth}")

        requirements = ANALYSIS_DEPTH_REQUIREMENTS.get(research_depth, ANALYSIS_DEPTH_REQUIREMENTS["标准"])
        logger.info(f"Analysis depth requirements:{requirements}")

        quick_config = self.get_model_config(quick_model)
        deep_config = self.get_model_config(deep_model)

        logger.info(f"Fast model configuration:{quick_config}")
        logger.info(f"Depth model configuration:{deep_config}")

        result = {
            "valid": True,
            "warnings": [],
            "recommendations": []
        }
        
        #Check fast models
        quick_level = quick_config["capability_level"]
        logger.info(f"Checking fast model capability levels:{quick_level} >= {requirements['quick_model_min']}?")
        if quick_level < requirements["quick_model_min"]:
            warning = f"⚠️ 快速模型 {quick_model} (能力等级{quick_level}) 低于 {research_depth} 分析的建议等级({requirements['quick_model_min']})"
            result["warnings"].append(warning)
            logger.warning(warning)

        #Check for rapid model role adaptation
        quick_roles = quick_config.get("suitable_roles", [])
        logger.info(f"Check for fast model role:{quick_roles}")
        if ModelRole.QUICK_ANALYSIS not in quick_roles and ModelRole.BOTH not in quick_roles:
            warning = f"💡 模型 {quick_model} 不是为快速分析优化的，可能影响数据收集效率"
            result["warnings"].append(warning)
            logger.warning(warning)

        #Checks if the fast model supports a tool call
        quick_features = quick_config.get("features", [])
        logger.info(f"Check for fast model characteristics:{quick_features}")
        if ModelFeature.TOOL_CALLING not in quick_features:
            result["valid"] = False
            warning = f"❌ 快速模型 {quick_model} 不支持工具调用，无法完成数据收集任务"
            result["warnings"].append(warning)
            logger.error(warning)

        #Check depth model
        deep_level = deep_config["capability_level"]
        logger.info(f"Check depth model capability levels:{deep_level} >= {requirements['deep_model_min']}?")
        if deep_level < requirements["deep_model_min"]:
            result["valid"] = False
            warning = f"❌ 深度模型 {deep_model} (能力等级{deep_level}) 不满足 {research_depth} 分析的最低要求(等级{requirements['deep_model_min']})"
            result["warnings"].append(warning)
            logger.error(warning)
            result["recommendations"].append(
                self._recommend_model("deep", requirements["deep_model_min"])
            )

        #Check depth model role fit
        deep_roles = deep_config.get("suitable_roles", [])
        logger.info(f"Check depth model role:{deep_roles}")
        if ModelRole.DEEP_ANALYSIS not in deep_roles and ModelRole.BOTH not in deep_roles:
            warning = f"💡 模型 {deep_model} 不是为深度推理优化的，可能影响分析质量"
            result["warnings"].append(warning)
            logger.warning(warning)

        #Check essential features
        logger.info(f"Check required characteristics:{requirements['required_features']}")
        for feature in requirements["required_features"]:
            if feature == ModelFeature.REASONING:
                deep_features = deep_config.get("features", [])
                logger.info(f"Checking depth model reasoning:{deep_features}")
                if feature not in deep_features:
                    warning = f"💡 {research_depth} 分析建议使用具有强推理能力的深度模型"
                    result["warnings"].append(warning)
                    logger.warning(warning)

        logger.info(f"Validation results:{result['valid']}, warnings={len(result['warnings'])}Article")
        logger.info(f"Warning details:{result['warnings']}")

        return result
    
    def recommend_models_for_depth(
        self,
        research_depth: str
    ) -> Tuple[str, str]:
        """Based on the depth of analysis, recommend a suitable model.

        Args:
            Research depth: Research depth (quick/basis/standard/deep/full)

        Returns:
            (quick model, Deep model)
        """
        requirements = ANALYSIS_DEPTH_REQUIREMENTS.get(research_depth, ANALYSIS_DEPTH_REQUIREMENTS["标准"])
        
        #Fetch all enabled models
        try:
            llm_configs = unified_config.get_llm_configs()
            enabled_models = [c for c in llm_configs if c.enabled]
        except Exception as e:
            logger.error(f"Fetching model configuration failed:{e}")
            #Use default model
            return self._get_default_models()
        
        if not enabled_models:
            logger.warning("No enabled model, use default configuration")
            return self._get_default_models()
        
        #Filter models suitable for rapid analysis
        quick_candidates = []
        for m in enabled_models:
            roles = getattr(m, 'suitable_roles', [ModelRole.BOTH])
            level = getattr(m, 'capability_level', 2)
            features = getattr(m, 'features', [])
            
            if (ModelRole.QUICK_ANALYSIS in roles or ModelRole.BOTH in roles) and \
               level >= requirements["quick_model_min"] and \
               ModelFeature.TOOL_CALLING in features:
                quick_candidates.append(m)
        
        #Filter models for depth analysis
        deep_candidates = []
        for m in enabled_models:
            roles = getattr(m, 'suitable_roles', [ModelRole.BOTH])
            level = getattr(m, 'capability_level', 2)
            
            if (ModelRole.DEEP_ANALYSIS in roles or ModelRole.BOTH in roles) and \
               level >= requirements["deep_model_min"]:
                deep_candidates.append(m)
        
        #Sort by value for money (capacity level vs cost)
        quick_candidates.sort(
            key=lambda x: (
                getattr(x, 'capability_level', 2),
                -getattr(x, 'performance_metrics', {}).get("cost", 3) if getattr(x, 'performance_metrics', None) else 0
            ),
            reverse=True
        )
        
        deep_candidates.sort(
            key=lambda x: (
                getattr(x, 'capability_level', 2),
                getattr(x, 'performance_metrics', {}).get("quality", 3) if getattr(x, 'performance_metrics', None) else 0
            ),
            reverse=True
        )
        
        #Select Best Model
        quick_model = quick_candidates[0].model_name if quick_candidates else None
        deep_model = deep_candidates[0].model_name if deep_candidates else None
        
        #Use system default if no fit is found
        if not quick_model or not deep_model:
            return self._get_default_models()
        
        logger.info(
            f"Yes.{research_depth}Analysis of recommended models:"
            f"quick={quick_model}(Role: rapid analysis),"
            f"deep={deep_model}(Role: deep reasoning)"
        )
        
        return quick_model, deep_model
    
    def _get_default_models(self) -> Tuple[str, str]:
        """Fetch default model pairs"""
        try:
            quick_model = unified_config.get_quick_analysis_model()
            deep_model = unified_config.get_deep_analysis_model()
            logger.info(f"Use system default model: Quick={quick_model}, deep={deep_model}")
            return quick_model, deep_model
        except Exception as e:
            logger.error(f"Failed to get default model:{e}")
            return "qwen-turbo", "qwen-plus"
    
    def _recommend_model(self, model_type: str, min_level: int) -> str:
        """Recommended models for meeting requirements"""
        try:
            llm_configs = unified_config.get_llm_configs()
            for config in llm_configs:
                if config.enabled and getattr(config, 'capability_level', 2) >= min_level:
                    display_name = config.model_display_name or config.model_name
                    return f"建议使用: {display_name}"
        except Exception as e:
            logger.warning(f"Failed to recommend model:{e}")
        
        return "建议升级模型配置"


#Single cases
_model_capability_service = None


def get_model_capability_service() -> ModelCapabilityService:
    """Individual cases of accessing model capacity services"""
    global _model_capability_service
    if _model_capability_service is None:
        _model_capability_service = ModelCapabilityService()
    return _model_capability_service

