"""Model capability hierarchy

The definition of the capabilities level of the model, the applicable role, the feature label, etc.
For intelligent matching depth analysis and model selection.

Convergence channels support:
- Support for the 302.AI, OpenRouter, One API, etc.
- Model name format for polymer channels:   FT 0 / FT 1  (e.g. openai/gpt-4)
- The system will automatically map the capability configuration of the original plant model.
"""

from enum import IntEnum, Enum
from typing import Dict, List, Any, Tuple


class ModelCapabilityLevel(IntEnum):
    """Model capabilities level (1-5)"""
    BASIC = 1          #Base: appropriate for level 1-2 analysis, light speed
    STANDARD = 2       #Standard: appropriate for level 1-3 analysis, daily use
    ADVANCED = 3       #Advanced: suitable for level 1-4 analysis, complex reasoning
    PROFESSIONAL = 4   #Professional: appropriate for Level 1-5 analysis, Professional level analysis
    FLAGSHIP = 5       #Flagships: fit for all levels, most powerful


class ModelRole(str, Enum):
    """Model Role Type"""
    QUICK_ANALYSIS = "quick_analysis"  #Rapid analysis (data collection, tool call)
    DEEP_ANALYSIS = "deep_analysis"    #In-depth analysis (adjection, decision-making)
    BOTH = "both"                      #Both fit.


class ModelFeature(str, Enum):
    """Model characterization label"""
    TOOL_CALLING = "tool_calling"      #Support tool call (required)
    LONG_CONTEXT = "long_context"      #Support context
    REASONING = "reasoning"            #Strong reasoning.
    VISION = "vision"                  #Support visual input
    FAST_RESPONSE = "fast_response"    #Quick Response
    COST_EFFECTIVE = "cost_effective"  #Cost-effective


#Capability Level Description
CAPABILITY_DESCRIPTIONS = {
    1: "基础模型 - 适合快速分析和简单任务，响应快速，成本低",
    2: "标准模型 - 适合日常分析和常规任务，平衡性能和成本",
    3: "高级模型 - 适合深度分析和复杂推理，质量较高",
    4: "专业模型 - 适合专业级分析和多轮辩论，高质量输出",
    5: "旗舰模型 - 最强能力，适合全面分析和关键决策"
}


#Minimum level of capability required to analyse depth
ANALYSIS_DEPTH_REQUIREMENTS = {
    "快速": {
        "min_capability": 1,
        "quick_model_min": 1,
        "deep_model_min": 1,
        "required_features": [ModelFeature.TOOL_CALLING],
        "description": "1级快速分析：任何模型都可以，优先选择快速响应的模型"
    },
    "基础": {
        "min_capability": 1,
        "quick_model_min": 1,
        "deep_model_min": 2,
        "required_features": [ModelFeature.TOOL_CALLING],
        "description": "2级基础分析：快速模型可用基础级，深度模型建议标准级以上"
    },
    "标准": {
        "min_capability": 2,
        "quick_model_min": 1,
        "deep_model_min": 2,
        "required_features": [ModelFeature.TOOL_CALLING],
        "description": "3级标准分析：快速模型可用基础级，深度模型需要标准级以上"
    },
    "深度": {
        "min_capability": 3,
        "quick_model_min": 2,
        "deep_model_min": 3,
        "required_features": [ModelFeature.TOOL_CALLING, ModelFeature.REASONING],
        "description": "4级深度分析：快速模型需标准级，深度模型需高级以上，需要推理能力"
    },
    "全面": {
        "min_capability": 4,
        "quick_model_min": 2,
        "deep_model_min": 4,
        "required_features": [ModelFeature.TOOL_CALLING, ModelFeature.REASONING],
        "description": "5级全面分析：快速模型需标准级，深度模型需专业级以上，强推理能力"
    }
}


#Default capability configuration of common models (for initialization and reference)
DEFAULT_MODEL_CAPABILITIES: Dict[str, Dict[str, Any]] = {
    #== sync, corrected by elderman == @elder man
    "qwen-turbo": {
        "capability_level": 1,
        "suitable_roles": [ModelRole.QUICK_ANALYSIS],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.FAST_RESPONSE, ModelFeature.COST_EFFECTIVE],
        "recommended_depths": ["快速", "基础"],
        "performance_metrics": {"speed": 5, "cost": 5, "quality": 3},
        "description": "通义千问轻量版，快速响应，适合数据收集"
    },
    "qwen-plus": {
        "capability_level": 2,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.LONG_CONTEXT],
        "recommended_depths": ["快速", "基础", "标准"],
        "performance_metrics": {"speed": 4, "cost": 4, "quality": 4},
        "description": "通义千问标准版，平衡性能和成本"
    },
    "qwen-max": {
        "capability_level": 4,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.LONG_CONTEXT, ModelFeature.REASONING],
        "recommended_depths": ["标准", "深度", "全面"],
        "performance_metrics": {"speed": 3, "cost": 2, "quality": 5},
        "description": "通义千问旗舰版，强大推理能力"
    },
    "qwen3-max": {
        "capability_level": 5,
        "suitable_roles": [ModelRole.DEEP_ANALYSIS],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.LONG_CONTEXT, ModelFeature.REASONING],
        "recommended_depths": ["深度", "全面"],
        "performance_metrics": {"speed": 2, "cost": 1, "quality": 5},
        "description": "通义千问长文本版，超长上下文"
    },
    
    # ==================== OpenAI ====================
    "gpt-3.5-turbo": {
        "capability_level": 1,
        "suitable_roles": [ModelRole.QUICK_ANALYSIS],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.FAST_RESPONSE, ModelFeature.COST_EFFECTIVE],
        "recommended_depths": ["快速", "基础"],
        "performance_metrics": {"speed": 5, "cost": 5, "quality": 3},
        "description": "GPT-3.5 Turbo，快速且经济"
    },
    "gpt-4": {
        "capability_level": 3,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.REASONING],
        "recommended_depths": ["基础", "标准", "深度"],
        "performance_metrics": {"speed": 3, "cost": 3, "quality": 4},
        "description": "GPT-4，强大的推理能力"
    },
    "gpt-4-turbo": {
        "capability_level": 4,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.LONG_CONTEXT, ModelFeature.REASONING, ModelFeature.VISION],
        "recommended_depths": ["标准", "深度", "全面"],
        "performance_metrics": {"speed": 4, "cost": 2, "quality": 5},
        "description": "GPT-4 Turbo，更快更强"
    },
    "gpt-4o-mini": {
        "capability_level": 2,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.FAST_RESPONSE, ModelFeature.COST_EFFECTIVE],
        "recommended_depths": ["快速", "基础", "标准"],
        "performance_metrics": {"speed": 5, "cost": 5, "quality": 3},
        "description": "GPT-4o Mini，经济实惠"
    },
    "o1-mini": {
        "capability_level": 4,
        "suitable_roles": [ModelRole.DEEP_ANALYSIS],
        "features": [ModelFeature.REASONING],
        "recommended_depths": ["深度", "全面"],
        "performance_metrics": {"speed": 2, "cost": 3, "quality": 5},
        "description": "O1 Mini，强推理模型"
    },
    "o1": {
        "capability_level": 5,
        "suitable_roles": [ModelRole.DEEP_ANALYSIS],
        "features": [ModelFeature.REASONING],
        "recommended_depths": ["全面"],
        "performance_metrics": {"speed": 1, "cost": 1, "quality": 5},
        "description": "O1，最强推理能力"
    },
    "o4-mini": {
        "capability_level": 4,
        "suitable_roles": [ModelRole.DEEP_ANALYSIS],
        "features": [ModelFeature.REASONING],
        "recommended_depths": ["深度", "全面"],
        "performance_metrics": {"speed": 2, "cost": 3, "quality": 5},
        "description": "O4 Mini，新一代推理模型"
    },
    
    # ==================== DeepSeek ====================
    "deepseek-chat": {
        "capability_level": 3,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.LONG_CONTEXT, ModelFeature.COST_EFFECTIVE],
        "recommended_depths": ["基础", "标准", "深度"],
        "performance_metrics": {"speed": 4, "cost": 5, "quality": 4},
        "description": "DeepSeek Chat，性价比高"
    },
    
    #== sync, corrected by elderman == @elder man
    "ernie-3.5": {
        "capability_level": 2,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING],
        "recommended_depths": ["快速", "基础", "标准"],
        "performance_metrics": {"speed": 4, "cost": 4, "quality": 3},
        "description": "文心一言3.5，标准版本"
    },
    "ernie-4.0": {
        "capability_level": 3,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.REASONING],
        "recommended_depths": ["基础", "标准", "深度"],
        "performance_metrics": {"speed": 3, "cost": 3, "quality": 4},
        "description": "文心一言4.0，高级版本"
    },
    "ernie-4.0-turbo": {
        "capability_level": 4,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.REASONING, ModelFeature.FAST_RESPONSE],
        "recommended_depths": ["标准", "深度", "全面"],
        "performance_metrics": {"speed": 4, "cost": 2, "quality": 5},
        "description": "文心一言4.0 Turbo，旗舰版本"
    },
    
    #== sync, corrected by elderman == @elder man
    "glm-3-turbo": {
        "capability_level": 1,
        "suitable_roles": [ModelRole.QUICK_ANALYSIS],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.FAST_RESPONSE, ModelFeature.COST_EFFECTIVE],
        "recommended_depths": ["快速", "基础"],
        "performance_metrics": {"speed": 5, "cost": 5, "quality": 3},
        "description": "智谱GLM-3 Turbo，快速版本"
    },
    "glm-4": {
        "capability_level": 3,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.REASONING],
        "recommended_depths": ["基础", "标准", "深度"],
        "performance_metrics": {"speed": 3, "cost": 3, "quality": 4},
        "description": "智谱GLM-4，标准版本"
    },
    "glm-4-plus": {
        "capability_level": 4,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.LONG_CONTEXT, ModelFeature.REASONING],
        "recommended_depths": ["标准", "深度", "全面"],
        "performance_metrics": {"speed": 3, "cost": 2, "quality": 5},
        "description": "智谱GLM-4 Plus，旗舰版本"
    },
    
    # ==================== Anthropic Claude ====================
    "claude-3-haiku": {
        "capability_level": 2,
        "suitable_roles": [ModelRole.QUICK_ANALYSIS],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.FAST_RESPONSE],
        "recommended_depths": ["快速", "基础", "标准"],
        "performance_metrics": {"speed": 5, "cost": 4, "quality": 3},
        "description": "Claude 3 Haiku，快速版本"
    },
    "claude-3-sonnet": {
        "capability_level": 3,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.LONG_CONTEXT, ModelFeature.VISION],
        "recommended_depths": ["基础", "标准", "深度"],
        "performance_metrics": {"speed": 4, "cost": 3, "quality": 4},
        "description": "Claude 3 Sonnet，平衡版本"
    },
    "claude-3-opus": {
        "capability_level": 4,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.LONG_CONTEXT, ModelFeature.REASONING, ModelFeature.VISION],
        "recommended_depths": ["标准", "深度", "全面"],
        "performance_metrics": {"speed": 3, "cost": 2, "quality": 5},
        "description": "Claude 3 Opus，旗舰版本"
    },
    "claude-3.5-sonnet": {
        "capability_level": 5,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.LONG_CONTEXT, ModelFeature.REASONING, ModelFeature.VISION],
        "recommended_depths": ["标准", "深度", "全面"],
        "performance_metrics": {"speed": 4, "cost": 2, "quality": 5},
        "description": "Claude 3.5 Sonnet，最新旗舰"
    },

    # ==================== Google Gemini ====================
    "gemini-pro": {
        "capability_level": 3,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.REASONING],
        "recommended_depths": ["基础", "标准", "深度"],
        "performance_metrics": {"speed": 4, "cost": 4, "quality": 4},
        "description": "Gemini Pro，经典稳定版本"
    },
    "gemini-1.5-pro": {
        "capability_level": 4,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.LONG_CONTEXT, ModelFeature.REASONING, ModelFeature.VISION],
        "recommended_depths": ["标准", "深度", "全面"],
        "performance_metrics": {"speed": 4, "cost": 3, "quality": 5},
        "description": "Gemini 1.5 Pro，长上下文旗舰"
    },
    "gemini-1.5-flash": {
        "capability_level": 2,
        "suitable_roles": [ModelRole.QUICK_ANALYSIS],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.FAST_RESPONSE, ModelFeature.COST_EFFECTIVE],
        "recommended_depths": ["快速", "基础", "标准"],
        "performance_metrics": {"speed": 5, "cost": 5, "quality": 3},
        "description": "Gemini 1.5 Flash，快速响应版本"
    },
    "gemini-2.0-flash": {
        "capability_level": 4,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.LONG_CONTEXT, ModelFeature.REASONING, ModelFeature.FAST_RESPONSE],
        "recommended_depths": ["标准", "深度", "全面"],
        "performance_metrics": {"speed": 5, "cost": 3, "quality": 5},
        "description": "Gemini 2.0 Flash，新一代快速旗舰"
    },
    "gemini-2.5-flash-lite-preview-06-17": {
        "capability_level": 2,
        "suitable_roles": [ModelRole.QUICK_ANALYSIS],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.FAST_RESPONSE, ModelFeature.COST_EFFECTIVE],
        "recommended_depths": ["快速", "基础"],
        "performance_metrics": {"speed": 5, "cost": 5, "quality": 3},
        "description": "Gemini 2.5 Flash Lite，轻量预览版"
    },

    #== sync, corrected by elderman == @elder man
    "moonshot-v1-8k": {
        "capability_level": 2,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING],
        "recommended_depths": ["快速", "基础", "标准"],
        "performance_metrics": {"speed": 4, "cost": 4, "quality": 3},
        "description": "Moonshot V1 8K，标准版本"
    },
    "moonshot-v1-32k": {
        "capability_level": 3,
        "suitable_roles": [ModelRole.BOTH],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.LONG_CONTEXT],
        "recommended_depths": ["基础", "标准", "深度"],
        "performance_metrics": {"speed": 3, "cost": 3, "quality": 4},
        "description": "Moonshot V1 32K，长上下文版本"
    },
    "moonshot-v1-128k": {
        "capability_level": 4,
        "suitable_roles": [ModelRole.DEEP_ANALYSIS],
        "features": [ModelFeature.TOOL_CALLING, ModelFeature.LONG_CONTEXT, ModelFeature.REASONING],
        "recommended_depths": ["标准", "深度", "全面"],
        "performance_metrics": {"speed": 2, "cost": 2, "quality": 5},
        "description": "Moonshot V1 128K，超长上下文旗舰"
    },
}


def get_model_capability_badge(level: int) -> Dict[str, str]:
    """Capability Level Emblem Style"""
    badges = {
        1: {"text": "基础", "color": "#909399", "icon": "⚡"},
        2: {"text": "标准", "color": "#409EFF", "icon": "📊"},
        3: {"text": "高级", "color": "#67C23A", "icon": "🎯"},
        4: {"text": "专业", "color": "#E6A23C", "icon": "🔥"},
        5: {"text": "旗舰", "color": "#F56C6C", "icon": "👑"}
    }
    return badges.get(level, badges[2])


def get_role_badge(role: ModelRole) -> Dict[str, str]:
    """Get Role Emblem Styles"""
    badges = {
        ModelRole.QUICK_ANALYSIS: {"text": "快速分析", "color": "success", "icon": "⚡"},
        ModelRole.DEEP_ANALYSIS: {"text": "深度推理", "color": "warning", "icon": "🧠"},
        ModelRole.BOTH: {"text": "通用", "color": "primary", "icon": "🎯"}
    }
    return badges.get(role, badges[ModelRole.BOTH])


def get_feature_badge(feature: ModelFeature) -> Dict[str, str]:
    """Fetch feature badge style"""
    badges = {
        ModelFeature.TOOL_CALLING: {"text": "工具调用", "color": "info", "icon": "🔧"},
        ModelFeature.LONG_CONTEXT: {"text": "长上下文", "color": "success", "icon": "📚"},
        ModelFeature.REASONING: {"text": "强推理", "color": "warning", "icon": "🧠"},
        ModelFeature.VISION: {"text": "视觉", "color": "primary", "icon": "👁️"},
        ModelFeature.FAST_RESPONSE: {"text": "快速", "color": "success", "icon": "⚡"},
        ModelFeature.COST_EFFECTIVE: {"text": "经济", "color": "success", "icon": "💰"}
    }
    return badges.get(feature, {"text": str(feature), "color": "info", "icon": "✨"})


#== sync, corrected by elderman == @elder man

#Default configuration of the polymer channel
AGGREGATOR_PROVIDERS = {
    "302ai": {
        "display_name": "302.AI",
        "description": "302.AI 聚合平台，提供多厂商模型统一接口",
        "website": "https://302.ai",
        "api_doc_url": "https://doc.302.ai",
        "default_base_url": "https://api.302.ai/v1",
        "model_name_format": "{provider}/{model}",  #Example: openai/gpt-4
        "supported_providers": ["openai", "anthropic", "google", "deepseek", "qwen"]
    },
    "openrouter": {
        "display_name": "OpenRouter",
        "description": "OpenRouter 聚合平台，支持多种 AI 模型",
        "website": "https://openrouter.ai",
        "api_doc_url": "https://openrouter.ai/docs",
        "default_base_url": "https://openrouter.ai/api/v1",
        "model_name_format": "{provider}/{model}",
        "supported_providers": ["openai", "anthropic", "google", "meta", "mistral"]
    },
    "oneapi": {
        "display_name": "One API",
        "description": "One API 开源聚合平台",
        "website": "https://github.com/songquanpeng/one-api",
        "api_doc_url": "https://github.com/songquanpeng/one-api",
        "default_base_url": "http://localhost:3000/v1",  #Users are required to deploy themselves
        "model_name_format": "{model}",  #One API usually doesn't need a prefix.
        "supported_providers": ["openai", "anthropic", "google", "azure", "claude"]
    },
    "newapi": {
        "display_name": "New API",
        "description": "New API 聚合平台",
        "website": "https://github.com/Calcium-Ion/new-api",
        "api_doc_url": "https://github.com/Calcium-Ion/new-api",
        "default_base_url": "http://localhost:3000/v1",
        "model_name_format": "{model}",
        "supported_providers": ["openai", "anthropic", "google", "azure", "claude"]
    }
}


def is_aggregator_model(model_name: str) -> bool:
    """Determines if it's a polymer channel model name

Args:
Model name: Model name

Returns:
Whether it's a polymer channel model
"""
    return "/" in model_name


def parse_aggregator_model(model_name: str) -> Tuple[str, str]:
    """Parsing polymer channel model names

Args:
Model name: Model name (e. g. openai/gpt-4)

Returns:
(provider, model)
"""
    if "/" in model_name:
        parts = model_name.split("/", 1)
        return parts[0], parts[1]
    return "", model_name

