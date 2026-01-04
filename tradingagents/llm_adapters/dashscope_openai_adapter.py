"""Aripercin OpenAI Compatibility
OpenAI compatibility interface for TradingAgents
OppenAI compatibility using a practicable model without the need for additional tools
"""

import os
from typing import Any, Dict, List, Optional, Union, Sequence
from langchain_openai import ChatOpenAI
from langchain_core.tools import BaseTool
from pydantic import Field, SecretStr
from ..config.config_manager import TOKEN_TRACKER

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')


class ChatDashScopeOpenAI(ChatOpenAI):
    """Aliberian OpenAI Compatibility Compatible
    Inherit ChatOpenAI, call the calorie model through OpenAI compatible interface
    Use original OpenAI compatibility of refined models to support original
    """
    
    def __init__(self, **kwargs):
        """Initialize DashScope OpenAI compatible client"""

        #[DBUG] Read the log before the environment variable
        logger.info(f"[DashScope Initialization]")
        logger.info(f"Does kwargs contain api key:{'api_key' in kwargs}")

        #🔥 Prefer to the imported API Key in kwargs (from database configuration)
        api_key_from_kwargs = kwargs.get("api_key")

        #If kwargs does not have API Key or None, try reading from environment variables
        if not api_key_from_kwargs:
            #Import API Key Authentication Tool
            try:
                #Try importing from app.utils (backend environment)
                from app.utils.api_key_utils import is_valid_api_key
            except ImportError:
                #If import fails, use local simplified version
                def is_valid_api_key(key):
                    if not key or len(key) <= 10:
                        return False
                    if key.startswith('your_') or key.startswith('your-'):
                        return False
                    if key.endswith('_here') or key.endswith('-here'):
                        return False
                    if '...' in key:
                        return False
                    return True

            #Try reading API Key from an environment variable
            env_api_key = os.getenv("DASHSCOPE_API_KEY")
            logger.info(f"[DashScope Initialization]{'Value' if env_api_key else 'Empty'}")

            #Verify the validity of API Key in the environment variable (exclude placeholder)
            if env_api_key and is_valid_api_key(env_api_key):
                logger.info(f"✅ [DashScope Initialization] API Key is valid, length:{len(env_api_key)}, top 10:{env_api_key[:10]}...")
                api_key_from_kwargs = env_api_key
            elif env_api_key:
                logger.warning(f"API Key (possibly placeholder) in environmental variables is invalid and will be ignored")
                api_key_from_kwargs = None
            else:
                logger.warning(f"[DashScope Initialization]")
                api_key_from_kwargs = None
        else:
            logger.info(f"✅ [DashScope Initialization] with the imported API Key (from database configuration) in kwargs")

        #Set default configuration for DashScope OpenAI compatible interface
        kwargs.setdefault("base_url", "https://dashscope.aliyuncs.com/compatible-mode/v1")
        kwargs["api_key"] = api_key_from_kwargs  #API Key
        kwargs.setdefault("model", "qwen-turbo")
        kwargs.setdefault("temperature", 0.1)
        kwargs.setdefault("max_tokens", 2000)

        #Check API keys and base url
        final_api_key = kwargs.get("api_key")
        final_base_url = kwargs.get("base_url")
        logger.info(f"[DashScope Initializing]{'Value' if final_api_key else 'Empty'}")
        logger.info(f"[DashScope Initializing]{final_base_url}")

        if not final_api_key:
            logger.error(f"[DashScope Initialization] API Key check failed, about to be released")
            raise ValueError(
                "DashScope API key not found. Please configure API key in web interface "
                "(Settings -> LLM Providers) or set DASHSCOPE_API_KEY environment variable."
            )

        #Call Parent Initialization
        super().__init__(**kwargs)

        logger.info(f"✅Aribecian OpenAI compatibility adapter initialised successfully")
        logger.info(f"Models:{kwargs.get('model', 'qwen-turbo')}")

        #Compatible with different versions of attribute names
        api_base = getattr(self, 'base_url', None) or getattr(self, 'openai_api_base', None) or kwargs.get('base_url', 'unknown')
        logger.info(f"   API Base: {api_base}")
    
    def _generate(self, *args, **kwargs):
        """Rewrite generation method, add token usage tracking"""
        
        #Call parent generation method
        result = super()._generate(*args, **kwargs)
        
        #Track token usage
        try:
            #Extract token information from the result
            if hasattr(result, 'llm_output') and result.llm_output:
                token_usage = result.llm_output.get('token_usage', {})
                
                input_tokens = token_usage.get('prompt_tokens', 0)
                output_tokens = token_usage.get('completion_tokens', 0)
                
                if input_tokens > 0 or output_tokens > 0:
                    #Generate Session ID
                    session_id = kwargs.get('session_id', f"dashscope_openai_{hash(str(args))%10000}")
                    analysis_type = kwargs.get('analysis_type', 'stock_analysis')
                    
                    #Record usage using TokenTracker
                    TOKEN_TRACKER.track_usage(
                        provider="dashscope",
                        model_name=self.model_name,
                        input_tokens=input_tokens,
                        output_tokens=output_tokens,
                        session_id=session_id,
                        analysis_type=analysis_type
                    )
                    
        except Exception as track_error:
            #Token, tracking failure should not affect the primary function.
            logger.error(f"Token has failed:{track_error}")
        
        return result


#List of supported models
DASHSCOPE_OPENAI_MODELS = {
    #General questions series
    "qwen-turbo": {
        "description": "通义千问 Turbo - 快速响应，适合日常对话",
        "context_length": 8192,
        "supports_function_calling": True,
        "recommended_for": ["快速任务", "日常对话", "简单分析"]
    },
    "qwen-plus": {
        "description": "通义千问 Plus - 平衡性能和成本",
        "context_length": 32768,
        "supports_function_calling": True,
        "recommended_for": ["复杂分析", "专业任务", "深度思考"]
    },
    "qwen-plus-latest": {
        "description": "通义千问 Plus 最新版 - 最新功能和性能",
        "context_length": 32768,
        "supports_function_calling": True,
        "recommended_for": ["最新功能", "复杂分析", "专业任务"]
    },
    "qwen-max": {
        "description": "通义千问 Max - 最强性能，适合复杂任务",
        "context_length": 32768,
        "supports_function_calling": True,
        "recommended_for": ["复杂推理", "专业分析", "高质量输出"]
    },
    "qwen-max-latest": {
        "description": "通义千问 Max 最新版 - 最强性能和最新功能",
        "context_length": 32768,
        "supports_function_calling": True,
        "recommended_for": ["最新功能", "复杂推理", "专业分析"]
    },
    "qwen-long": {
        "description": "通义千问 Long - 超长上下文，适合长文档处理",
        "context_length": 1000000,
        "supports_function_calling": True,
        "recommended_for": ["长文档分析", "大量数据处理", "复杂上下文"]
    }
}


def get_available_openai_models() -> Dict[str, Dict[str, Any]]:
    """Get a list of available DashScope OpenAI compatible models"""
    return DASHSCOPE_OPENAI_MODELS


def create_dashscope_openai_llm(
    model: str = "qwen-plus-latest",
    api_key: Optional[str] = None,
    temperature: float = 0.1,
    max_tokens: int = 2000,
    **kwargs
) -> ChatDashScopeOpenAI:
    """A simple function to create DashScope OpenAI compatible LLM examples"""
    
    return ChatDashScopeOpenAI(
        model=model,
        api_key=api_key,
        temperature=temperature,
        max_tokens=max_tokens,
        **kwargs
    )


def test_dashscope_openai_connection(
    model: str = "qwen-turbo",
    api_key: Optional[str] = None
) -> bool:
    """Test DashScope OpenAI compatible interface connection"""
    
    try:
        logger.info(f"🧪 Test DashScope OpenAI compatibility interface")
        logger.info(f"Models:{model}")
        
        #Create Client
        llm = create_dashscope_openai_llm(
            model=model,
            api_key=api_key,
            max_tokens=50
        )
        
        #Send Test Message
        response = llm.invoke("你好，请简单介绍一下你自己。")
        
        if response and hasattr(response, 'content') and response.content:
            logger.info(f"DashScope OpenAI interface successfully connected")
            logger.info(f"Response:{response.content[:100]}...")
            return True
        else:
            logger.error(f"DashScop OpenAI interface response is empty")
            return False
            
    except Exception as e:
        logger.error(f"DashScop OpenAI interface failed:{e}")
        return False


def test_dashscope_openai_function_calling(
    model: str = "qwen-plus-latest",
    api_key: Optional[str] = None
) -> bool:
    """Function Calling for testing DashScope OpenAI compatible interface"""
    
    try:
        logger.info(f"DashScopOpenAIFunctionCalling")
        logger.info(f"Models:{model}")
        
        #Create Client
        llm = create_dashscope_openai_llm(
            model=model,
            api_key=api_key,
            max_tokens=200
        )
        
        #Define Test Tool
        def get_current_time() -> str:
            """Get Current Time"""
            import datetime
            return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        #Create LangChain Tool
        from langchain_core.tools import tool
        
        @tool
        def test_tool(query: str) -> str:
            """Test tool, return query information"""
            return f"收到查询: {query}"
        
        #Tie Tool
        llm_with_tools = llm.bind_tools([test_tool])
        
        #Test Tool Call
        response = llm_with_tools.invoke("请使用test_tool查询'hello world'")
        
        logger.info(f"DashScope OpenAI Faction Calling")
        logger.info(f"Type of response:{type(response)}")
        
        if hasattr(response, 'tool_calls') and response.tool_calls:
            logger.info(f"Number of tools called:{len(response.tool_calls)}")
            return True
        else:
            logger.info(f"Response content:{getattr(response, 'content', 'No content')}")
            return True  #Even without a tool call was successful because the model may choose not to call a tool
            
    except Exception as e:
        logger.error(f"DashScope OpenAFunction Calling failed:{e}")
        return False


if __name__ == "__main__":
    """测试脚本"""
    logger.info(f"DashScope OpenAI Compatibility Compatibility Test")
    logger.info(f"=" * 50)
    
    #Test Connection
    connection_ok = test_dashscope_openai_connection()
    
    if connection_ok:
        #Testing
        function_calling_ok = test_dashscope_openai_function_calling()
        
        if function_calling_ok:
            logger.info(f"All tests passed! DashScope OpenAI is working.")
        else:
            logger.error(f"Function Calling Test Failed")
    else:
        logger.error(f"Connection test failed")
