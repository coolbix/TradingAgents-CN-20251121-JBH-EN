"""OpenAI Compatible Adapter Base Category
Provide a unified basis for all LLM providers supporting the OpenAI interface
"""

import os
import time
from typing import Any, Dict, List, Optional, Union
from langchain_core.messages import BaseMessage
from langchain_core.outputs import ChatResult
from langchain_openai import ChatOpenAI
from langchain_core.callbacks import CallbackManagerForLLMRun

#Import Unified Log System
from tradingagents.utils.logging_init import setup_llm_logging

#Import Log Module
from tradingagents.utils.logging_manager import get_logger, get_logger_manager
logger = get_logger('agents')
logger = setup_llm_logging()

#Import token tracker
try:
    from tradingagents.config.config_manager import token_tracker
    TOKEN_TRACKING_ENABLED = True
    logger.info("Token tracking enabled")
except ImportError:
    TOKEN_TRACKING_ENABLED = False
    logger.warning("Token tracking is not enabled")


class OpenAICompatibleBase(ChatOpenAI):
    """OpenAI Compatible Adapter Base Category
Provide uniform realization for all LLM providers supporting OpenAI interface
"""
    
    def __init__(
        self,
        provider_name: str,
        model: str,
        api_key_env_var: str,
        base_url: str,
        api_key: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: Optional[int] = None,
        **kwargs
    ):
        """Initialize OpenAI Compatibility Compatibility

Args:
provider name: Provider name (e. g. "deepseek", "dashscope")
Model name
api key env var: API Key Environment Variable First Name
base url: API baseURL
api key: API keys, if not available, from environmental variables
temperature: temperature parameters
max tokens: Max tokens
**kwargs: Other parameters
"""
        
        #[DBUG] Read the log before the environment variable
        logger.info(f"🔍 [{provider_name}Initializing OpenAI Compatibility")
        logger.info(f"🔍 [{provider_name}Initializing] models:{model}")
        logger.info(f"🔍 [{provider_name}Initialize] API Key environment variable name:{api_key_env_var}")
        logger.info(f"🔍 [{provider_name}Initialize] Whether api key parameters are introduced:{api_key is not None}")

        #Cache Meta Information to Private Properties Before Parent Initialization (avoid Pydantic Field Limit)
        object.__setattr__(self, "_provider_name", provider_name)
        object.__setattr__(self, "_model_name_alias", model)

        #Get API Keys
        if api_key is None:
            #Import API Key Authentication Tool
            try:
                from app.utils.api_key_utils import is_valid_api_key
            except ImportError:
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

            #Read API Key from Environmental Variables
            env_api_key = os.getenv(api_key_env_var)
            logger.info(f"🔍 [{provider_name}Initializing] Read from environmental variables{api_key_env_var}: {'Value' if env_api_key else 'Empty'}")

            #Verify the validity of API Key in the environment variable (exclude placeholder)
            if env_api_key and is_valid_api_key(env_api_key):
                logger.info(f"✅ [{provider_name}Initialization] API Key is valid for environment variables, length:{len(env_api_key)}, top 10:{env_api_key[:10]}...")
                api_key = env_api_key
            elif env_api_key:
                logger.warning(f"⚠️ [{provider_name}Initialize] API Key is invalid in environment variables (possibly placeholders) and will be ignored")
                api_key = None
            else:
                logger.warning(f"⚠️ [{provider_name}Initialization]{api_key_env_var}Environment variable is empty")
                api_key = None

            if not api_key:
                logger.error(f"❌ [{provider_name}Initializing] API Key check failed, about to be thrown out of anomaly")
                raise ValueError(
                    f"{provider_name} API密钥未找到。"
                    f"请在 Web 界面配置 API Key (设置 -> 大模型厂家) 或设置 {api_key_env_var} 环境变量。"
                )
        else:
            logger.info(f"✅ [{provider_name}Initialize] Use imported API Key (from database configuration), length:{len(api_key)}")
        
        #Set OpenAI compatible parameters
        #Note: Model parameters are mapd by Pydantic to model name field
        openai_kwargs = {
            "model": model,  #This will be mapd to model name field
            "temperature": temperature,
            "max_tokens": max_tokens,
            **kwargs
        }
        
        #Use different parameters according to the Langchain version First Name
        try:
            #New version of Langchain
            openai_kwargs.update({
                "api_key": api_key,
                "base_url": base_url
            })
        except:
            #Old LangChain
            openai_kwargs.update({
                "openai_api_key": api_key,
                "openai_api_base": base_url
            })
        
        #Initialised Parent
        super().__init__(**openai_kwargs)

        #Ensure the presence of meta-information once again (some realizations will be reset in sub() dec.)
        object.__setattr__(self, "_provider_name", provider_name)
        object.__setattr__(self, "_model_name_alias", model)

        logger.info(f"✅ {provider_name}Initiation of OpenAI compatibility adaptor successfully")
        logger.info(f"Models:{model}")
        logger.info(f"   API Base: {base_url}")

    @property
    def provider_name(self) -> Optional[str]:
        return getattr(self, "_provider_name", None)

    #Remove model name definition, using Pydantic field
    #Model name field provided by Pydantic field of ChatOpenAI base class
    
    def _generate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> ChatResult:
        """Generate chat responses and record token usage
"""
        
        #Record start time
        start_time = time.time()
        
        #Call parent generation method
        result = super()._generate(messages, stop, run_manager, **kwargs)
        
        #Record token
        self._track_token_usage(result, kwargs, start_time)
        
        return result

    def _track_token_usage(self, result: ChatResult, kwargs: Dict, start_time: float):
        """Record token usage and output log"""
        if not TOKEN_TRACKING_ENABLED:
            return
        try:
            #Statistical token information
            usage = getattr(result, "usage_metadata", None)
            total_tokens = usage.get("total_tokens") if usage else None
            prompt_tokens = usage.get("input_tokens") if usage else None
            completion_tokens = usage.get("output_tokens") if usage else None

            elapsed = time.time() - start_time
            logger.info(
                f"Token uses -Provider:{getattr(self, 'provider_name', 'unknown')}, Model: {getattr(self, 'model_name', 'unknown')}, "
                f"General tokens:{total_tokens},Tip:{prompt_tokens}, Completion:{completion_tokens}, when:{elapsed:.2f}s"
            )
        except Exception as e:
            logger.warning(f"Token's track record failed:{e}")


class ChatDeepSeekOpenAI(OpenAICompatibleBase):
    """DeepSeek OpenAI Compatibility Compatible"""
    
    def __init__(
        self,
        model: str = "deepseek-chat",
        api_key: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: Optional[int] = None,
        **kwargs
    ):
        super().__init__(
            provider_name="deepseek",
            model=model,
            api_key_env_var="DEEPSEEK_API_KEY",
            base_url="https://api.deepseek.com",
            api_key=api_key,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs
        )


class ChatDashScopeOpenAIUnified(OpenAICompatibleBase):
    """DashScope OpenAI Compatibility Compatibility"""
    
    def __init__(
        self,
        model: str = "qwen-turbo",
        api_key: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: Optional[int] = None,
        **kwargs
    ):
        super().__init__(
            provider_name="dashscope",
            model=model,
            api_key_env_var="DASHSCOPE_API_KEY",
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
            api_key=api_key,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs
        )


class ChatQianfanOpenAI(OpenAICompatibleBase):
    """OpenAI Compatibility Compatible Body"""
    
    def __init__(
        self,
        model: str = "ernie-3.5-8k",
        api_key: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: Optional[int] = None,
        **kwargs
    ):
        #API for the new generation of thousands of sails
        #Format: bce-v3/ALTAK-xx/xx

        #Try reading from environment variables if no API Key is imported
        if not api_key:
            #Import API Key Authentication Tool
            try:
                from app.utils.api_key_utils import is_valid_api_key
            except ImportError:
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

            env_api_key = os.getenv('QIANFAN_API_KEY')
            if env_api_key and is_valid_api_key(env_api_key):
                qianfan_api_key = env_api_key
            else:
                qianfan_api_key = None
        else:
            qianfan_api_key = api_key

        if not qianfan_api_key:
            raise ValueError(
                "千帆模型需要配置 API Key。"
                "请在 Web 界面配置 (设置 -> 大模型厂家) 或设置 QIANFAN_API_KEY 环境变量，"
                "格式为: bce-v3/ALTAK-xxx/xxx"
            )

        if not qianfan_api_key.startswith('bce-v3/'):
            raise ValueError(
                "QIANFAN_API_KEY格式错误，应为: bce-v3/ALTAK-xxx/xxx"
            )
        
        super().__init__(
            provider_name="qianfan",
            model=model,
            api_key_env_var="QIANFAN_API_KEY",
            base_url="https://qianfan.baidubce.com/v2",
            api_key=qianfan_api_key,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs
        )
    
    def _estimate_tokens(self, text: str) -> int:
        """Estimated number of tokens in text (specialized in thousands of sail models)"""
        #Token estimate for the thousands sail model: approximately 1.5 characters/token in Chinese, approximately 4 characters/token in English
        #Conservative estimate: 2 characters/token
        return max(1, len(text) // 2)
    
    def _truncate_messages(self, messages: List[BaseMessage], max_tokens: int = 4500) -> List[BaseMessage]:
        """Intercept messages to fit the token limit of the thousand sail model"""
        #Set aside some token space for a thousand sail models, using 4,500 instead of 5120.
        truncated_messages = []
        total_tokens = 0
        
        #From the last message, keep the message forward.
        for message in reversed(messages):
            content = str(message.content) if hasattr(message, 'content') else str(message)
            message_tokens = self._estimate_tokens(content)
            
            if total_tokens + message_tokens <= max_tokens:
                truncated_messages.insert(0, message)
                total_tokens += message_tokens
            else:
                #If the first message is too long, cut it off.
                if not truncated_messages:
                    remaining_tokens = max_tokens - 100  #100 tokens reserved
                    max_chars = remaining_tokens * 2  #2 Characters/token
                    truncated_content = content[:max_chars] + "...(内容已截断)"
                    
                    #Other Organiser
                    if hasattr(message, 'content'):
                        message.content = truncated_content
                    truncated_messages.insert(0, message)
                break
        
        if len(truncated_messages) < len(messages):
            logger.warning(f"⚠️ The thousand sail model has been entered too long.{len(messages) - len(truncated_messages)}Message")
        
        return truncated_messages
    
    def _generate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> ChatResult:
        """Generate chat responses, token cut-off logic with thousands of sails"""
        
        #Input token cut-off of a thousand sail model
        truncated_messages = self._truncate_messages(messages)
        
        #Call parent generate method
        return super()._generate(truncated_messages, stop, run_manager, **kwargs)


class ChatZhipuOpenAI(OpenAICompatibleBase):
    """Intelligent AI GLM OpenAI Compatibility"""
    
    def __init__(
        self,
        model: str = "glm-4.6",
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: Optional[int] = None,
        **kwargs
    ):
        if base_url is None:
            env_base_url = os.getenv("ZHIPU_BASE_URL")
            #Use only valid environment variables (not placeholders)
            if env_base_url and not env_base_url.startswith('your_') and not env_base_url.startswith('your-'):
                base_url = env_base_url
            else:
                base_url = "https://open.bigmodel.cn/api/paas/v4"
                
        super().__init__(
            provider_name="zhipu",
            model=model,
            api_key_env_var="ZHIPU_API_KEY",
            base_url=base_url,
            api_key=api_key,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs
        )
    
    def _estimate_tokens(self, text: str) -> int:
        """Estimated number of tokens of text (GLM model specific)"""
        #Token estimate for GLM model: approximately 1.5 characters/token in Chinese, approximately 4 characters/token in English
        #Conservative estimate: 2 characters/token
        return max(1, len(text) // 2)


class ChatCustomOpenAI(OpenAICompatibleBase):
    """Custom OpenAI endpoint adapter (agent/polymer platform)"""

    def __init__(
        self,
        model: str = "gpt-3.5-turbo",
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: Optional[int] = None,
        **kwargs
    ):
        #Try reading from environment variables if no base url is imported
        if base_url is None:
            env_base_url = os.getenv("CUSTOM_OPENAI_BASE_URL")
            #Use only valid environment variables (not placeholders)
            if env_base_url and not env_base_url.startswith('your_') and not env_base_url.startswith('your-'):
                base_url = env_base_url
            else:
                base_url = "https://api.openai.com/v1"

        super().__init__(
            provider_name="custom_openai",
            model=model,
            api_key_env_var="CUSTOM_OPENAI_API_KEY",
            base_url=base_url,
            api_key=api_key,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs
        )


#Supported OpenAI compatible model configuration
OPENAI_COMPATIBLE_PROVIDERS = {
    "deepseek": {
        "adapter_class": ChatDeepSeekOpenAI,
        "base_url": "https://api.deepseek.com",
        "api_key_env": "DEEPSEEK_API_KEY",
        "models": {
            "deepseek-chat": {"context_length": 32768, "supports_function_calling": True},
            "deepseek-coder": {"context_length": 16384, "supports_function_calling": True}
        }
    },
    "dashscope": {
        "adapter_class": ChatDashScopeOpenAIUnified,
        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "api_key_env": "DASHSCOPE_API_KEY",
        "models": {
            "qwen-turbo": {"context_length": 8192, "supports_function_calling": True},
            "qwen-plus": {"context_length": 32768, "supports_function_calling": True},
            "qwen-plus-latest": {"context_length": 32768, "supports_function_calling": True},
            "qwen-max": {"context_length": 32768, "supports_function_calling": True},
            "qwen-max-latest": {"context_length": 32768, "supports_function_calling": True}
        }
    },
    "qianfan": {
        "adapter_class": ChatQianfanOpenAI,
        "base_url": "https://qianfan.baidubce.com/v2",
        "api_key_env": "QIANFAN_API_KEY",
        "models": {
            "ernie-3.5-8k": {"context_length": 5120, "supports_function_calling": True},
            "ernie-4.0-turbo-8k": {"context_length": 5120, "supports_function_calling": True},
            "ERNIE-Speed-8K": {"context_length": 5120, "supports_function_calling": True},
            "ERNIE-Lite-8K": {"context_length": 5120, "supports_function_calling": True}
        }
    },
    "zhipu": {
        "adapter_class": ChatZhipuOpenAI,
        "base_url": "https://open.bigmodel.cn/api/paas/v4",
        "api_key_env": "ZHIPU_API_KEY",
        "models": {
            "glm-4.6": {"context_length": 200000, "supports_function_calling": True},
            "glm-4": {"context_length": 128000, "supports_function_calling": True},
            "glm-4-plus": {"context_length": 128000, "supports_function_calling": True},
            "glm-3-turbo": {"context_length": 128000, "supports_function_calling": True}
        }
    },
    "custom_openai": {
        "adapter_class": ChatCustomOpenAI,
        "base_url": None,  #By User
        "api_key_env": "CUSTOM_OPENAI_API_KEY",
        "models": {
            "gpt-3.5-turbo": {"context_length": 16384, "supports_function_calling": True},
            "gpt-4": {"context_length": 8192, "supports_function_calling": True},
            "gpt-4-turbo": {"context_length": 128000, "supports_function_calling": True},
            "gpt-4o": {"context_length": 128000, "supports_function_calling": True},
            "gpt-4o-mini": {"context_length": 128000, "supports_function_calling": True},
            "claude-3-haiku": {"context_length": 200000, "supports_function_calling": True},
            "claude-3-sonnet": {"context_length": 200000, "supports_function_calling": True},
            "claude-3-opus": {"context_length": 200000, "supports_function_calling": True},
            "claude-3.5-sonnet": {"context_length": 200000, "supports_function_calling": True},
            "gemini-pro": {"context_length": 32768, "supports_function_calling": True},
            "gemini-1.5-pro": {"context_length": 1000000, "supports_function_calling": True},
            "llama-3.1-8b": {"context_length": 128000, "supports_function_calling": True},
            "llama-3.1-70b": {"context_length": 128000, "supports_function_calling": True},
            "llama-3.1-405b": {"context_length": 128000, "supports_function_calling": True},
            "custom-model": {"context_length": 32768, "supports_function_calling": True}
        }
    }
}


def create_openai_compatible_llm(
    provider: str,
    model: str,
    api_key: Optional[str] = None,
    temperature: float = 0.1,
    max_tokens: Optional[int] = None,
    base_url: Optional[str] = None,
    **kwargs
) -> OpenAICompatibleBase:
    """Unified plant function for creating OpenAI compatible LLM examples"""
    provider_info = OPENAI_COMPATIBLE_PROVIDERS.get(provider)
    if not provider_info:
        raise ValueError(f"不支持的OpenAI兼容提供商: {provider}")

    adapter_class = provider_info["adapter_class"]

    #If no base url is provided, the default value for provider is used (possibly notone)
    if base_url is None:
        base_url = provider_info.get("base_url")

    #Only when provider does not have a base url (e.g. custom openai) pass the base url to the adapter.
    #Avoid conflict with the sub()   init  (..., base url=...) within the adaptor leads to a "multiple values" error.
    init_kwargs = dict(
        model=model,
        api_key=api_key,
        temperature=temperature,
        max_tokens=max_tokens,
        **kwargs,
    )
    if provider_info.get("base_url") is None and base_url:
        init_kwargs["base_url"] = base_url

    return adapter_class(**init_kwargs)


def test_openai_compatible_adapters():
    """Quick test if all adapters can be properly demonstrated (no real request initiated)"""
    for provider, info in OPENAI_COMPATIBLE_PROVIDERS.items():
        cls = info["adapter_class"]
        try:
            if provider == "custom_openai":
                cls(model="gpt-3.5-turbo", api_key="test", base_url="https://api.openai.com/v1")
            elif provider == "qianfan":
                #Qianfan API KEY, format: bce-v3/ALTAK-xx/xx
                cls(model="ernie-3.5-8k", api_key="bce-v3/test-key/test-secret")
            else:
                cls(model=list(info["models"].keys())[0], api_key="test")
            logger.info(f"✅ The adaptor was successfully adapted:{provider}")
        except Exception as e:
            logger.warning(f"Example of adaptor failed (expected or negligible):{provider} - {e}")


if __name__ == "__main__":
    test_openai_compatible_adapters()
