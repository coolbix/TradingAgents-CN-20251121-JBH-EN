# TradingAgents/graph/trading_graph.py

import os
from pathlib import Path
import json
from datetime import date
from typing import Dict, Any, Tuple, List, Optional
import time

from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_google_genai import ChatGoogleGenerativeAI
from tradingagents.llm_adapters import ChatDashScopeOpenAI, ChatGoogleOpenAI

from langgraph.prebuilt import ToolNode

from tradingagents.agents import *
from tradingagents.default_config import DEFAULT_CONFIG
from tradingagents.agents.utils.memory import FinancialSituationMemory

#Import Unified Log System
from tradingagents.utils.logging_init import get_logger

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')
from tradingagents.agents.utils.agent_states import (
    AgentState,
    InvestDebateState,
    RiskDebateState,
)
from tradingagents.dataflows.interface import set_config

from .conditional_logic import ConditionalLogic
from .setup import GraphSetup
from .propagation import Propagator
from .reflection import Reflector
from .signal_processing import SignalProcessor


def create_llm_by_provider(provider: str, model: str, backend_url: str, temperature: float, max_tokens: int, timeout: int, api_key: str = None):
    """Create corresponding LLM instance from provider

    Args:
        Provider: supplier name (google, Dashscope, Deepseek, openai, etc.)
        Model name
        Back url: API Address
        temperature: temperature parameters
        max tokens: maximum number of token
        Timeout: Timeout
        api key: API Key (optional, read from environmental variables if not available)

    Returns:
        LLM instance
    """
    from tradingagents.llm_adapters.deepseek_adapter import ChatDeepSeek
    from tradingagents.llm_adapters.openai_compatible_base import create_openai_compatible_llm

    logger.info(f"🔧 [create LLM]{provider}, model={model}, url={backend_url}")
    logger.info(f"🔑 [API Key] Source:{'Database Configuration' if api_key else 'Environmental variables'}")

    if provider.lower() == "google":
        #Prefer imported API Key to read from environment variables
        google_api_key = api_key or os.getenv('GOOGLE_API_KEY')
        if not google_api_key:
            raise ValueError("使用Google需要设置GOOGLE_API_KEY环境变量或在数据库中配置API Key")

        #Pass the base url parameter to make the configuration of the manufacturer effective
        return ChatGoogleOpenAI(
            model=model,
            google_api_key=google_api_key,
            base_url=backend_url if backend_url else None,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout
        )

    elif provider.lower() == "dashscope":
        #Prefer imported API Key to read from environment variables
        dashscope_api_key = api_key or os.getenv('DASHSCOPE_API_KEY')

        #Pass the base url parameter to make the configuration of the manufacturer effective
        return ChatDashScopeOpenAI(
            model=model,
            api_key=dashscope_api_key,  #API Key
            base_url=backend_url if backend_url else None,  #Use custom URLs if available
            temperature=temperature,
            max_tokens=max_tokens,
            request_timeout=timeout
        )

    elif provider.lower() == "deepseek":
        #Prefer imported API Key to read from environment variables
        deepseek_api_key = api_key or os.getenv('DEEPSEEK_API_KEY')
        if not deepseek_api_key:
            raise ValueError("使用DeepSeek需要设置DEEPSEEK_API_KEY环境变量或在数据库中配置API Key")

        return ChatDeepSeek(
            model=model,
            api_key=deepseek_api_key,
            base_url=backend_url,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout
        )

    elif provider.lower() == "zhipu":
        #Intellectual AI processing
        zhipu_api_key = api_key or os.getenv('ZHIPU_API_KEY')
        if not zhipu_api_key:
            raise ValueError("使用智谱AI需要设置ZHIPU_API_KEY环境变量或在数据库中配置API Key")
        
        return create_openai_compatible_llm(
            provider="zhipu",
            model=model,
            api_key=zhipu_api_key,
            base_url=backend_url,  #Use user-providedback url
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout
        )

    elif provider.lower() in ["openai", "siliconflow", "openrouter", "ollama"]:
        #Prefer imported API Key to read from environment variables
        if not api_key:
            if provider.lower() == "siliconflow":
                api_key = os.getenv('SILICONFLOW_API_KEY')
            elif provider.lower() == "openrouter":
                api_key = os.getenv('OPENROUTER_API_KEY') or os.getenv('OPENAI_API_KEY')
            elif provider.lower() == "openai":
                api_key = os.getenv('OPENAI_API_KEY')

        return ChatOpenAI(
            model=model,
            base_url=backend_url,
            api_key=api_key,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout
        )

    elif provider.lower() == "anthropic":
        return ChatAnthropic(
            model=model,
            base_url=backend_url,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout
        )

    elif provider.lower() in ["qianfan", "custom_openai"]:
        return create_openai_compatible_llm(
            provider=provider,
            model=model,
            base_url=backend_url,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout
        )

    else:
        #Custom manufacturer: using OpenAI compatible mode
        logger.info(f"🔧 handles custom manufacturers using OpenAI compatible mode:{provider}")

        #Try to get API Key from environment variables (support multiple naming formats)
        api_key_candidates = [
            f"{provider.upper()}_API_KEY",  #For example: KYX API KEY
            f"{provider}_API_KEY",          #Example: kyx API KEY
            "CUSTOM_OPENAI_API_KEY"         #Common environmental variables
        ]

        custom_api_key = None
        for env_var in api_key_candidates:
            custom_api_key = os.getenv(env_var)
            if custom_api_key:
                logger.info(f"From the environment variable{env_var}Fetch API Key")
                break

        if not custom_api_key:
            logger.warning(f"No custom manufacturers found{provider}API Key, try default configuration")

        return ChatOpenAI(
            model=model,
            base_url=backend_url,
            api_key=custom_api_key,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout
        )


class TradingAgentsGraph:
    """Main class that orchestrates the trading agents framework."""

    def __init__(
        self,
        selected_analysts=["market", "social", "news", "fundamentals"],
        debug=False,
        config: Dict[str, Any] = None,
    ):
        """Initialize the trading agents graph and components.

        Args:
            selected_analysts: List of analyst types to include
            debug: Whether to run in debug mode
            config: Configuration dictionary. If None, uses default config
        """
        self.debug = debug
        self.config = config or DEFAULT_CONFIG

        # Update the interface's config
        set_config(self.config)

        # Create necessary directories
        os.makedirs(
            os.path.join(self.config["project_dir"], "dataflows/data_cache"),
            exist_ok=True,
        )

        # Initialize LLMs
        #🔧 Read model parameters from configuration (prefer user configuration, otherwise use default values)
        quick_config = self.config.get("quick_model_config", {})
        deep_config = self.config.get("deep_model_config", {})

        #Read fast model parameters
        quick_max_tokens = quick_config.get("max_tokens", 4000)
        quick_temperature = quick_config.get("temperature", 0.7)
        quick_timeout = quick_config.get("timeout", 180)

        #Read depth model parameters
        deep_max_tokens = deep_config.get("max_tokens", 4000)
        deep_temperature = deep_config.get("temperature", 0.7)
        deep_timeout = deep_config.get("timeout", 180)

        #Check if it's a hybrid mode.
        quick_provider = self.config.get("quick_provider")
        deep_provider = self.config.get("deep_provider")
        quick_backend_url = self.config.get("quick_backend_url")
        deep_backend_url = self.config.get("deep_backend_url")

        if quick_provider and deep_provider and quick_provider != deep_provider:
            #Mixing mode: Fast model and depth model from different manufacturers
            logger.info(f"[Mixed Mode]")
            logger.info(f"Quick model:{self.config['quick_think_llm']} ({quick_provider})")
            logger.info(f"Depth model:{self.config['deep_think_llm']} ({deep_provider})")

            #Create LLM instance using a unified function
            self.quick_thinking_llm = create_llm_by_provider(
                provider=quick_provider,
                model=self.config["quick_think_llm"],
                backend_url=quick_backend_url or self.config.get("backend_url", ""),
                temperature=quick_temperature,
                max_tokens=quick_max_tokens,
                timeout=quick_timeout,
                api_key=self.config.get("quick_api_key")  #API Key
            )

            self.deep_thinking_llm = create_llm_by_provider(
                provider=deep_provider,
                model=self.config["deep_think_llm"],
                backend_url=deep_backend_url or self.config.get("backend_url", ""),
                temperature=deep_temperature,
                max_tokens=deep_max_tokens,
                timeout=deep_timeout,
                api_key=self.config.get("deep_api_key")  #API Key
            )

            logger.info(f"✅ [Mixed Mode] LLM instance created successfully")

        elif self.config["llm_provider"].lower() == "openai":
            logger.info(f"[OpenAI-Quick Model] max tokens={quick_max_tokens}, temperature={quick_temperature}, timeout={quick_timeout}s")
            logger.info(f"[OpenAI-Depth Model]{deep_max_tokens}, temperature={deep_temperature}, timeout={deep_timeout}s")

            self.deep_thinking_llm = ChatOpenAI(
                model=self.config["deep_think_llm"],
                base_url=self.config["backend_url"],
                temperature=deep_temperature,
                max_tokens=deep_max_tokens,
                timeout=deep_timeout
            )
            self.quick_thinking_llm = ChatOpenAI(
                model=self.config["quick_think_llm"],
                base_url=self.config["backend_url"],
                temperature=quick_temperature,
                max_tokens=quick_max_tokens,
                timeout=quick_timeout
            )
        elif self.config["llm_provider"] == "siliconflow":
            #SiliconFlow support: using OpenAI compatible API
            siliconflow_api_key = os.getenv('SILICONFLOW_API_KEY')
            if not siliconflow_api_key:
                raise ValueError("使用SiliconFlow需要设置SILICONFLOW_API_KEY环境变量")

            logger.info(f"[SiliconFlow]{siliconflow_api_key[:20]}...")
            logger.info(f"[SiliconFlow-Quick Model] max tokens={quick_max_tokens}, temperature={quick_temperature}, timeout={quick_timeout}s")
            logger.info(f"[SiliconFlow-Depth Model] max tokens={deep_max_tokens}, temperature={deep_temperature}, timeout={deep_timeout}s")

            self.deep_thinking_llm = ChatOpenAI(
                model=self.config["deep_think_llm"],
                base_url=self.config["backend_url"],
                api_key=siliconflow_api_key,
                temperature=deep_temperature,
                max_tokens=deep_max_tokens,
                timeout=deep_timeout
            )
            self.quick_thinking_llm = ChatOpenAI(
                model=self.config["quick_think_llm"],
                base_url=self.config["backend_url"],
                api_key=siliconflow_api_key,
                temperature=quick_temperature,
                max_tokens=quick_max_tokens,
                timeout=quick_timeout
            )
        elif self.config["llm_provider"] == "openrouter":
            #OpenRouter Support: Give priority to OPENROUTER API KEY or to OPENAI API KEY
            openrouter_api_key = os.getenv('OPENROUTER_API_KEY') or os.getenv('OPENAI_API_KEY')
            if not openrouter_api_key:
                raise ValueError("使用OpenRouter需要设置OPENROUTER_API_KEY或OPENAI_API_KEY环境变量")

            logger.info(f"[OpenRouter] With the API key:{openrouter_api_key[:20]}...")
            logger.info(f"[OpenRouter -- Fast Model] max tokens={quick_max_tokens}, temperature={quick_temperature}, timeout={quick_timeout}s")
            logger.info(f"[OpenRouter-Deep Model]{deep_max_tokens}, temperature={deep_temperature}, timeout={deep_timeout}s")

            self.deep_thinking_llm = ChatOpenAI(
                model=self.config["deep_think_llm"],
                base_url=self.config["backend_url"],
                api_key=openrouter_api_key,
                temperature=deep_temperature,
                max_tokens=deep_max_tokens,
                timeout=deep_timeout
            )
            self.quick_thinking_llm = ChatOpenAI(
                model=self.config["quick_think_llm"],
                base_url=self.config["backend_url"],
                api_key=openrouter_api_key,
                temperature=quick_temperature,
                max_tokens=quick_max_tokens,
                timeout=quick_timeout
            )
        elif self.config["llm_provider"] == "ollama":
            logger.info(f"[Olama fast model] max tokens={quick_max_tokens}, temperature={quick_temperature}, timeout={quick_timeout}s")
            logger.info(f"[Olama-Deep Model]{deep_max_tokens}, temperature={deep_temperature}, timeout={deep_timeout}s")

            self.deep_thinking_llm = ChatOpenAI(
                model=self.config["deep_think_llm"],
                base_url=self.config["backend_url"],
                temperature=deep_temperature,
                max_tokens=deep_max_tokens,
                timeout=deep_timeout
            )
            self.quick_thinking_llm = ChatOpenAI(
                model=self.config["quick_think_llm"],
                base_url=self.config["backend_url"],
                temperature=quick_temperature,
                max_tokens=quick_max_tokens,
                timeout=quick_timeout
            )
        elif self.config["llm_provider"].lower() == "anthropic":
            logger.info(f"[Anthropic-fast model] max tokens={quick_max_tokens}, temperature={quick_temperature}, timeout={quick_timeout}s")
            logger.info(f"[Anthropic-Depth Model]{deep_max_tokens}, temperature={deep_temperature}, timeout={deep_timeout}s")

            self.deep_thinking_llm = ChatAnthropic(
                model=self.config["deep_think_llm"],
                base_url=self.config["backend_url"],
                temperature=deep_temperature,
                max_tokens=deep_max_tokens,
                timeout=deep_timeout
            )
            self.quick_thinking_llm = ChatAnthropic(
                model=self.config["quick_think_llm"],
                base_url=self.config["backend_url"],
                temperature=quick_temperature,
                max_tokens=quick_max_tokens,
                timeout=quick_timeout
            )
        elif self.config["llm_provider"].lower() == "google":
            #Use Google OpenAI Compatibility Compatibility to address the mismatch of tool call formats
            logger.info(f"🔧Use Google AI OpenAI Compatibility Compatibility (Resolve tool calls) I'm not sure.")

            #🔥 Prefer to API Key for database configuration, otherwise read from environmental variables
            google_api_key = self.config.get("quick_api_key") or self.config.get("deep_api_key") or os.getenv('GOOGLE_API_KEY')
            if not google_api_key:
                raise ValueError("使用Google AI需要在数据库中配置API Key或设置GOOGLE_API_KEY环境变量")

            logger.info(f"[Google AI] API Key source:{'Database Configuration' if self.config.get('quick_api_key') or self.config.get('deep_api_key') else 'Environmental variables'}")

            #🔧 Read model parameters from configuration (prefer user configuration, otherwise use default values)
            quick_config = self.config.get("quick_model_config", {})
            deep_config = self.config.get("deep_model_config", {})

            quick_max_tokens = quick_config.get("max_tokens", 4000)
            quick_temperature = quick_config.get("temperature", 0.7)
            quick_timeout = quick_config.get("timeout", 180)

            deep_max_tokens = deep_config.get("max_tokens", 4000)
            deep_temperature = deep_config.get("temperature", 0.7)
            deep_timeout = deep_config.get("timeout", 180)

            logger.info(f"[Google-Quick Model] max tokens={quick_max_tokens}, temperature={quick_temperature}, timeout={quick_timeout}s")
            logger.info(f"[Google-Deep Model] max tokens={deep_max_tokens}, temperature={deep_temperature}, timeout={deep_timeout}s")

            #Fetch back url (if any)
            backend_url = self.config.get("backend_url")
            if backend_url:
                logger.info(f"[Google AI] Use the configuration back url:{backend_url}")
            else:
                logger.info(f"[Google AI] Unconfigured backend url, with defaultend Points")

            self.deep_thinking_llm = ChatGoogleOpenAI(
                model=self.config["deep_think_llm"],
                google_api_key=google_api_key,
                base_url=backend_url if backend_url else None,
                temperature=deep_temperature,
                max_tokens=deep_max_tokens,
                timeout=deep_timeout
            )
            self.quick_thinking_llm = ChatGoogleOpenAI(
                model=self.config["quick_think_llm"],
                google_api_key=google_api_key,
                base_url=backend_url if backend_url else None,
                temperature=quick_temperature,
                max_tokens=quick_max_tokens,
                timeout=quick_timeout,
                transport="rest"
            )

            logger.info(f"✅ [Google AI] Optimized tool to call and content format and apply user configured model parameters")
        elif (self.config["llm_provider"].lower() == "dashscope" or
              self.config["llm_provider"].lower() == "alibaba" or
              "dashscope" in self.config["llm_provider"].lower() or
              "阿里百炼" in self.config["llm_provider"]):
            #Use OpenAI compatibility adapter to support original
            logger.info(f"🔧 with Aliblanc OpenAI Compatibility Compatibility (support for original tool call)")

            #🔥 Prefer to API Key for database configuration, otherwise read from environmental variables
            dashscope_api_key = self.config.get("quick_api_key") or self.config.get("deep_api_key") or os.getenv('DASHSCOPE_API_KEY')
            logger.info(f"API Key source:{'Database Configuration' if self.config.get('quick_api_key') or self.config.get('deep_api_key') else 'Environmental variables'}")

            #🔧 Read model parameters from configuration (prefer user configuration, otherwise use default values)
            quick_config = self.config.get("quick_model_config", {})
            deep_config = self.config.get("deep_model_config", {})

            #Read fast model parameters
            quick_max_tokens = quick_config.get("max_tokens", 4000)
            quick_temperature = quick_config.get("temperature", 0.7)
            quick_timeout = quick_config.get("timeout", 180)

            #Read depth model parameters
            deep_max_tokens = deep_config.get("max_tokens", 4000)
            deep_temperature = deep_config.get("temperature", 0.7)
            deep_timeout = deep_config.get("timeout", 180)

            logger.info(f"== sync, corrected by elderman =={quick_max_tokens}, temperature={quick_temperature}, timeout={quick_timeout}s")
            logger.info(f"== sync, corrected by elderman =={deep_max_tokens}, temperature={deep_temperature}, timeout={deep_timeout}s")

            #Fetch back url (if any)
            backend_url = self.config.get("backend_url")
            if backend_url:
                logger.info(f"Use custom API addresses:{backend_url}")

            #Detailed log: print all LLM initialization parameters
            logger.info("=" * 80)
            logger.info("🤖 [LLLM initialization] Ariball depth model parameters:")
            logger.info(f"   model: {self.config['deep_think_llm']}")
            logger.info(f"   api_key: {'Value' if dashscope_api_key else 'Empty'}(Long:{len(dashscope_api_key) if dashscope_api_key else 0})")
            logger.info(f"   base_url: {backend_url if backend_url else 'Default'}")
            logger.info(f"   temperature: {deep_temperature}")
            logger.info(f"   max_tokens: {deep_max_tokens}")
            logger.info(f"   request_timeout: {deep_timeout}")
            logger.info("=" * 80)

            self.deep_thinking_llm = ChatDashScopeOpenAI(
                model=self.config["deep_think_llm"],
                api_key=dashscope_api_key,  #API Key
                base_url=backend_url if backend_url else None,  #Pass
                temperature=deep_temperature,
                max_tokens=deep_max_tokens,
                request_timeout=deep_timeout
            )

            logger.info("=" * 80)
            logger.info("[LLM Initialization]")
            logger.info(f"   model: {self.config['quick_think_llm']}")
            logger.info(f"   api_key: {'Value' if dashscope_api_key else 'Empty'}(Long:{len(dashscope_api_key) if dashscope_api_key else 0})")
            logger.info(f"   base_url: {backend_url if backend_url else 'Default'}")
            logger.info(f"   temperature: {quick_temperature}")
            logger.info(f"   max_tokens: {quick_max_tokens}")
            logger.info(f"   request_timeout: {quick_timeout}")
            logger.info("=" * 80)

            self.quick_thinking_llm = ChatDashScopeOpenAI(
                model=self.config["quick_think_llm"],
                api_key=dashscope_api_key,  #API Key
                base_url=backend_url if backend_url else None,  #Pass
                temperature=quick_temperature,
                max_tokens=quick_max_tokens,
                request_timeout=quick_timeout
            )
            logger.info(f"✅ [Aliberian] used model parameters for user configuration")
        elif (self.config["llm_provider"].lower() == "deepseek" or
              "deepseek" in self.config["llm_provider"].lower()):
            #DeepSeek V3 Configuration - Use adapter for token statistics
            from tradingagents.llm_adapters.deepseek_adapter import ChatDeepSeek

            deepseek_api_key = os.getenv('DEEPSEEK_API_KEY')
            if not deepseek_api_key:
                raise ValueError("使用DeepSeek需要设置DEEPSEEK_API_KEY环境变量")

            deepseek_base_url = os.getenv('DEEPSEEK_BASE_URL', 'https://api.deepseek.com')

            #🔧 Read model parameters from configuration (prefer user configuration, otherwise use default values)
            quick_config = self.config.get("quick_model_config", {})
            deep_config = self.config.get("deep_model_config", {})

            #Read fast model parameters
            quick_max_tokens = quick_config.get("max_tokens", 4000)
            quick_temperature = quick_config.get("temperature", 0.7)
            quick_timeout = quick_config.get("timeout", 180)

            #Read depth model parameters
            deep_max_tokens = deep_config.get("max_tokens", 4000)
            deep_temperature = deep_config.get("temperature", 0.7)
            deep_timeout = deep_config.get("timeout", 180)

            logger.info(f"[DeepSeek-Quick Model] max tokens={quick_max_tokens}, temperature={quick_temperature}, timeout={quick_timeout}s")
            logger.info(f"[DeepSeek-Depth Model]{deep_max_tokens}, temperature={deep_temperature}, timeout={deep_timeout}s")

            #Use DeepSeek adapter to support token statistics
            self.deep_thinking_llm = ChatDeepSeek(
                model=self.config["deep_think_llm"],
                api_key=deepseek_api_key,
                base_url=deepseek_base_url,
                temperature=deep_temperature,
                max_tokens=deep_max_tokens,
                timeout=deep_timeout
            )
            self.quick_thinking_llm = ChatDeepSeek(
                model=self.config["quick_think_llm"],
                api_key=deepseek_api_key,
                base_url=deepseek_base_url,
                temperature=quick_temperature,
                max_tokens=quick_max_tokens,
                timeout=quick_timeout
            )

            logger.info(f"✅ [DeepSeek] enabled token statistical function and applied model parameters for user configuration")
        elif self.config["llm_provider"].lower() == "custom_openai":
            #Customise OpenAI Peer Configuration
            from tradingagents.llm_adapters.openai_compatible_base import create_openai_compatible_llm

            custom_api_key = os.getenv('CUSTOM_OPENAI_API_KEY')
            if not custom_api_key:
                raise ValueError("使用自定义OpenAI端点需要设置CUSTOM_OPENAI_API_KEY环境变量")

            custom_base_url = self.config.get("custom_openai_base_url", "https://api.openai.com/v1")

            #🔧 Read model parameters from configuration (prefer user configuration, otherwise use default values)
            quick_config = self.config.get("quick_model_config", {})
            deep_config = self.config.get("deep_model_config", {})

            quick_max_tokens = quick_config.get("max_tokens", 4000)
            quick_temperature = quick_config.get("temperature", 0.7)
            quick_timeout = quick_config.get("timeout", 180)

            deep_max_tokens = deep_config.get("max_tokens", 4000)
            deep_temperature = deep_config.get("temperature", 0.7)
            deep_timeout = deep_config.get("timeout", 180)

            logger.info(f"🔧 [Custom OpenAI] Use peer:{custom_base_url}")
            logger.info(f"🔧 [Caucasual OpenAI-Quick Model] max tokens={quick_max_tokens}, temperature={quick_temperature}, timeout={quick_timeout}s")
            logger.info(f"🔧 [Custom OpenAI-Depth Model] max tokens={deep_max_tokens}, temperature={deep_temperature}, timeout={deep_timeout}s")

            #Create example of LLM using OpenAI compatible adapter
            self.deep_thinking_llm = create_openai_compatible_llm(
                provider="custom_openai",
                model=self.config["deep_think_llm"],
                base_url=custom_base_url,
                temperature=deep_temperature,
                max_tokens=deep_max_tokens,
                timeout=deep_timeout
            )
            self.quick_thinking_llm = create_openai_compatible_llm(
                provider="custom_openai",
                model=self.config["quick_think_llm"],
                base_url=custom_base_url,
                temperature=quick_temperature,
                max_tokens=quick_max_tokens,
                timeout=quick_timeout
            )

            logger.info(f"✅ [Custom OpenAI] has configured custom endpoints and applied user configured model parameters")
        elif self.config["llm_provider"].lower() == "qianfan":
            #Qianfan API KEY
            from tradingagents.llm_adapters.openai_compatible_base import create_openai_compatible_llm

            #🔧 Read model parameters from configuration (prefer user configuration, otherwise use default values)
            quick_config = self.config.get("quick_model_config", {})
            deep_config = self.config.get("deep_model_config", {})

            quick_max_tokens = quick_config.get("max_tokens", 4000)
            quick_temperature = quick_config.get("temperature", 0.7)
            quick_timeout = quick_config.get("timeout", 180)

            deep_max_tokens = deep_config.get("max_tokens", 4000)
            deep_temperature = deep_config.get("temperature", 0.7)
            deep_timeout = deep_config.get("timeout", 180)

            logger.info(f"== sync, corrected by elderman =={quick_max_tokens}, temperature={quick_temperature}, timeout={quick_timeout}s")
            logger.info(f"== sync, corrected by elderman =={deep_max_tokens}, temperature={deep_temperature}, timeout={deep_timeout}s")

            #Create an example of LLM using OpenAI Compatibility Compatibility (the base class will use a thousand sail default base url and be responsible for key verification)
            self.deep_thinking_llm = create_openai_compatible_llm(
                provider="qianfan",
                model=self.config["deep_think_llm"],
                temperature=deep_temperature,
                max_tokens=deep_max_tokens,
                timeout=deep_timeout
            )
            self.quick_thinking_llm = create_openai_compatible_llm(
                provider="qianfan",
                model=self.config["quick_think_llm"],
                temperature=quick_temperature,
                max_tokens=quick_max_tokens,
                timeout=quick_timeout
            )
            logger.info("✅ [thousands of sails] Documentor adaptor configured successfully and applied model parameters configured by user")
        elif self.config["llm_provider"].lower() == "zhipu":
            #IGLM configuration - special ChatZhipuOpenAI adapter
            from tradingagents.llm_adapters.openai_compatible_base import ChatZhipuOpenAI
            
            #🔥 Prefer to API Key for database configuration, otherwise read from environmental variables
            zhipu_api_key = self.config.get("quick_api_key") or self.config.get("deep_api_key") or os.getenv('ZHIPU_API_KEY')
            logger.info(f"API Key source:{'Database Configuration' if self.config.get('quick_api_key') or self.config.get('deep_api_key') else 'Environmental variables'}")
            
            if not zhipu_api_key:
                raise ValueError("使用智谱AI需要在数据库中配置API Key或设置ZHIPU_API_KEY环境变量")
            
            #🔧 Read model parameters from configuration (prefer user configuration, otherwise use default values)
            quick_config = self.config.get("quick_model_config", {})
            deep_config = self.config.get("deep_model_config", {})
            
            quick_max_tokens = quick_config.get("max_tokens", 4000)
            quick_temperature = quick_config.get("temperature", 0.7)
            quick_timeout = quick_config.get("timeout", 180)
            
            deep_max_tokens = deep_config.get("max_tokens", 4000)
            deep_temperature = deep_config.get("temperature", 0.7)
            deep_timeout = deep_config.get("timeout", 180)
            
            logger.info(f"== sync, corrected by elderman =={quick_max_tokens}, temperature={quick_temperature}, timeout={quick_timeout}s")
            logger.info(f"== sync, corrected by elderman == @elder man{deep_max_tokens}, temperature={deep_temperature}, timeout={deep_timeout}s")
            
            #Fetch back url (if any)
            backend_url = self.config.get("backend_url")
            if backend_url:
                logger.info(f"[Intelligent AI] Use configured back url:{backend_url}")
            else:
                logger.info(f"🔧 [Swiss spectrum AI] not configured back url, with defaultend Points")
            
            #Create an example of LLM using a special ChatZhipuOpenAI adapter
            self.deep_thinking_llm = ChatZhipuOpenAI(
                model=self.config["deep_think_llm"],
                api_key=zhipu_api_key,
                base_url=backend_url,  #Use user-configured Backend url
                temperature=deep_temperature,
                max_tokens=deep_max_tokens,
                timeout=deep_timeout
            )
            self.quick_thinking_llm = ChatZhipuOpenAI(
                model=self.config["quick_think_llm"],
                api_key=zhipu_api_key,
                base_url=backend_url,  #Use user-configured Backend url
                temperature=quick_temperature,
                max_tokens=quick_max_tokens,
                timeout=quick_timeout
            )
            
            logger.info("✅ [Swiss spectrum AI] has successfully configured and applied model parameters for user configuration")
        else:
            #🔧 Generic OpenAI compatible plant support (for custom manufacturers)
            logger.info(f"🔧 Process custom manufacturers using universal OpenAI compatible adapters:{self.config['llm_provider']}")
            from tradingagents.llm_adapters.openai_compatible_base import create_openai_compatible_llm

            #Fetch the API Key and base url configuration
            provider_name = self.config['llm_provider']

            #Try to get API Key from environment variables (support multiple naming formats)
            api_key_candidates = [
                f"{provider_name.upper()}_API_KEY",  #For example: KYX API KEY
                f"{provider_name}_API_KEY",          #Example: kyx API KEY
                "CUSTOM_OPENAI_API_KEY"              #Common environmental variables
            ]

            custom_api_key = None
            for env_var in api_key_candidates:
                custom_api_key = os.getenv(env_var)
                if custom_api_key:
                    logger.info(f"From the environment variable{env_var}Fetch API Key")
                    break

            if not custom_api_key:
                raise ValueError(
                    f"使用自定义厂家 {provider_name} 需要设置以下环境变量之一:\n"
                    f"  - {provider_name.upper()}_API_KEY\n"
                    f"  - CUSTOM_OPENAI_API_KEY"
                )

            #Get backend url (from configuration)
            backend_url = self.config.get("backend_url")
            if not backend_url:
                raise ValueError(
                    f"使用自定义厂家 {provider_name} 需要在数据库配置中设置 default_base_url"
                )

            logger.info(f"[Customs]{provider_name}Use of endpoints:{backend_url}")

            #Read model parameters from configuration
            quick_config = self.config.get("quick_model_config", {})
            deep_config = self.config.get("deep_model_config", {})

            quick_max_tokens = quick_config.get("max_tokens", 4000)
            quick_temperature = quick_config.get("temperature", 0.7)
            quick_timeout = quick_config.get("timeout", 180)

            deep_max_tokens = deep_config.get("max_tokens", 4000)
            deep_temperature = deep_config.get("temperature", 0.7)
            deep_timeout = deep_config.get("timeout", 180)

            logger.info(f"🔧 [{provider_name}- Quick model.{quick_max_tokens}, temperature={quick_temperature}, timeout={quick_timeout}s")
            logger.info(f"🔧 [{provider_name}- Depth model.{deep_max_tokens}, temperature={deep_temperature}, timeout={deep_timeout}s")

            #Create an example of LLM using a custom openai adapter
            self.deep_thinking_llm = create_openai_compatible_llm(
                provider="custom_openai",
                model=self.config["deep_think_llm"],
                api_key=custom_api_key,
                base_url=backend_url,
                temperature=deep_temperature,
                max_tokens=deep_max_tokens,
                timeout=deep_timeout
            )
            self.quick_thinking_llm = create_openai_compatible_llm(
                provider="custom_openai",
                model=self.config["quick_think_llm"],
                api_key=custom_api_key,
                base_url=backend_url,
                temperature=quick_temperature,
                max_tokens=quick_max_tokens,
                timeout=quick_timeout
            )

            logger.info(f"[Customs]{provider_name}:: Model parameters for custom endpoints and user configurations are configured")
        
        self.toolkit = Toolkit(config=self.config)

        #(if enabled)
        memory_enabled = self.config.get("memory_enabled", True)
        if memory_enabled:
            #Use a single ChromaDB manager to avoid and create conflicts
            self.bull_memory = FinancialSituationMemory("bull_memory", self.config)
            self.bear_memory = FinancialSituationMemory("bear_memory", self.config)
            self.trader_memory = FinancialSituationMemory("trader_memory", self.config)
            self.invest_judge_memory = FinancialSituationMemory("invest_judge_memory", self.config)
            self.risk_manager_memory = FinancialSituationMemory("risk_manager_memory", self.config)
        else:
            #Create an empty memory object
            self.bull_memory = None
            self.bear_memory = None
            self.trader_memory = None
            self.invest_judge_memory = None
            self.risk_manager_memory = None

        # Create tool nodes
        self.tool_nodes = self._create_tool_nodes()

        # Initialize components
        #🔥 [Rehabilitation] Read the debate cycle parameters from the configuration
        self.conditional_logic = ConditionalLogic(
            max_debate_rounds=self.config.get("max_debate_rounds", 1),
            max_risk_discuss_rounds=self.config.get("max_risk_discuss_rounds", 1)
        )
        logger.info(f"[Conditionsal Logic]")
        logger.info(f"   - max_debate_rounds: {self.conditional_logic.max_debate_rounds}")
        logger.info(f"   - max_risk_discuss_rounds: {self.conditional_logic.max_risk_discuss_rounds}")

        self.graph_setup = GraphSetup(
            self.quick_thinking_llm,
            self.deep_thinking_llm,
            self.toolkit,
            self.tool_nodes,
            self.bull_memory,
            self.bear_memory,
            self.trader_memory,
            self.invest_judge_memory,
            self.risk_manager_memory,
            self.conditional_logic,
            self.config,
            getattr(self, 'react_llm', None),
        )

        self.propagator = Propagator()
        self.reflector = Reflector(self.quick_thinking_llm)
        self.signal_processor = SignalProcessor(self.quick_thinking_llm)

        # State tracking
        self.curr_state = None
        self.ticker = None
        self.log_states_dict = {}  # date to full state dict

        # Set up the graph
        self.graph = self.graph_setup.setup_graph(selected_analysts)

    def _create_tool_nodes(self) -> Dict[str, ToolNode]:
        """Create zerodes for different data sources.

        Note: ToolNode contains all possible tools, but LLM will only call on its binding tools.
        ToolNode's role is to execute the tool calls generated by LLM, not to limit which tools LLM can call.
        """
        return {
            "market": ToolNode(
                [
                    #Harmonization tools (recommended)
                    self.toolkit.get_stock_market_data_unified,
                    #Online tools (stand-by)
                    self.toolkit.get_YFin_data_online,
                    self.toolkit.get_stockstats_indicators_report_online,
                    #Offline tool (stand-by)
                    self.toolkit.get_YFin_data,
                    self.toolkit.get_stockstats_indicators_report,
                ]
            ),
            "social": ToolNode(
                [
                    #Harmonization tools (recommended)
                    self.toolkit.get_stock_sentiment_unified,
                    #Online tools (stand-by)
                    self.toolkit.get_stock_news_openai,
                    #Offline tool (stand-by)
                    self.toolkit.get_reddit_stock_info,
                ]
            ),
            "news": ToolNode(
                [
                    #Harmonization tools (recommended)
                    self.toolkit.get_stock_news_unified,
                    #Online tools (stand-by)
                    self.toolkit.get_global_news_openai,
                    self.toolkit.get_google_news,
                    #Offline tool (stand-by)
                    self.toolkit.get_finnhub_news,
                    self.toolkit.get_reddit_news,
                ]
            ),
            "fundamentals": ToolNode(
                [
                    #Harmonization tools (recommended)
                    self.toolkit.get_stock_fundamentals_unified,
                    #Offline tool (stand-by)
                    self.toolkit.get_finnhub_company_insider_sentiment,
                    self.toolkit.get_finnhub_company_insider_transactions,
                    self.toolkit.get_simfin_balance_sheet,
                    self.toolkit.get_simfin_cashflow,
                    self.toolkit.get_simfin_income_stmt,
                    #Market instruments in China (back-up)
                    self.toolkit.get_china_stock_data,
                    self.toolkit.get_china_fundamentals,
                ]
            ),
        }

    def propagate(self, company_name, trade_date, progress_callback=None, task_id=None):
        """Run the trading agents graph for a company on a specific date.

        Args:
            company_name: Company name or stock symbol
            trade_date: Date for analysis
            progress_callback: Optional callback function for progress updates
            task_id: Optional task ID for tracking performance data
        """

        #Add detailed reception log
        logger.debug(f"== sync, corrected by elderman == @elder man")
        logger.debug(f"[GRAPH DEBUG]{company_name}' (type:{type(company_name)})")
        logger.debug(f"[GRAPH DEBUG]{trade_date}' (type:{type(trade_date)})")
        logger.debug(f"[GRAPH DEBUG]{task_id}'")

        self.ticker = company_name
        logger.debug(f"[GRAPH DEBUG] Sets self.ticker: '{self.ticker}'")

        # Initialize state
        logger.debug(f"🔍 [GRAPH DEBUG] Creates initial state, transport parameters: company name='{company_name}', trade_date='{trade_date}'")
        init_agent_state = self.propagator.create_initial_state(
            company_name, trade_date
        )
        logger.debug(f"[GRAPH DEBUG] Commany of interest: '{init_agent_state.get('company_of_interest', 'NOT_FOUND')}'")
        logger.debug(f"[GRAPH DEBUG]{init_agent_state.get('trade_date', 'NOT_FOUND')}'")

        #Initializing Timer
        node_timings = {}  #Record the execution time for each node
        total_start_time = time.time()  #Overall start time
        current_node_start = None  #Current node start time
        current_node_name = None  #Current Node Name

        #Save tax id for subsequent preservation of performance data
        self._current_task_id = task_id

        #Choose a different sstream mode depending on whether there is progress
        args = self.propagator.get_graph_args(use_progress_callback=bool(progress_callback))

        if self.debug:
            # Debug mode with tracing and progress updates
            trace = []
            final_state = None
            for chunk in self.graph.stream(init_agent_state, **args):
                #Record Node Timing
                for node_name in chunk.keys():
                    if not node_name.startswith('__'):
                        #If you have the last node, record the end of it.
                        if current_node_name and current_node_start:
                            elapsed = time.time() - current_node_start
                            node_timings[current_node_name] = elapsed
                            logger.info(f"⏱️ [{current_node_name}Time-consuming:{elapsed:.2f}sec")

                        #Start new node timer
                        current_node_name = node_name
                        current_node_start = time.time()
                        break

                #In updates mode, chunk format is   FT 0 
                #In Values mode, chunk format is full state
                if progress_callback and args.get("stream_mode") == "updates":
                    #FMT 0}
                    self._send_progress_update(chunk, progress_callback)
                    #Cumulative status update
                    if final_state is None:
                        final_state = init_agent_state.copy()
                    for node_name, node_update in chunk.items():
                        if not node_name.startswith('__'):
                            final_state.update(node_update)
                else:
                    #Values mode: chunk = FMT 0 
                    if len(chunk.get("messages", [])) > 0:
                        chunk["messages"][-1].pretty_print()
                    trace.append(chunk)
                    final_state = chunk

            if not trace and final_state:
                #Use cumulative status in updates mode
                pass
            elif trace:
                final_state = trace[-1]
        else:
            # Standard mode without tracing but with progress updates
            if progress_callback:
                #Use updates mode to get progress at node level
                trace = []
                final_state = None
                for chunk in self.graph.stream(init_agent_state, **args):
                    #Record Node Timing
                    for node_name in chunk.keys():
                        if not node_name.startswith('__'):
                            #If you have the last node, record the end of it.
                            if current_node_name and current_node_start:
                                elapsed = time.time() - current_node_start
                                node_timings[current_node_name] = elapsed
                                logger.info(f"⏱️ [{current_node_name}Time-consuming:{elapsed:.2f}sec")
                                logger.info(f"[TIMING] Switch:{current_node_name} → {node_name}")

                            #Start new node timer
                            current_node_name = node_name
                            current_node_start = time.time()
                            logger.info(f"[Timing]{node_name}")
                            break

                    self._send_progress_update(chunk, progress_callback)
                    #Cumulative status update
                    if final_state is None:
                        final_state = init_agent_state.copy()
                    for node_name, node_update in chunk.items():
                        if not node_name.startswith('__'):
                            final_state.update(node_update)
            else:
                #The old invoke mode.
                logger.info("⏱️Perform analysis using invoke mode (no progress echo)")
                #Use sstream mode for timing, but do not send progress updates
                trace = []
                final_state = None
                for chunk in self.graph.stream(init_agent_state, **args):
                    #Record Node Timing
                    for node_name in chunk.keys():
                        if not node_name.startswith('__'):
                            #If you have the last node, record the end of it.
                            if current_node_name and current_node_start:
                                elapsed = time.time() - current_node_start
                                node_timings[current_node_name] = elapsed
                                logger.info(f"⏱️ [{current_node_name}Time-consuming:{elapsed:.2f}sec")

                            #Start new node timer
                            current_node_name = node_name
                            current_node_start = time.time()
                            break

                    #Cumulative status update
                    if final_state is None:
                        final_state = init_agent_state.copy()
                    for node_name, node_update in chunk.items():
                        if not node_name.startswith('__'):
                            final_state.update(node_update)

        #Record the last node
        if current_node_name and current_node_start:
            elapsed = time.time() - current_node_start
            node_timings[current_node_name] = elapsed
            logger.info(f"⏱️ [{current_node_name}Time-consuming:{elapsed:.2f}sec")

        #Calculate total time
        total_elapsed = time.time() - total_start_time

        #Debug Log
        logger.info(f"Number of timed nodes:{len(node_timings)}")
        logger.info(f"[TIMING DEBUG]{total_elapsed:.2f}sec")
        logger.info(f"List of nodes:{list(node_timings.keys())}")

        #Print detailed time statistics
        logger.info("[TIMING DEBUG]")
        self._print_timing_summary(node_timings, total_elapsed)
        logger.info("[TIMING DEBUG]  print timing summary call complete")

        #Build Performance Data
        performance_data = self._build_performance_data(node_timings, total_elapsed)

        #Add Performance Data to Status
        final_state['performance_metrics'] = performance_data

        # Store current state for reflection
        self.curr_state = final_state

        # Log state
        self._log_state(trade_date, final_state)

        #Get Model Information
        model_info = ""
        try:
            if hasattr(self.deep_thinking_llm, 'model_name'):
                model_info = f"{self.deep_thinking_llm.__class__.__name__}:{self.deep_thinking_llm.model_name}"
            else:
                model_info = self.deep_thinking_llm.__class__.__name__
        except Exception:
            model_info = "Unknown"

        #Process decision-making and add model information
        decision = self.process_signal(final_state["final_trade_decision"], company_name)
        decision['model_info'] = model_info

        # Return decision and processed signal
        return final_state, decision

    def _send_progress_update(self, chunk, progress_callback):
        """Send progress update to a return function

        chunk format returned by LangGraph stread:  FMT 0}
        Example of node name:
        "Market Analyst," "Fundamentals Analyst", "News Analyst", "Social Analyst."
        - "tools market", "tools fundamentals", "tools news", "tools social"
        - "Msg Clear Market," "Msg Clear Fundamentals", etc.
        - "Bull Researcher," "Bear Researcher," "Research Manager."
        - "Trader."
        - "Risky Analyst," "Safe Analyst", "Neutral Analyst", "Risk Judge"
        """
        try:
            #Can not open message
            if not isinstance(chunk, dict):
                return

            #Get the first non-special key as node First Name
            node_name = None
            for key in chunk.keys():
                if not key.startswith('__'):
                    node_name = key
                    break

            if not node_name:
                return

            logger.info(f"[Progress] Node name:{node_name}")

            #Check for end node
            if '__end__' in chunk:
                logger.info(f"[Progress]")
                progress_callback("📊 生成报告")
                return

            #Node name map (matching LangGraph actual node name)
            node_mapping = {
                #Analyst Node
                'Market Analyst': "📊 市场分析师",
                'Fundamentals Analyst': "💼 基本面分析师",
                'News Analyst': "📰 新闻分析师",
                'Social Analyst': "💬 社交媒体分析师",
                #Tool nodes (no progress updates sent, avoid duplication)
                'tools_market': None,
                'tools_fundamentals': None,
                'tools_news': None,
                'tools_social': None,
                #Message Cleanup Node (no progress update sent)
                'Msg Clear Market': None,
                'Msg Clear Fundamentals': None,
                'Msg Clear News': None,
                'Msg Clear Social': None,
                #Researcher Node
                'Bull Researcher': "🐂 看涨研究员",
                'Bear Researcher': "🐻 看跌研究员",
                'Research Manager': "👔 研究经理",
                #Trader Node
                'Trader': "💼 交易员决策",
                #Risk assessment nodes
                'Risky Analyst': "🔥 激进风险评估",
                'Safe Analyst': "🛡️ 保守风险评估",
                'Neutral Analyst': "⚖️ 中性风险评估",
                'Risk Judge': "🎯 风险经理",
            }

            #Find Map Messages
            message = node_mapping.get(node_name)

            if message is None:
                #None means Skip (tool node, message clean node)
                logger.debug(f"[Progress] Skipping nodes:{node_name}")
                return

            if message:
                #Send progress update
                logger.info(f"[Progress]{message}")
                progress_callback(message)
            else:
                #Unknown node, use node name
                logger.warning(f"[Progress] Unknown node:{node_name}")
                progress_callback(f"🔍 {node_name}")

        except Exception as e:
            logger.error(f"Progress update failed:{e}", exc_info=True)

    def _build_performance_data(self, node_timings: Dict[str, float], total_elapsed: float) -> Dict[str, Any]:
        """Build performance data structure

        Args:
            Node timings: Time dictionary per node
            Total elapsed: total execution time

        Returns:
            Performance data dictionary
        """
        #Node classification (note: risk management nodes are preceded by analyst nodes because they also contain 'Anallyst')
        analyst_nodes = {}
        tool_nodes = {}
        msg_clear_nodes = {}
        research_nodes = {}
        trader_nodes = {}
        risk_nodes = {}
        other_nodes = {}

        for node_name, elapsed in node_timings.items():
            #Priority matching of risk management teams (because they also include 'Analyst')
            if 'Risky' in node_name or 'Safe' in node_name or 'Neutral' in node_name or 'Risk Judge' in node_name:
                risk_nodes[node_name] = elapsed
            #And match the team of analysts.
            elif 'Analyst' in node_name:
                analyst_nodes[node_name] = elapsed
            #Tool Nodes
            elif node_name.startswith('tools_'):
                tool_nodes[node_name] = elapsed
            #Message Cleanup Node
            elif node_name.startswith('Msg Clear'):
                msg_clear_nodes[node_name] = elapsed
            #Research team
            elif 'Researcher' in node_name or 'Research Manager' in node_name:
                research_nodes[node_name] = elapsed
            #Trading team
            elif 'Trader' in node_name:
                trader_nodes[node_name] = elapsed
            #Other Nodes
            else:
                other_nodes[node_name] = elapsed

        #Compute statistics
        slowest_node = max(node_timings.items(), key=lambda x: x[1]) if node_timings else (None, 0)
        fastest_node = min(node_timings.items(), key=lambda x: x[1]) if node_timings else (None, 0)
        avg_time = sum(node_timings.values()) / len(node_timings) if node_timings else 0

        return {
            "total_time": round(total_elapsed, 2),
            "total_time_minutes": round(total_elapsed / 60, 2),
            "node_count": len(node_timings),
            "average_node_time": round(avg_time, 2),
            "slowest_node": {
                "name": slowest_node[0],
                "time": round(slowest_node[1], 2)
            } if slowest_node[0] else None,
            "fastest_node": {
                "name": fastest_node[0],
                "time": round(fastest_node[1], 2)
            } if fastest_node[0] else None,
            "node_timings": {k: round(v, 2) for k, v in node_timings.items()},
            "category_timings": {
                "analyst_team": {
                    "nodes": {k: round(v, 2) for k, v in analyst_nodes.items()},
                    "total": round(sum(analyst_nodes.values()), 2),
                    "percentage": round(sum(analyst_nodes.values()) / total_elapsed * 100, 1) if total_elapsed > 0 else 0
                },
                "tool_calls": {
                    "nodes": {k: round(v, 2) for k, v in tool_nodes.items()},
                    "total": round(sum(tool_nodes.values()), 2),
                    "percentage": round(sum(tool_nodes.values()) / total_elapsed * 100, 1) if total_elapsed > 0 else 0
                },
                "message_clearing": {
                    "nodes": {k: round(v, 2) for k, v in msg_clear_nodes.items()},
                    "total": round(sum(msg_clear_nodes.values()), 2),
                    "percentage": round(sum(msg_clear_nodes.values()) / total_elapsed * 100, 1) if total_elapsed > 0 else 0
                },
                "research_team": {
                    "nodes": {k: round(v, 2) for k, v in research_nodes.items()},
                    "total": round(sum(research_nodes.values()), 2),
                    "percentage": round(sum(research_nodes.values()) / total_elapsed * 100, 1) if total_elapsed > 0 else 0
                },
                "trader_team": {
                    "nodes": {k: round(v, 2) for k, v in trader_nodes.items()},
                    "total": round(sum(trader_nodes.values()), 2),
                    "percentage": round(sum(trader_nodes.values()) / total_elapsed * 100, 1) if total_elapsed > 0 else 0
                },
                "risk_management_team": {
                    "nodes": {k: round(v, 2) for k, v in risk_nodes.items()},
                    "total": round(sum(risk_nodes.values()), 2),
                    "percentage": round(sum(risk_nodes.values()) / total_elapsed * 100, 1) if total_elapsed > 0 else 0
                },
                "other": {
                    "nodes": {k: round(v, 2) for k, v in other_nodes.items()},
                    "total": round(sum(other_nodes.values()), 2),
                    "percentage": round(sum(other_nodes.values()) / total_elapsed * 100, 1) if total_elapsed > 0 else 0
                }
            },
            "llm_config": {
                "provider": self.config.get('llm_provider', 'unknown'),
                "deep_think_model": self.config.get('deep_think_llm', 'unknown'),
                "quick_think_model": self.config.get('quick_think_llm', 'unknown')
            }
        }

    def _print_timing_summary(self, node_timings: Dict[str, float], total_elapsed: float):
        """Print detailed statistical time reports

        Args:
            Node timings: Time dictionary per node
            Total elapsed: total execution time
        """
        logger.info("[ print timing summary]")
        logger.info("Node timings:" + str(len(node_timings)))
        logger.info("🔍 [_print_timing_summary] total_elapsed: " + str(total_elapsed))

        logger.info("=" * 80)
        logger.info("Analytical statistical report")
        logger.info("=" * 80)

        #Node classification (note: risk management nodes are preceded by analyst nodes because they also contain 'Anallyst')
        analyst_nodes = []
        tool_nodes = []
        msg_clear_nodes = []
        research_nodes = []
        trader_nodes = []
        risk_nodes = []
        other_nodes = []

        for node_name, elapsed in node_timings.items():
            #Priority matching of risk management teams (because they also include 'Analyst')
            if 'Risky' in node_name or 'Safe' in node_name or 'Neutral' in node_name or 'Risk Judge' in node_name:
                risk_nodes.append((node_name, elapsed))
            #And match the team of analysts.
            elif 'Analyst' in node_name:
                analyst_nodes.append((node_name, elapsed))
            #Tool Nodes
            elif node_name.startswith('tools_'):
                tool_nodes.append((node_name, elapsed))
            #Message Cleanup Node
            elif node_name.startswith('Msg Clear'):
                msg_clear_nodes.append((node_name, elapsed))
            #Research team
            elif 'Researcher' in node_name or 'Research Manager' in node_name:
                research_nodes.append((node_name, elapsed))
            #Trading team
            elif 'Trader' in node_name:
                trader_nodes.append((node_name, elapsed))
            #Other Nodes
            else:
                other_nodes.append((node_name, elapsed))

        #Print Classification Statistics
        def print_category(title: str, nodes: List[Tuple[str, float]]):
            if not nodes:
                return
            logger.info(f"\n📊 {title}")
            logger.info("-" * 80)
            total_category_time = sum(t for _, t in nodes)
            for node_name, elapsed in sorted(nodes, key=lambda x: x[1], reverse=True):
                percentage = (elapsed / total_elapsed * 100) if total_elapsed > 0 else 0
                logger.info(f"  • {node_name:40s} {elapsed:8.2f}sec ({percentage:5.1f}%)")
            logger.info(f"  {'Subtotal':40s} {total_category_time:8.2f}sec ({total_category_time/total_elapsed*100:5.1f}%)")

        print_category("分析师团队", analyst_nodes)
        print_category("工具调用", tool_nodes)
        print_category("消息清理", msg_clear_nodes)
        print_category("研究团队", research_nodes)
        print_category("交易团队", trader_nodes)
        print_category("风险管理团队", risk_nodes)
        print_category("其他节点", other_nodes)

        #Print General Statistics
        logger.info("\n" + "=" * 80)
        logger.info(f"Total time of execution:{total_elapsed:.2f}sec ({total_elapsed/60:.2f}minutes)")
        logger.info(f"Total number of nodes 📈:{len(node_timings)}")
        if node_timings:
            avg_time = sum(node_timings.values()) / len(node_timings)
            logger.info(f"The average node is time-consuming:{avg_time:.2f}sec")
            slowest_node = max(node_timings.items(), key=lambda x: x[1])
            logger.info(f"The slowest point:{slowest_node[0]} ({slowest_node[1]:.2f}sec)")
            fastest_node = min(node_timings.items(), key=lambda x: x[1])
            logger.info(f"Best point:{fastest_node[0]} ({fastest_node[1]:.2f}sec)")

        #Print LLM configuration information
        logger.info(f"LLM configuration:")
        logger.info(f"• Provider:{self.config.get('llm_provider', 'unknown')}")
        logger.info(f"• Deep thinking models:{self.config.get('deep_think_llm', 'unknown')}")
        logger.info(f"• Rapid thinking models:{self.config.get('quick_think_llm', 'unknown')}")
        logger.info("=" * 80)

    def _log_state(self, trade_date, final_state):
        """Log the final state to a JSON file."""
        self.log_states_dict[str(trade_date)] = {
            "company_of_interest": final_state["company_of_interest"],
            "trade_date": final_state["trade_date"],
            "market_report": final_state["market_report"],
            "sentiment_report": final_state["sentiment_report"],
            "news_report": final_state["news_report"],
            "fundamentals_report": final_state["fundamentals_report"],
            "investment_debate_state": {
                "bull_history": final_state["investment_debate_state"]["bull_history"],
                "bear_history": final_state["investment_debate_state"]["bear_history"],
                "history": final_state["investment_debate_state"]["history"],
                "current_response": final_state["investment_debate_state"][
                    "current_response"
                ],
                "judge_decision": final_state["investment_debate_state"][
                    "judge_decision"
                ],
            },
            "trader_investment_decision": final_state["trader_investment_plan"],
            "risk_debate_state": {
                "risky_history": final_state["risk_debate_state"]["risky_history"],
                "safe_history": final_state["risk_debate_state"]["safe_history"],
                "neutral_history": final_state["risk_debate_state"]["neutral_history"],
                "history": final_state["risk_debate_state"]["history"],
                "judge_decision": final_state["risk_debate_state"]["judge_decision"],
            },
            "investment_plan": final_state["investment_plan"],
            "final_trade_decision": final_state["final_trade_decision"],
        }

        # Save to file
        directory = Path(f"eval_results/{self.ticker}/TradingAgentsStrategy_logs/")
        directory.mkdir(parents=True, exist_ok=True)

        with open(
            f"eval_results/{self.ticker}/TradingAgentsStrategy_logs/full_states_log.json",
            "w",
        ) as f:
            json.dump(self.log_states_dict, f, indent=4)

    def reflect_and_remember(self, returns_losses):
        """Reflect on decisions and update memory based on returns."""
        self.reflector.reflect_bull_researcher(
            self.curr_state, returns_losses, self.bull_memory
        )
        self.reflector.reflect_bear_researcher(
            self.curr_state, returns_losses, self.bear_memory
        )
        self.reflector.reflect_trader(
            self.curr_state, returns_losses, self.trader_memory
        )
        self.reflector.reflect_invest_judge(
            self.curr_state, returns_losses, self.invest_judge_memory
        )
        self.reflector.reflect_risk_manager(
            self.curr_state, returns_losses, self.risk_manager_memory
        )

    def process_signal(self, full_signal, stock_symbol=None):
        """Process a signal to extract the core decision."""
        return self.signal_processor.process_signal(full_signal, stock_symbol)
