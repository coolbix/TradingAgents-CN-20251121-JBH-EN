"""DeepSeek LLM adapter for Token
"""

import os
import time
from typing import Any, Dict, List, Optional, Union
from langchain_core.messages import BaseMessage, AIMessage, HumanMessage, SystemMessage
from langchain_core.outputs import ChatGeneration, ChatResult
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


class ChatDeepSeek(ChatOpenAI):
    """DeepSeek chat model adapter to support Token use of statistics

    Inherited from ChatOpenAI, added Token Usage Statistics function
    """
    
    def __init__(
        self,
        model: str = "deepseek-chat",
        api_key: Optional[str] = None,
        base_url: str = "https://api.deepseek.com",
        temperature: float = 0.1,
        max_tokens: Optional[int] = None,
        **kwargs
    ):
        """Initialize DeepSeek adapter

        Args:
            Model: Model Name, default is Deepseek-chat
            api key: API key, if not available, from the environmental variable DEPESEK API KEY
            base url: API baseURL
            temperature: temperature parameters
            max tokens: Max tokens
            **kwargs: Other parameters
        """
        
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
            env_api_key = os.getenv("DEEPSEEK_API_KEY")

            #Verify the validity of API Key in the environment variable (exclude placeholder)
            if env_api_key and is_valid_api_key(env_api_key):
                api_key = env_api_key
                logger.info("[DeepSeek Initialization]")
            elif env_api_key:
                logger.warning("API Key (possibly placeholder) in the environment variable is invalid and will be ignored")
                api_key = None
            else:
                api_key = None

            if not api_key:
                raise ValueError(
                    "DeepSeek API密钥未找到。请在 Web 界面配置 API Key "
                    "(设置 -> 大模型厂家) 或设置 DEEPSEEK_API_KEY 环境变量。"
                )
        
        #Initialised Parent
        super().__init__(
            model=model,
            openai_api_key=api_key,
            openai_api_base=base_url,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs
        )
        
        self.model_name = model
        
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

        #Ripping and removing custom parameters to avoid passing to Parent
        session_id = kwargs.pop('session_id', None)
        analysis_type = kwargs.pop('analysis_type', None)

        try:
            #Call parent to generate response
            result = super()._generate(messages, stop, run_manager, **kwargs)
            
            #Extract token usage
            input_tokens = 0
            output_tokens = 0
            
            #Try to extract token usage from the response
            if hasattr(result, 'llm_output') and result.llm_output:
                token_usage = result.llm_output.get('token_usage', {})
                if token_usage:
                    input_tokens = token_usage.get('prompt_tokens', 0)
                    output_tokens = token_usage.get('completion_tokens', 0)
            
            #If token usage is not obtained, estimate
            if input_tokens == 0 and output_tokens == 0:
                input_tokens = self._estimate_input_tokens(messages)
                output_tokens = self._estimate_output_tokens(result)
                logger.debug(f"[DeepSeek] Use estimation token: Input={input_tokens},out ={output_tokens}")
            else:
                logger.info(f"[DeepSeek] Actual token usage: Input={input_tokens},out ={output_tokens}")
            
            #Record token usage
            if TOKEN_TRACKING_ENABLED and (input_tokens > 0 or output_tokens > 0):
                try:
                    #Use extracted parameters or create default values
                    if session_id is None:
                        session_id = f"deepseek_{hash(str(messages))%10000}"
                    if analysis_type is None:
                        analysis_type = 'stock_analysis'

                    #Record usage
                    usage_record = token_tracker.track_usage(
                        provider="deepseek",
                        model_name=self.model_name,
                        input_tokens=input_tokens,
                        output_tokens=output_tokens,
                        session_id=session_id,
                        analysis_type=analysis_type
                    )

                    if usage_record:
                        if usage_record.cost == 0.0:
                            logger.warning(f"[DeepSeek] Costed at 0, possible configuration problem")
                        else:
                            logger.info(f"[DeepSeek]{usage_record.cost:.6f}")

                        #Token recording method using the Unified Log Manager
                        logger_manager = get_logger_manager()
                        logger_manager.log_token_usage(
                            logger, "deepseek", self.model_name,
                            input_tokens, output_tokens, usage_record.cost,
                            session_id
                        )
                    else:
                        logger.warning(f"[DeepSeek]")

                except Exception as track_error:
                    logger.error(f"[DeepSeek] Token statistical failure:{track_error}", exc_info=True)
            
            return result
            
        except Exception as e:
            logger.error(f"[DeepSeek] Call failed:{e}", exc_info=True)
            raise
    
    def _estimate_input_tokens(self, messages: List[BaseMessage]) -> int:
        """Estimating number of inputs tokens

        Args:
            Messages: Enter Message List

        Returns:
            Estimated number of inputs tokens
        """
        total_chars = 0
        for message in messages:
            if hasattr(message, 'content'):
                total_chars += len(str(message.content))
        
        #Crude estimate: approximately 1.5 characters/token in Chinese, approximately 4 characters/token in English
        #Use conservative estimate: 2 characters/token
        estimated_tokens = max(1, total_chars // 2)
        return estimated_tokens
    
    def _estimate_output_tokens(self, result: ChatResult) -> int:
        """Estimated number of outputs tokens

        Args:
            result: chat results

        Returns:
            Estimated number of outputs tokens
        """
        total_chars = 0
        for generation in result.generations:
            if hasattr(generation, 'message') and hasattr(generation.message, 'content'):
                total_chars += len(str(generation.message.content))
        
        #Crude estimate: 2 characters/token
        estimated_tokens = max(1, total_chars // 2)
        return estimated_tokens
    
    def invoke(
        self,
        input: Union[str, List[BaseMessage]],
        config: Optional[Dict] = None,
        **kwargs: Any,
    ) -> AIMessage:
        """Call model to generate response

        Args:
            input: input messages
            config: Configure parameters
            **kwargs: other parameters (including session id and anallysis  type)

        Returns:
            AI Message Response
        """
        
        #Process Inputs
        if isinstance(input, str):
            messages = [HumanMessage(content=input)]
        else:
            messages = input
        
        #Call method
        result = self._generate(messages, **kwargs)
        
        #Returns first generated message
        if result.generations:
            return result.generations[0].message
        else:
            return AIMessage(content="")


def create_deepseek_llm(
    model: str = "deepseek-chat",
    temperature: float = 0.1,
    max_tokens: Optional[int] = None,
    **kwargs
) -> ChatDeepSeek:
    """A convenient function to create the DeepSeek LLM instance

    Args:
        Model name
        temperature: temperature parameters
        max tokens: Max tokens
        **kwargs: Other parameters

    Returns:
        Example of Chat DeepSeek
    """
    return ChatDeepSeek(
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        **kwargs
    )


#For backward compatibility, provide an alias.
DeepSeekLLM = ChatDeepSeek
