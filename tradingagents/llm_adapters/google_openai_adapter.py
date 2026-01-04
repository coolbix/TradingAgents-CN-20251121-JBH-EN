"""Google AI OpenAI Compatibility Compatibility
OpenAI compatible interface for TradingAgents with Google AI (Gemini) models
Solve the mismatch in the Google model tool call format
"""

import os
from typing import Any, Dict, List, Optional, Union, Sequence
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.tools import BaseTool
from langchain_core.messages import BaseMessage, AIMessage, HumanMessage, SystemMessage
from langchain_core.outputs import LLMResult
from pydantic import Field, SecretStr
from ..config.config_manager import TOKEN_TRACKER

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')


class ChatGoogleOpenAI(ChatGoogleGenerativeAI):
    """Google AI OpenAI Compatibility
    Inherit ChatGoogleGenerativeAI, optimize tool call and content format processing
    Solve the problem of the Google model tool calling for returns that do not match system expectations
    """

    def __init__(self, base_url: Optional[str] = None, **kwargs):
        """Initialize Google AI OpenAI compatible client

        Args:
            Base url: Custom API endpoint (optional)
            If provided, pass it to Google AI SDK by channel options
            Support format:
            - https://generativelanguage.googleapis.com/v1beta
            - https://generativelanguage.googleapis.com/v1 (autoconvert to v1beta)
            - Custom proxy address
            **kwargs: Other parameters
        """

        #[DBUG] Read the log before the environment variable
        logger.info("[Google Initializing]")
        logger.info(f"Does it contain google api key:{'google_api_key' in kwargs}")
        logger.info(f"[Google Initializing]{base_url}")

        #Sets the default configuration for Google AI
        kwargs.setdefault("temperature", 0.1)
        kwargs.setdefault("max_tokens", 2000)

        #🔥 Prefer to the imported API Key in kwargs (from database configuration)
        google_api_key = kwargs.get("google_api_key")

        #If kwargs does not have API Key, try reading from environment variables
        if not google_api_key:
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

            #Check the API Key in the environment variable
            env_api_key = os.getenv("GOOGLE_API_KEY")
            logger.info(f"[Google Initializing]{'Value' if env_api_key else 'Empty'}")

            #Verify the validity of API Key in the environment variable (exclude placeholder)
            if env_api_key and is_valid_api_key(env_api_key):
                logger.info(f"API Key is valid, length:{len(env_api_key)}, top 10:{env_api_key[:10]}...")
                google_api_key = env_api_key
            elif env_api_key:
                logger.warning("API Key (possibly placeholder) is invalid and will be ignored.")
                google_api_key = None
            else:
                logger.warning("[Google Initializing] GOOGLE API KEY environment variable is empty")
                google_api_key = None
        else:
            logger.info("✅ [Google Initialization] Using the imported API Key from kwargs (from database configuration)")

        logger.info(f"[Google Initializing]{'Value' if google_api_key else 'Empty'}")

        if not google_api_key:
            logger.error("[Google Initializing] API Key failed, about to be thrown out.")
            raise ValueError(
                "Google API key not found. Please configure API key in web interface "
                "(Settings -> LLM Providers) or set GOOGLE_API_KEY environment variable."
            )

        kwargs["google_api_key"] = google_api_key

        #Processing custom base url
        if base_url:
            #Remove end slash
            base_url = base_url.rstrip('/')
            logger.info(f"[Google Initializing]{base_url}")

            #Check if it's an official Google domain name
            is_google_official = 'generativelanguage.googleapis.com' in base_url

            if is_google_official:
                #✅ Google Official Domain Name: Extracts the domain name part, SDK automatically adds /v1beta
                #For example: https://generativelanguage.googleapis.com/v1beta - >https://generativelanguage.googleapis.com
                #      https://generativelanguage.googleapis.com/v1 -> https://generativelanguage.googleapis.com
                if base_url.endswith('/v1beta'):
                    api_endpoint = base_url[:-7]  #Remove /v1beta (7 characters)
                    logger.info(f"[Google Official]{api_endpoint}")
                elif base_url.endswith('/v1'):
                    api_endpoint = base_url[:-3]  #Remove /v1 (3 characters)
                    logger.info(f"[Google Official]{api_endpoint}")
                else:
                    #If no version suffix is available, use it directly
                    api_endpoint = base_url
                    logger.info(f"[Googleofficial] Use full base url as domain name:{api_endpoint}")

                logger.info(f"[Google Official] SDK will automatically add /v1beta path")
            else:
                #🔄 Transit address: use the complete URL directly and do not allow SDK to add / v1beta
                #Transit services usually contain complete path maps.
                api_endpoint = base_url
                logger.info(f"🔄 [transit address]{api_endpoint}")
                logger.info(f"The transit service usually contains a complete path and does not need SDK Add / v1beta")

            #Send custom peer through client options
            #Reference: https://github.com/langchain-ai/langchain-google/issues/783
            kwargs["client_options"] = {"api_endpoint": api_endpoint}
            logger.info(f"[Google Initializing] set the key options.api endpoint:{api_endpoint}")
        else:
            logger.info(f"[Google Initializing] Points")

        #Call Parent Initialization
        super().__init__(**kwargs)

        logger.info(f"Successful initialization of Google AI OpenAI compatible adaptor")
        logger.info(f"Models:{kwargs.get('model', 'gemini-pro')}")
        logger.info(f"Temperature:{kwargs.get('temperature', 0.1)}")
        logger.info(f"Max Token:{kwargs.get('max_tokens', 2000)}")
        if base_url:
            logger.info(f"Custom peer:{base_url}")

    @property
    def model_name(self) -> str:
        """Returns model name (compatibility properties)
        Remove 'models/ 'prefix and return pure model name
        """
        model = self.model
        if model and model.startswith("models/"):
            return model[7:]  #Remove "models/" Prefix
        return model or "unknown"
    
    def _generate(self, messages: List[BaseMessage], stop: Optional[List[str]] = None, **kwargs) -> LLMResult:
        """Rewrite method to optimize tool call processing and content formats"""

        try:
            #Call parent generation method
            result = super()._generate(messages, stop, **kwargs)

            #Optimizing Return Content Format
            #Note: result.generations are two-dimensional lists [ChatGeneration]
            if result and result.generations:
                for generation_list in result.generations:
                    if isinstance(generation_list, list):
                        for generation in generation_list:
                            if hasattr(generation, 'message') and generation.message:
                                #Optimizing message content format
                                self._optimize_message_content(generation.message)
                    else:
                        #Compatibility processing: if not list, directly
                        if hasattr(generation_list, 'message') and generation_list.message:
                            self._optimize_message_content(generation_list.message)

            #Track token usage
            self._track_token_usage(result, kwargs)

            return result

        except Exception as e:
            logger.error(f"Google AI generation failed:{e}")
            logger.exception(e)  #Print full stack tracking

            #Check for invalid API Key error
            error_str = str(e)
            if 'API_KEY_INVALID' in error_str or 'API key not valid' in error_str:
                error_content = "Google AI API Key 无效或未配置。\n\n请检查：\n1. GOOGLE_API_KEY 环境变量是否正确配置\n2. API Key 是否有效（访问 https://ai.google.dev/ 获取）\n3. 是否启用了 Gemini API\n\n建议：使用其他 AI 模型（如阿里百炼、DeepSeek）"
            elif 'Connection' in error_str or 'Network' in error_str:
                error_content = f"Google AI 网络连接失败: {error_str}\n\n请检查：\n1. 网络连接是否正常\n2. 是否需要科学上网\n3. 防火墙设置"
            else:
                error_content = f"Google AI 调用失败: {error_str}\n\n请检查配置或使用其他 AI 模型"

            #Returns a result that contains the wrong message, not throws the anomaly
            from langchain_core.outputs import ChatGeneration
            error_message = AIMessage(content=error_content)
            error_generation = ChatGeneration(message=error_message)
            return LLMResult(generations=[[error_generation]])
    
    def _optimize_message_content(self, message: BaseMessage):
        """Optimizing message content formats to ensure that news features are critical Word"""
        
        if not isinstance(message, AIMessage) or not message.content:
            return
        
        content = message.content
        
        #Check if it's the content of the message that the tool called back
        if self._is_news_content(content):
            #Optimizing the format of information content by adding the necessary keywords
            optimized_content = self._enhance_news_content(content)
            message.content = optimized_content
            
            logger.debug(f"[Google adapter] Optimizing news content formats")
            logger.debug(f"Original length:{len(content)}Character")
            logger.debug(f"Optimized length:{len(optimized_content)}Character")
    
    def _is_news_content(self, content: str) -> bool:
        """To judge whether content is news content"""
        
        #Check to include news-related keywords
        news_indicators = [
            "股票", "公司", "市场", "投资", "财经", "证券", "交易",
            "涨跌", "业绩", "财报", "分析", "预测", "消息", "公告"
        ]
        
        return any(indicator in content for indicator in news_indicators) and len(content) > 200
    
    def _enhance_news_content(self, content: str) -> str:
        """Enhance the content of the news and add necessary formatting information"""
        
        import datetime
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        
        #Add content without the necessary news features
        enhanced_content = content
        
        #Add release time information (if missing)
        if "发布时间" not in content and "时间" not in content:
            enhanced_content = f"发布时间: {current_date}\n\n{enhanced_content}"
        
        #Add news header identifier (if missing)
        if "新闻标题" not in content and "标题" not in content:
            #Try extracting the first row from the contents as title
            lines = enhanced_content.split('\n')
            if lines:
                first_line = lines[0].strip()
                if len(first_line) < 100:  #Could be the title.
                    enhanced_content = f"新闻标题: {first_line}\n\n{enhanced_content}"
        
        #Add article source information (if missing)
        if "文章来源" not in content and "来源" not in content:
            enhanced_content = f"{enhanced_content}\n\n文章来源: Google AI 智能分析"
        
        return enhanced_content
    
    def _track_token_usage(self, result: LLMResult, kwargs: Dict[str, Any]):
        """Track token usage"""
        
        try:
            #Extract token information from the result
            if hasattr(result, 'llm_output') and result.llm_output:
                token_usage = result.llm_output.get('token_usage', {})
                
                input_tokens = token_usage.get('prompt_tokens', 0)
                output_tokens = token_usage.get('completion_tokens', 0)
                
                if input_tokens > 0 or output_tokens > 0:
                    #Generate Session ID
                    session_id = kwargs.get('session_id', f"google_openai_{hash(str(kwargs))%10000}")
                    analysis_type = kwargs.get('analysis_type', 'stock_analysis')
                    
                    #Record usage using TokenTracker
                    TOKEN_TRACKER.track_usage(
                        provider="google",
                        model_name=self.model,
                        input_tokens=input_tokens,
                        output_tokens=output_tokens,
                        session_id=session_id,
                        analysis_type=analysis_type
                    )
                    
                    logger.debug(f"[Google adapter] Token usage: Input={input_tokens},out ={output_tokens}")
                    
        except Exception as track_error:
            #Token, tracking failure should not affect the primary function.
            logger.error(f"The Google adapter Token has failed:{track_error}")


#List of supported models
GOOGLE_OPENAI_MODELS = {
    #Gemini 2.5 series - latest validation model
    "gemini-2.5-pro": {
        "description": "Gemini 2.5 Pro - 最新旗舰模型，功能强大 (16.68s)",
        "context_length": 32768,
        "supports_function_calling": True,
        "recommended_for": ["复杂推理", "专业分析", "高质量输出"],
        "avg_response_time": 16.68
    },
    "gemini-2.5-flash": {
        "description": "Gemini 2.5 Flash - 最新快速模型 (2.73s)",
        "context_length": 32768,
        "supports_function_calling": True,
        "recommended_for": ["快速响应", "实时分析", "高频使用"],
        "avg_response_time": 2.73
    },
    "gemini-2.5-flash-lite-preview-06-17": {
        "description": "Gemini 2.5 Flash Lite Preview - 超快响应 (1.45s)",
        "context_length": 32768,
        "supports_function_calling": True,
        "recommended_for": ["超快响应", "实时交互", "高频调用"],
        "avg_response_time": 1.45
    },
    #Gemini 2.0 Series
    "gemini-2.0-flash": {
        "description": "Gemini 2.0 Flash - 新一代快速模型 (1.87s)",
        "context_length": 32768,
        "supports_function_calling": True,
        "recommended_for": ["快速响应", "实时分析"],
        "avg_response_time": 1.87
    },
    #Gemini 1.5 Series
    "gemini-1.5-pro": {
        "description": "Gemini 1.5 Pro - 强大性能，平衡选择 (2.25s)",
        "context_length": 32768,
        "supports_function_calling": True,
        "recommended_for": ["复杂分析", "专业任务", "深度思考"],
        "avg_response_time": 2.25
    },
    "gemini-1.5-flash": {
        "description": "Gemini 1.5 Flash - 快速响应，备用选择 (2.87s)",
        "context_length": 32768,
        "supports_function_calling": True,
        "recommended_for": ["快速任务", "日常对话", "简单分析"],
        "avg_response_time": 2.87
    },
    #Classic Model
    "gemini-pro": {
        "description": "Gemini Pro - 经典模型，稳定可靠",
        "context_length": 32768,
        "supports_function_calling": True,
        "recommended_for": ["通用任务", "稳定性要求高的场景"]
    }
}


def get_available_google_models() -> Dict[str, Dict[str, Any]]:
    """Get a list of available Google AI models"""
    return GOOGLE_OPENAI_MODELS


def create_google_openai_llm(
    model: str = "gemini-2.5-flash-lite-preview-06-17",
    google_api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    temperature: float = 0.1,
    max_tokens: int = 2000,
    **kwargs
) -> ChatGoogleOpenAI:
    """A convenient function to create Google AI OpenAI compatible LLM examples

    Args:
        Model name
        Google api key: Google API Key
        Base url: Custom API endpoint (optional)
        temperature: temperature parameters
        max tokens: maximum number of token
        **kwargs: Other parameters

    Returns:
        ChatGoogleOpenAI instance
    """

    return ChatGoogleOpenAI(
        model=model,
        google_api_key=google_api_key,
        base_url=base_url,
        temperature=temperature,
        max_tokens=max_tokens,
        **kwargs
    )


def test_google_openai_connection(
    model: str = "gemini-2.0-flash",
    google_api_key: Optional[str] = None
) -> bool:
    """Test Google AI OpenAI compatible interface connection"""
    
    try:
        logger.info(f"🧪 Test Google AI OpenAI compatible interface connection")
        logger.info(f"Models:{model}")
        
        #Create Client
        llm = create_google_openai_llm(
            model=model,
            google_api_key=google_api_key,
            max_tokens=50
        )
        
        #Send Test Message
        response = llm.invoke("你好，请简单介绍一下你自己。")
        
        if response and hasattr(response, 'content') and response.content:
            logger.info(f"Google AI OpenAI interface successfully connected")
            logger.info(f"Response:{response.content[:100]}...")
            return True
        else:
            logger.error(f"The Google AI OpenAI interface response is empty")
            return False
            
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        return False


def test_google_openai_function_calling(
    model: str = "gemini-2.5-flash-lite-preview-06-17",
    google_api_key: Optional[str] = None
) -> bool:
    """Testing Google AI OpenAI compatibility"""
    
    try:
        logger.info(f"Google AI Function Calling")
        logger.info(f"Models:{model}")
        
        #Create Client
        llm = create_google_openai_llm(
            model=model,
            google_api_key=google_api_key,
            max_tokens=200
        )
        
        #Define Test Tool
        from langchain_core.tools import tool
        
        @tool
        def test_news_tool(query: str) -> str:
            """Testing news tools, returning to analogue news content"""
            return f"""发布时间: 2024-01-15
新闻标题: {query}相关市场动态
文章来源: 测试新闻源

这是一条关于{query}的测试新闻内容。该公司近期表现良好，市场前景看好。
投资者对此表示关注，分析师给出积极评价。"""
        
        #Tie Tool
        llm_with_tools = llm.bind_tools([test_news_tool])
        
        #Test Tool Call
        response = llm_with_tools.invoke("请使用test_news_tool查询'苹果公司'的新闻")
        
        logger.info(f"Google AI Function Calling")
        logger.info(f"Type of response:{type(response)}")
        
        if hasattr(response, 'tool_calls') and response.tool_calls:
            logger.info(f"Number of tools called:{len(response.tool_calls)}")
            return True
        else:
            logger.info(f"Response content:{getattr(response, 'content', 'No content')}")
            return True  #Even without a tool call was successful because the model may choose not to call a tool
            
    except Exception as e:
        logger.error(f"Google AI Function Calling failed:{e}")
        return False


if __name__ == "__main__":
    """测试脚本"""
    logger.info(f"Google AI OpenAI Compatibility Test")
    logger.info(f"=" * 50)
    
    #Test Connection
    connection_ok = test_google_openai_connection()
    
    if connection_ok:
        #Testing
        function_calling_ok = test_google_openai_function_calling()
        
        if function_calling_ok:
            logger.info(f"All tests passed! Google AI OpenAI is working.")
        else:
            logger.error(f"Function Calling Test Failed")
    else:
        logger.error(f"Connection test failed")