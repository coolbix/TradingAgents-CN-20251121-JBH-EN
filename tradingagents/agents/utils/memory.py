import chromadb
from chromadb.config import Settings
from openai import OpenAI
import dashscope
from dashscope import TextEmbedding
import os
import threading
import hashlib
from typing import Dict, Optional

#Import Unified Log System
from tradingagents.utils.logging_init import get_logger
logger = get_logger("agents.utils.memory")


class ChromaDBManager:
    """Single ChromaDB Manager to avoid conflicts that create pools"""

    _instance = None
    _lock = threading.Lock()
    _collections: Dict[str, any] = {}
    _client = None

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ChromaDBManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            try:
                #Use uniform configuration module
                from .chromadb_config import get_optimal_chromadb_client, is_windows_11
                import platform

                self._client = get_optimal_chromadb_client()

                #Record initialised information
                system = platform.system()
                if system == "Windows":
                    if is_windows_11():
                        logger.info(f"[ChromaDB] Windows 11 optimized configuration has been initialised.{platform.version()})")
                    else:
                        logger.info(f"Initialisation of Windows 10 compatible configuration completed")
                else:
                    logger.info(f"📚 [ChromaDB] {system}Standard configuration initialised")

                self._initialized = True
            except Exception as e:
                logger.error(f"[ChromaDB] Initialization failed:{e}")
                #Use simplest configuration as backup
                try:
                    settings = Settings(
                        allow_reset=True,
                        anonymized_telemetry=False,  #Critical: Disable telemetry
                        is_persistent=False
                    )
                    self._client = chromadb.Client(settings)
                    logger.info(f"[ChromaDB]")
                except Exception as backup_error:
                    #The last alternative.
                    self._client = chromadb.Client()
                    logger.warning(f"[ChromaDB] Initializing with the simplest configuration:{backup_error}")
                self._initialized = True

    def get_or_create_collection(self, name: str):
        """Line securely capture or create a collection"""
        with self._lock:
            if name in self._collections:
                logger.info(f"[ChromaDB] With caches:{name}")
                return self._collections[name]

            try:
                #Try to access the existing collection
                collection = self._client.get_collection(name=name)
                logger.info(f"[ChromaDB]{name}")
            except Exception:
                try:
                    #Create a new set
                    collection = self._client.create_collection(name=name)
                    logger.info(f"[ChromaDB] Create a new set:{name}")
                except Exception as e:
                    #Could be co-created, trying again to get
                    try:
                        collection = self._client.get_collection(name=name)
                        logger.info(f"[ChromaDB]{name}")
                    except Exception as final_error:
                        logger.error(f"[ChromaDB] Pool operation failed:{name}, Error:{final_error}")
                        raise final_error

            #Cache Collective
            self._collections[name] = collection
            return collection


class FinancialSituationMemory:
    def __init__(self, name, config):
        self.config = config
        self.llm_provider = config.get("llm_provider", "openai").lower()

        #Configure length limits for vector caches (the vector cache default enabled length check)
        self.max_embedding_length = int(os.getenv('MAX_EMBEDDING_CONTENT_LENGTH', '50000'))  #Default 50K character
        self.enable_embedding_length_check = os.getenv('ENABLE_EMBEDDING_LENGTH_CHECK', 'true').lower() == 'true'  #Vector cache default enabled
        
        #Select embedded model and client according to LLM provider
        #Initialize the downgrade option sign
        self.fallback_available = False
        
        if self.llm_provider == "dashscope" or self.llm_provider == "alibaba":
            self.embedding = "text-embedding-v3"
            self.client = None  #DashScope does not need OpenAI client

            #Set DashScope API Key
            dashscope_key = os.getenv('DASHSCOPE_API_KEY')
            if dashscope_key:
                try:
                    #Try importing and initializing DashScope
                    import dashscope
                    from dashscope import TextEmbedding

                    dashscope.api_key = dashscope_key
                    logger.info(f"DashScope API key configured, memory enabled")

                    #Optional: Test API connection (simple authentication)
                    #No actual calls made here, only import and key settings verified

                except ImportError as e:
                    #DashScope package not installed
                    logger.error(f"The DashScop package is not installed:{e}")
                    self.client = "DISABLED"
                    logger.warning(f"Memory is disabled")

                except Exception as e:
                    #Other Organiser
                    logger.error(f"The initialization of DashScope failed:{e}")
                    self.client = "DISABLED"
                    logger.warning(f"Memory is disabled")
            else:
                #No DashScope key. Disable memory.
                self.client = "DISABLED"
                logger.warning(f"No DASHSCOPE API KEY found, memory function disabled")
                logger.info(f"The system will continue to operate but will not preserve or retrieve historical memory")
        elif self.llm_provider == "qianfan":
            #thousands of sails, embedding configuration
            #Chifa has no independent embedding API, using Alibri as an option for downgrading
            dashscope_key = os.getenv('DASHSCOPE_API_KEY')
            if dashscope_key:
                try:
                    #Using the Aliblanc Embedding Service as a thousands of sails solution
                    import dashscope
                    from dashscope import TextEmbedding

                    dashscope.api_key = dashscope_key
                    self.embedding = "text-embedding-v3"
                    self.client = None
                    logger.info(f"♪ Chifa uses the Aliblanc embedded service ♪")
                except ImportError as e:
                    logger.error(f"The DashScop package is not installed:{e}")
                    self.client = "DISABLED"
                    logger.warning(f"A thousand sail memory is disabled.")
                except Exception as e:
                    logger.error(f"The initialization of a thousand sails failed:{e}")
                    self.client = "DISABLED"
                    logger.warning(f"A thousand sail memory is disabled.")
            else:
                #No DashScope key. Disable memory.
                self.client = "DISABLED"
                logger.warning(f"The memory function is disabled.")
                logger.info(f"The system will continue to operate but will not preserve or retrieve historical memory")
        elif self.llm_provider == "deepseek":
            #Check for mandatory OpenAI embedding
            force_openai = os.getenv('FORCE_OPENAI_EMBEDDING', 'false').lower() == 'true'

            if not force_openai:
                #Try embedding with Alibri
                dashscope_key = os.getenv('DASHSCOPE_API_KEY')
                if dashscope_key:
                    try:
                        #Test the albino for availability.
                        import dashscope
                        from dashscope import TextEmbedding

                        dashscope.api_key = dashscope_key
                        #Validate TextEmbeding usability (no actual call required)
                        self.embedding = "text-embedding-v3"
                        self.client = None
                        logger.info(f"DeepSeek uses the Aliblanc embedded service")
                    except ImportError as e:
                        logger.error(f"The DashScop package is not installed:{e}")
                        dashscope_key = None  #Force demotion
                    except Exception as e:
                        logger.error(f"The initialization of the Aliblanc plant failed:{e}")
                        dashscope_key = None  #Force demotion
            else:
                dashscope_key = None  #Skip Ariperium

            if not dashscope_key or force_openai:
                #Down to OpenAI Embedding
                self.embedding = "text-embedding-3-small"
                openai_key = os.getenv('OPENAI_API_KEY')
                if openai_key:
                    self.client = OpenAI(
                        api_key=openai_key,
                        base_url=config.get("backend_url", "https://api.openai.com/v1")
                    )
                    logger.warning(f"DeepSeek back to OpenAI Embedding Service")
                else:
                    #Finally try DeepSeek's own embedded.
                    deepseek_key = os.getenv('DEEPSEEK_API_KEY')
                    if deepseek_key:
                        try:
                            self.client = OpenAI(
                                api_key=deepseek_key,
                                base_url="https://api.deepseek.com"
                            )
                            logger.info(f"DeepSeek uses its embedded services")
                        except Exception as e:
                            logger.error(f"DeepSeek embedded service is not available:{e}")
                            #Disable Memory
                            self.client = "DISABLED"
                            logger.info(f"🚨 Memory is disabled and the system will continue to run without preserving historical memory")
                    else:
                        #Disable memory function instead of dropping anomaly
                        self.client = "DISABLED"
                        logger.info(f"No embedded service available, memory disabled")
        elif self.llm_provider == "google":
            #Google AI uses Aliblanc (if available) to disable memory functions
            dashscope_key = os.getenv('DASHSCOPE_API_KEY')
            openai_key = os.getenv('OPENAI_API_KEY')
            
            if dashscope_key:
                try:
                    #Try Initialising DashScope
                    import dashscope
                    from dashscope import TextEmbedding

                    self.embedding = "text-embedding-v3"
                    self.client = None
                    dashscope.api_key = dashscope_key
                    
                    #Check for OpenAI keys as a downgrade option
                    if openai_key:
                        logger.info(f"💡Google AI uses the Aliblanc embedded service (OpenAI as a downgrading option)")
                        self.fallback_available = True
                        self.fallback_client = OpenAI(api_key=openai_key, base_url=config["backend_url"])
                        self.fallback_embedding = "text-embedding-3-small"
                    else:
                        logger.info(f"💡Google AI uses the Alibrico Embedding Service (no downgrading option)")
                        self.fallback_available = False
                        
                except ImportError as e:
                    logger.error(f"The DashScop package is not installed:{e}")
                    self.client = "DISABLED"
                    logger.warning(f"Google AI memory is disabled")
                except Exception as e:
                    logger.error(f"The initialization of DashScope failed:{e}")
                    self.client = "DISABLED"
                    logger.warning(f"Google AI memory is disabled")
            else:
                #No DashScope key. Disable memory.
                self.client = "DISABLED"
                self.fallback_available = False
                logger.warning(f"Google AI did not find DASHSCOPE API KEY, memory function disabled")
                logger.info(f"The system will continue to operate but will not preserve or retrieve historical memory")
        elif self.llm_provider == "openrouter":
            #OpenRouter Support: Prioritize the use of Alibri, otherwise memory functionality is disabled
            dashscope_key = os.getenv('DASHSCOPE_API_KEY')
            if dashscope_key:
                try:
                    #Try embedding with Alibri
                    import dashscope
                    from dashscope import TextEmbedding

                    self.embedding = "text-embedding-v3"
                    self.client = None
                    dashscope.api_key = dashscope_key
                    logger.info(f"OpenRouter uses Aliblanc embedded services")
                except ImportError as e:
                    logger.error(f"The DashScop package is not installed:{e}")
                    self.client = "DISABLED"
                    logger.warning(f"OpenRouter Memory is disabled")
                except Exception as e:
                    logger.error(f"The initialization of DashScope failed:{e}")
                    self.client = "DISABLED"
                    logger.warning(f"OpenRouter Memory is disabled")
            else:
                #No DashScope key. Disable memory.
                self.client = "DISABLED"
                logger.warning(f"OpenRouter did not find DASHSCOPE API KEY, memory function disabled")
                logger.info(f"The system will continue to operate but will not preserve or retrieve historical memory")
        elif config["backend_url"] == "http://localhost:11434/v1":
            self.embedding = "nomic-embed-text"
            self.client = OpenAI(base_url=config["backend_url"])
        else:
            self.embedding = "text-embedding-3-small"
            openai_key = os.getenv('OPENAI_API_KEY')
            if openai_key:
                self.client = OpenAI(
                    api_key=openai_key,
                    base_url=config["backend_url"]
                )
            else:
                self.client = "DISABLED"
                logger.warning(f"No OPENAI API KEY found, memory function disabled")

        #Use a single ChromaDB manager
        self.chroma_manager = ChromaDBManager()
        self.situation_collection = self.chroma_manager.get_or_create_collection(name)

    def _smart_text_truncation(self, text, max_length=8192):
        """Smart text cut, semantic integrity and cache compatibility"""
        if len(text) <= max_length:
            return text, False  #Returns original text and cut marks
        
        #Try to cut at sentence boundary
        sentences = text.split('。')
        if len(sentences) > 1:
            truncated = ""
            for sentence in sentences:
                if len(truncated + sentence + '。') <= max_length - 50:  #Keep 50 Characters
                    truncated += sentence + '。'
                else:
                    break
            if len(truncated) > max_length // 2:  #At least half the content.
                logger.info(f"📝 smart cut: cut at sentence boundary, retain{len(truncated)}/{len(text)}Character")
                return truncated, True
        
        #Try to cut a paragraph boundary
        paragraphs = text.split('\n')
        if len(paragraphs) > 1:
            truncated = ""
            for paragraph in paragraphs:
                if len(truncated + paragraph + '\n') <= max_length - 50:
                    truncated += paragraph + '\n'
                else:
                    break
            if len(truncated) > max_length // 2:
                logger.info(f"Smart cut-off: cut-off at a paragraph boundary, retain{len(truncated)}/{len(text)}Character")
                return truncated, True
        
        #Final choice: retaining key information for the first and second parts
        front_part = text[:max_length//2]
        back_part = text[-(max_length//2-100):]  #Leave 100 characters to connector
        truncated = front_part + "\n...[内容截断]...\n" + back_part
        logger.warning(f"Forced cut: keep the key message at the end,{len(text)}character cut to{len(truncated)}Character")
        return truncated, True

    def get_embedding(self, text):
        """Get embedding for a text using the configured provider"""

        #Check if memory functions are disabled
        if self.client == "DISABLED":
            #Memory disabled, return empty vector
            logger.debug(f"Memory is disabled and returns empty vectors")
            return [0.0] * 1024  #returns zero vector of 1024 D

        #Verify input text
        if not text or not isinstance(text, str):
            logger.warning(f"Input text is empty or invalid, return empty vector")
            return [0.0] * 1024

        text_length = len(text)
        if text_length == 0:
            logger.warning(f"Enter text length 0, return empty vector")
            return [0.0] * 1024
        
        #Check to enable length limits
        if self.enable_embedding_length_check and text_length > self.max_embedding_length:
            logger.warning(f"Too long text (⚠️){text_length:,}Character >{self.max_embedding_length:,}Character), Skip Quantification")
            #Can not open message
            self._last_text_info = {
                'original_length': text_length,
                'processed_length': 0,
                'was_truncated': False,
                'was_skipped': True,
                'provider': self.llm_provider,
                'strategy': 'length_limit_skip',
                'max_length': self.max_embedding_length
            }
            return [0.0] * 1024
        
        #Record text information (without any cut-off)
        if text_length > 8192:
            logger.info(f"Process long text:{text_length}Character, provider:{self.llm_provider}")
        
        #Store text processing information
        self._last_text_info = {
            'original_length': text_length,
            'processed_length': text_length,  #Do not interrupt. Keep the length.
            'was_truncated': False,  #Never stop.
            'was_skipped': False,
            'provider': self.llm_provider,
            'strategy': 'no_truncation_with_fallback'  #Tag Policy
        }

        if (self.llm_provider == "dashscope" or
            self.llm_provider == "alibaba" or
            self.llm_provider == "qianfan" or
            (self.llm_provider == "google" and self.client is None) or
            (self.llm_provider == "deepseek" and self.client is None) or
            (self.llm_provider == "openrouter" and self.client is None)):
            #Use Alibri's embedded model
            try:
                #Import DashScope Module
                import dashscope
                from dashscope import TextEmbedding

                #Check for DashScope API key availability
                if not hasattr(dashscope, 'api_key') or not dashscope.api_key:
                    logger.warning(f"DashScope API key unset, memory function down")
                    return [0.0] * 1024  #Return empty vector

                #Try DashScope API
                response = TextEmbedding.call(
                    model=self.embedding,
                    input=text
                )

                #Check response status
                if response.status_code == 200:
                    #Successfully accessed embedding
                    embedding = response.output['embeddings'][0]['embedding']
                    logger.debug(f"DashScape embeding, dimension:{len(embedding)}")
                    return embedding
                else:
                    #API returns the wrong status code
                    error_msg = f"{response.code} - {response.message}"
                    
                    #Check if the length limit is wrong
                    if any(keyword in error_msg.lower() for keyword in ['length', 'token', 'limit', 'exceed']):
                        logger.warning(f"DashScope length limit:{error_msg}")
                        
                        #Check for downgrading options
                        if hasattr(self, 'fallback_available') and self.fallback_available:
                            logger.info(f"Try to use OpenAI to downgrade long text")
                            try:
                                response = self.fallback_client.embeddings.create(
                                    model=self.fallback_embedding,
                                    input=text
                                )
                                embedding = response.data[0].embedding
                                logger.info(f"OpenAI successfully downgraded dimensions:{len(embedding)}")
                                return embedding
                            except Exception as fallback_error:
                                logger.error(f"OpenAI failed to downgrade:{str(fallback_error)}")
                                logger.info(f"💡 All downgrading options failed, memory function downgraded")
                                return [0.0] * 1024
                        else:
                            logger.info(f"💡 No options for downgrading, memory downgrading")
                            return [0.0] * 1024
                    else:
                        logger.error(f"DashScapeAPI error:{error_msg}")
                        return [0.0] * 1024  #Return empty vectors instead of dropping anomalies

            except Exception as e:
                error_str = str(e).lower()
                
                #Check if the length limit is wrong
                if any(keyword in error_str for keyword in ['length', 'token', 'limit', 'exceed', 'too long']):
                    logger.warning(f"DashScope length limit abnormal:{str(e)}")
                    
                    #Check for downgrading options
                    if hasattr(self, 'fallback_available') and self.fallback_available:
                        logger.info(f"Try to use OpenAI to downgrade long text")
                        try:
                            response = self.fallback_client.embeddings.create(
                                model=self.fallback_embedding,
                                input=text
                            )
                            embedding = response.data[0].embedding
                            logger.info(f"OpenAI successfully downgraded dimensions:{len(embedding)}")
                            return embedding
                        except Exception as fallback_error:
                            logger.error(f"OpenAI failed to downgrade:{str(fallback_error)}")
                            logger.info(f"💡 All downgrading options failed, memory function downgraded")
                            return [0.0] * 1024
                    else:
                        logger.info(f"💡 No options for downgrading, memory downgrading")
                        return [0.0] * 1024
                elif 'import' in error_str:
                    logger.error(f"The DashScop package is not installed:{str(e)}")
                elif 'connection' in error_str:
                    logger.error(f"DashScop network connection error:{str(e)}")
                elif 'timeout' in error_str:
                    logger.error(f"DashScop requests timeout:{str(e)}")
                else:
                    logger.error(f"DashScape embeding:{str(e)}")
                
                logger.warning(f"Memory downgrade, return empty vector")
                return [0.0] * 1024
        else:
            #Use OpenAI compatible embedded model
            if self.client is None:
                logger.warning(f"Embedded client not initialized, return empty vector")
                return [0.0] * 1024  #Return empty vector
            elif self.client == "DISABLED":
                #Memory disabled, return empty vector
                logger.debug(f"⚠️ Memory disabled, return empty vector")
                return [0.0] * 1024  #returns zero vector of 1024 D

            #Try to call OpenAI compatible embedding API
            try:
                response = self.client.embeddings.create(
                    model=self.embedding,
                    input=text
                )
                embedding = response.data[0].embedding
                logger.debug(f"✅ {self.llm_provider}EMbeding succeeded, dimension:{len(embedding)}")
                return embedding

            except Exception as e:
                error_str = str(e).lower()
                
                #Check if the length limit is wrong
                length_error_keywords = [
                    'token', 'length', 'too long', 'exceed', 'maximum', 'limit',
                    'context', 'input too large', 'request too large'
                ]
                
                is_length_error = any(keyword in error_str for keyword in length_error_keywords)
                
                if is_length_error:
                    #Length limit error: Directly downgraded, uninterrupted retry
                    logger.warning(f"⚠️ {self.llm_provider}Length limit:{str(e)}")
                    logger.info(f"To ensure analytical accuracy, text is not cut, memory function is downgraded")
                else:
                    #Other types of error
                    if 'attributeerror' in error_str:
                        logger.error(f"❌ {self.llm_provider}API call error:{str(e)}")
                    elif 'connectionerror' in error_str or 'connection' in error_str:
                        logger.error(f"❌ {self.llm_provider}Network connection error:{str(e)}")
                    elif 'timeout' in error_str:
                        logger.error(f"❌ {self.llm_provider}Request timeout:{str(e)}")
                    elif 'keyerror' in error_str:
                        logger.error(f"❌ {self.llm_provider}Reply format error:{str(e)}")
                    else:
                        logger.error(f"❌ {self.llm_provider}EMbedding anomaly:{str(e)}")
                
                logger.warning(f"Memory downgrade, return empty vector")
                return [0.0] * 1024

    def get_embedding_config_status(self):
        """Fetch vector cache configuration"""
        return {
            'enabled': self.enable_embedding_length_check,
            'max_embedding_length': self.max_embedding_length,
            'max_embedding_length_formatted': f"{self.max_embedding_length:,}字符",
            'provider': self.llm_provider,
            'client_status': 'DISABLED' if self.client == "DISABLED" else 'ENABLED'
        }

    def get_last_text_info(self):
        """Get final processed text information"""
        return getattr(self, '_last_text_info', None)

    def add_situations(self, situations_and_advice):
        """Add financial situations and their corresponding advice. Parameter is a list of tuples (situation, rec)"""

        situations = []
        advice = []
        ids = []
        embeddings = []

        offset = self.situation_collection.count()

        for i, (situation, recommendation) in enumerate(situations_and_advice):
            situations.append(situation)
            advice.append(recommendation)
            ids.append(str(offset + i))
            embeddings.append(self.get_embedding(situation))

        self.situation_collection.add(
            documents=situations,
            metadatas=[{"recommendation": rec} for rec in advice],
            embeddings=embeddings,
            ids=ids,
        )

    def get_memories(self, current_situation, n_matches=1):
        """Find matching recommendations using embeddings with smart truncation handling"""
        
        #Fetching current status
        query_embedding = self.get_embedding(current_situation)
        
        #Check for empty vectors (memory functionality disabled or error)
        if all(x == 0.0 for x in query_embedding):
            logger.debug(f"Query for embedding as an empty vector, return empty result")
            return []
        
        #Check if there's enough data to search
        collection_count = self.situation_collection.count()
        if collection_count == 0:
            logger.debug(f"The memory library is empty.")
            return []
        
        #Adjust the number of queries to not exceed the number of documents in the collection
        actual_n_matches = min(n_matches, collection_count)
        
        try:
            #Perform Similarity Query
            results = self.situation_collection.query(
                query_embeddings=[query_embedding],
                n_results=actual_n_matches
            )
            
            #Process query results
            memories = []
            if results and 'documents' in results and results['documents']:
                documents = results['documents'][0]
                metadatas = results.get('metadatas', [[]])[0]
                distances = results.get('distances', [[]])[0]
                
                for i, doc in enumerate(documents):
                    metadata = metadatas[i] if i < len(metadatas) else {}
                    distance = distances[i] if i < len(distances) else 1.0
                    
                    memory_item = {
                        'situation': doc,
                        'recommendation': metadata.get('recommendation', ''),
                        'similarity': 1.0 - distance,  #Convert to Similarity Scores
                        'distance': distance
                    }
                    memories.append(memory_item)
                
                #Record query information
                if hasattr(self, '_last_text_info') and self._last_text_info.get('was_truncated'):
                    logger.info(f"Interrupted text query completed.{len(memories)}A relevant memory.")
                    logger.debug(f"Original length:{self._last_text_info['original_length']}, "
                               f"Processing length:{self._last_text_info['processed_length']}")
                else:
                    logger.debug(f"Memory check complete.{len(memories)}A relevant memory.")
            
            return memories
            
        except Exception as e:
            logger.error(f"Memory query failed:{str(e)}")
            return []

    def get_cache_info(self):
        """Get cache information for debugging and monitoring"""
        info = {
            'collection_count': self.situation_collection.count(),
            'client_status': 'enabled' if self.client != "DISABLED" else 'disabled',
            'embedding_model': self.embedding,
            'provider': self.llm_provider
        }
        
        #Add Last Text Processing Information
        if hasattr(self, '_last_text_info'):
            info['last_text_processing'] = self._last_text_info
            
        return info


if __name__ == "__main__":
    # Example usage
    matcher = FinancialSituationMemory()

    # Example data
    example_data = [
        (
            "High inflation rate with rising interest rates and declining consumer spending",
            "Consider defensive sectors like consumer staples and utilities. Review fixed-income portfolio duration.",
        ),
        (
            "Tech sector showing high volatility with increasing institutional selling pressure",
            "Reduce exposure to high-growth tech stocks. Look for value opportunities in established tech companies with strong cash flows.",
        ),
        (
            "Strong dollar affecting emerging markets with increasing forex volatility",
            "Hedge currency exposure in international positions. Consider reducing allocation to emerging market debt.",
        ),
        (
            "Market showing signs of sector rotation with rising yields",
            "Rebalance portfolio to maintain target allocations. Consider increasing exposure to sectors benefiting from higher rates.",
        ),
    ]

    # Add the example situations and recommendations
    matcher.add_situations(example_data)

    # Example query
    current_situation = """
    Market showing increased volatility in tech sector, with institutional investors 
    reducing positions and rising interest rates affecting growth stock valuations
    """

    try:
        recommendations = matcher.get_memories(current_situation, n_matches=2)

        for i, rec in enumerate(recommendations, 1):
            logger.info(f"\nMatch {i}:")
            logger.info(f"Similarity Score: {rec.get('similarity', 0):.2f}")
            logger.info(f"Matched Situation: {rec.get('situation', '')}")
            logger.info(f"Recommendation: {rec.get('recommendation', '')}")

    except Exception as e:
        logger.error(f"Error during recommendation: {str(e)}")
