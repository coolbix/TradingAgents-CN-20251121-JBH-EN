"""Alpha Vantage API Public Module

Provides a common request function for Alpha Vantage API, including:
- API Requesting Cover
- Mishandling and retrying
- Speed limit processing.
- Respond to resolution.

Reference original TradingAgents Achieved
"""

import os
import time
import json
import requests
from typing import Dict, Any, Optional
from datetime import datetime

#Import Log Module
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')


class AlphaVantageRateLimitError(Exception):
    """Alpha Vantage limit error"""
    pass


class AlphaVantageAPIError(Exception):
    """AlphaVantage API Error"""
    pass


def _get_api_key_from_database() -> Optional[str]:
    """Read from database Alpha Vantage API Key

Priority: Database Configuration > Environmental Variable
This will take effect immediately after the user changes configuration in the Web backstage

Returns:
Optional [str]: API Key, if not returned None
"""
    try:
        logger.debug("[DB query] Start reading Alpha Vantage API Key...")
        from app.core.database import get_mongo_db_sync
        db = get_mongo_db_sync()
        config_collection = db.system_configs

        #Get the latest active configuration
        logger.debug("[DB Query] is active=True configuration...")
        config_data = config_collection.find_one(
            {"is_active": True},
            sort=[("version", -1)]
        )

        if config_data:
            logger.debug(f"[DB Query]{config_data.get('version')}")
            if config_data.get('data_source_configs'):
                logger.debug(f"[DB query]{len(config_data['data_source_configs'])}Data sources")
                for ds_config in config_data['data_source_configs']:
                    ds_type = ds_config.get('type')
                    logger.debug(f"Checking data sources:{ds_type}")
                    if ds_type == 'alpha_vantage':
                        api_key = ds_config.get('api_key')
                        logger.debug(f"[DB Query]{len(api_key) if api_key else 0}")
                        if api_key and not api_key.startswith("your_"):
                            logger.debug(f"[DB query] API Key valid (long:{len(api_key)})")
                            return api_key
                        else:
                            logger.debug(f"[DB Query] API Key is invalid or occupied Arguments")
            else:
                logger.debug("Cannot initialise Evolution's mail component.")
        else:
            logger.debug("[DB query] No active configuration found")

        logger.debug("No valid Alpha Vantage API Key found in database ⚠️")
    except Exception as e:
        logger.debug(f"Could not close temporary folder: %s{e}")

    return None


def get_api_key() -> str:
    """Get Alpha Vantage API Key

Priority:
1. Database configuration (system configs collection)
2. Environmental variable ALPHA VANTAGE API KEY
3. Profile

Returns:
str: API Key

Rices:
ValueError: If API Key is not configured
"""
    #1. Access to databases (highest priority)
    logger.debug("[Step 1] Start reading Alpha Vantage API Key...")
    db_api_key = _get_api_key_from_database()
    if db_api_key:
        logger.debug(f"[Step 1] API Key found in the database{len(db_api_key)})")
        return db_api_key
    else:
        logger.debug("No API Key found in database ⚠️")

    #2. Access to environmental variables
    logger.debug("[Step 2] Read API Key...")
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if api_key:
        logger.debug(f"API Key found in [step 2].env{len(api_key)})")
        return api_key
    else:
        logger.debug("No API Key found in [step 2].env")

    #3. Access from profile
    logger.debug("[Step 3] Read the API Key...")
    try:
        from tradingagents.config.config_manager import ConfigManager
        config_manager = ConfigManager()
        api_key = config_manager.get("ALPHA_VANTAGE_API_KEY")
        if api_key:
            logger.debug(f"API Key (Long:{len(api_key)})")
            return api_key
    except Exception as e:
        logger.debug(f"[Step 3]{e}")

    #Every way failed.
    raise ValueError(
        "❌ Alpha Vantage API Key 未配置！\n"
        "请通过以下任一方式配置：\n"
        "1. Web 后台配置（推荐）: http://localhost:3000/api/config/datasource\n"
        "2. 设置环境变量: ALPHA_VANTAGE_API_KEY\n"
        "3. 在配置文件中配置\n"
        "获取 API Key: https://www.alphavantage.co/support/#api-key"
    )

    return api_key


def format_datetime_for_api(date_str: str) -> str:
    """Format date time to format required by Alpha Vantage API

Args:
date str: Date string, format YYYY-MM-DD

Returns:
Date time string after formatting, format YYYYMMDDHHMM
"""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%Y%m%dT0000")
    except Exception as e:
        logger.warning(f"Could not close temporary folder: %s{date_str}: {e}, using original value")
        return date_str


def _make_api_request(
    function: str,
    params: Dict[str, Any],
    max_retries: int = 3,
    retry_delay: int = 2
) -> Dict[str, Any] | str:
    """Launch AlphaVantage API Request

Args:
funct: API function name (e. g. NEWS SENTIMENT, OWERVIEW, etc.)
Params: Request for digitization
max retries: maximum number of retries
retry delay: retry delay (sec)

Returns:
JSON data or error message string for API response

Rices:
AlphaVantageRateLimitError: Speed Limit Error
AlphaVantageAPIError: API error
"""
    api_key = get_api_key()
    base_url = "https://www.alphavantage.co/query"
    
    #Build Request Parameters
    request_params = {
        "function": function,
        "apikey": api_key,
        **params
    }
    
    logger.debug(f"[Alpha Vantage]{function}: {params}")
    
    for attempt in range(max_retries):
        try:
            #Request initiated
            response = requests.get(base_url, params=request_params, timeout=30)
            response.raise_for_status()
            
            #Parsing Response
            data = response.json()
            
            #Can not open message
            if "Error Message" in data:
                error_msg = data["Error Message"]
                logger.error(f"[Alpha Vantage] API error:{error_msg}")
                raise AlphaVantageAPIError(f"Alpha Vantage API Error: {error_msg}")
            
            #Check speed limit
            if "Note" in data and "API call frequency" in data["Note"]:
                logger.warning(f"[Alpha Vantage]{data['Note']}")
                
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (attempt + 1)
                    logger.info(f"Wait.{wait_time}Try again in seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise AlphaVantageRateLimitError(
                        "Alpha Vantage API rate limit exceeded. "
                        "Please wait a moment and try again, or upgrade your API plan."
                    )
            
            #Check information fields (possibly containing restraining hints)
            if "Information" in data:
                info_msg = data["Information"]
                logger.warning(f"[Alpha Vantage]{info_msg}")
                
                #If speed limit information
                if "premium" in info_msg.lower() or "limit" in info_msg.lower():
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (attempt + 1)
                        logger.info(f"Wait.{wait_time}Try again in seconds...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise AlphaVantageRateLimitError(
                            f"Alpha Vantage API limit: {info_msg}"
                        )
            
            #Successfully accessed data
            logger.debug(f"[Alpha Vantage]{function}")
            return data
            
        except requests.exceptions.Timeout:
            logger.warning(f"[Alpha Vantage]{attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            else:
                raise AlphaVantageAPIError("Alpha Vantage API request timeout")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"[Alpha Vantage]{e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            else:
                raise AlphaVantageAPIError(f"Alpha Vantage API request failed: {e}")
        
        except json.JSONDecodeError as e:
            logger.error(f"[Alpha Vantage] JSON deciphered:{e}")
            raise AlphaVantageAPIError(f"Failed to parse Alpha Vantage API response: {e}")
    
    #All retrying failed.
    raise AlphaVantageAPIError(f"Failed to get data from Alpha Vantage after {max_retries} attempts")


def format_response_as_string(data: Dict[str, Any], title: str = "Alpha Vantage Data") -> str:
    """Format API responses into strings

Args:
Data: API response data
type: Data title

Returns:
Formatted String
"""
    try:
        #Add Head Information
        header = f"# {title}\n"
        header += f"# Data retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        #Convert to JSON String (Formatting)
        json_str = json.dumps(data, indent=2, ensure_ascii=False)
        
        return header + json_str
        
    except Exception as e:
        logger.error(f"Formatting response failed:{e}")
        return str(data)


def check_api_key_valid() -> bool:
    """Check if Alpha Vantage API Key is valid

Returns:
True If API Key is valid, or False
"""
    try:
        #Use simple API call test
        data = _make_api_request("GLOBAL_QUOTE", {"symbol": "IBM"})
        
        #Check for errors
        if isinstance(data, dict) and "Global Quote" in data:
            logger.info("Alpha Vantage API Key is working.")
            return True
        else:
            logger.warning("Alpha Vantage API Key may not be valid")
            return False
            
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        return False

