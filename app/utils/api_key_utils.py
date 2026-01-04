"""API Key Processing Tool Function

Provide uniform API Key authentication, abbreviation, environmental variable reading, etc.
"""

import os
from typing import Optional


def is_valid_api_key(api_key: Optional[str]) -> bool:
    """Determines whether API Key is valid

    Effective API Key must satisfy:
    1. Can't be empty
    2. Length must > 10
    3. Not a placeholder (prefix: your , your-)
    4. Not a placeholder (suffix:  here,-here)
    A key that cannot be cut off (includes '...')

    Args:
        api key: API Key to verify

    Returns:
        Bool: Effective
    """
    if not api_key:
        return False
    
    api_key = api_key.strip()
    
    #1. Can't be empty
    if not api_key:
        return False
    
    #2. Length must > 10
    if len(api_key) <= 10:
        return False
    
    #Not a placeholder (prefix)
    if api_key.startswith('your_') or api_key.startswith('your-'):
        return False
    
    #No placeholder (suffix)
    if api_key.endswith('_here') or api_key.endswith('-here'):
        return False
    
    #A key that cannot be cut off (includes '...')
    if '...' in api_key:
        return False
    
    return True


def truncate_api_key(api_key: Optional[str]) -> Optional[str]:
    """API Key, show top six and bottom six.

    Example:
    Enter:'d1el869r01qghj41hgd1el869r01qghj41hai0'
    Output: 'd1el86...j41hai0'

    Args:
        api key: abbreviated API Key

    Returns:
        str: API Key after abbreviation returns the original value if the input is empty or long < = 12
    """
    if not api_key or len(api_key) <= 12:
        return api_key
    
    return f"{api_key[:6]}...{api_key[-6:]}"


def get_env_api_key_for_provider(provider_name: str) -> Optional[str]:
    """API Key from a large modeler from an environmental variable

    Environmental variable name format:   FMT 0   API KEY

    Args:
        provider name: manufacturer's name (e. g. 'deepseek', 'dashscope')

    Returns:
        str: API Key from the environment variable, returns None if it does not exist or is invalid
    """
    env_key_name = f"{provider_name.upper()}_API_KEY"
    env_key = os.getenv(env_key_name)
    
    if env_key and is_valid_api_key(env_key):
        return env_key
    
    return None


def get_env_api_key_for_datasource(ds_type: str) -> Optional[str]:
    """API Key for data sources from environmental variables

    Map of data source type to environmental variable name:
    - TUSHARE TOKEN
    - Finnhub.
    - Polygon.
    IEX API KEY
    Quindl API KEY
    - Alphavantage — ALPHAVANTAGE API KEY

    Args:
        ds type: data source type (e.g. 'tushare', 'finnhub')

    Returns:
        str: API Key from the environment variable, returns None if it does not exist or is invalid
    """
    #Map of data source type to environmental variable name
    env_key_map = {
        "tushare": "TUSHARE_TOKEN",
        "finnhub": "FINNHUB_API_KEY",
        "polygon": "POLYGON_API_KEY",
        "iex": "IEX_API_KEY",
        "quandl": "QUANDL_API_KEY",
        "alphavantage": "ALPHAVANTAGE_API_KEY",
    }
    
    env_key_name = env_key_map.get(ds_type.lower())
    if not env_key_name:
        return None
    
    env_key = os.getenv(env_key_name)
    
    if env_key and is_valid_api_key(env_key):
        return env_key
    
    return None


def should_skip_api_key_update(api_key: Optional[str]) -> bool:
    """Judge whether to skip API Key updates

    The following should skip the update (main value retained):
    API Key is the cut-off key (includes '...')
    API Key is a placeholder (your *, your-*)

    Args:
        api key: API Key to check

    Returns:
        Bool: Should Update
    """
    if not api_key:
        return False
    
    api_key = api_key.strip()
    
    #1. Interrupted key (includes '...')
    if '...' in api_key:
        return True
    
    #Placeholders
    if api_key.startswith('your_') or api_key.startswith('your-'):
        return True
    
    return False

