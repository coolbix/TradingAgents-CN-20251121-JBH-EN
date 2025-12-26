#!/usr/bin/env python3
"""Environmental variable resolution tool
Provide robust environmental variable resolution compatible with Python 3.13+
"""

import os
from typing import Any, Union, Optional


def parse_bool_env(env_var: str, default: bool = False) -> bool:
    """Parsing Boolean-type environment variables in many formats

Supported format:
- True/True/True
- false/False/FALSE
- 1/0.
- Yes/yes/YES
- No/No/NO
- On/On/ON
- off/off/IFF

Args:
env var: Environment variable First Name
default:

Returns:
Bool: parsed boolean value
"""
    value = os.getenv(env_var)
    
    if value is None:
        return default
    
    #Convert to string and remove blanks
    value_str = str(value).strip()
    
    if not value_str:
        return default
    
    #Convert to lowercase comparison
    value_lower = value_str.lower()
    
    #Real List
    true_values = {
        'true', '1', 'yes', 'on', 'enable', 'enabled', 
        't', 'y', 'ok', 'okay'
    }
    
    #Fake Value List
    false_values = {
        'false', '0', 'no', 'off', 'disable', 'disabled',
        'f', 'n', 'none', 'null', 'nil'
    }
    
    if value_lower in true_values:
        return True
    elif value_lower in false_values:
        return False
    else:
        #If not recognized, record warning and return default value
        print(f"⚠️ 无法解析环境变量 {env_var}='{value}'，使用默认值 {default}")
        return default


def parse_int_env(env_var: str, default: int = 0) -> int:
    """Parsing integer-type environment variables

Args:
env var: Environment variable First Name
default:

Returns:
int: integer value after resolution
"""
    value = os.getenv(env_var)
    
    if value is None:
        return default
    
    try:
        return int(value.strip())
    except (ValueError, AttributeError):
        print(f"⚠️ 无法解析环境变量 {env_var}='{value}' 为整数，使用默认值 {default}")
        return default


def parse_float_env(env_var: str, default: float = 0.0) -> float:
    """Parsing floating point type environment variable

Args:
env var: Environment variable First Name
default:

Returns:
Float: float value after resolution
"""
    value = os.getenv(env_var)
    
    if value is None:
        return default
    
    try:
        return float(value.strip())
    except (ValueError, AttributeError):
        print(f"⚠️ 无法解析环境变量 {env_var}='{value}' 为浮点数，使用默认值 {default}")
        return default


def parse_str_env(env_var: str, default: str = "") -> str:
    """Parsing string type environment variable

Args:
env var: Environment variable First Name
default:

Returns:
st: string values after resolution
"""
    value = os.getenv(env_var)
    
    if value is None:
        return default
    
    return str(value).strip()


def parse_list_env(env_var: str, separator: str = ",", default: Optional[list] = None) -> list:
    """Parsing list type environment variable

Args:
env var: Environment variable First Name
separator:
default:

Returns:
list: list after resolution
"""
    if default is None:
        default = []
    
    value = os.getenv(env_var)
    
    if value is None:
        return default
    
    try:
        #Split and remove spaces
        items = [item.strip() for item in value.split(separator)]
        #Filter empty string
        return [item for item in items if item]
    except AttributeError:
        print(f"⚠️ 无法解析环境变量 {env_var}='{value}' 为列表，使用默认值 {default}")
        return default


def get_env_info(env_var: str) -> dict:
    """Get detailed information on environmental variables

Args:
env var: Environment variable First Name

Returns:
dict: Environmental variable information
"""
    value = os.getenv(env_var)
    
    return {
        'name': env_var,
        'value': value,
        'exists': value is not None,
        'empty': value is None or str(value).strip() == '',
        'type': type(value).__name__ if value is not None else 'None',
        'length': len(str(value)) if value is not None else 0
    }


def validate_required_env_vars(required_vars: list) -> dict:
    """Verify whether the necessary environmental variables are set

Args:
list of required environment variables

Returns:
dict: Verify results
"""
    results = {
        'all_set': True,
        'missing': [],
        'empty': [],
        'valid': []
    }
    
    for var in required_vars:
        info = get_env_info(var)
        
        if not info['exists']:
            results['missing'].append(var)
            results['all_set'] = False
        elif info['empty']:
            results['empty'].append(var)
            results['all_set'] = False
        else:
            results['valid'].append(var)
    
    return results


#Compatibility function: maintain backward compatibility
def get_bool_env(env_var: str, default: bool = False) -> bool:
    """Backward compatible boolean resolution function"""
    return parse_bool_env(env_var, default)


def get_int_env(env_var: str, default: int = 0) -> int:
    """Backcompatible Integer Parsing Function"""
    return parse_int_env(env_var, default)


def get_str_env(env_var: str, default: str = "") -> str:
    """Backcompatible string resolution function"""
    return parse_str_env(env_var, default)


#Export Main Functions
__all__ = [
    'parse_bool_env',
    'parse_int_env', 
    'parse_float_env',
    'parse_str_env',
    'parse_list_env',
    'get_env_info',
    'validate_required_env_vars',
    'get_bool_env',  #Backward compatibility
    'get_int_env',   #Backward compatibility
    'get_str_env'    #Backward compatibility
]
