"""Configure System Compatibility Layer

For the old Tradingagents library, a configuration compatible interface is provided.
Enables them to use the new configuration system without changing the code.

This module is only for backward compatibility, and the new code should use ConfigService directly
"""

import os
import asyncio
from typing import Dict, Any, Optional, List
from functools import lru_cache
import warnings

from app.core.config import SETTINGS


class ConfigManagerCompat:
    """ConfigManager Compatible Class

    Provides the same interface as the old ConfigManager but uses the new configuration system.
    """
    
    def __init__(self):
        """Initialise Compatibility Layer"""
        self._warned = False
        self._emit_deprecation_warning()
    
    def _emit_deprecation_warning(self):
        """Warning of abandonment (only once)"""
        if not self._warned:
            warnings.warn(
                "ConfigManagerCompat is a compatibility layer for legacy code. "
                "Please migrate to app.services.config_service.ConfigService. "
                "See docs/DEPRECATION_NOTICE.md for details.",
                DeprecationWarning,
                stacklevel=3
            )
            self._warned = True
    
    def get_data_dir(self) -> str:
        """Get Data Directory

        Returns:
            str: Data Directory Path
        """
        #Prioritize reading from environmental variables
        data_dir = os.getenv("DATA_DIR")
        if data_dir:
            return data_dir
        
        #Default value
        return "./data"
    
    def load_settings(self) -> Dict[str, Any]:
        """Load System Settings

        Returns:
            Dict[str, Any]: System settings dictionary
        """
        try:
            #Try loading from the new configuration system
            from app.services.config_service import CONFIG_SERVICE
            
            #Run step code in sync context
            loop = asyncio.get_event_loop()
            if loop.is_running():
                #Returns the default if the event cycle is running
                return self._get_default_settings()
            else:
                config = loop.run_until_complete(CONFIG_SERVICE.get_system_config_from_database())
                if config and config.system_settings:
                    return config.system_settings
        except Exception:
            pass
        
        #Return Default Settings
        return self._get_default_settings()
    
    def save_settings(self, settings_dict: Dict[str, Any]) -> bool:
        """Save System Settings

        Args:
            Settings dict: System settings dictionary

        Returns:
            Bool: Save successfully
        """
        try:
            from app.services.config_service import CONFIG_SERVICE
            
            loop = asyncio.get_event_loop()
            if loop.is_running():
                #Could not save event cycle if running
                warnings.warn("Cannot save settings in running event loop", RuntimeWarning)
                return False
            else:
                loop.run_until_complete(
                    CONFIG_SERVICE.update_system_settings(settings_dict)
                )
                return True
        except Exception as e:
            warnings.warn(f"Failed to save settings: {e}", RuntimeWarning)
            return False
    
    def get_models(self) -> List[Dict[str, Any]]:
        """Get Model Configuration List

        Returns:
            List [Dict [str, Any]: Model configuration list
        """
        try:
            from app.services.config_service import CONFIG_SERVICE
            
            loop = asyncio.get_event_loop()
            if loop.is_running():
                return []
            else:
                config = loop.run_until_complete(CONFIG_SERVICE.get_system_config_from_database())
                if config and config.llm_configs:
                    return [
                        {
                            "provider": llm.provider,
                            "model_name": llm.model_name,
                            "api_key": llm.api_key or "",
                            "base_url": llm.base_url,
                            "max_tokens": llm.max_tokens,
                            "temperature": llm.temperature,
                            "enabled": llm.enabled,
                        }
                        for llm in config.llm_configs
                    ]
        except Exception:
            pass
        
        return []
    
    def get_model_config(self, provider: str, model_name: str) -> Optional[Dict[str, Any]]:
        """Get the configuration of the specified model

        Args:
            provider: Provider name
            Model name: Model name

        Returns:
            Optional [Dict [str, Any]: model configuration, return None if not available
        """
        models = self.get_models()
        for model in models:
            if model["provider"] == provider and model["model_name"] == model_name:
                return model
        return None
    
    def _get_default_settings(self) -> Dict[str, Any]:
        """Get Default System Settings

        Returns:
            Dict[str, Any]: Default settings
        """
        return {
            "max_debate_rounds": 1,
            "max_risk_discuss_rounds": 1,
            "online_tools": True,
            "online_news": True,
            "realtime_data": False,
            "memory_enabled": True,
            "debug": False,
        }


class TokenTrackerCompat:
    """TokenTracker Compatible

    Provides the same interface as the old TokenTracker.
    """
    
    def __init__(self):
        """Initialise Compatibility Layer"""
        self._usage_data = {}
    
    def track_usage(
        self,
        provider: str,
        model_name: str,
        input_tokens: int,
        output_tokens: int,
        cost: float = 0.0
    ):
        """Record Token Usage

        Args:
            provider: Provider name
            Model name: Model name
            Input tokens: Enter Token Number
            Output tokens: Output Token Number
            cost
        """
        key = f"{provider}:{model_name}"
        
        if key not in self._usage_data:
            self._usage_data[key] = {
                "provider": provider,
                "model_name": model_name,
                "total_input_tokens": 0,
                "total_output_tokens": 0,
                "total_cost": 0.0,
                "call_count": 0,
            }
        
        self._usage_data[key]["total_input_tokens"] += input_tokens
        self._usage_data[key]["total_output_tokens"] += output_tokens
        self._usage_data[key]["total_cost"] += cost
        self._usage_data[key]["call_count"] += 1

        #Note: This compatibility layer provides memory caches only and does not last to data Library
        #Use app.services.llm service for sustainability
    
    def get_usage_summary(self) -> Dict[str, Any]:
        """Access to statistical summaries

        Returns:
            Dict[str, Any]: Use statistical abstract
        """
        return self._usage_data.copy()
    
    def reset_usage(self):
        """Reset Usage Statistics"""
        self._usage_data.clear()


#Create global examples (for backward compatibility)
config_manager_compat = ConfigManagerCompat()
token_tracker_compat = TokenTrackerCompat()


#Easy Functions
def get_config_manager() -> ConfigManagerCompat:
    """Get Profile Manager Compatibility Examples

    Returns:
        ConfigManagercompat: Profile Manager Compatibility Example
    """
    return config_manager_compat


def get_token_tracker() -> TokenTrackerCompat:
    """Get Token Tracker Compatibility Examples

    Returns:
        TokenTrackercompat: Token tracker compatibility examples
    """
    return token_tracker_compat

