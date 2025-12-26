"""Start Configuration Validator

Required configuration items for the start-up of a certification system, providing friendly error tips.
"""

import os
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class ConfigLevel(Enum):
    """Configure Level"""
    REQUIRED = "required"      #Cannot start without necessary configuration
    RECOMMENDED = "recommended"  #Recommended configuration, missing will affect functionality
    OPTIONAL = "optional"      #Optional configuration, missing without affecting basic functions


@dataclass
class ConfigItem:
    """Configure Item"""
    key: str                    #Configure Keyname
    level: ConfigLevel          #Configure Level
    description: str            #Configure Description
    example: Optional[str] = None  #Configure Example
    help_url: Optional[str] = None  #Help Link
    validator: Optional[callable] = None  #Custom Authentication Functions


@dataclass
class ValidationResult:
    """Authentication Results"""
    success: bool               #Verify success
    missing_required: List[ConfigItem]  #Missing Required Configuration
    missing_recommended: List[ConfigItem]  #Missing Recommended Configuration
    invalid_configs: List[tuple[ConfigItem, str]]  #Invalid configuration (configuration item, error information)
    warnings: List[str]         #Warning Information


class StartupValidator:
    """Start Configuration Validator"""
    
    #Required Profile
    REQUIRED_CONFIGS = [
        ConfigItem(
            key="MONGODB_HOST",
            level=ConfigLevel.REQUIRED,
            description="MongoDB主机地址",
            example="localhost"
        ),
        ConfigItem(
            key="MONGODB_PORT",
            level=ConfigLevel.REQUIRED,
            description="MongoDB端口",
            example="27017",
            validator=lambda v: v.isdigit() and 1 <= int(v) <= 65535
        ),
        ConfigItem(
            key="MONGODB_DATABASE",
            level=ConfigLevel.REQUIRED,
            description="MongoDB数据库名称",
            example="tradingagents"
        ),
        ConfigItem(
            key="REDIS_HOST",
            level=ConfigLevel.REQUIRED,
            description="Redis主机地址",
            example="localhost"
        ),
        ConfigItem(
            key="REDIS_PORT",
            level=ConfigLevel.REQUIRED,
            description="Redis端口",
            example="6379",
            validator=lambda v: v.isdigit() and 1 <= int(v) <= 65535
        ),
        ConfigItem(
            key="JWT_SECRET",
            level=ConfigLevel.REQUIRED,
            description="JWT密钥（用于生成认证令牌）",
            example="your-super-secret-jwt-key-change-in-production",
            validator=lambda v: len(v) >= 16
        ),
    ]
    
    #Recommended Configuration
    RECOMMENDED_CONFIGS = [
        ConfigItem(
            key="DEEPSEEK_API_KEY",
            level=ConfigLevel.RECOMMENDED,
            description="DeepSeek API密钥（推荐，性价比高）",
            example="sk-xxx",
            help_url="https://platform.deepseek.com/"
        ),
        ConfigItem(
            key="DASHSCOPE_API_KEY",
            level=ConfigLevel.RECOMMENDED,
            description="阿里百炼API密钥（推荐，国产稳定）",
            example="sk-xxx",
            help_url="https://dashscope.aliyun.com/"
        ),
        ConfigItem(
            key="TUSHARE_TOKEN",
            level=ConfigLevel.RECOMMENDED,
            description="Tushare Token（推荐，专业A股数据）",
            example="xxx",
            help_url="https://tushare.pro/register?reg=tacn"
        ),
    ]
    
    def __init__(self):
        self.result = ValidationResult(
            success=True,
            missing_required=[],
            missing_recommended=[],
            invalid_configs=[],
            warnings=[]
        )

    def _is_valid_api_key(self, api_key: str) -> bool:
        """Determines whether API Key is valid (not placeholder)

Args:
api key: API Key to be validated

Returns:
Bool: True is valid, False is invalid or occupied Arguments
"""
        if not api_key:
            return False

        #Remove First End Spaces and Quotes
        api_key = api_key.strip().strip('"').strip("'")

        #Check if empty
        if not api_key:
            return False

        #Check for placeholder (prefix)
        if api_key.startswith('your_') or api_key.startswith('your-'):
            return False

        #Check for placeholders (suffix)
        if api_key.endswith('_here') or api_key.endswith('-here'):
            return False

        #Check length (most API Key > 10 characters)
        if len(api_key) <= 10:
            return False

        return True

    def validate(self) -> ValidationResult:
        """Authentication Configuration

Returns:
ValidationResult: Validation results
"""
        logger.info("Start authenticating startup configuration...")
        
        #Authentication Required Configuration
        self._validate_required_configs()
        
        #Verify Recommended Configuration
        self._validate_recommended_configs()
        
        #Check security configuration
        self._check_security_configs()
        
        #Set validation results
        self.result.success = len(self.result.missing_required) == 0 and len(self.result.invalid_configs) == 0
        
        #Output validation results
        self._print_validation_result()
        
        return self.result
    
    def _validate_required_configs(self):
        """Authentication Required Configuration"""
        for config in self.REQUIRED_CONFIGS:
            value = os.getenv(config.key)
            
            if not value:
                self.result.missing_required.append(config)
                logger.error(f"There is a lack of necessary configuration:{config.key}")
            elif config.validator and not config.validator(value):
                self.result.invalid_configs.append((config, "配置值格式不正确"))
                logger.error(f"Configure format error:{config.key}")
            else:
                logger.debug(f"✅ {config.key}: configured")
    
    def _validate_recommended_configs(self):
        """Verify Recommended Configuration"""
        for config in self.RECOMMENDED_CONFIGS:
            value = os.getenv(config.key)

            if not value:
                self.result.missing_recommended.append(config)
                logger.warning(f"There is no recommended configuration:{config.key}")
            elif not self._is_valid_api_key(value):
                #API Key exists but is not configured
                self.result.missing_recommended.append(config)
                logger.warning(f"⚠️  {config.key}Configure as placeholder, not configured")
            else:
                logger.debug(f"✅ {config.key}: configured")
    
    def _check_security_configs(self):
        """Check security configuration"""
        #Checks whether JWT keys use default values
        jwt_secret = os.getenv("JWT_SECRET", "")
        if jwt_secret in ["change-me-in-production", "your-super-secret-jwt-key-change-in-production"]:
            self.result.warnings.append(
                "⚠️  JWT_SECRET 使用默认值，生产环境请务必修改！"
            )
        
        #Check if CSRF keys use default values
        csrf_secret = os.getenv("CSRF_SECRET", "")
        if csrf_secret in ["change-me-csrf-secret", "your-csrf-secret-key-change-in-production"]:
            self.result.warnings.append(
                "⚠️  CSRF_SECRET 使用默认值，生产环境请务必修改！"
            )
        
        #Check for DEBUG models in the production environment
        debug = os.getenv("DEBUG", "true").lower() in ("true", "1", "yes", "on")
        if not debug:
            logger.info("Production environment model")
        else:
            logger.info("ℹ️ Develop an environmental model (DBUG=true)")
    
    def _print_validation_result(self):
        """Output validation results"""
        logger.info("\n" + "=" * 70)
        logger.info("TradingAgents-CN Configuration Validation Result")
        logger.info("=" * 70)
        
        #Required Configuration
        if self.result.missing_required:
            logger.info("\nMissing required configurations:")
            for config in self.result.missing_required:
                logger.info(f"   - {config.key}")
                logger.info(f"     Description: {config.description}")
                if config.example:
                    logger.info(f"     Example: {config.example}")
                if config.help_url:
                    logger.info(f"     Help: {config.help_url}")
        else:
            logger.info("\nAll required configurations are complete")

        #Invalid Configuration
        if self.result.invalid_configs:
            logger.info("\nInvalid configurations:")
            for config, error in self.result.invalid_configs:
                logger.info(f"   - {config.key}: {error}")
                if config.example:
                    logger.info(f"     Example: {config.example}")

        #Recommended Configuration
        if self.result.missing_recommended:
            logger.info("\nMissing recommended configurations (won't affect startup):")
            for config in self.result.missing_recommended:
                logger.info(f"   - {config.key}")
                logger.info(f"     Description: {config.description}")
                if config.help_url:
                    logger.info(f"     Get it from: {config.help_url}")

        #Warning Information
        if self.result.warnings:
            logger.info("\nSecurity warnings:")
            for warning in self.result.warnings:
                logger.info(f"   - {warning}")

        #Summary
        logger.info("\n" + "=" * 70)
        if self.result.success:
            logger.info("Configuration validation passed, system can start")
            if self.result.missing_recommended:
                logger.info("Tip: Configure recommended items for better functionality")
        else:
            logger.info("Configuration validation failed, please check the above items")
            logger.info("Configuration guide: docs/configuration_guide.md")
        logger.info("=" * 70 + "\n")
    
    def raise_if_failed(self):
        """If the authentication fails, the anomaly is dropped."""
        if not self.result.success:
            error_messages = []
            
            if self.result.missing_required:
                error_messages.append(
                    f"缺少必需配置: {', '.join(c.key for c in self.result.missing_required)}"
                )
            
            if self.result.invalid_configs:
                error_messages.append(
                    f"配置格式错误: {', '.join(c.key for c, _ in self.result.invalid_configs)}"
                )
            
            raise ConfigurationError(
                "配置验证失败:\n" + "\n".join(f"  • {msg}" for msg in error_messages) +
                "\n\n请检查 .env 文件并参考 docs/configuration_guide.md"
            )


class ConfigurationError(Exception):
    """Cannot initialise Evolution's mail component."""
    pass


def validate_startup_config() -> ValidationResult:
    """Validate startup configuration (facility function)

Returns:
ValidationResult: Validation results

Rices:
Configuration Error: If authentication fails
"""
    validator = StartupValidator()
    result = validator.validate()
    validator.raise_if_failed()
    return result

