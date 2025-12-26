#!/usr/bin/env python3
"""Use log data model
Use statistics and cost tracking for Token
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class UsageRecord:
    """Use records"""
    timestamp: str  #Timetamp
    provider: str  #Vendors
    model_name: str  #Model Name
    input_tokens: int  #Enter number of tokens
    output_tokens: int  #Output number tokens
    cost: float  #Cost
    currency: str = "CNY"  #Currency units
    session_id: str = ""  #Session ID
    analysis_type: str = "stock_analysis"  #Analysis Type


@dataclass
class ModelConfig:
    """Model Configuration"""
    provider: str  #Vendor: Dashscope, openai, google, etc.
    model_name: str  #Model Name
    api_key: str  #API Key
    base_url: Optional[str] = None  #Custom API Address
    max_tokens: int = 4000  #Maximum number of tokens
    temperature: float = 0.7  #Temperature parameters
    enabled: bool = True  #Enable


@dataclass
class PricingConfig:
    """Pricing Configuration"""
    provider: str  #Vendors
    model_name: str  #Model Name
    input_price_per_1k: float  #Enter token price (per 1,000 tokens)
    output_price_per_1k: float  #Output token price (per 1,000 tokens)
    currency: str = "CNY"  #Currency units

