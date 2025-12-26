"""Example Data Provider

Show how to create new data source providers
"""

from .example_sdk import ExampleSDKProvider

__all__ = [
    'ExampleSDKProvider',
]


def get_example_sdk_provider(**kwargs):
    """Get Example of SDK Provider"""
    return ExampleSDKProvider(**kwargs)

