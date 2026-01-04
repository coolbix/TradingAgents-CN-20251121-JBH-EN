"""ChromaDB Unified Configuration Module
Support automatic adaptation of Windows 10/11 and other operating systems
"""
import os
import platform
import chromadb
from chromadb.config import Settings


def is_windows_11() -> bool:
    """Test for Windows 11

    Returns:
        Bool: If Windows 11 returns True, otherwise returns False
    """
    if platform.system() != "Windows":
        return False
    
    #Windows 11's version number is usually 10.22,000 or higher.
    version = platform.version()
    try:
        #Ripping version number, usually in "10.0.26100."
        version_parts = version.split('.')
        if len(version_parts) >= 3:
            build_number = int(version_parts[2])
            #Windows 11 build code from 22000
            return build_number >= 22000
    except (ValueError, IndexError):
        pass
    
    return False


def get_win10_chromadb_client():
    """Get Windows 10 compatible ChromaDB client

    Returns:
        chromadb. Clinic: ChromaDB client example
    """
    settings = Settings(
        allow_reset=True,
        anonymized_telemetry=False,
        is_persistent=False,
        #Windows 10 Specific Configuration
        chroma_db_impl="duckdb+parquet",
        chroma_api_impl="chromadb.api.segment.SegmentAPI",
        #Use a temporary directory to avoid privileges
        persist_directory=None
    )
    
    try:
        client = chromadb.Client(settings)
        return client
    except Exception as e:
        #Down to Minimum Configuration
        basic_settings = Settings(
            allow_reset=True,
            is_persistent=False
        )
        return chromadb.Client(basic_settings)


def get_win11_chromadb_client():
    """Get Windows 11 Optimized ChromaDB Client

    Returns:
        chromadb. Clinic: ChromaDB client example
    """
    #Windows 11 Better support for ChromaDB, using a more modern configuration
    settings = Settings(
        allow_reset=True,
        anonymized_telemetry=False,  #Disable telemetry to avoid posthog error
        is_persistent=False,
        #Windows 11 can be achieved by default and performance is better
        chroma_db_impl="duckdb+parquet",
        chroma_api_impl="chromadb.api.segment.SegmentAPI"
        #Remove paper directory=None to use the default value
    )
    
    try:
        client = chromadb.Client(settings)
        return client
    except Exception as e:
        #If there's any problem, use the simplest configuration.
        minimal_settings = Settings(
            allow_reset=True,
            anonymized_telemetry=False,  #Critical: Disable telemetry
            is_persistent=False
        )
        return chromadb.Client(minimal_settings)


def get_optimal_chromadb_client():
    """Automatically select the preferred ChromaDB configuration from the operating system

    Returns:
        chromadb. Clinic: ChromaDB client example
    """
    system = platform.system()
    
    if system == "Windows":
        #Use more accurate Windows 11 testing
        if is_windows_11():
            #Windows 11 or update
            return get_win11_chromadb_client()
        else:
            #Windows 10 or older, use compatible configuration
            return get_win10_chromadb_client()
    else:
        #Non Windows, using standard configuration
        settings = Settings(
            allow_reset=True,
            anonymized_telemetry=False,
            is_persistent=False
        )
        return chromadb.Client(settings)


#Export Configuration
__all__ = [
    'get_optimal_chromadb_client',
    'get_win10_chromadb_client',
    'get_win11_chromadb_client',
    'is_windows_11'
]

