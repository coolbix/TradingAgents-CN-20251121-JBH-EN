"""Develop Environmental Configuration
Optimizing development experience and reducing unnecessary document monitoring
"""

import logging
from typing import List, Optional


class DevelopConfig:
    """Develop Environmental Configuration Category"""
    
    #File Monitor Configuration
    RELOAD_DIRS: List[str] = ["app"]
    
    #Excluded files and directories
    RELOAD_EXCLUDES: List[str] = [
        #Python cache file
        "__pycache__",
        "*.pyc",
        "*.pyo", 
        "*.pyd",
        
        #Version Control
        ".git",
        ".gitignore",
        
        #Test and Cache
        ".pytest_cache",
        ".coverage",
        "htmlcov",
        
        #Log File
        "*.log",
        "logs",
        
        #Temporary documents
        "*.tmp",
        "*.temp",
        "*.swp",
        "*.swo",
        
        #System File
        ".DS_Store",
        "Thumbs.db",
        "desktop.ini",
        
        #IDE File
        ".vscode",
        ".idea",
        "*.sublime-*",
        
        #Data Files
        "*.db",
        "*.sqlite",
        "*.sqlite3",
        
        #Profile (avoiding reloading of sensitive information)
        ".env",
        ".env.local",
        ".env.production",
        
        #Documents and static files
        "*.md",
        "*.txt",
        "*.json",
        "*.yaml",
        "*.yml",
        "*.toml",
        
        #Frontend File
        "node_modules",
        "dist",
        "build",
        "*.js",
        "*.css",
        "*.html",
        
        #Other
        "requirements*.txt",
        "Dockerfile*",
        "docker-compose*"
    ]
    
    #File type monitored only
    RELOAD_INCLUDES: List[str] = [
        "*.py"
    ]
    
    #Reload delay (sec)
    RELOAD_DELAY: float = 0.5
    
    #Log Configuration
    LOG_LEVEL: str = "info"
    
    #Whether to show access logs
    ACCESS_LOG: bool = True
    
    @classmethod
    def get_uvicorn_config(cls, debug: bool = True) -> dict:
        """Can not open message"""
        #Uniquely disable reload to avoid log configuration conflicts
        return {
            "reload": False,  #Disable auto-reload, restart manually
            "log_level": cls.LOG_LEVEL,
            "access_log": cls.ACCESS_LOG,
            #Ensure the use of our custom log configuration
            "log_config": None  #Disable uvicorn default log configuration, using our configuration
        }
    
    @classmethod
    def setup_logging(cls, debug: bool = True):
        """Set a simplified log configuration"""
        #Set a uniform log format
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            force=True  #Force reconfiguration to overwrite previous settings
        )

        if debug:
            #Development environment: noise reduction log
            logging.getLogger("watchfiles").setLevel(logging.ERROR)
            logging.getLogger("watchfiles.main").setLevel(logging.ERROR)
            logging.getLogger("watchfiles.watcher").setLevel(logging.ERROR)

            #Ensure that important logs are displayed properly
            logging.getLogger("webapi").setLevel(logging.INFO)
            logging.getLogger("app.core.database").setLevel(logging.INFO)
            logging.getLogger("uvicorn.error").setLevel(logging.INFO)

            #Test whether webapi logger works
            webapi_logger = logging.getLogger("webapi")
            webapi_logger.info("Test Message")
        else:
            #Production environment: stricter log control
            logging.getLogger("watchfiles").setLevel(logging.ERROR)
            logging.getLogger("uvicorn").setLevel(logging.WARNING)


#Develop Environment Shortcut
DEVELOP_CONFIG = DevelopConfig()
