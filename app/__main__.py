"""TradingAgendas-CN Boxend Entry Point
Support python-m app startup mode
"""

import uvicorn
import sys
import os
from pathlib import Path

# ============================================================================
#Global UTF-8 coding settings (must start at start, support emoji and Chinese)
# ============================================================================
if sys.platform == 'win32':
    try:
        #1. Set up environmental variables to use UTF-8 globally for Python
        os.environ['PYTHONIOENCODING'] = 'utf-8'
        os.environ['PYTHONUTF8'] = '1'

        #2. Set standard output and error output to UTF-8
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

        #3. Attempt to set the console code page to UTF-8 (65001)
        try:
            import ctypes
            ctypes.windll.kernel32.SetConsoleCP(65001)
            ctypes.windll.kernel32.SetConsoleOutputCP(65001)
        except Exception:
            pass

    except Exception as e:
        #If settings fail, print warning but continue running
        print(f"Warning: Failed to set UTF-8 encoding: {e}", file=sys.stderr)

#Add Item Root Directory to Python Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

#Check and print. Env files to load information
def check_env_file():
    """Check and print. Env files to load information"""
    import logging
    logger = logging.getLogger("app.startup")
    
    logger.info("Check the environment profile...")

    #Check Current Working Directory
    current_dir = Path.cwd()
    logger.info(f"Current working directory:{current_dir}")

    #Check project root directory
    logger.info(f"Project Root Directory:{project_root}")
    
    #Check for possible .env file locations (in order of priority)
    env_locations = [
        project_root / ".env",          #Priority: Project Root Directory (standard location)
        current_dir / ".env",           #Subselection: Current working directory
        Path(__file__).parent / ".env"  #Final: under app directory (not recommended)
    ]

    env_found = False

    for env_path in env_locations:
        if env_path.exists():
            if not env_found:  #Show only first found file details
                logger.info(f"Found .env files:{env_path}")
                logger.info(f"File size:{env_path.stat().st_size} bytes")
                env_found = True

                #Read and display parts (hidden sensitive information)
                try:
                    with open(env_path, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                    logger.info(f"📄.env document preview{len(lines)}Line:")
                    for i, line in enumerate(lines[:10]):  #Show top 10 lines only
                        line = line.strip()
                        if line and not line.startswith('#'):
                            #Hide Sensitive Information
                            if any(keyword in line.upper() for keyword in ['SECRET', 'PASSWORD', 'TOKEN', 'KEY']):
                                key = line.split('=')[0] if '=' in line else line
                                logger.info(f"  {key}=***")
                            else:
                                logger.info(f"  {line}")
                    if len(lines) > 10:
                        logger.info(f"And...{len(lines) - 10}All right.")
                except Exception as e:
                    logger.warning(f"Error reading.env file:{e}")
            else:
                #If one is found, only other locations are recorded and there are files (possibly repeated)
                logger.debug(f".env files:{env_path}")

    if not env_found:
        logger.warning("⚠️ No. env files found, using default configuration")
        logger.info(f"💡 Hint: Please be at the root of the item(s){project_root}Create .env files")
    
    logger.info("-" * 50)

try:
    from app.core.config import settings
    from app.core.dev_config import DEV_CONFIG
except Exception as e:
    import traceback
    print(f"❌ 导入配置模块失败: {e}")
    print("📋 详细错误信息:")
    print("-" * 50)
    traceback.print_exc()
    print("-" * 50)
    sys.exit(1)


def main():
    """Main Start Function"""
    import logging
    logger = logging.getLogger("app.startup")
    
    logger.info("🚀 Starting TradingAgents-CN Backend...")
    logger.info(f"📍 Host: {settings.HOST}")
    logger.info(f"🔌 Port: {settings.PORT}")
    logger.info(f"🐛 Debug Mode: {settings.DEBUG}")
    logger.info(f"📚 API Docs: http://{settings.HOST}:{settings.PORT}/docs" if settings.DEBUG else "📚 API Docs: Disabled in production")
    
    #Print key configuration information
    logger.info("Key configuration information:")
    logger.info(f"  📊 MongoDB: {settings.MONGODB_HOST}:{settings.MONGODB_PORT}/{settings.MONGODB_DATABASE}")
    logger.info(f"  🔴 Redis: {settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}")
    logger.info(f"  🔐 JWT Secret: {'Configured' if settings.JWT_SECRET != 'change-me-in-production' else 'Use default value'}")
    logger.info(f"Log level:{settings.LOG_LEVEL}")
    
    #Check the loading status of environmental variables
    logger.info("State of loading of environmental variables:")
    env_vars_to_check = [
        ('MONGODB_HOST', settings.MONGODB_HOST, 'localhost'),
        ('MONGODB_PORT', str(settings.MONGODB_PORT), '27017'),
        ('MONGODB_DATABASE', settings.MONGODB_DATABASE, 'tradingagents'),
        ('REDIS_HOST', settings.REDIS_HOST, 'localhost'),
        ('REDIS_PORT', str(settings.REDIS_PORT), '6379'),
        ('JWT_SECRET', '***' if settings.JWT_SECRET != 'change-me-in-production' else settings.JWT_SECRET, 'change-me-in-production')
    ]
    
    for env_name, current_value, default_value in env_vars_to_check:
        status = "✅ 已设置" if current_value != default_value else "⚠️ 默认值"
        logger.info(f"  {env_name}: {current_value} ({status})")
    
    logger.info("-" * 50)

    #Can not open message
    uvicorn_config = DEV_CONFIG.get_uvicorn_config(settings.DEBUG)

    #Set a simplified log configuration
    logger.info("Setting up log configuration...")
    try:
        from app.core.logging_config import setup_logging as app_setup_logging
        app_setup_logging(settings.LOG_LEVEL)
    except Exception:
        #Back to development environment simplified log configuration
        DEV_CONFIG.setup_logging(settings.DEBUG)
    logger.info("Log configuration complete")

    #Check .env files after initialization of log system
    logger.info("📋 Configuration Loading Phase:")
    check_env_file()

    try:
        uvicorn.run(
            "app.main:app",
            host=settings.HOST,
            port=settings.PORT,
            **uvicorn_config
        )
    except KeyboardInterrupt:
        logger.info("🛑 Server stopped by user")
    except Exception as e:
        import traceback
        logger.error(f"❌ Failed to start server: {e}")
        logger.error("Can not open message")
        logger.error("-" * 50)
        traceback.print_exc()
        logger.error("-" * 50)
        sys.exit(1)


if __name__ == "__main__":
    main()
