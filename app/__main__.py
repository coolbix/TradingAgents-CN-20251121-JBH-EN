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

def check_env_file_existence():
    """Check .env files existence. Verify the validity of the .env file"""
    import logging
    logger = logging.getLogger("app.startup")
    
    logger.info("Check .env files existence...")

    #Check Current Working Directory
    current_dir = Path.cwd()
    #logger.info(f"Current working directory:{current_dir}")

    #Check project root directory
    #logger.info(f"Project Root Directory:{project_root}")
    
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
                logger.info(f"Found .env file:{env_path}")
                #logger.info(f"File size:{env_path.stat().st_size} bytes")
                env_found = True

                #JBH TOBEDEL  #Read and display parts
                #JBH TOBEDEL  try:
                #JBH TOBEDEL      with open(env_path, 'r', encoding='utf-8') as f:
                #JBH TOBEDEL          lines = f.readlines()
                #JBH TOBEDEL      #logger.info(f"📄.env lines: {len(lines)}")
                #JBH TOBEDEL      for i, line in enumerate(lines[:10]):  #Show top 10 lines only
                #JBH TOBEDEL          line = line.strip()
                #JBH TOBEDEL          if line and not line.startswith('#'):
                #JBH TOBEDEL              #Hide Sensitive Information
                #JBH TOBEDEL              if any(keyword in line.upper() for keyword in ['SECRET', 'PASSWORD', 'TOKEN', 'KEY']):
                #JBH TOBEDEL                  key = line.split('=')[0] if '=' in line else line
                #JBH TOBEDEL                  logger.debug(f"  {key}=***")
                #JBH TOBEDEL              else:
                #JBH TOBEDEL                  logger.debug(f"  {line}")
                #JBH TOBEDEL      if len(lines) > 10:
                #JBH TOBEDEL          logger.info(f"And...{len(lines) - 10} All right.")
                #JBH TOBEDEL  except Exception as e:
                #JBH TOBEDEL      logger.warning(f"Error reading.env file:{e}")
            else:
                #If one is found, only other locations are recorded and there are files (possibly repeated)
                logger.debug(f".env files:{env_path}")

    if not env_found:
        logger.warning("⚠️ No. env files found, using default configuration")
        logger.info(f"💡 Hint: Please be at the root of the item(s){project_root}Create .env files")
    
    #logger.info("-" * 50)

try:
    from app.core.config import SETTINGS
    from app.core.develop_config import DEVELOP_CONFIG
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

    #Can not open message ??
    uvicorn_config = DEVELOP_CONFIG.get_uvicorn_config(SETTINGS.DEBUG)

    #Set the log configuration
    print("Setting up log configuration...")
    try:
        from app.core.logging_config import setup_logging as app_setup_logging
        app_setup_logging(SETTINGS.LOG_LEVEL)
    except Exception:
        #Back to development environment simplified log configuration
        DEVELOP_CONFIG.setup_logging(SETTINGS.DEBUG)
    #print("Log configuration complete")

    import logging
    logger = logging.getLogger("app.startup")
    
    logger.info("🚀 Starting TradingAgents-CN Backend...")
    logger.info(f"📍 Host: {SETTINGS.HOST}")
    logger.info(f"🔌 Port: {SETTINGS.PORT}")
    logger.info(f"🐛 Debug Mode: {SETTINGS.DEBUG}")
    logger.info(f"📚 API Docs: http://{SETTINGS.HOST}:{SETTINGS.PORT}/docs" if SETTINGS.DEBUG else "📚 API Docs: Disabled in production")
    
    #Print key configuration information
    logger.info("Key configuration information:")
    logger.info(f"  📊 MongoDB: {SETTINGS.MONGODB_HOST}:{SETTINGS.MONGODB_PORT}/{SETTINGS.MONGODB_DATABASE}")
    logger.info(f"  🔴 Redis: {SETTINGS.REDIS_HOST}:{SETTINGS.REDIS_PORT}/{SETTINGS.REDIS_DB}")
    logger.info(f"  🔐 JWT Secret: {'Configured' if SETTINGS.JWT_SECRET != 'change-me-in-production' else 'Use default value'}")
    logger.info(f"Log level:{SETTINGS.LOG_LEVEL}")
    
    #Check the loading status of environmental variables
    logger.info("State of loading of environmental variables:")
    env_vars_to_check = [
        ('MONGODB_HOST', SETTINGS.MONGODB_HOST, 'localhost'),
        ('MONGODB_PORT', str(SETTINGS.MONGODB_PORT), '27017'),
        ('MONGODB_DATABASE', SETTINGS.MONGODB_DATABASE, 'tradingagents'),
        ('REDIS_HOST', SETTINGS.REDIS_HOST, 'localhost'),
        ('REDIS_PORT', str(SETTINGS.REDIS_PORT), '6379'),
        ('JWT_SECRET', '***' if SETTINGS.JWT_SECRET != 'change-me-in-production' else SETTINGS.JWT_SECRET, 'change-me-in-production')
    ]
    
    for env_name, current_value, default_value in env_vars_to_check:
        status = "✅ 已设置" if current_value != default_value else "⚠️ 默认值"
        logger.info(f"  {env_name}: {current_value} ({status})")
    
    #logger.info("-" * 50)

    #JBH TOBEDEL  #Can not open message ??
    #JBH TOBEDEL  uvicorn_config = DEVELOP_CONFIG.get_uvicorn_config(SETTINGS.DEBUG)
    #JBH TOBEDEL  
    #JBH TOBEDEL  #Set the log configuration
    #JBH TOBEDEL  logger.info("Setting up log configuration...")
    #JBH TOBEDEL  try:
    #JBH TOBEDEL      from app.core.logging_config import setup_logging as app_setup_logging
    #JBH TOBEDEL      app_setup_logging(SETTINGS.LOG_LEVEL)
    #JBH TOBEDEL  except Exception:
    #JBH TOBEDEL      #Back to development environment simplified log configuration
    #JBH TOBEDEL      DEVELOP_CONFIG.setup_logging(SETTINGS.DEBUG)
    #JBH TOBEDEL  logger.info("Log configuration complete")

    #Check .env files after initialization of log system
    #logger.info("📋 Configuration Loading Phase:")
    check_env_file_existence()

    try:
        uvicorn.run(
            "app.main:app",
            host=SETTINGS.HOST,
            port=SETTINGS.PORT,
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
