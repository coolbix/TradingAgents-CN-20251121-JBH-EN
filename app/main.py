"""TradingAgendas-CN v1.0.0-preview FastAPI Boxend
Main Application Entry

All rights served.
Copyright (c) 2025 hsliuping. Retention of title.

This software is proprietory and conservative.
Or use of this software, via any media, is perfectly protected.
This software is proprietary and confidential. The unauthorized reproduction, distribution or use of this software through any medium is prohibited.

For commercial purposes, please contact: hsliup@163.com
Business permit counseling, contact: hsliup@163.com
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import logging
import time
from datetime import datetime
from contextlib import asynccontextmanager
import asyncio
from pathlib import Path

from app.core.config import SETTINGS
from app.core.database import init_database_async, close_database_async
from app.core.logging_config import setup_logging
from app.routers import auth_db as auth, analysis, screening, queue, sse, health, favorites, config, reports, database, operation_logs, tags, tushare_init, akshare_init, baostock_init, historical_data, multi_period_sync, financial_data, news_data, social_media, internal_messages, usage_statistics, model_capabilities, cache, logs
from app.routers import sync as sync_router, multi_source_sync
from app.routers import stocks as stocks_router
from app.routers import stock_data as stock_data_router
from app.routers import stock_sync as stock_sync_router
from app.routers import multi_market_stocks as multi_market_stocks_router
from app.routers import notifications as notifications_router
from app.routers import websocket_notifications as websocket_notifications_router
from app.routers import scheduler as scheduler_router
from app.services.basics_sync_service import get_basics_sync_service
from app.services.multi_source_basics_sync_service import MultiSourceBasicsSyncService
from app.services.scheduler_service import set_scheduler_instance
from app.worker.tushare_sync_service import (
    run_tushare_basic_info_sync,
    run_tushare_quotes_sync,
    run_tushare_historical_sync,
    run_tushare_financial_sync,
    run_tushare_status_check
)
from app.worker.akshare_sync_service import (
    run_akshare_basic_info_sync,
    run_akshare_quotes_sync,
    run_akshare_historical_sync,
    run_akshare_financial_sync,
    run_akshare_status_check
)
from app.worker.baostock_sync_service import (
    run_baostock_basic_info_sync,
    run_baostock_daily_quotes_sync,
    run_baostock_historical_sync,
    run_baostock_status_check
)
#For Hong Kong and United States units read Acquire+Cache mode, no regular synchronization of tasks
# from app.worker.hk_sync_service import ...
# from app.worker.us_sync_service import ...
from app.middleware.operation_log_middleware import OperationLogMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from app.services.quotes_ingestion_service import QuotesIngestionService
from app.routers import paper as paper_router


def get_version() -> str:
    """Read version numbers from Version files"""
    try:
        version_file = Path(__file__).parent.parent / "VERSION"
        if version_file.exists():
            return version_file.read_text(encoding='utf-8').strip()
    except Exception:
        pass
    return "1.0.0"  #Default Version Number


async def _print_config_summary(logger):
    """Show Profile Summary"""
    try:
        logger.info("=" * 70)
        logger.info("📋 TradingAgents-CN Configuration Summary")
        logger.info("=" * 70)

        #.env file path information
        import os
        from pathlib import Path
        
        current_dir = Path.cwd()
        logger.info(f"📁 Current working directory: {current_dir}")
        
        #Check for possible .env file locations
        env_files_to_check = [
            current_dir / ".env",
            current_dir / "app" / ".env",
            Path(__file__).parent.parent / ".env",  #Project Root Directory
        ]
        
        logger.info("🔍 Checking .env file locations:")
        env_file_found = False
        for env_file in env_files_to_check:
            if env_file.exists():
                logger.info(f"  ✅ Found: {env_file} (size: {env_file.stat().st_size} bytes)")
                env_file_found = True
                #Show the front lines of the file (hidden sensitive information)
                try:
                    with open(env_file, 'r', encoding='utf-8') as f:
                        lines = f.readlines()[:5]  #First 5 lines only
                        logger.info(f"     Preview (first 5 lines):")
                        for i, line in enumerate(lines, 1):
                            #Hide rows containing sensitive information such as passwords, keys, etc.
                            if any(keyword in line.upper() for keyword in ['PASSWORD', 'SECRET', 'KEY', 'TOKEN']):
                                logger.info(f"       {i}: {line.split('=')[0]}=***")
                            else:
                                logger.info(f"       {i}: {line.strip()}")
                except Exception as e:
                    logger.warning(f"     Could not preview file: {e}")
            else:
                logger.info(f"  ❌ Not found: {env_file}")
        
        if not env_file_found:
            logger.warning("⚠️  No .env file found in checked locations")
        
        #Pydantic Settings Configuration Loading State
        logger.info("⚙️  Pydantic Settings Configuration:")
        logger.info(f"  • Settings class: {SETTINGS.__class__.__name__}")
        logger.info(f"  • Config source: {getattr(SETTINGS.model_config, 'env_file', 'Not specified')}")
        logger.info(f"  • Encoding: {getattr(SETTINGS.model_config, 'env_file_encoding', 'Not specified')}")
        
        #Shows the source of some key configuration values (environmental variable vs default)
        key_settings = ['HOST', 'PORT', 'DEBUG', 'MONGODB_HOST', 'REDIS_HOST']
        logger.info("  • Key settings sources:")
        for setting_name in key_settings:
            env_var_name = setting_name
            env_value = os.getenv(env_var_name)
            config_value = getattr(SETTINGS, setting_name, None)
            if env_value is not None:
                logger.info(f"    - {setting_name}: from environment variable ({config_value})")
            else:
                logger.info(f"    - {setting_name}: using default value ({config_value})")
        
        #Environmental information
        env = "Production" if SETTINGS.is_production else "Development"
        logger.info(f"Environment: {env}")

        #Database Connections
        logger.info(f"MongoDB: {SETTINGS.MONGODB_HOST}:{SETTINGS.MONGODB_PORT}/{SETTINGS.MONGODB_DATABASE}")
        logger.info(f"Redis: {SETTINGS.REDIS_HOST}:{SETTINGS.REDIS_PORT}/{SETTINGS.REDIS_DB}")

        #Proxy Configuration
        import os
        if SETTINGS.HTTP_PROXY or SETTINGS.HTTPS_PROXY:
            logger.info("Proxy Configuration:")
            if SETTINGS.HTTP_PROXY:
                logger.info(f"  HTTP_PROXY: {SETTINGS.HTTP_PROXY}")
            if SETTINGS.HTTPS_PROXY:
                logger.info(f"  HTTPS_PROXY: {SETTINGS.HTTPS_PROXY}")
            if SETTINGS.NO_PROXY:
                #Show only the top 3 domain names
                no_proxy_list = SETTINGS.NO_PROXY.split(',')
                if len(no_proxy_list) <= 3:
                    logger.info(f"  NO_PROXY: {SETTINGS.NO_PROXY}")
                else:
                    logger.info(f"  NO_PROXY: {','.join(no_proxy_list[:3])}... ({len(no_proxy_list)} domains)")
            logger.info(f"  ✅ Proxy environment variables set successfully")
        else:
            logger.info("Proxy: Not configured (direct connection)")

        #Check large model configuration
        try:
            from app.services.config_service import CONFIG_SERVICE
            config = await CONFIG_SERVICE.get_system_config_from_database()
            if config and config.llm_configs:
                enabled_llms = [llm for llm in config.llm_configs if llm.enabled]
                logger.info(f"Enabled LLMs: {len(enabled_llms)}")
                if enabled_llms:
                    for llm in enabled_llms[:3]:  #Show only first three
                        logger.info(f"  • {llm.provider}: {llm.model_name}")
                    if len(enabled_llms) > 3:
                        logger.info(f"  • ... and {len(enabled_llms) - 3} more")
                else:
                    logger.warning("⚠️  No LLM enabled. Please configure at least one LLM in Web UI.")
            else:
                logger.warning("⚠️  No LLM configured. Please configure at least one LLM in Web UI.")
        except Exception as e:
            logger.warning(f"⚠️  Failed to check LLM configs: {e}")

        #Check data source configuration
        try:
            if config and config.data_source_configs:
                enabled_sources = [ds for ds in config.data_source_configs if ds.enabled]
                logger.info(f"Enabled Data Sources: {len(enabled_sources)}")
                if enabled_sources:
                    for ds in enabled_sources[:3]:  #Show only first three
                        logger.info(f"  • {ds.type.value}: {ds.name}")
                    if len(enabled_sources) > 3:
                        logger.info(f"  • ... and {len(enabled_sources) - 3} more")
            else:
                logger.info("Data Sources: Using default (AKShare)")
        except Exception as e:
            logger.warning(f"⚠️  Failed to check data source configs: {e}")

        logger.info("=" * 70)
    except Exception as e:
        logger.error(f"Failed to print config summary: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application of life-cycle management"""
    #Initialize on startup
    setup_logging()
    logger = logging.getLogger("app.main")

    #-----------------------------------------------------------------------------------------------------
    #Verify Start Configuration
    #-----------------------------------------------------------------------------------------------------
    try:
        from app.core.startup_validator import validate_startup_config
        # validate startup configuration
        #   Required Configs: MONGODB, REDIS, JWT
        #   Optional Configs: API Keys for DEEPSEEK, DASHSCOPE, TUSHARE
        validate_startup_config()
    except Exception as e:
        logger.error(f"Configure authentication failed:{e}")
        raise

    await init_database_async()

    #-----------------------------------------------------------------------------------------------------
    #Configure Bridges: Write Unified Configurations to Environmental Variables for TradingAgents Core Library
    #-----------------------------------------------------------------------------------------------------
    try:
        from app.core.config_bridge import consolidate_configs_to_osenviron
        consolidate_configs_to_osenviron()
    except Exception as e:
        logger.warning(f"The bridge failed:{e}")
        logger.warning("⚠️ TradingAgents will use configurations in .env files")

    #-----------------------------------------------------------------------------------------------------
    # Apply dynamic settings (log_level, enable_monitoring) from ConfigProvider
    #-----------------------------------------------------------------------------------------------------
    try:
        from app.services.config_provider import CONFIG_PROVIDER as config_provider  # local import to avoid early DB init issues
        eff = await config_provider.get_effective_system_settings()
        desired_level = str(eff.get("log_level", "INFO")).upper()
        setup_logging(log_level=desired_level)
        for name in ("webapi", "worker", "uvicorn", "fastapi"):
            logging.getLogger(name).setLevel(desired_level)
        try:
            from app.middleware.operation_log_middleware import set_operation_log_enabled
            set_operation_log_enabled(bool(eff.get("enable_monitoring", True)))
        except Exception:
            pass
    except Exception as e:
        logging.getLogger("webapi").warning(f"Failed to apply dynamic settings: {e}")

    #-----------------------------------------------------------------------------------------------------
    #Show Profile Summary
    #-----------------------------------------------------------------------------------------------------
    await _print_config_summary(logger)

    logger.info("TradingAgents FastAPI backend started")

    #-----------------------------------------------------------------------------------------------------
    #Start-up period: If necessary, a closing snapshot of the previous trading date should be added at the break
    #-----------------------------------------------------------------------------------------------------
    if SETTINGS.QUOTES_BACKFILL_ON_STARTUP:
        try:
            qi = QuotesIngestionService()
            await qi.ensure_indexes()
            await qi.backfill_last_close_snapshot_if_needed()
        except Exception as e:
            logger.warning(f"Startup backfill failed (ignored): {e}")

    #-----------------------------------------------------------------------------------------------------
    #Starts a daily timed task
    #-----------------------------------------------------------------------------------------------------
    scheduler: AsyncIOScheduler | None = None
    try:
        from croniter import croniter
    except Exception:
        croniter = None  #Optional Dependencies
    try:
        scheduler = AsyncIOScheduler(timezone=SETTINGS.TIMEZONE)

        #Use multiple data source sync service (support automatic switching)
        multi_source_service = MultiSourceBasicsSyncService()

        #Determine priority data sources by TUSHARE ENABLED configuration
        #If Tushare is disabled, the system will automatically use other available data sources (AKshare/ BaoStock)
        preferred_sources = None  #None means to use default priority order

        if SETTINGS.TUSHARE_ENABLED:
            #Use Tushare first when Tushare is enabled
            preferred_sources = ["tushare", "akshare", "baostock"]
            logger.info(f"📊Smart priority data source: Tushare > AKshare > BaoStock")
        else:
            #Use AKshare and BaoStock when Tushare is disabled
            preferred_sources = ["akshare", "baostock"]
            logger.info(f"📊 Stock Basic Information Sync Priority Data Source: AKShare > BaoStock (Tushare disabled)")

        #-----------------------------------------------------------------------------------------------------
        #Run one-time task immediately after startup.
        #-----------------------------------------------------------------------------------------------------
        async def run_sync_with_sources():
            await multi_source_service.run_full_sync(force=False, preferred_sources=preferred_sources)

        asyncio.create_task(run_sync_with_sources())


        #-----------------------------------------------------------------------------------------------------
        #Schedule periodic tasks: Prioritize Cron, followed by HH:MM
        #-----------------------------------------------------------------------------------------------------

        #-----------------------------------------------------------------------------------------------------
        #Stock basic information sync task
        if SETTINGS.SYNC_STOCK_BASICS_ENABLED:
            if SETTINGS.SYNC_STOCK_BASICS_CRON:
                #If a cron expression is provided
                scheduler.add_job(
                    lambda: multi_source_service.run_full_sync(force=False, preferred_sources=preferred_sources),
                    CronTrigger.from_crontab(SETTINGS.SYNC_STOCK_BASICS_CRON, timezone=SETTINGS.TIMEZONE),
                    id="basics_sync_service",
                    name="股票基础信息同步（多数据源）"
                )
                logger.info(f"📅 Stock basics sync scheduled by CRON: {SETTINGS.SYNC_STOCK_BASICS_CRON} ({SETTINGS.TIMEZONE})")
            else:
                hh, mm = (SETTINGS.SYNC_STOCK_BASICS_TIME or "06:30").split(":")
                scheduler.add_job(
                    lambda: multi_source_service.run_full_sync(force=False, preferred_sources=preferred_sources),
                    CronTrigger(hour=int(hh), minute=int(mm), timezone=SETTINGS.TIMEZONE),
                    id="basics_sync_service",
                    name="股票基础信息同步（多数据源）"
                )
                logger.info(f"📅 Stock basics sync scheduled daily at {SETTINGS.SYNC_STOCK_BASICS_TIME} ({SETTINGS.TIMEZONE})")

        #-----------------------------------------------------------------------------------------------------
        # stock real-time quotes ingestion task
        #Real-time database tasks (per N-s), internal self-determination period
        if SETTINGS.QUOTES_INGEST_ENABLED:
            quotes_ingestion = QuotesIngestionService()
            await quotes_ingestion.ensure_indexes()
            scheduler.add_job(
                quotes_ingestion.run_once,  # coroutine function; AsyncIOScheduler will await it
                IntervalTrigger(seconds=SETTINGS.QUOTES_INGEST_INTERVAL_SECONDS, timezone=SETTINGS.TIMEZONE),
                id="quotes_ingestion_service",
                name="实时行情入库服务"
            )
            logger.info(f"Real-time database mission started:{SETTINGS.QUOTES_INGEST_INTERVAL_SECONDS}s")

        #-----------------------------------------------------------------------------------------------------
        #Tushare: Stock Basic Information Sync Task
        logger.info("Configure the Tushare Unified Data Sync Task...")
        scheduler.add_job(
            run_tushare_basic_info_sync,
            CronTrigger.from_crontab(SETTINGS.TUSHARE_BASIC_INFO_SYNC_CRON, timezone=SETTINGS.TIMEZONE),
            id="tushare_basic_info_sync",
            name="股票基础信息同步（Tushare）",
            kwargs={"force_update": False}
        )
        if not (SETTINGS.TUSHARE_UNIFIED_ENABLED and SETTINGS.TUSHARE_BASIC_INFO_SYNC_ENABLED):
            scheduler.pause_job("tushare_basic_info_sync")
            logger.info(f"⏸️Tushare: Stock Basic Information Synchronization has been added but suspended:{SETTINGS.TUSHARE_BASIC_INFO_SYNC_CRON}")
        else:
            logger.info(f"Tushare: Stock Basic Information Synchronized:{SETTINGS.TUSHARE_BASIC_INFO_SYNC_CRON}")

        #-----------------------------------------------------------------------------------------------------
        #Tushare: Real-time Quotes Sync Task
        scheduler.add_job(
            run_tushare_quotes_sync,
            CronTrigger.from_crontab(SETTINGS.TUSHARE_QUOTES_SYNC_CRON, timezone=SETTINGS.TIMEZONE),
            id="tushare_quotes_sync",
            name="实时行情同步（Tushare）"
        )
        if not (SETTINGS.TUSHARE_UNIFIED_ENABLED and SETTINGS.TUSHARE_QUOTES_SYNC_ENABLED):
            scheduler.pause_job("tushare_quotes_sync")
            logger.info(f"Tushare: quotes sync has been added but suspended:{SETTINGS.TUSHARE_QUOTES_SYNC_CRON}")
        else:
            logger.info(f"Tushare: quotes sync configured:{SETTINGS.TUSHARE_QUOTES_SYNC_CRON}")

        #-----------------------------------------------------------------------------------------------------
        #Tushare: Historical Data Sync Task
        scheduler.add_job(
            run_tushare_historical_sync,
            CronTrigger.from_crontab(SETTINGS.TUSHARE_HISTORICAL_SYNC_CRON, timezone=SETTINGS.TIMEZONE),
            id="tushare_historical_sync",
            name="历史数据同步（Tushare）",
            kwargs={"incremental": True}
        )
        if not (SETTINGS.TUSHARE_UNIFIED_ENABLED and SETTINGS.TUSHARE_HISTORICAL_SYNC_ENABLED):
            scheduler.pause_job("tushare_historical_sync")
            logger.info(f"Tushare: historical data sync has been added but suspended:{SETTINGS.TUSHARE_HISTORICAL_SYNC_CRON}")
        else:
            logger.info(f"📊Tushare: historical data sync configured:{SETTINGS.TUSHARE_HISTORICAL_SYNC_CRON}")

        #-----------------------------------------------------------------------------------------------------
        #Tushare: Financial Data Sync Task
        scheduler.add_job(
            run_tushare_financial_sync,
            CronTrigger.from_crontab(SETTINGS.TUSHARE_FINANCIAL_SYNC_CRON, timezone=SETTINGS.TIMEZONE),
            id="tushare_financial_sync",
            name="财务数据同步（Tushare）"
        )
        if not (SETTINGS.TUSHARE_UNIFIED_ENABLED and SETTINGS.TUSHARE_FINANCIAL_SYNC_ENABLED):
            scheduler.pause_job("tushare_financial_sync")
            logger.info(f"⏸️Tushare: financial data sync has been added but suspended:{SETTINGS.TUSHARE_FINANCIAL_SYNC_CRON}")
        else:
            logger.info(f"Tushare: financial data sync configured:{SETTINGS.TUSHARE_FINANCIAL_SYNC_CRON}")

        #-----------------------------------------------------------------------------------------------------
        #Tushare: Status check task
        scheduler.add_job(
            run_tushare_status_check,
            CronTrigger.from_crontab(SETTINGS.TUSHARE_STATUS_CHECK_CRON, timezone=SETTINGS.TIMEZONE),
            id="tushare_status_check",
            name="数据源状态检查（Tushare）"
        )
        if not (SETTINGS.TUSHARE_UNIFIED_ENABLED and SETTINGS.TUSHARE_STATUS_CHECK_ENABLED):
            scheduler.pause_job("tushare_status_check")
            logger.info(f"Tushare status check added but suspended:{SETTINGS.TUSHARE_STATUS_CHECK_CRON}")
        else:
            logger.info(f"Tushare status check configured:{SETTINGS.TUSHARE_STATUS_CHECK_CRON}")

        #-----------------------------------------------------------------------------------------------------
        #AKShare: Stock Basic Info Sync Task
        logger.info("Configure AKShare Unified Data Synchronization...")
        scheduler.add_job(
            run_akshare_basic_info_sync,
            CronTrigger.from_crontab(SETTINGS.AKSHARE_BASIC_INFO_SYNC_CRON, timezone=SETTINGS.TIMEZONE),
            id="akshare_basic_info_sync",
            name="股票基础信息同步（AKShare）",
            kwargs={"force_update": False}
        )
        if not (SETTINGS.AKSHARE_UNIFIED_ENABLED and SETTINGS.AKSHARE_BASIC_INFO_SYNC_ENABLED):
            scheduler.pause_job("akshare_basic_info_sync")
            logger.info(f"⏸️AKShare Basic Information Synchronization has been added but suspended:{SETTINGS.AKSHARE_BASIC_INFO_SYNC_CRON}")
        else:
            logger.info(f"AKShare's basic information is synchronised:{SETTINGS.AKSHARE_BASIC_INFO_SYNC_CRON}")

        #-----------------------------------------------------------------------------------------------------
        #AKShare: Stock Quotes Sync Task
        scheduler.add_job(
            run_akshare_quotes_sync,
            CronTrigger.from_crontab(SETTINGS.AKSHARE_QUOTES_SYNC_CRON, timezone=SETTINGS.TIMEZONE),
            id="akshare_quotes_sync",
            name="实时行情同步（AKShare）"
        )
        if not (SETTINGS.AKSHARE_UNIFIED_ENABLED and SETTINGS.AKSHARE_QUOTES_SYNC_ENABLED):
            scheduler.pause_job("akshare_quotes_sync")
            logger.info(f"⏸️ AKShare: stock quotes sync has been added but suspended:{SETTINGS.AKSHARE_QUOTES_SYNC_CRON}")
        else:
            logger.info(f"AKShare: stock quotes sync configured:{SETTINGS.AKSHARE_QUOTES_SYNC_CRON}")

        #-----------------------------------------------------------------------------------------------------
        #AKShare: Stock Historical Data Sync Task
        scheduler.add_job(
            run_akshare_historical_sync,
            CronTrigger.from_crontab(SETTINGS.AKSHARE_HISTORICAL_SYNC_CRON, timezone=SETTINGS.TIMEZONE),
            id="akshare_historical_sync",
            name="历史数据同步（AKShare）",
            kwargs={"incremental": True}
        )
        if not (SETTINGS.AKSHARE_UNIFIED_ENABLED and SETTINGS.AKSHARE_HISTORICAL_SYNC_ENABLED):
            scheduler.pause_job("akshare_historical_sync")
            logger.info(f"⏸️HistoryShareSync has been added but suspended:{SETTINGS.AKSHARE_HISTORICAL_SYNC_CRON}")
        else:
            logger.info(f"AKShare's historical data synchronisation is configured:{SETTINGS.AKSHARE_HISTORICAL_SYNC_CRON}")

        #-----------------------------------------------------------------------------------------------------
        #AKShare: Stock Financial Data Sync Task
        scheduler.add_job(
            run_akshare_financial_sync,
            CronTrigger.from_crontab(SETTINGS.AKSHARE_FINANCIAL_SYNC_CRON, timezone=SETTINGS.TIMEZONE),
            id="akshare_financial_sync",
            name="财务数据同步（AKShare）"
        )
        if not (SETTINGS.AKSHARE_UNIFIED_ENABLED and SETTINGS.AKSHARE_FINANCIAL_SYNC_ENABLED):
            scheduler.pause_job("akshare_financial_sync")
            logger.info(f"Synchronization of AKshare financial data has been added but suspended:{SETTINGS.AKSHARE_FINANCIAL_SYNC_CRON}")
        else:
            logger.info(f"💰AKShare ' s financial data synchronized:{SETTINGS.AKSHARE_FINANCIAL_SYNC_CRON}")

        #-----------------------------------------------------------------------------------------------------
        #AKShare: Stock Status Check Task
        scheduler.add_job(
            run_akshare_status_check,
            CronTrigger.from_crontab(SETTINGS.AKSHARE_STATUS_CHECK_CRON, timezone=SETTINGS.TIMEZONE),
            id="akshare_status_check",
            name="数据源状态检查（AKShare）"
        )
        if not (SETTINGS.AKSHARE_UNIFIED_ENABLED and SETTINGS.AKSHARE_STATUS_CHECK_ENABLED):
            scheduler.pause_job("akshare_status_check")
            logger.info(f"AKShare status check added but suspended:{SETTINGS.AKSHARE_STATUS_CHECK_CRON}")
        else:
            logger.info(f"AKShare status check configured:{SETTINGS.AKSHARE_STATUS_CHECK_CRON}")

        #-----------------------------------------------------------------------------------------------------
        #BaoStock: Stock Basic Info Sync Task
        logger.info("Configure the BaoStock Unified Data Sync Task...")
        scheduler.add_job(
            run_baostock_basic_info_sync,
            CronTrigger.from_crontab(SETTINGS.BAOSTOCK_BASIC_INFO_SYNC_CRON, timezone=SETTINGS.TIMEZONE),
            id="baostock_basic_info_sync",
            name="股票基础信息同步（BaoStock）"
        )
        if not (SETTINGS.BAOSTOCK_UNIFIED_ENABLED and SETTINGS.BAOSTOCK_BASIC_INFO_SYNC_ENABLED):
            scheduler.pause_job("baostock_basic_info_sync")
            logger.info(f"⏸️ BaoStock Basic Information Synchronization has been added but suspended:{SETTINGS.BAOSTOCK_BASIC_INFO_SYNC_CRON}")
        else:
            logger.info(f"BaoStock Basic Information Synchronized:{SETTINGS.BAOSTOCK_BASIC_INFO_SYNC_CRON}")

        #-----------------------------------------------------------------------------------------------------
        #BaoStock: Stock Quotes Sync Task
        #DayKline Sync Task (note: BaoStock does not support real-time lines)
        scheduler.add_job(
            run_baostock_daily_quotes_sync,
            CronTrigger.from_crontab(SETTINGS.BAOSTOCK_DAILY_QUOTES_SYNC_CRON, timezone=SETTINGS.TIMEZONE),
            id="baostock_daily_quotes_sync",
            name="日K线数据同步（BaoStock）"
        )
        if not (SETTINGS.BAOSTOCK_UNIFIED_ENABLED and SETTINGS.BAOSTOCK_DAILY_QUOTES_SYNC_ENABLED):
            scheduler.pause_job("baostock_daily_quotes_sync")
            logger.info(f"BaoStockK-line sync has been added but is suspended:{SETTINGS.BAOSTOCK_DAILY_QUOTES_SYNC_CRON}")
        else:
            logger.info(f"BaoStockKline is configured:{SETTINGS.BAOSTOCK_DAILY_QUOTES_SYNC_CRON}(Note: BaoStock does not support real time.)")

        #-----------------------------------------------------------------------------------------------------
        #BaoStock: Stock Historical Data Sync Task
        scheduler.add_job(
            run_baostock_historical_sync,
            CronTrigger.from_crontab(SETTINGS.BAOSTOCK_HISTORICAL_SYNC_CRON, timezone=SETTINGS.TIMEZONE),
            id="baostock_historical_sync",
            name="历史数据同步（BaoStock）"
        )
        if not (SETTINGS.BAOSTOCK_UNIFIED_ENABLED and SETTINGS.BAOSTOCK_HISTORICAL_SYNC_ENABLED):
            scheduler.pause_job("baostock_historical_sync")
            logger.info(f"BaoStock historical data sync has been added but is suspended:{SETTINGS.BAOSTOCK_HISTORICAL_SYNC_CRON}")
        else:
            logger.info(f"BaoStock has been configured:{SETTINGS.BAOSTOCK_HISTORICAL_SYNC_CRON}")

        #-----------------------------------------------------------------------------------------------------
        #BaoStock: Status Check Task
        scheduler.add_job(
            run_baostock_status_check,
            CronTrigger.from_crontab(SETTINGS.BAOSTOCK_STATUS_CHECK_CRON, timezone=SETTINGS.TIMEZONE),
            id="baostock_status_check",
            name="数据源状态检查（BaoStock）"
        )
        if not (SETTINGS.BAOSTOCK_UNIFIED_ENABLED and SETTINGS.BAOSTOCK_STATUS_CHECK_ENABLED):
            scheduler.pause_job("baostock_status_check")
            logger.info(f"BaoStock status check added but suspended:{SETTINGS.BAOSTOCK_STATUS_CHECK_CRON}")
        else:
            logger.info(f"BaoStock status check configured:{SETTINGS.BAOSTOCK_STATUS_CHECK_CRON}")

        #-----------------------------------------------------------------------------------------------------
        #News Sync Task by AKShare
        #Synchronization of news data tasking (using AKShare to synchronize all stock news)
        logger.info("Configure news synchronisation...")

        from app.worker.akshare_sync_service import get_akshare_sync_service

        async def run_news_sync():
            """Run NewsSync Tasks - Sync Self-Selected Unit News with AKShare"""
            try:
                logger.info("Starting news synchronisation.")
                akshare_sync_service = await get_akshare_sync_service()
                result = await akshare_sync_service.sync_news_data(
                    symbols=None,  #None + options only=True
                    max_news_per_stock=SETTINGS.NEWS_SYNC_MAX_PER_SOURCE,
                    favorites_only=True  #Synchronization of selected units only
                )
                logger.info(
                    f"News synchronised:"
                    f"Processing{result['total_processed']}It's only for self-selection."
                    f"Success{result['success_count']}Only,"
                    f"Failed{result['error_count']}Only,"
                    f"Total public information{result['news_count']}Article,"
                    f"Time-consuming{(datetime.utcnow() - result['start_time']).total_seconds():.2f}sec"
                )
            except Exception as e:
                logger.error(f"The news has failed:{e}", exc_info=True)

        #== sync, corrected by elderman == @elder man
        #The Hong Kong and United States units no longer deploy scheduled synchronization tasks using a needs-based + cache model
        logger.info("🇭🇰 Port Unit data are available on demand + cache mode")
        logger.info("🇺🇸United States share data are available on demand + cache mode")

        scheduler.add_job(
            run_news_sync,
            CronTrigger.from_crontab(SETTINGS.NEWS_SYNC_CRON, timezone=SETTINGS.TIMEZONE),
            id="news_sync",
            name="新闻数据同步（AKShare - 仅自选股）"
        )
        if not SETTINGS.NEWS_SYNC_ENABLED:
            scheduler.pause_job("news_sync")
            logger.info(f"Synchronization of news data has been added but suspended:{SETTINGS.NEWS_SYNC_CRON}")
        else:
            logger.info(f"📰Syncs of news data configured (selected units only):{SETTINGS.NEWS_SYNC_CRON}")

        #-----------------------------------------------------------------------------------------------------
        #Start the scheduler
        scheduler.start()
        #Set the scheduler instance to service so that API can manage tasks
        set_scheduler_instance(scheduler)
        logger.info("✅ Scheduler service has been initiated")
    except Exception as e:
        logger.error(f"The scheduler failed:{e}", exc_info=True)
        raise  #Throw out the anomaly and stop the application.

    try:
        yield
    finally:
        #Clear on close
        if scheduler:
            try:
                scheduler.shutdown(wait=False)
                logger.info("🛑 Scheduler stopped")
            except Exception as e:
                logger.warning(f"Scheduler shutdown error: {e}")

        #Close Uservice MongoDB connection
        try:
            from app.services.user_service import user_service
            user_service.close()
        except Exception as e:
            logger.warning(f"UserService cleanup error: {e}")

        await close_database_async()
        logger.info("TradingAgents FastAPI backend stopped")


#Create FastAPI Application
app = FastAPI(
    title="TradingAgents-CN API",
    description="股票分析与批量队列系统 API",
    version=get_version(),
    docs_url="/docs" if SETTINGS.DEBUG else None,
    redoc_url="/redoc" if SETTINGS.DEBUG else None,
    lifespan=lifespan
)

#Security middle
if not SETTINGS.DEBUG:
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=SETTINGS.ALLOWED_HOSTS
    )

#CORS Middle
app.add_middleware(
    CORSMiddleware,
    allow_origins=SETTINGS.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)


#Operation log middle
app.add_middleware(OperationLogMiddleware)


#Requested Log Intermediate
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()

    #Skip health check and static file request logs
    if request.url.path in ["/health", "/favicon.ico"] or request.url.path.startswith("/static"):
        response = await call_next(request)
        return response

    #Record requests using webpi logger
    logger = logging.getLogger("webapi")
    logger.info(f"🔄 {request.method} {request.url.path}- Start processing.")

    response = await call_next(request)
    process_time = time.time() - start_time

    #Recording request completed
    status_emoji = "✅" if response.status_code < 400 else "❌"
    logger.info(f"{status_emoji} {request.method} {request.url.path}- Status:{response.status_code}- Time-consuming:{process_time:.3f}s")

    return response


#Global anomalies
#Request ID/Trace-ID intermediate (to be used as the outermost layer, after the function intermediate)
from app.middleware.request_id import RequestIDMiddleware
app.add_middleware(RequestIDMiddleware)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logging.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "INTERNAL_SERVER_ERROR",
                "message": "Internal server error occurred",
                "request_id": getattr(request.state, "request_id", None)
            }
        }
    )


#Test End - Verify if the middle is working
@app.get("/api/test-log")
async def test_log():
    """Test if the log middle is working"""
    print("🧪 测试端点被调用 - 这条消息应该出现在控制台")
    return {"message": "测试成功", "timestamp": time.time()}

#Registration route
app.include_router(health.router, prefix="/api", tags=["health"])
app.include_router(auth.router, prefix="/api/auth", tags=["authentication"])
app.include_router(analysis.router, prefix="/api/analysis", tags=["analysis"])
app.include_router(reports.router, tags=["reports"])
app.include_router(screening.router, prefix="/api/screening", tags=["screening"])
app.include_router(queue.router, prefix="/api/queue", tags=["queue"])
app.include_router(favorites.router, prefix="/api", tags=["favorites"])
app.include_router(stocks_router.router, prefix="/api", tags=["stocks"])
app.include_router(multi_market_stocks_router.router, prefix="/api", tags=["multi-market"])
app.include_router(stock_data_router.router, tags=["stock-data"])
app.include_router(stock_sync_router.router, tags=["stock-sync"])
app.include_router(tags.router, prefix="/api", tags=["tags"])
app.include_router(config.router, prefix="/api", tags=["config"])
app.include_router(model_capabilities.router, tags=["model-capabilities"])
app.include_router(usage_statistics.router, tags=["usage-statistics"])
app.include_router(database.router, prefix="/api/system", tags=["database"])
app.include_router(cache.router, tags=["cache"])
app.include_router(operation_logs.router, prefix="/api/system", tags=["operation_logs"])
app.include_router(logs.router, prefix="/api/system", tags=["logs"])
#New: System Configure Read-only Summary
from app.routers import system_config as system_config_router
app.include_router(system_config_router.router, prefix="/api/system", tags=["system"])

#Notification module (REST + SSE)
app.include_router(notifications_router.router, prefix="/api", tags=["notifications"])

#WebSocket Notification Module (substitute SSE + Redis PubSub)
app.include_router(websocket_notifications_router.router, prefix="/api", tags=["websocket"])

#Timed Task Management
app.include_router(scheduler_router.router, tags=["scheduler"])

app.include_router(sse.router, prefix="/api/stream", tags=["streaming"])
app.include_router(sync_router.router)
app.include_router(multi_source_sync.router)
app.include_router(paper_router.router, prefix="/api", tags=["paper"])
app.include_router(tushare_init.router, prefix="/api", tags=["tushare-init"])
app.include_router(akshare_init.router, prefix="/api", tags=["akshare-init"])
app.include_router(baostock_init.router, prefix="/api", tags=["baostock-init"])
app.include_router(historical_data.router, tags=["historical-data"])
app.include_router(multi_period_sync.router, tags=["multi-period-sync"])
app.include_router(financial_data.router, tags=["financial-data"])
app.include_router(news_data.router, tags=["news-data"])
app.include_router(social_media.router, tags=["social-media"])
app.include_router(internal_messages.router, tags=["internal-messages"])


@app.get("/")
async def root():
    """Root path, return API information"""
    print("🏠 根路径被访问")
    return {
        "name": "TradingAgents-CN API",
        "version": get_version(),
        "status": "running",
        "docs_url": "/docs" if SETTINGS.DEBUG else None
    }


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=SETTINGS.HOST,
        port=SETTINGS.PORT,
        reload=SETTINGS.DEBUG,
        log_level="info",
        reload_dirs=["app"] if SETTINGS.DEBUG else None,
        reload_excludes=[
            "__pycache__",
            "*.pyc",
            "*.pyo",
            "*.pyd",
            ".git",
            ".pytest_cache",
            "*.log",
            "*.tmp"
        ] if SETTINGS.DEBUG else None,
        reload_includes=["*.py"] if SETTINGS.DEBUG else None
    )