import logging
import logging.config
import sys
from pathlib import Path
import os
import platform
import inspect

from app.core.logging_context import LoggingContextFilter, trace_id_var

#Use concurent-log-handler on Windows to avoid file occupancy problems
_IS_WINDOWS = platform.system() == "Windows"
if _IS_WINDOWS:
    try:
        from concurrent_log_handler import ConcurrentRotatingFileHandler
        _USE_CONCURRENT_HANDLER = True
    except ImportError:
        _USE_CONCURRENT_HANDLER = False
        logging.warning("Concurrent-log-handler is not installed and may encounter log rotation problems on Windows")
else:
    _USE_CONCURRENT_HANDLER = False

try:
    import tomllib as toml_loader  # Python 3.11+
except Exception:
    try:
        import tomli as toml_loader  # Python 3.10 fallback
    except Exception:
        toml_loader = None

_CLASSNAME_FACTORY_INSTALLED = False
_ORIGINAL_RECORD_FACTORY = logging.getLogRecordFactory()


def _find_classname_from_stack() -> str:
    frame = inspect.currentframe()
    try:
        while frame:
            module_name = frame.f_globals.get("__name__", "")
            if module_name.startswith("logging"):
                frame = frame.f_back
                continue
            if "self" in frame.f_locals:
                return frame.f_locals["self"].__class__.__name__
            if "cls" in frame.f_locals and isinstance(frame.f_locals["cls"], type):
                return frame.f_locals["cls"].__name__
            frame = frame.f_back
    finally:
        del frame
    return "-"


def _install_classname_record_factory() -> None:
    global _CLASSNAME_FACTORY_INSTALLED
    if _CLASSNAME_FACTORY_INSTALLED:
        return

    def record_factory(*args, **kwargs):
        record = _ORIGINAL_RECORD_FACTORY(*args, **kwargs)
        if not hasattr(record, "classname"):
            record.classname = _find_classname_from_stack()
        return record

    logging.setLogRecordFactory(record_factory)
    _CLASSNAME_FACTORY_INSTALLED = True


def resolve_logging_cfg_path() -> Path:
    """Select a path to the profile according to the environment (may not exist)
    Prefers the docker configuration, with the second default configuration.
    """
    profile = os.environ.get("LOGGING_PROFILE", "").lower()
    is_docker_env = os.environ.get("DOCKER", "").lower() in {"1", "true", "yes"} or Path("/.dockerenv").exists()
    cfg_candidate = "config/logging_docker.toml" if profile == "docker" or is_docker_env else "config/logging.toml"
    return Path(cfg_candidate)


class SimpleJsonFormatter(logging.Formatter):
    """Minimal JSON formatter without external deps."""
    def format(self, record: logging.LogRecord) -> str:
        import json
        obj = {
            "time": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "name": record.name,
            "level": record.levelname,
            "classname": getattr(record, "classname", "-"),
            "trace_id": getattr(record, "trace_id", "-"),
            "message": record.getMessage(),
        }
        return json.dumps(obj, ensure_ascii=False)


_ANSI_RESET = "\x1b[0m"
_LEVEL_COLORS = {
    logging.DEBUG: "\x1b[36m",
    logging.INFO: "\x1b[32m",
    logging.WARNING: "\x1b[33m",
    logging.ERROR: "\x1b[31m",
    logging.CRITICAL: "\x1b[35m",
}


class ColorizedLevelFormatter(logging.Formatter):
    """Colorize level names for console output only."""
    def format(self, record: logging.LogRecord) -> str:
        original_levelname = record.levelname
        color = _LEVEL_COLORS.get(record.levelno)
        if color:
            record.levelname = f"{color}{original_levelname}{_ANSI_RESET}"
        try:
            return super().format(record)
        finally:
            record.levelname = original_levelname


def _parse_size(size_str: str) -> int:
    """Parsing size strings (e. g. '10MB') as bytes"""
    if isinstance(size_str, int):
        return size_str
    if isinstance(size_str, str) and size_str.upper().endswith("MB"):
        try:
            return int(float(size_str[:-2]) * 1024 * 1024)
        except Exception:
            return 10 * 1024 * 1024
    return 10 * 1024 * 1024

def setup_logging(log_level: str = "INFO"):
    """Set application log configuration:
    1) Prioritize reading from config/ logging.toml to dictConfig
    2) Back to the built-in default configuration when failed or non-existent
    """
    _install_classname_record_factory()
    #1) Priority if TOML configuration exists and is parsable
    try:
        logging_cfg_path = resolve_logging_cfg_path()
        print(f"🔍 [setup_logging] 日志配置文件路径: {logging_cfg_path}")
        print(f"🔍 [setup_logging] 配置文件存在: {logging_cfg_path.exists()}")
        print(f"🔍 [setup_logging] TOML加载器可用: {toml_loader is not None}")

        if logging_cfg_path.exists() and toml_loader is not None:
            with logging_cfg_path.open("rb") as f:
                toml_data = toml_loader.load(f)

            print(f"🔍 [setup_logging] 成功加载TOML配置")

            #Read base field
            logging_root = toml_data.get("logging", {})
            level = logging_root.get("level", log_level)
            fmt_cfg = logging_root.get("format", {})
            fmt_console = fmt_cfg.get(
                "console", "%(asctime)s - %(name)s - %(levelname)s - %(classname)s - %(message)s"
            )
            fmt_file = fmt_cfg.get(
                "file", "%(asctime)s - %(name)s - %(levelname)s - %(classname)s - %(message)s"
            )
            #Ensure text format contains track id (if not visible)
            if "%(trace_id)" not in str(fmt_console):
                fmt_console = str(fmt_console) + " trace=%(trace_id)s"
            if "%(trace_id)" not in str(fmt_file):
                fmt_file = str(fmt_file) + " trace=%(trace_id)s"

            handlers_cfg = logging_root.get("handlers", {})
            file_handler_cfg = handlers_cfg.get("file", {})
            file_dir = file_handler_cfg.get("directory", "./logs")
            file_level = file_handler_cfg.get("level", "DEBUG")
            max_bytes = file_handler_cfg.get("max_size", "10MB")
            #Support "10MB" forms
            if isinstance(max_bytes, str) and max_bytes.upper().endswith("MB"):
                try:
                    max_bytes = int(float(max_bytes[:-2]) * 1024 * 1024)
                except Exception:
                    max_bytes = 10 * 1024 * 1024
            elif not isinstance(max_bytes, int):
                max_bytes = 10 * 1024 * 1024
            backup_count = int(file_handler_cfg.get("backup_count", 5))

            Path(file_dir).mkdir(parents=True, exist_ok=True)

            #Read log file paths from TOML configuration
            main_handler_cfg = handlers_cfg.get("main", {})
            webapi_handler_cfg = handlers_cfg.get("webapi", {})
            worker_handler_cfg = handlers_cfg.get("worker", {})

            print(f"🔍 [setup_logging] handlers配置: {list(handlers_cfg.keys())}")
            print(f"🔍 [setup_logging] main_handler_cfg: {main_handler_cfg}")
            print(f"🔍 [setup_logging] webapi_handler_cfg: {webapi_handler_cfg}")
            print(f"🔍 [setup_logging] worker_handler_cfg: {worker_handler_cfg}")

            #Main Log Files (tradingAGents.log)
            main_log = main_handler_cfg.get("filename", str(Path(file_dir) / "tradingagents.log"))
            main_enabled = main_handler_cfg.get("enabled", True)
            main_level = main_handler_cfg.get("level", "INFO")
            main_max_bytes = _parse_size(main_handler_cfg.get("max_size", "100MB"))
            main_backup_count = int(main_handler_cfg.get("backup_count", 5))

            print(f"🔍 [setup_logging] 主日志文件配置:")
            print(f"  - 文件路径: {main_log}")
            print(f"  - 是否启用: {main_enabled}")
            print(f"  - 日志级别: {main_level}")
            print(f"  - 最大大小: {main_max_bytes} bytes")
            print(f"  - 备份数量: {main_backup_count}")

            #WebAPI Log File
            webapi_log = webapi_handler_cfg.get("filename", str(Path(file_dir) / "webapi.log"))
            webapi_enabled = webapi_handler_cfg.get("enabled", True)
            webapi_level = webapi_handler_cfg.get("level", "DEBUG")
            webapi_max_bytes = _parse_size(webapi_handler_cfg.get("max_size", "100MB"))
            webapi_backup_count = int(webapi_handler_cfg.get("backup_count", 5))

            print(f"🔍 [setup_logging] WebAPI日志文件: {webapi_log}, 启用: {webapi_enabled}")

            #Worker Log File
            worker_log = worker_handler_cfg.get("filename", str(Path(file_dir) / "worker.log"))
            worker_enabled = worker_handler_cfg.get("enabled", True)
            worker_level = worker_handler_cfg.get("level", "DEBUG")
            worker_max_bytes = _parse_size(worker_handler_cfg.get("max_size", "100MB"))
            worker_backup_count = int(worker_handler_cfg.get("backup_count", 5))

            print(f"🔍 [setup_logging] Worker日志文件: {worker_log}, 启用: {worker_enabled}")

            #Error Log File
            error_handler_cfg = handlers_cfg.get("error", {})
            error_log = error_handler_cfg.get("filename", str(Path(file_dir) / "error.log"))
            error_enabled = error_handler_cfg.get("enabled", True)
            error_level = error_handler_cfg.get("level", "WARNING")
            error_max_bytes = _parse_size(error_handler_cfg.get("max_size", "100MB"))
            error_backup_count = int(error_handler_cfg.get("backup_count", 5))

            #JSON switch: Keep backward compatibility (json/mode console only); add file json/file mode control file handler
            use_json_console = bool(fmt_cfg.get("json", False)) or str(fmt_cfg.get("mode", "")).lower() == "json"
            use_json_file = (
                bool(fmt_cfg.get("file_json", False))
                or bool(fmt_cfg.get("json_file", False))
                or str(fmt_cfg.get("file_mode", "")).lower() == "json"
            )

            #Build Processor Configuration
            handlers_config = {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "json_console_fmt" if use_json_console else "console_fmt",
                    "level": level,
                    "filters": ["request_context"],
                    "stream": sys.stdout,
                },
            }

            print(f"🔍 [setup_logging] 开始构建handlers配置")

            #🔥 Select the log processor class (Windows using ConcurrentRotatingFilehandler)
            handler_class = "concurrent_log_handler.ConcurrentRotatingFileHandler" if _USE_CONCURRENT_HANDLER else "logging.handlers.RotatingFileHandler"

            #Main Log Files (tradingAGents.log)
            if main_enabled:
                print(f"✅ [setup_logging] 添加 main_file handler: {main_log} (使用 {handler_class})")
                handlers_config["main_file"] = {
                    "class": handler_class,
                    "formatter": "json_file_fmt" if use_json_file else "file_fmt",
                    "level": main_level,
                    "filename": main_log,
                    "maxBytes": main_max_bytes,
                    "backupCount": main_backup_count,
                    "encoding": "utf-8",
                    "filters": ["request_context"],
                }
            else:
                print(f"⚠️ [setup_logging] main_file handler 未启用")

            #WebAPI Log File
            if webapi_enabled:
                handlers_config["file"] = {
                    "class": handler_class,
                    "formatter": "json_file_fmt" if use_json_file else "file_fmt",
                    "level": webapi_level,
                    "filename": webapi_log,
                    "maxBytes": webapi_max_bytes,
                    "backupCount": webapi_backup_count,
                    "encoding": "utf-8",
                    "filters": ["request_context"],
                }

            #Worker Log File
            if worker_enabled:
                handlers_config["worker_file"] = {
                    "class": handler_class,
                    "formatter": "json_file_fmt" if use_json_file else "file_fmt",
                    "level": worker_level,
                    "filename": worker_log,
                    "maxBytes": worker_max_bytes,
                    "backupCount": worker_backup_count,
                    "encoding": "utf-8",
                    "filters": ["request_context"],
                }

            #Add Error Log Processor (if enabled)
            if error_enabled:
                handlers_config["error_file"] = {
                    "class": "logging.handlers.RotatingFileHandler",
                    "formatter": "json_file_fmt" if use_json_file else "file_fmt",
                    "level": error_level,
                    "filename": error_log,
                    "maxBytes": error_max_bytes,
                    "backupCount": error_backup_count,
                    "encoding": "utf-8",
                    "filters": ["request_context"],
                }

            #Build a logger handlers list
            main_handlers = ["console"]
            if main_enabled:
                main_handlers.append("main_file")
            if error_enabled:
                main_handlers.append("error_file")

            print(f"🔍 [setup_logging] main_handlers: {main_handlers}")

            webapi_handlers = ["console"]
            if webapi_enabled:
                webapi_handlers.append("file")
            if main_enabled:
                webapi_handlers.append("main_file")
            if error_enabled:
                webapi_handlers.append("error_file")

            print(f"🔍 [setup_logging] webapi_handlers: {webapi_handlers}")

            worker_handlers = ["console"]
            if worker_enabled:
                worker_handlers.append("worker_file")
            if main_enabled:
                worker_handlers.append("main_file")
            if error_enabled:
                worker_handlers.append("error_file")

            print(f"🔍 [setup_logging] worker_handlers: {worker_handlers}")

            logging_config = {
                "version": 1,
                "disable_existing_loggers": False,
                "filters": {
                    "request_context": {"()": "app.core.logging_context.LoggingContextFilter"}
                },
                "formatters": {
                    "console_fmt": {
                        "()": "app.core.logging_config.ColorizedLevelFormatter",
                        "format": fmt_console,
                        "datefmt": "%Y-%m-%d %H:%M:%S",
                    },
                    "file_fmt": {
                        "format": fmt_file,
                        "datefmt": "%Y-%m-%d %H:%M:%S",
                    },
                    "json_console_fmt": {
                        "()": "app.core.logging_config.SimpleJsonFormatter"
                    },
                    "json_file_fmt": {
                        "()": "app.core.logging_config.SimpleJsonFormatter"
                    },
                },
                "handlers": handlers_config,
                "loggers": {
                    "tradingagents": {
                        "level": "INFO",
                        "handlers": main_handlers,
                        "propagate": False
                    },
                    "webapi": {
                        "level": "INFO",
                        "handlers": webapi_handlers,
                        "propagate": False
                    },
                    "worker": {
                        "level": "DEBUG",
                        "handlers": worker_handlers,
                        "propagate": False
                    },
                    "uvicorn": {
                        "level": "INFO",
                        "handlers": webapi_handlers,
                        "propagate": False
                    },
                    "fastapi": {
                        "level": "INFO",
                        "handlers": webapi_handlers,
                        "propagate": False
                    },
                    "app": {
                        "level": "INFO",
                        "handlers": main_handlers,
                        "propagate": False
                    },
                },
                "root": {"level": level, "handlers": main_handlers},
            }

            print(f"🔍 [setup_logging] 最终handlers配置: {list(handlers_config.keys())}")
            print(f"🔍 [setup_logging] 开始应用 dictConfig")

            logging.config.dictConfig(logging_config)

            print(f"✅ [setup_logging] dictConfig 应用成功")

            logging.getLogger("webapi").info(f"Logging configured from {logging_cfg_path}")

            #Test whether the main log file is written
            if main_enabled:
                test_logger = logging.getLogger("tradingagents")
                test_logger.info(f"🔍 Test Master Log files to write:{main_log}")
                print(f"🔍 [setup_logging] 已向 tradingagents logger 写入测试日志")

            return
    except Exception as e:
        #TOML exists but loading failed. Back to default configuration
        logging.getLogger("webapi").warning(f"Failed to load logging.toml, fallback to defaults: {e}")

    #2) Default built-in configuration (same)
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    #🔥 Select the log processor class (Windows using ConcurrentRotatingFilehandler)
    handler_class = "concurrent_log_handler.ConcurrentRotatingFileHandler" if _USE_CONCURRENT_HANDLER else "logging.handlers.RotatingFileHandler"

    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {"request_context": {"()": "app.core.logging_context.LoggingContextFilter"}},
        "formatters": {
            "default": {
                "()": "app.core.logging_config.ColorizedLevelFormatter",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(classname)s - %(message)s trace=%(trace_id)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "detailed": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(classname)s - %(pathname)s:%(lineno)d - %(message)s trace=%(trace_id)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": log_level,
                "filters": ["request_context"],
                "stream": sys.stdout,
            },
            "file": {
                "class": handler_class,
                "formatter": "detailed",
                "level": "DEBUG",
                "filters": ["request_context"],
                "filename": "logs/webapi.log",
                "maxBytes": 10485760,
                "backupCount": 5,
                "encoding": "utf-8",
            },
            "worker_file": {
                "class": handler_class,
                "formatter": "detailed",
                "level": "DEBUG",
                "filters": ["request_context"],
                "filename": "logs/worker.log",
                "maxBytes": 10485760,
                "backupCount": 5,
                "encoding": "utf-8",
            },
            "error_file": {
                "class": handler_class,
                "formatter": "detailed",
                "level": "WARNING",
                "filters": ["request_context"],
                "filename": "logs/error.log",
                "maxBytes": 10485760,
                "backupCount": 5,
                "encoding": "utf-8",
            },
        },
        "loggers": {
            "webapi": {"level": "INFO", "handlers": ["console", "file", "error_file"], "propagate": True},
            "worker": {"level": "DEBUG", "handlers": ["console", "worker_file", "error_file"], "propagate": False},
            "uvicorn": {"level": "INFO", "handlers": ["console", "file", "error_file"], "propagate": False},
            "fastapi": {"level": "INFO", "handlers": ["console", "file", "error_file"], "propagate": False},
        },
        "root": {"level": log_level, "handlers": ["console"]},
    }

    logging.config.dictConfig(logging_config)
    logging.getLogger("webapi").info("Logging configured successfully (built-in)")
