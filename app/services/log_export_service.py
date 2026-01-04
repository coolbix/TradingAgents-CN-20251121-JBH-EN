"""Log Export Service
Provide query, filter and export functions for log files
"""

import logging
import os
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any
import re
import json

logger = logging.getLogger("webapi")


class LogExportService:
    """Log Export Service"""

    def __init__(self, log_dir: str = "./logs"):
        """Initialization log export service

        Args:
            log dir: logfile directory
        """
        self.log_dir = Path(log_dir)
        logger.info(f"[LogExport Service] Initialization log export service")
        logger.info(f"[LogExportService]{log_dir}")
        logger.info(f"[LogExport Service]{self.log_dir}")
        logger.info(f"[LogExport Service] Absolute path:{self.log_dir.absolute()}")
        logger.info(f"[LogExport Service]{self.log_dir.exists()}")

        if not self.log_dir.exists():
            logger.warning(f"The log directory does not exist:{self.log_dir}")
            try:
                self.log_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"[LogExportService]{self.log_dir}")
            except Exception as e:
                logger.error(f"Could not close temporary folder: %s{e}")
        else:
            logger.info(f"[LogExportService] Log directory exists")

    def list_log_files(self) -> List[Dict[str, Any]]:
        """List all log files

        Returns:
            Log file list containing information on file name, size, change time, etc.
        """
        log_files = []

        try:
            logger.info(f"Start listing log files 🔍 [list log files]")
            logger.info(f"[list log files]{self.log_dir}")
            logger.info(f"[list log files] Absolute path:{self.log_dir.absolute()}")
            logger.info(f"[list log files]{self.log_dir.exists()}")
            logger.info(f"[list log files]{self.log_dir.is_dir()}")

            if not self.log_dir.exists():
                logger.error(f"The log directory does not exist:{self.log_dir}")
                return []

            if not self.log_dir.is_dir():
                logger.error(f"[list log files] Path is not a directory:{self.log_dir}")
                return []

            #List all files in the directory (modified)
            try:
                all_items = list(self.log_dir.iterdir())
                logger.info(f"[list log files]{len(all_items)}Projects")
                for item in all_items[:10]:  #Show top 10 only
                    logger.info(f"🔍 [list_log_files]   - {item.name} (is_file: {item.is_file()})")
            except Exception as e:
                logger.error(f"[list log files]{e}")

            #Search Log Files
            logger.info(f"[list log files]")
            for file_path in self.log_dir.glob("*.log*"):
                logger.info(f"[list log files]{file_path.name}")
                if file_path.is_file():
                    stat = file_path.stat()
                    log_file_info = {
                        "name": file_path.name,
                        "path": str(file_path),
                        "size": stat.st_size,
                        "size_mb": round(stat.st_size / (1024 * 1024), 2),
                        "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                        "type": self._get_log_type(file_path.name)
                    }
                    log_files.append(log_file_info)
                    logger.info(f"Add log file:{file_path.name} ({log_file_info['size_mb']} MB)")
                else:
                    logger.warning(f"[list log files] Skip non-file items:{file_path.name}")

            #Sort in reverse by change time
            log_files.sort(key=lambda x: x["modified_at"], reverse=True)

            logger.info(f"[list log files]{len(log_files)}Log File")
            return log_files

        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}", exc_info=True)
            return []

    def _get_log_type(self, filename: str) -> str:
        """Log type by filename

        Args:
            Filename: File First Name

        Returns:
            Log Type
        """
        if "error" in filename.lower():
            return "error"
        elif "webapi" in filename.lower():
            return "webapi"
        elif "worker" in filename.lower():
            return "worker"
        elif "access" in filename.lower():
            return "access"
        else:
            return "other"

    def read_log_file(
        self,
        filename: str,
        lines: int = 1000,
        level: Optional[str] = None,
        keyword: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> Dict[str, Any]:
        """Read log file contents (support filtering)

        Args:
            filename: Log file First Name
            Lines: Number of lines read (starting at the end)
            level: log level filter (ERRO, WARNING, INFO, DEBUG)
            Keyword: Keyword Filter
            Start time: Start time (ISO format)
            End time: End time (ISO format)

        Returns:
            Log contents and statistical information
        """
        file_path = self.log_dir / filename
        
        if not file_path.exists():
            raise FileNotFoundError(f"日志文件不存在: {filename}")
        
        try:
            #Read File Contents
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                all_lines = f.readlines()
            
            #Read specified lines from end
            recent_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
            
            #Apply Filter
            filtered_lines = []
            stats = {
                "total_lines": len(all_lines),
                "filtered_lines": 0,
                "error_count": 0,
                "warning_count": 0,
                "info_count": 0,
                "debug_count": 0
            }
            
            for line in recent_lines:
                #Statistical log level
                if "ERROR" in line:
                    stats["error_count"] += 1
                elif "WARNING" in line:
                    stats["warning_count"] += 1
                elif "INFO" in line:
                    stats["info_count"] += 1
                elif "DEBUG" in line:
                    stats["debug_count"] += 1
                
                #Apply filter conditions
                if level and level.upper() not in line:
                    continue
                
                if keyword and keyword.lower() not in line.lower():
                    continue
                
                #Time filter (simply achieved, assuming log format YYY-MM-DD HH:MM:SS)
                if start_time or end_time:
                    time_match = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', line)
                    if time_match:
                        log_time = time_match.group()
                        if start_time and log_time < start_time:
                            continue
                        if end_time and log_time > end_time:
                            continue
                
                filtered_lines.append(line.rstrip())
            
            stats["filtered_lines"] = len(filtered_lines)
            
            return {
                "filename": filename,
                "lines": filtered_lines,
                "stats": stats
            }
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            raise

    def export_logs(
        self,
        filenames: Optional[List[str]] = None,
        level: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        format: str = "zip"
    ) -> str:
        """Export Log File

        Args:
            Filenames: List of log filenames to export (None for export all)
            level: log level filter
            Start time: start time
            End time: End time
            Format: Export Format (zip, txt)

        Returns:
            Path to Export File
        """
        try:
            #Determine File to Export
            if filenames:
                files_to_export = [self.log_dir / f for f in filenames if (self.log_dir / f).exists()]
            else:
                files_to_export = list(self.log_dir.glob("*.log*"))
            
            if not files_to_export:
                raise ValueError("没有找到要导出的日志文件")
            
            #Create Export Directory
            export_dir = Path("./exports/logs")
            export_dir.mkdir(parents=True, exist_ok=True)
            
            #Generate Export File Name
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if format == "zip":
                export_path = export_dir / f"logs_export_{timestamp}.zip"
                
                #Create ZIP file
                with zipfile.ZipFile(export_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for file_path in files_to_export:
                        #If there are filter conditions, filter and add
                        if level or start_time or end_time:
                            filtered_data = self.read_log_file(
                                file_path.name,
                                lines=999999,  #Read All Lines
                                level=level,
                                start_time=start_time,
                                end_time=end_time
                            )
                            #Write filtered contents to temporary files
                            temp_file = export_dir / f"temp_{file_path.name}"
                            with open(temp_file, 'w', encoding='utf-8') as f:
                                f.write('\n'.join(filtered_data['lines']))
                            zipf.write(temp_file, file_path.name)
                            temp_file.unlink()  #Remove Temporary File
                        else:
                            zipf.write(file_path, file_path.name)
                
                logger.info(f"The log has been successfully exported:{export_path}")
                return str(export_path)
            
            elif format == "txt":
                export_path = export_dir / f"logs_export_{timestamp}.txt"
                
                #Merge all logs to a text file
                with open(export_path, 'w', encoding='utf-8') as outf:
                    for file_path in files_to_export:
                        outf.write(f"\n{'='*80}\n")
                        outf.write(f"文件: {file_path.name}\n")
                        outf.write(f"{'='*80}\n\n")
                        
                        if level or start_time or end_time:
                            filtered_data = self.read_log_file(
                                file_path.name,
                                lines=999999,
                                level=level,
                                start_time=start_time,
                                end_time=end_time
                            )
                            outf.write('\n'.join(filtered_data['lines']))
                        else:
                            with open(file_path, 'r', encoding='utf-8', errors='ignore') as inf:
                                outf.write(inf.read())
                        
                        outf.write('\n\n')
                
                logger.info(f"The log has been successfully exported:{export_path}")
                return str(export_path)
            
            else:
                raise ValueError(f"不支持的导出格式: {format}")
                
        except Exception as e:
            logger.error(f"Export log failed:{e}")
            raise

    def get_log_statistics(self, days: int = 7) -> Dict[str, Any]:
        """Get Log Statistics

        Args:
            Days: Statistics for the last few days

        Returns:
            Log Statistics
        """
        try:
            cutoff_time = datetime.now() - timedelta(days=days)
            
            stats = {
                "total_files": 0,
                "total_size_mb": 0,
                "error_files": 0,
                "recent_errors": [],
                "log_types": {}
            }
            
            for file_path in self.log_dir.glob("*.log*"):
                if not file_path.is_file():
                    continue
                
                stat = file_path.stat()
                modified_time = datetime.fromtimestamp(stat.st_mtime)
                
                if modified_time < cutoff_time:
                    continue
                
                stats["total_files"] += 1
                stats["total_size_mb"] += stat.st_size / (1024 * 1024)
                
                log_type = self._get_log_type(file_path.name)
                stats["log_types"][log_type] = stats["log_types"].get(log_type, 0) + 1
                
                #Statistical Error Log
                if log_type == "error":
                    stats["error_files"] += 1
                    #Read Recent Errors
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            lines = f.readlines()
                            error_lines = [line for line in lines[-100:] if "ERROR" in line]
                            stats["recent_errors"].extend(error_lines[-10:])
                    except Exception:
                        pass
            
            stats["total_size_mb"] = round(stats["total_size_mb"], 2)
            
            return stats
            
        except Exception as e:
            logger.error(f"Can not get folder: %s: %s{e}")
            return {}


#Examples of global services
_log_export_service: Optional[LogExportService] = None


def get_log_export_service() -> LogExportService:
    """Access log export instance"""
    global _log_export_service

    if _log_export_service is None:
        #Get log directory from log configuration
        log_dir = _get_log_directory()
        _log_export_service = LogExportService(log_dir=log_dir)

    return _log_export_service


def _get_log_directory() -> str:
    """Get Log Directory Path
    Priority:
    1. Read from log profile (support to Docker environment)
    Read from Settings configuration
    3. Use default values./logs
    """
    import os
    from pathlib import Path

    try:
        logger.info(f"[Get log directory]")

        #Check for Docker environment.
        docker_env = os.environ.get("DOCKER", "")
        dockerenv_exists = Path("/.dockerenv").exists()
        is_docker = docker_env.lower() in {"1", "true", "yes"} or dockerenv_exists

        logger.info(f"Docker environment variable:{docker_env}")
        logger.info(f"[Get log directory] /.dockerenv exists:{dockerenv_exists}")
        logger.info(f"This is the first time I've ever seen anything like this.{is_docker}")

        #Try reading from the log profile
        try:
            import tomllib as toml_loader
            logger.info(f"Use tomllib to load TOML")
        except ImportError:
            try:
                import tomli as toml_loader
                logger.info(f"Use tomli to load TOML")
            except ImportError:
                toml_loader = None
                logger.warning(f"Could not import TOML loader")

        if toml_loader:
            #Select Profile From Environment
            profile = os.environ.get("LOGGING_PROFILE", "")
            logger.info(f"🔍 [_get_log_directory] LOGGING_PROFILE: {profile}")

            cfg_path = Path("config/logging_docker.toml") if profile.lower() == "docker" or is_docker else Path("config/logging.toml")
            logger.info(f"Select a profile:{cfg_path}")
            logger.info(f"The configuration file exists:{cfg_path.exists()}")

            if cfg_path.exists():
                try:
                    with cfg_path.open("rb") as f:
                        toml_data = toml_loader.load(f)

                    logger.info(f"Successfully loaded profile")

                    #Read log directory from profile
                    handlers_cfg = toml_data.get("logging", {}).get("handlers", {})
                    file_handler_cfg = handlers_cfg.get("file", {})
                    log_dir = file_handler_cfg.get("directory")

                    logger.info(f"The log directory in the profile:{log_dir}")

                    if log_dir:
                        logger.info(f"[Get log directory] Read the log directory from the log configuration file:{log_dir}")
                        return log_dir
                except Exception as e:
                    logger.warning(f"Reading log profile failed:{e}", exc_info=True)

        #Back to Settings Configuration
        try:
            from app.core.config import settings
            log_dir = settings.log_dir
            logger.info(f"🔍 [_get_log_directory] settings.log_dir: {log_dir}")
            if log_dir:
                logger.info(f"[Get log directory]{log_dir}")
                return log_dir
        except Exception as e:
            logger.warning(f"The log directory from Settings failed:{e}", exc_info=True)

        #Docker Environment Default Use /app/logs
        if is_docker:
            logger.info("Docker Environment, using the default log directory: /app/logs")
            return "/app/logs"

        #Non-Docker environment default use./logs
        logger.info("Use the default log directory: ./logs")
        return "./logs"

    except Exception as e:
        logger.error(f"Can not get folder: %s: %s{e}, using default values./logs", exc_info=True)
        return "./logs"

