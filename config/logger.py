"""Legacy logging interface backed by Python's logging module.

This maintains backward compatibility with existing code while using
Python's standard logging under the hood.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional
import logging

# Try to import Python logging config, fall back to basic logging
try:
    from config.python_logging import get_logger
except ImportError:
    def get_logger(name: str = "stock_market") -> logging.Logger:
        """Fallback logger creation."""
        return logging.getLogger(name)

# Get the logger instance
_logger = get_logger("stock_market")


def log(message: str, level: str = "info") -> None:
    """Write a message to the log using Python's logging module.
    
    This is the primary logging function used throughout the application.
    It routes to Python's logging system for proper management and rotation.
    
    Args:
        message: The message to log.
        level: Log level ('debug', 'info', 'warning', 'error', 'critical')
    """
    try:
        level_lower = level.lower()
        
        if level_lower == "debug":
            _logger.debug(message)
        elif level_lower == "warning":
            _logger.warning(message)
        elif level_lower == "error":
            _logger.error(message)
        elif level_lower == "critical":
            _logger.critical(message)
        else:
            _logger.info(message)
            
    except Exception as e:
        # NEVER let logging crash the app - fall back to stderr
        import sys
        print(f"[LOG ERROR] {type(e).__name__}: {e} | Original message: {message}", file=sys.stderr)


def log_error(message: str, exc_info: bool = False) -> None:
    """Log an error message.
    
    Args:
        message: The error message.
        exc_info: Include exception traceback.
    """
    try:
        _logger.error(message, exc_info=exc_info)
    except Exception as e:
        import sys
        print(f"[LOG ERROR] {type(e).__name__}: {e}", file=sys.stderr)


def log_exception(message: str) -> None:
    """Log an exception with full traceback.
    
    Args:
        message: Context message for the exception.
    """
    try:
        _logger.exception(message)
    except Exception as e:
        import sys
        print(f"[LOG ERROR] {type(e).__name__}: {e}", file=sys.stderr)


def set_log_level(level: str) -> None:
    """Set the logging level.
    
    Args:
        level: Logging level ('debug', 'info', 'warning', 'error', 'critical')
    """
    level_map = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL,
    }
    _logger.setLevel(level_map.get(level.lower(), logging.INFO))


def get_log_level() -> str:
    """Get current logging level.
    
    Returns:
        Current logging level name.
    """
    return logging.getLevelName(_logger.level)


# Legacy functions for backward compatibility
def ensure_log_folder() -> None:
    """Legacy function - logs now use Python's logging module."""
    pass


def clear_log() -> None:
    """Clear (truncate) log files."""
    from config.python_logging import LOG_FILE, ERROR_LOG_FILE
    
    try:
        # Truncate app.log
        if LOG_FILE.exists():
            LOG_FILE.write_text("")
        
        # Truncate errors.log
        if ERROR_LOG_FILE.exists():
            ERROR_LOG_FILE.write_text("")
    except Exception as e:
        import sys
        print(f"[LOG CLEAR ERROR] {type(e).__name__}: {e}", file=sys.stderr)