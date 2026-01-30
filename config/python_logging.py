"""Python logging configuration for the application.

Provides standardized logging setup with console and file outputs,
proper log levels, and rotation for production use.
"""

import logging
import logging.handlers
import os
from pathlib import Path
from config.paths import DATA_DIR

# Create logs directory if it doesn't exist
LOGS_DIR = Path(DATA_DIR).parent / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Log file paths
LOG_FILE = LOGS_DIR / "app.log"
ERROR_LOG_FILE = LOGS_DIR / "errors.log"


def setup_logging(
    level: int = logging.INFO,
    log_file: str = str(LOG_FILE),
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> logging.Logger:
    """Configure logging with console and file handlers.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to main log file
        max_bytes: Max size of log file before rotation (default 10MB)
        backup_count: Number of backup log files to keep
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger("stock_market")
    logger.setLevel(level)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        fmt='[%(asctime)s] [%(levelname)-8s] [%(name)s:%(lineno)d] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    simple_formatter = logging.Formatter(
        fmt='[%(levelname)-8s] %(message)s'
    )
    
    # Console handler (INFO and above)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)
    
    # File handler with rotation (all levels)
    try:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)
    except (IOError, OSError) as e:
        logger.warning(f"Could not create file handler: {e}")
    
    # Error file handler (WARNING and above)
    try:
        error_handler = logging.handlers.RotatingFileHandler(
            ERROR_LOG_FILE,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        error_handler.setLevel(logging.WARNING)
        error_handler.setFormatter(detailed_formatter)
        logger.addHandler(error_handler)
    except (IOError, OSError) as e:
        logger.warning(f"Could not create error handler: {e}")
    
    return logger


# Get or create the main logger
def get_logger(name: str = "stock_market") -> logging.Logger:
    """Get logger instance.
    
    Args:
        name: Logger name (default: "stock_market")
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


# Initialize default logger
def initialize_logging(level: int = logging.INFO) -> None:
    """Initialize logging for the application.
    
    Args:
        level: Logging level to use
    """
    setup_logging(level=level)


# Quick access functions for backward compatibility
def debug(msg: str, *args, **kwargs) -> None:
    """Log debug message."""
    logger = get_logger()
    logger.debug(msg, *args, **kwargs)


def info(msg: str, *args, **kwargs) -> None:
    """Log info message."""
    logger = get_logger()
    logger.info(msg, *args, **kwargs)


def warning(msg: str, *args, **kwargs) -> None:
    """Log warning message."""
    logger = get_logger()
    logger.warning(msg, *args, **kwargs)


def error(msg: str, *args, **kwargs) -> None:
    """Log error message."""
    logger = get_logger()
    logger.error(msg, *args, **kwargs)


def critical(msg: str, *args, **kwargs) -> None:
    """Log critical message."""
    logger = get_logger()
    logger.critical(msg, *args, **kwargs)


# Alias for compatibility
exception = error


# Initialize logging on module import
initialize_logging()
