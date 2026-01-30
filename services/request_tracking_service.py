"""Request tracking IDs for correlated logging and debugging.

Provides unique request IDs that can be used to trace operations
across multiple functions and log messages for better debugging.
"""

import uuid
from typing import Optional
from contextvars import ContextVar
from datetime import datetime

# Context variable to store current request ID (thread-safe)
_request_id: ContextVar[Optional[str]] = ContextVar('request_id', default=None)


def generate_request_id(prefix: str = "") -> str:
    """Generate a unique request ID.
    
    Args:
        prefix: Optional prefix for the request ID (e.g., "IMPORT", "SCAN")
        
    Returns:
        Unique request ID (e.g., "IMPORT-a3f2e1d8" or "a3f2e1d8")
    """
    unique_id = str(uuid.uuid4())[:8]
    
    if prefix:
        return f"{prefix}-{unique_id}"
    
    return unique_id


def set_request_id(request_id: str) -> None:
    """Set the current request ID for context.
    
    Args:
        request_id: Request ID to set
        
    Example:
        >>> set_request_id(generate_request_id("IMPORT"))
        >>> log("Processing...")  # Log will include request ID
    """
    _request_id.set(request_id)


def get_request_id() -> Optional[str]:
    """Get the current request ID.
    
    Returns:
        Current request ID or None if not set
    """
    return _request_id.get()


def clear_request_id() -> None:
    """Clear the current request ID."""
    _request_id.set(None)


def with_request_id(prefix: str = ""):
    """Decorator to automatically set request ID for a function.
    
    Args:
        prefix: Prefix for generated request ID
        
    Returns:
        Decorator function
        
    Example:
        >>> @with_request_id("SCAN")
        ... def run_scanner():
        ...     log("Running scanner")  # Logs with SCAN-xxxxx ID
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            request_id = generate_request_id(prefix)
            set_request_id(request_id)
            
            try:
                return func(*args, **kwargs)
            finally:
                clear_request_id()
        
        return wrapper
    
    return decorator


def format_with_request_id(message: str) -> str:
    """Format a message with request ID if available.
    
    Args:
        message: Message to format
        
    Returns:
        Message with request ID prefix (if set)
        
    Example:
        >>> set_request_id("IMPORT-a1b2c3d4")
        >>> format_with_request_id("Processing file")
        "[IMPORT-a1b2c3d4] Processing file"
    """
    request_id = get_request_id()
    
    if request_id:
        return f"[{request_id}] {message}"
    
    return message


class RequestContext:
    """Context manager for request IDs.
    
    Example:
        >>> with RequestContext("DOWNLOAD"):
        ...     log("Downloading data")  # Includes request ID
    """
    
    def __init__(self, prefix: str = ""):
        """Initialize context.
        
        Args:
            prefix: Prefix for generated request ID
        """
        self.prefix = prefix
        self.request_id: Optional[str] = None
        self.previous_id: Optional[str] = None
    
    def __enter__(self) -> str:
        """Enter context and set request ID.
        
        Returns:
            Generated request ID
        """
        self.previous_id = get_request_id()
        self.request_id = generate_request_id(self.prefix)
        set_request_id(self.request_id)
        return self.request_id
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context and restore previous request ID."""
        set_request_id(self.previous_id)


# Example usage functions
def log_with_tracking(message: str, log_func=None) -> None:
    """Log a message with request ID tracking.
    
    Args:
        message: Message to log
        log_func: Optional logging function (defaults to print)
        
    Example:
        >>> set_request_id("OP-12345678")
        >>> log_with_tracking("Operation started")
        # Output: [OP-12345678] Operation started
    """
    formatted = format_with_request_id(message)
    
    if log_func:
        log_func(formatted)
    else:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {formatted}")
