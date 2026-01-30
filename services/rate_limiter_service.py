"""Rate limiting for external API calls.

Provides rate limiting to ensure respectful API usage and prevent overwhelming
external services with excessive requests.
"""

import time
from typing import Callable, TypeVar, Any, Optional

T = TypeVar('T')

# Rate limit settings (seconds between requests)
RATE_LIMIT_DEFAULTS = {
    'nse_api': 0.5,        # NSE: 2 requests/second
    'yahoo_api': 0.3,      # Yahoo: ~3 requests/second
    'generic_api': 1.0,    # Generic: 1 request/second
}


class RateLimiter:
    """Rate limiter for API calls."""
    
    def __init__(self, requests_per_second: float = 1.0):
        """Initialize rate limiter.
        
        Args:
            requests_per_second: Number of requests allowed per second
        """
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0.0
    
    def wait_if_needed(self) -> None:
        """Wait if necessary to maintain rate limit."""
        elapsed = time.time() - self.last_request_time
        
        if elapsed < self.min_interval:
            wait_time = self.min_interval - elapsed
            time.sleep(wait_time)
        
        self.last_request_time = time.time()
    
    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator to apply rate limiting to a function.
        
        Args:
            func: Function to rate limit
            
        Returns:
            Wrapped function with rate limiting
            
        Example:
            >>> limiter = RateLimiter(requests_per_second=2)
            >>> @limiter
            ... def fetch_data():
            ...     return requests.get(url)
        """
        def wrapper(*args, **kwargs) -> T:
            self.wait_if_needed()
            return func(*args, **kwargs)
        
        return wrapper


# Pre-configured limiters for common APIs
nse_limiter = RateLimiter(1 / RATE_LIMIT_DEFAULTS['nse_api'])
yahoo_limiter = RateLimiter(1 / RATE_LIMIT_DEFAULTS['yahoo_api'])
generic_limiter = RateLimiter(1 / RATE_LIMIT_DEFAULTS['generic_api'])


def rate_limited_call(
    func: Callable[..., T],
    delay: float = 1.0,
    *args,
    **kwargs
) -> T:
    """Execute function with rate limiting.
    
    Args:
        func: Function to execute
        delay: Delay in seconds before execution
        *args: Positional arguments for func
        **kwargs: Keyword arguments for func
        
    Returns:
        Return value from func
        
    Example:
        >>> result = rate_limited_call(fetch_data, delay=0.5, symbol="INFY")
    """
    time.sleep(delay)
    return func(*args, **kwargs)
