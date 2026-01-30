"""Network retry logic with exponential backoff.

Provides robust retry mechanisms for external API calls and network operations,
with exponential backoff to handle temporary failures gracefully.
"""

import time
from typing import Callable, TypeVar, Any, Optional
import requests
from config.logger import log

T = TypeVar('T')


def retry_with_backoff(
    func: Callable[..., T],
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (requests.RequestException,),
    context: str = "Operation"
) -> T:
    """Execute function with exponential backoff retry logic.
    
    Args:
        func: Callable to execute
        max_retries: Total attempts (3 = 1 initial + 2 retries)
        initial_delay: Initial delay in seconds before first retry
        backoff_factor: Multiplier for delay (1s, 2s, 4s with factor=2)
        exceptions: Tuple of exceptions to catch and retry on
        context: Operation context for logging
        
    Returns:
        Return value from successful function execution
        
    Raises:
        Exception: The last exception if all retries fail
        
    Example:
        >>> def fetch_data():
        ...     return requests.get(url).json()
        >>> data = retry_with_backoff(fetch_data, context="Fetch symbol data")
    """
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            
            if attempt == max_retries - 1:
                # Last attempt failed
                log(f"❌ {context} failed after {max_retries} attempts: {str(e)[:100]}")
                raise
            
            # Calculate delay with exponential backoff
            delay = initial_delay * (backoff_factor ** attempt)
            log(f"⚠ {context} failed, retry {attempt + 1}/{max_retries} after {delay:.1f}s")
            time.sleep(delay)
    
    # Should not reach here
    raise RuntimeError(f"Unexpected: {context} retry loop exited without exception")


def download_with_retry(
    url: str,
    max_retries: int = 3,
    timeout: int = 20,
    headers: Optional[dict] = None,
    context: str = "Download"
) -> requests.Response:
    """Download URL with exponential backoff retry logic.
    
    Args:
        url: URL to download
        max_retries: Total attempts (3 = 1 initial + 2 retries)
        timeout: Request timeout in seconds
        headers: Optional HTTP headers
        context: Operation context for logging
        
    Returns:
        Response object
        
    Raises:
        requests.RequestException: If all retries fail
        
    Example:
        >>> response = download_with_retry(url, context="Download bhavcopy")
        >>> data = response.json()
    """
    # def _download():
    #     response = requests.get(url, timeout=timeout, headers=headers)
    #     response.raise_for_status()
    #     return response
    def _download():
        response = requests.get(url, timeout=timeout, headers=headers)

        # NSE-specific: 404 = data not available (holiday/weekend) → do NOT raise
        if response.status_code == 404:
            log(f"ℹ️ File not available (HTTP 404) for {url[:50]}...")
            return response

        response.raise_for_status()  # raise for other HTTP errors
        return response
    
    return retry_with_backoff(
        _download,
        max_retries=max_retries,
        initial_delay=1.0,
        backoff_factor=2.0,
        exceptions=(requests.RequestException,),
        context=f"{context} from {url[:50]}..."
    )


def execute_with_retry(
    func: Callable[..., T],
    *args,
    max_retries: int = 3,
    context: str = "Operation",
    **kwargs
) -> T:
    """Execute function with args/kwargs and exponential backoff retry.
    
    Args:
        func: Function to execute
        *args: Positional arguments for func
        max_retries: Total attempts
        context: Operation context for logging
        **kwargs: Keyword arguments for func
        
    Returns:
        Return value from func
        
    Raises:
        Exception: If all retries fail
        
    Example:
        >>> result = execute_with_retry(
        ...     fetch_data,
        ...     symbol="INFY",
        ...     max_retries=3,
        ...     context="Fetch INFY data"
        ... )
    """
    def _execute():
        return func(*args, **kwargs)
    
    return retry_with_backoff(
        _execute,
        max_retries=max_retries,
        context=context
    )
