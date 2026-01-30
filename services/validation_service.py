"""Input validation and data quality checks.

Provides centralized validation for common parameters and data structures
used throughout the application. Ensures early detection of invalid inputs
with clear, actionable error messages.
"""

from typing import List, Optional, Tuple, Set
from datetime import datetime, timedelta
import pandas as pd
import requests


def validate_dataframe_columns(
    df: pd.DataFrame,
    required_cols: List[str],
    context: str = "Operation"
) -> None:
    """Validate DataFrame contains required columns.
    
    Args:
        df: DataFrame to validate
        required_cols: List of required column names
        context: Operation context for error message (e.g., "Symbol data import")
        
    Raises:
        ValueError: If required columns are missing
        
    Example:
        >>> validate_dataframe_columns(df, ['open', 'close'], context="Price data")
        # Raises ValueError if columns missing
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"{context}: Expected DataFrame, got {type(df).__name__}")
    
    if df.empty:
        raise ValueError(f"{context}: DataFrame is empty")
    
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        available = list(df.columns)
        raise ValueError(
            f"{context}: Missing columns {missing_cols}. "
            f"Available columns: {available}"
        )


def validate_date_range(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    date_format: str = "%Y-%m-%d",
    context: str = "Date range"
) -> Tuple[str, str]:
    """Validate and parse date range.
    
    Args:
        start_date: Optional start date string (default: 1 year ago)
        end_date: Optional end date string (default: today)
        date_format: Date format string (default: YYYY-MM-DD)
        context: Operation context for error message
        
    Returns:
        Tuple of (start_date_str, end_date_str) in specified format
        
    Raises:
        ValueError: If dates are invalid or start > end
        
    Example:
        >>> start, end = validate_date_range("2025-01-01", "2026-01-23")
        >>> start, end = validate_date_range()  # Last 365 days
    """
    try:
        # Parse dates or use defaults
        if start_date:
            start_dt = datetime.strptime(start_date, date_format)
        else:
            start_dt = datetime.now() - timedelta(days=365)
        
        if end_date:
            end_dt = datetime.strptime(end_date, date_format)
        else:
            end_dt = datetime.now()
        
        # Validate range
        if start_dt > end_dt:
            raise ValueError(
                f"{context}: start_date ({start_dt.date()}) cannot be > "
                f"end_date ({end_dt.date()})"
            )
        
        return start_dt.strftime(date_format), end_dt.strftime(date_format)
        
    except ValueError as e:
        if "time data" in str(e):
            raise ValueError(
                f"{context}: Invalid date format. Expected {date_format}, "
                f"got start='{start_date}', end='{end_date}'"
            )
        raise


def validate_timeframe(timeframe: str) -> str:
    """Validate timeframe is valid.
    
    Args:
        timeframe: Timeframe to validate (e.g., '1d', '1wk', '1mo')
        
    Returns:
        The valid timeframe
        
    Raises:
        ValueError: If timeframe not in valid options
        
    Example:
        >>> validate_timeframe('1d')
        '1d'
        >>> validate_timeframe('2h')  # Raises ValueError
    """
    valid_timeframes: Set[str] = {'1d', '1wk', '1mo'}
    
    if not isinstance(timeframe, str):
        raise TypeError(f"Timeframe must be string, got {type(timeframe).__name__}")
    
    if timeframe not in valid_timeframes:
        raise ValueError(
            f"Invalid timeframe: '{timeframe}'. "
            f"Valid options: {sorted(valid_timeframes)}"
        )
    
    return timeframe


def validate_asset_type(
    asset_type: str,
    valid_types: Optional[Set[str]] = None,
    context: str = "Operation"
) -> str:
    """Validate asset type is valid.
    
    Args:
        asset_type: Asset type to validate (e.g., 'equity', 'fno', 'index')
        valid_types: Set of valid asset types (default: standard types)
        context: Operation context for error message
        
    Returns:
        The valid asset_type
        
    Raises:
        ValueError: If asset_type not in valid options
        
    Example:
        >>> validate_asset_type('equity')
        'equity'
        >>> validate_asset_type('crypto')  # Raises ValueError
    """
    # if valid_types is None:
    #     valid_types = {'equity', 'fno', 'index'}
    
    # if not isinstance(asset_type, str):
    #     raise TypeError(
    #         f"{context}: Asset type must be string, got {type(asset_type).__name__}"
    #     )
    
    # asset_type_lower = asset_type.lower()
    # if asset_type_lower not in valid_types:
    #     raise ValueError(
    #         f"{context}: Invalid asset_type '{asset_type}'. "
    #         f"Valid options: {sorted(valid_types)}"
    #     )
    
    # return asset_type_lower
    if not isinstance(asset_type, str):
        raise TypeError(
            f"{context}: Asset type must be string, got {type(asset_type).__name__}"
        )

    asset_type_lower = asset_type.lower()

    # 🔑 AUTO-DERIVE VALID TYPES FROM DB CONFIG
    if valid_types is None:
        try:
            from config.db_table import ASSET_TABLE_MAP
            valid_types = set(ASSET_TABLE_MAP.keys())
        except Exception as e:
            raise RuntimeError(
                f"{context}: Failed to load ASSET_TABLE_MAP for asset validation | {e}"
            )

    if asset_type_lower not in valid_types:
        raise ValueError(
            f"{context}: Invalid asset_type '{asset_type}'. "
            f"Valid options: {sorted(valid_types)}"
        )

    return asset_type_lower


def validate_symbol_format(symbol: str) -> str:
    """Validate symbol format is valid NSE/equity symbol.
    
    Args:
        symbol: Symbol to validate
        
    Returns:
        The validated symbol (uppercase)
        
    Raises:
        ValueError: If symbol format invalid
        
    Example:
        >>> validate_symbol_format('INFY')
        'INFY'
        >>> validate_symbol_format('INFY&')  # Raises ValueError
    """
    if not isinstance(symbol, str):
        raise TypeError(f"Symbol must be string, got {type(symbol).__name__}")
    
    symbol = symbol.strip().upper()
    
    if not symbol:
        raise ValueError("Symbol cannot be empty")
    
    if len(symbol) > 20:
        raise ValueError(f"Symbol too long: '{symbol}' (max 20 chars)")
    
    if not symbol.replace('&', '').replace('-', '').isalnum():
        raise ValueError(
            f"Invalid symbol format: '{symbol}'. "
            f"Only alphanumeric, '&', and '-' allowed"
        )
    
    return symbol


def validate_connection(conn) -> None:
    """Validate database connection is open and usable.
    
    Args:
        conn: Connection object to validate
        
    Raises:
        RuntimeError: If connection not available or closed
        
    Example:
        >>> validate_connection(conn)
        # Raises RuntimeError if connection is None or closed
    """
    if conn is None:
        raise RuntimeError("Database connection not available (None)")
    
    if hasattr(conn, 'closed') and conn.closed:
        raise RuntimeError("Database connection is closed")


def validate_positive_int(
    value: int,
    param_name: str = "Value",
    min_value: int = 1
) -> int:
    """Validate value is positive integer.
    
    Args:
        value: Value to validate
        param_name: Parameter name for error messages
        min_value: Minimum allowed value (default: 1)
        
    Returns:
        The validated value
        
    Raises:
        ValueError: If value invalid
        
    Example:
        >>> validate_positive_int(14, "RSI period")
        14
        >>> validate_positive_int(0, "Period")  # Raises ValueError
    """
    if not isinstance(value, int):
        raise TypeError(
            f"{param_name} must be integer, got {type(value).__name__}"
        )
    
    if value < min_value:
        raise ValueError(
            f"{param_name} must be >= {min_value}, got {value}"
        )
    
    return value


def validate_file_exists(file_path: str) -> str:
    """Validate file exists at path.
    
    Args:
        file_path: Path to file
        
    Returns:
        The file path (absolute)
        
    Raises:
        FileNotFoundError: If file doesn't exist
        
    Example:
        >>> validate_file_exists('/path/to/data.csv')
        '/path/to/data.csv'
    """
    from pathlib import Path
    
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if not path.is_file():
        raise FileNotFoundError(f"Not a file: {file_path}")
    
    return str(path.absolute())
