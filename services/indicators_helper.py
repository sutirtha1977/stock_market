import pandas as pd
import numpy as np
import traceback
from config.logger import log
from typing import Tuple, Callable, Any

#################################################################################################
# Decorator to handle errors in indicator calculations
#################################################################################################
def safe_indicator(func: Callable[..., Any]) -> Callable[..., Any]:
    """Safely executes indicator functions, logs failures,
    and returns empty Series with preserved index on error.
    
    Args:
        func: The indicator calculation function to wrap.
        
    Returns:
        Wrapped function with error handling.
    """
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            log(f"❌ {func.__name__} FAILED | {e}")
            traceback.print_exc()

            index: pd.Index = args[0].index if args and hasattr(args[0], "index") else pd.Index([])

            if func.__name__ == "calculate_supertrend":
                return (
                    pd.Series(index=index, dtype=float),
                    pd.Series(index=index, dtype=int)
                )

            return pd.Series(index=index, dtype=float)

    return wrapper

#################################################################################################
# RSI Calculation using Wilder's EMA method
#################################################################################################
@safe_indicator
def calculate_rsi_series(close: pd.Series, period: int) -> pd.Series:
    """Calculates RSI using Wilder's EMA method.
    
    Args:
        close: Series of closing prices.
        period: RSI period.
        
    Returns:
        Series of RSI values.
    """
    delta: pd.Series = close.diff()
    gain: pd.Series = delta.clip(lower=0)
    loss: pd.Series = -delta.clip(upper=0)

    avg_gain: pd.Series = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    avg_loss: pd.Series = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()

    rs: pd.Series = avg_gain / avg_loss.replace(0, np.nan)
    rsi: pd.Series = 100 - (100 / (1 + rs))

    return rsi.fillna(100).round(2)

#################################################################################################
# Bollinger Bands Calculation
#################################################################################################
@safe_indicator
def calculate_bollinger(
    close: pd.Series, period: int = 20, std_mult: int = 2
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Calculates Bollinger Bands (Upper, Middle, Lower).
    
    Args:
        close: Series of closing prices.
        period: Window period for moving average.
        std_mult: Standard deviation multiplier.
        
    Returns:
        Tuple of (upper_band, middle_band, lower_band) Series.
    """
    mid: pd.Series = close.rolling(period).mean()
    std: pd.Series = close.rolling(period).std()

    upper: pd.Series = mid + std_mult * std
    lower: pd.Series = mid - std_mult * std

    return upper.round(2), mid.round(2), lower.round(2)

#################################################################################################
# ATR Calculation
#################################################################################################
@safe_indicator
def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculates Average True Range (ATR).
    
    Args:
        df: DataFrame with OHLC data.
        period: ATR period.
        
    Returns:
        Series of ATR values.
    """
    high_low: pd.Series = df["high"] - df["low"]
    high_close: pd.Series = (df["high"] - df["close"].shift()).abs()
    low_close: pd.Series = (df["low"] - df["close"].shift()).abs()

    tr: pd.Series = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr: pd.Series = tr.ewm(alpha=1/period, adjust=False, min_periods=period).mean()

    return atr.round(2)

#################################################################################################
# MACD Calculation
#################################################################################################
@safe_indicator
def calculate_macd(close: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """Calculates MACD line and signal line.
    
    Args:
        close: Series of closing prices.
        
    Returns:
        Tuple of (MACD line, signal line) Series.
    """
    ema_12: pd.Series = close.ewm(span=12, adjust=False).mean()
    ema_26: pd.Series = close.ewm(span=26, adjust=False).mean()

    macd: pd.Series = ema_12 - ema_26
    signal: pd.Series = macd.ewm(span=9, adjust=False).mean()

    return macd.round(2), signal.round(2)

#################################################################################################
# Supertrend Calculation
#################################################################################################
@safe_indicator
def calculate_supertrend(
    df: pd.DataFrame, atr_period: int = 10, multiplier: int = 3
) -> Tuple[pd.Series, pd.Series]:
    """Calculates Supertrend and trend direction.
    
    Args:
        df: DataFrame with OHLC data.
        atr_period: ATR period.
        multiplier: ATR multiplier.
        
    Returns:
        Tuple of (supertrend line, direction) Series.
    """
    atr: pd.Series = calculate_atr(df, atr_period)
    hl2: pd.Series = (df["high"] + df["low"]) / 2

    basic_ub: pd.Series = hl2 + multiplier * atr
    basic_lb: pd.Series = hl2 - multiplier * atr

    final_ub: pd.Series = basic_ub.copy()
    final_lb: pd.Series = basic_lb.copy()

    for i in range(1, len(df)):
        final_ub.iloc[i] = (
            basic_ub.iloc[i]
            if basic_ub.iloc[i] < final_ub.iloc[i-1]
            or df["close"].iloc[i-1] > final_ub.iloc[i-1]
            else final_ub.iloc[i-1]
        )

        final_lb.iloc[i] = (
            basic_lb.iloc[i]
            if basic_lb.iloc[i] > final_lb.iloc[i-1]
            or df["close"].iloc[i-1] < final_lb.iloc[i-1]
            else final_lb.iloc[i-1]
        )

    supertrend: pd.Series = pd.Series(index=df.index, dtype=float)
    direction: pd.Series = pd.Series(index=df.index, dtype=int)

    supertrend.iloc[0] = final_ub.iloc[0]
    direction.iloc[0] = -1

    for i in range(1, len(df)):
        if df["close"].iloc[i] > supertrend.iloc[i-1]:
            direction.iloc[i] = 1
            supertrend.iloc[i] = final_lb.iloc[i]
        else:
            direction.iloc[i] = -1
            supertrend.iloc[i] = final_ub.iloc[i]

    return supertrend.round(2), direction

#################################################################################################
# EMA Calculation
#################################################################################################
@safe_indicator
def calculate_ema(series: pd.Series, period: int) -> pd.Series:
    """Calculates Exponential Moving Average.
    
    Args:
        series: Input data series.
        period: EMA period.
        
    Returns:
        Series of EMA values.
    """
    return series.ewm(span=period, adjust=False).mean().round(2)

#################################################################################################
# WMA Calculation
#################################################################################################
@safe_indicator
def calculate_wma(series: pd.Series, period: int) -> pd.Series:
    """Calculates Weighted Moving Average.
    
    Args:
        series: Input data series.
        period: WMA period.
        
    Returns:
        Series of WMA values.
    """
    weights: np.ndarray = np.arange(1, period + 1)
    wma: pd.Series = series.rolling(period).apply(
        lambda x: np.dot(x, weights) / weights.sum(), raw=True
    )
    return wma.round(2)