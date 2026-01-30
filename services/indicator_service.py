import traceback
import time
import sys
import warnings
from typing import Optional, List, Any

import pandas as pd
from tqdm import tqdm

from database.connection import get_db_connection
from config.constants import FREQUENCIES
from config.db_table import ASSET_TABLE_MAP
from services.indicators_helper import (
    calculate_rsi_series, calculate_bollinger, 
    calculate_atr, calculate_macd, 
    calculate_supertrend, calculate_ema, calculate_wma
)
from services.validation_service import validate_dataframe_columns
from database.sql import SQL_INSERT
from config.logger import log

warnings.simplefilter(action='ignore', category=UserWarning)

#################################################################################################
# Calculates various technical indicators for the given DataFrame.
#################################################################################################
def calculate_indicators(df: pd.DataFrame, latest_only: bool = False) -> pd.DataFrame:
    validate_dataframe_columns(
        df,
        ['open', 'high', 'low', 'close', 'volume'],
        context="Calculate indicators"
    )

    try:
        df["sma_20"] = df["close"].rolling(20).mean().round(2)
        df["sma_50"] = df["close"].rolling(50).mean().round(2)
        df["sma_200"] = df["close"].rolling(200).mean().round(2)

        df["rsi_3"] = calculate_rsi_series(df["close"], 3)
        df["rsi_9"] = calculate_rsi_series(df["close"], 9)
        df["rsi_14"] = calculate_rsi_series(df["close"], 14)

        df["ema_rsi_9_3"] = calculate_ema(df["rsi_9"], 3)
        df["wma_rsi_9_21"] = calculate_wma(df["rsi_9"], 21)

        df["bb_upper"], df["bb_middle"], df["bb_lower"] = calculate_bollinger(df["close"])
        df["atr_14"] = calculate_atr(df)
        df["supertrend"], df["supertrend_dir"] = calculate_supertrend(df)
        df["macd"], df["macd_signal"] = calculate_macd(df["close"])
        df["pct_price_change"] = df["close"].pct_change(fill_method=None).mul(100).round(2)

        numeric_cols = [
            "sma_20", "sma_50", "sma_200",
            "rsi_3", "rsi_9", "rsi_14",
            "ema_rsi_9_3", "wma_rsi_9_21",
            "bb_upper", "bb_middle", "bb_lower",
            "atr_14", "supertrend", "macd", "macd_signal",
            "pct_price_change"
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].round(2)

        if latest_only:
            return df.iloc[[-1]].reset_index(drop=True)
        return df

    except Exception as e:
        log(f"{sys._getframe().f_code.co_name} FAILED | {e}")
        traceback.print_exc()
        return df

#################################################################################################
# Refreshes technical indicators for multiple asset types
#################################################################################################
def refresh_indicators(
    asset_types: Optional[List[str]] = None,
    lookback_rows: int = 250
) -> None:
    """Refresh technical indicators safely with connection context managers."""
    try:
        log("🛠 Started refresh_indicators")

        asset_keys = asset_types or ASSET_TABLE_MAP.keys()
        log(f"🔑 Asset keys to process: {list(asset_keys)}")

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for asset_key in asset_keys:
                    symbol_table, price_table, indicator_table, _ = ASSET_TABLE_MAP[asset_key]
                    col_id = "symbol_id"
                    log(f"\n📂 Processing asset: {asset_key}")
                    log(f"   Symbol table: {symbol_table}, Price table: {price_table}, Indicator table: {indicator_table}")

                    # Load asset IDs
                    cur.execute(f"SELECT {col_id} FROM {symbol_table}")
                    asset_ids = [r[0] for r in cur.fetchall()]
                    log(f"   🔢 Loaded {len(asset_ids)} assets from {symbol_table}")

                    insert_sql = SQL_INSERT["generic"].format(
                        indicator_table=indicator_table,
                        col_id=col_id
                    )

                    for timeframe in FREQUENCIES:
                        log(f"\n⏳ Processing timeframe: {timeframe}")
                        tf_start = time.time()
                        inserted_rows = 0
                        processed_assets = 0

                        for asset_id in tqdm(asset_ids, desc=f"{asset_key} | {timeframe}", ncols=100):
                            try:
                                # Last indicator date
                                cur.execute(f"""
                                    SELECT MAX(date) FROM {indicator_table}
                                    WHERE {col_id} = %s AND timeframe = %s
                                """, (asset_id, timeframe))
                                last_dt = cur.fetchone()[0]

                                # Fetch price data
                                if last_dt:
                                    df = pd.read_sql(f"""
                                        SELECT date, open, high, low, close, volume
                                        FROM {price_table}
                                        WHERE {col_id}=%s AND timeframe=%s
                                          AND date >= (
                                              SELECT date
                                              FROM {price_table}
                                              WHERE {col_id}=%s AND timeframe=%s AND date <= %s
                                              ORDER BY date DESC
                                              OFFSET {lookback_rows} LIMIT 1
                                          )
                                        ORDER BY date
                                    """, conn, params=(asset_id, timeframe, asset_id, timeframe, last_dt))
                                else:
                                    df = pd.read_sql(f"""
                                        SELECT date, open, high, low, close, volume
                                        FROM {price_table}
                                        WHERE {col_id}=%s AND timeframe=%s
                                        ORDER BY date
                                    """, conn, params=(asset_id, timeframe))

                                if df.empty:
                                    continue

                                # Calculate indicators
                                df = calculate_indicators(df, latest_only=False)

                                # Keep only new rows
                                if last_dt:
                                    df = df[df["date"] > last_dt]
                                if df.empty:
                                    continue

                                # Prepare records
                                records = [
                                    (
                                        asset_id, timeframe, row["date"],
                                        row["sma_20"], row["sma_50"], row["sma_200"],
                                        row["rsi_3"], row["rsi_9"], row["rsi_14"],
                                        row["bb_upper"], row["bb_middle"], row["bb_lower"],
                                        row["atr_14"], row["supertrend"], row["supertrend_dir"],
                                        row["ema_rsi_9_3"], row["wma_rsi_9_21"],
                                        row["pct_price_change"],
                                        row["macd"], row["macd_signal"]
                                    )
                                    for _, row in df.iterrows()
                                ]

                                # Insert
                                cur.executemany(insert_sql, records)
                                conn.commit()

                                inserted_rows += len(records)
                                processed_assets += 1

                            except Exception as e:
                                log(f"❌ ERROR {asset_key} {asset_id} {timeframe} | {e}")
                                traceback.print_exc()

                        log(
                            f"  ✔ {asset_key} {timeframe} DONE | "
                            f"{processed_assets} assets | {inserted_rows} rows | "
                            f"{time.time() - tf_start:.1f}s"
                        )

        log("🎉 All indicators refreshed successfully!")

    except Exception as e:
        log(f"❌ CRITICAL FAILURE — REFRESH INDICATORS | {e}")
        traceback.print_exc()