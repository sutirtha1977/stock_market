import os
import traceback
import pandas as pd
from datetime import datetime
from typing import Optional
from config.db_table import ASSET_TABLE_MAP
from database.connection import get_db_connection, close_db_connection
from config.logger import log
from config.paths import SCANNER_FOLDER_WEEKLY
from services.cleanup_service import delete_files_in_folder
from services.validation_service import validate_asset_type

#################################################################################################
# Fetch weekly base data for a particular scan date
# Includes lookbacks (1w close, 2w sma, 4w min low)
#################################################################################################
def fetch_base_data_for_scan_date(asset_type: str, scan_date: str) -> pd.DataFrame:
    """
    Fetch weekly price + indicator data with lookbacks for a single scan_date.
    """
    if asset_type not in ASSET_TABLE_MAP:
        raise ValueError(f"Unsupported asset_type: {asset_type}")

    symbol_table, price_table, indicator_table, _ = ASSET_TABLE_MAP[asset_type]
    conn = get_db_connection()

    sql = f"""
        WITH weekly_history AS (
            SELECT 
                p.symbol_id,
                s.yahoo_symbol,
                p.date,
                p.open AS weekly_open,
                p.high AS weekly_high,
                p.low AS weekly_low,
                p.close AS weekly_close,
                i.sma_20,
                i.rsi_3 AS rsi_3_weekly,
                i.rsi_9 AS rsi_9_weekly,
                i.ema_rsi_9_3,
                i.wma_rsi_9_21
            FROM {price_table} p
            JOIN {indicator_table} i
              ON p.symbol_id = i.symbol_id
             AND p.date = i.date
             AND i.timeframe = '1wk'
            JOIN {symbol_table} s
              ON s.symbol_id = p.symbol_id
            WHERE p.timeframe = '1wk'
              AND p.date <= %(scan_date)s
              AND s.is_active = TRUE
        ),
        weekly_with_lookbacks AS (
            SELECT *,
                LAG(weekly_close, 1) OVER w AS close_1w_ago,
                LAG(sma_20, 2) OVER w AS sma_20_2w_ago,
                MIN(weekly_low) OVER (
                    PARTITION BY symbol_id
                    ORDER BY date
                    ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
                ) AS min_low_4w_ago
            FROM weekly_history
            WINDOW w AS (PARTITION BY symbol_id ORDER BY date)
        )
        SELECT *
        FROM weekly_with_lookbacks
        WHERE date = %(scan_date)s
        ORDER BY yahoo_symbol;
    """

    try:
        df = pd.read_sql(sql, conn, params={"scan_date": scan_date})
        return df
    finally:
        close_db_connection(conn)


#################################################################################################
# Apply weekly scanner logic
#################################################################################################
def apply_scanner_logic(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # Ensure all lookbacks exist
    df = df.dropna(subset=[
        "close_1w_ago",
        "sma_20_2w_ago",
        "min_low_4w_ago",
        "ema_rsi_9_3",
        "wma_rsi_9_21"
    ])

    signals = df[
        (df["weekly_close"] > df["sma_20"]) &
        (df["weekly_low"] <= df["min_low_4w_ago"]) &
        (df["sma_20_2w_ago"] < df["sma_20"]) &
        (df["weekly_close"] >= df["close_1w_ago"]) &
        (df["weekly_close"] > 100) &
        (df["rsi_3_weekly"] / df["rsi_9_weekly"] >= 1.15) &
        (df["rsi_9_weekly"] / df["ema_rsi_9_3"] >= 1.04) &
        (df["ema_rsi_9_3"] / df["wma_rsi_9_21"] >= 1) &
        (df["rsi_9_weekly"] > 50)
    ]

    return signals.sort_values(["yahoo_symbol"]).reset_index(drop=True)


#################################################################################################
# Run Weekly Scanner for a single scan date
#################################################################################################
def run_scanner_weekly(scan_date: str, asset_type: str = "india_equity_yahoo") -> pd.DataFrame:
    try:
        validate_asset_type(asset_type, context="Weekly scanner")

        folder_path = os.path.join(SCANNER_FOLDER_WEEKLY, asset_type)
        os.makedirs(folder_path, exist_ok=True)
        delete_files_in_folder(folder_path)

        log(f"🔍 Weekly scanner | scan_date={scan_date}")

        df = fetch_base_data_for_scan_date(asset_type=asset_type, scan_date=scan_date)
        signals = apply_scanner_logic(df)

        if signals.empty:
            log("⚠ No weekly signals found")
            return pd.DataFrame()

        ts = datetime.now().strftime("%d%b%Y")
        filename = f"WEEKLY_{ts}.csv"
        filepath = os.path.join(folder_path, filename)
        signals.to_csv(filepath, index=False)
        log(f"✅ Weekly scanner results exported | {filepath}")

        return signals

    except Exception as e:
        log(f"❌ Weekly scanner failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()