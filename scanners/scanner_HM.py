import os
import traceback
import pandas as pd
from datetime import datetime
from typing import Optional

from config.db_table import ASSET_TABLE_MAP
from database.connection import get_db_connection, close_db_connection
from config.logger import log
from config.paths import SCANNER_FOLDER_HM
from services.cleanup_service import delete_files_in_folder
from services.validation_service import validate_asset_type

#################################################################################################
# HTF-FIRST BASE DATA (MONTH → WEEK → DAY) FOR A SINGLE SCAN DATE
#################################################################################################
def get_hilega_milega_base_data(
    scan_date: str,
    asset_type: str = "india_equity_yahoo"
) -> pd.DataFrame:
    """
    Fetch daily price + indicator data for the scan_date.
    Weekly and monthly RSI are as-of-date <= scan_date.
    """
    if asset_type not in ASSET_TABLE_MAP:
        raise ValueError(f"Unsupported asset_type: {asset_type}")

    symbol_table, price_table, indicator_table, _ = ASSET_TABLE_MAP[asset_type]
    conn = get_db_connection()

    try:
        sql = f"""
        SELECT
            d.symbol_id,
            s.yahoo_symbol,
            d.date,

            d.rsi_3,
            d.rsi_9,
            d.ema_rsi_9_3,
            d.wma_rsi_9_21,

            p.close,

            w.rsi_3 AS rsi_3_weekly,
            m.rsi_3 AS rsi_3_monthly

        FROM {indicator_table} d

        JOIN {price_table} p
          ON p.symbol_id = d.symbol_id
         AND p.date = d.date
         AND p.timeframe = '1d'

        JOIN {symbol_table} s
          ON s.symbol_id = d.symbol_id

        -- weekly as-of-date
        LEFT JOIN LATERAL (
            SELECT rsi_3
            FROM {indicator_table} w
            WHERE w.symbol_id = d.symbol_id
              AND w.timeframe='1wk'
              AND w.date <= d.date
            ORDER BY w.date DESC
            LIMIT 1
        ) w ON TRUE

        -- monthly as-of-date
        LEFT JOIN LATERAL (
            SELECT rsi_3
            FROM {indicator_table} m
            WHERE m.symbol_id = d.symbol_id
              AND m.timeframe='1mo'
              AND m.date <= d.date
            ORDER BY m.date DESC
            LIMIT 1
        ) m ON TRUE

        WHERE d.timeframe='1d'
          AND d.date = %(scan_date)s

        ORDER BY d.symbol_id, d.date
        """

        df = pd.read_sql(sql, conn, params={"scan_date": scan_date})

        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])

        log(f"📊 Base rows fetched: {len(df)}")
        return df

    finally:
        close_db_connection(conn)

#################################################################################################
# Hilega–Milega Scanner Logic
#################################################################################################
def apply_hilega_milega_logic(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    required_cols = [
        "date", "yahoo_symbol",
        "close",
        "rsi_3", "rsi_9",
        "ema_rsi_9_3", "wma_rsi_9_21",
        "rsi_3_weekly", "rsi_3_monthly"
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    signals = df[
        (df["close"] >= 60)
        & (df["rsi_3"] / df["rsi_9"] >= 1.15)
        & (df["rsi_9"] / df["ema_rsi_9_3"] > 1.05)
        & (df["ema_rsi_9_3"] / df["wma_rsi_9_21"] >= 1.00)
        & (df["rsi_3"] < 60)
        & (df["rsi_3_weekly"] > 60)
        & (df["rsi_3_monthly"] > 50)
    ]

    return (
        signals
        .sort_values(["date", "yahoo_symbol"], ascending=[False, True])
        .reset_index(drop=True)
    )

#################################################################################################
# Run Hilega–Milega Scanner for a single scan date
#################################################################################################
def run_scanner_hilega_milega(scan_date: str, asset_type: str = "india_equity_yahoo") -> pd.DataFrame:

    try:
        validate_asset_type(asset_type, context="Scanner HM")

        folder_path = os.path.join(SCANNER_FOLDER_HM, asset_type)
        os.makedirs(folder_path, exist_ok=True)
        delete_files_in_folder(folder_path)

        log(f"🔍 HM scanner | scan_date={scan_date}")

        df_base = get_hilega_milega_base_data(
            scan_date=scan_date,
            asset_type=asset_type
        )

        if df_base.empty:
            log("⚠ No base data found for scan_date")
            return pd.DataFrame()

        signals = apply_hilega_milega_logic(df_base)

        if signals.empty:
            log("⚠ No HM signals found")
            return pd.DataFrame()

        ts = datetime.now().strftime("%d%b%Y")
        filename = f"HM_{ts}.csv"
        filepath = os.path.join(folder_path, filename)
        signals.to_csv(filepath, index=False)
        log(f"✅ Hilega–Milega scanner results exported | {filepath}")

        return signals

    except Exception as e:
        log(f"❌ HM scanner failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()