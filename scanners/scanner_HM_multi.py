import os
import traceback
import pandas as pd
from datetime import datetime
from config.logger import log
from config.paths import SCANNER_FOLDER_HM
from database.connection import get_db_connection, close_db_connection
from services.cleanup_service import delete_files_in_folder
from services.validation_service import validate_asset_type
from config.db_table import ASSET_TABLE_MAP

#################################################################################################
# Fetch daily HM base data for a date range (includes weekly & monthly RSI lookbacks)
#################################################################################################
def fetch_hilega_milega_data_for_range(asset_type: str, start_date: str, end_date: str) -> pd.DataFrame:
    if asset_type not in ASSET_TABLE_MAP:
        raise ValueError(f"Unsupported asset_type: {asset_type}")

    symbol_table, price_table, indicator_table, _ = ASSET_TABLE_MAP[asset_type]
    conn = get_db_connection()

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
         AND p.timeframe='1d'
        JOIN {symbol_table} s
          ON s.symbol_id = d.symbol_id
        -- weekly RSI as-of-date
        LEFT JOIN LATERAL (
            SELECT rsi_3
            FROM {indicator_table} w
            WHERE w.symbol_id = d.symbol_id
              AND w.timeframe='1wk'
              AND w.date <= d.date
            ORDER BY w.date DESC
            LIMIT 1
        ) w ON TRUE
        -- monthly RSI as-of-date
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
          AND d.date BETWEEN %(start_date)s AND %(end_date)s
          AND s.is_active=TRUE
        ORDER BY d.date, s.yahoo_symbol
    """

    try:
        df = pd.read_sql(sql, conn, params={"start_date": start_date, "end_date": end_date})
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        log(f"📊 HM base rows fetched: {len(df)}")
        return df
    finally:
        close_db_connection(conn)

#################################################################################################
# Apply Hilega–Milega scanner logic
#################################################################################################
def apply_hilega_milega_logic(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    required_cols = [
        "date", "yahoo_symbol", "close",
        "rsi_3", "rsi_9", "ema_rsi_9_3", "wma_rsi_9_21",
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

    return signals.sort_values(["date", "yahoo_symbol"], ascending=[False, True]).reset_index(drop=True)

#################################################################################################
# Multi-year Hilega–Milega backtest
# Exports YEARLY_YYYY.csv for each year
#################################################################################################
def scanner_backtest_multi_years_hm(
    asset_type: str = "india_equity_yahoo",
    start_year: int = 2026,
    lookback_years: int = 5
) -> pd.DataFrame:
    try:
        base_folder = os.path.join(SCANNER_FOLDER_HM, asset_type)
        os.makedirs(base_folder, exist_ok=True)
        delete_files_in_folder(base_folder)
        log(f"🗑 Cleared existing HM scanner files in {base_folder}")

        validate_asset_type(asset_type, context="HM Multi-year Scanner")
        log(f"📆 Multi-year HM backtest | {lookback_years} years up to {start_year}")

        start_date = f"{start_year - lookback_years + 1}-01-01"
        end_date = f"{start_year}-12-31"
        log(f"🗓 Fetching daily HM data from {start_date} to {end_date}")

        df_hm = fetch_hilega_milega_data_for_range(asset_type, start_date, end_date)
        if df_hm.empty:
            log("⚠ No HM base data found for date range")
            return pd.DataFrame()
        log(f"📊 HM data rows fetched: {len(df_hm)}")

        df_signals = apply_hilega_milega_logic(df_hm)
        if df_signals.empty:
            log("⚠ No HM signals found in the entire backtest period")
            return pd.DataFrame()
        log(f"✅ Total HM signals detected: {len(df_signals)}")

        # -------------------- EXPORT YEARLY CSV FILES --------------------
        df_signals["year"] = pd.to_datetime(df_signals["date"]).dt.year
        years = df_signals["year"].unique()
        log(f"📁 Exporting HM signals for years: {sorted(years, reverse=True)}")

        for yr in sorted(years, reverse=True):
            yearly_df = df_signals[df_signals["year"] == yr].drop(columns=["year"])
            yearly_csv_path = os.path.join(base_folder, f"HM_YEARLY_{yr}.csv")
            yearly_df.to_csv(yearly_csv_path, index=False)
            log(f"💾 Exported HM_YEARLY_{yr}.csv | rows={len(yearly_df)}")

        # -------------------- RUN BACKTEST SUMMARY --------------------
        from scanners.backtest_service import backtest_scanners  # reuse your existing summary
        summary_df = backtest_scanners(asset_type=asset_type, folder_path=base_folder)
        log(f"🎯 HM Backtest summary complete | {len(summary_df)} rows")

        return summary_df

    except Exception as e:
        log(f"❌ Multi-year HM backtest failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()