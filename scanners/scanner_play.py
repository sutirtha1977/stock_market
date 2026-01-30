import os
import traceback
import pandas as pd
from datetime import datetime
from config.logger import log
from config.paths import SCANNER_FOLDER_PLAY
from database.connection import get_db_connection, close_db_connection
from services.cleanup_service import delete_files_in_folder
from scanners.backtest_service import backtest_scanners
from config.db_table import ASSET_TABLE_MAP

#################################################################################################
# Fetch weekly base data for a date range (includes lookbacks)
#################################################################################################
def fetch_weekly_data_for_range(asset_type: str, start_date: str, end_date: str) -> pd.DataFrame:
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
              AND p.date BETWEEN %(start_date)s AND %(end_date)s
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
        ORDER BY date, yahoo_symbol;
    """

    try:
        df = pd.read_sql(sql, conn, params={"start_date": start_date, "end_date": end_date})
        return df
    finally:
        close_db_connection(conn)


#################################################################################################
# Apply weekly scanner logic
#################################################################################################
def apply_weekly_scanner_logic(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

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

    return signals.sort_values(["date", "yahoo_symbol"]).reset_index(drop=True)


#################################################################################################
# Multi-year weekly backtest using date-range fetch
# Export signals year-wise as YEARLY_YYYY.csv
#################################################################################################
def scanner_backtest_multi_years(
    asset_type: str = "india_equity_yahoo",
    start_year: int = 2026,
    lookback_years: int = 5
) -> pd.DataFrame:

    try:
        base_folder = os.path.join(SCANNER_FOLDER_PLAY, asset_type)
        os.makedirs(base_folder, exist_ok=True)
        delete_files_in_folder(base_folder)
        log(f"🗑 Cleared existing scanner files in {base_folder}")

        log(f"📆 Multi-year weekly backtest | {lookback_years} years up to {start_year}")

        # -------------------- CALCULATE DATE RANGE --------------------
        start_date = f"{start_year - lookback_years + 1}-01-01"
        end_date = f"{start_year}-12-31"
        log(f"🗓 Fetching weekly data from {start_date} to {end_date}")

        # -------------------- FETCH WEEKLY DATA FOR RANGE --------------------
        df_weekly = fetch_weekly_data_for_range(asset_type, start_date, end_date)
        if df_weekly.empty:
            log("⚠ No weekly data found for the date range")
            return pd.DataFrame()
        log(f"📊 Weekly data rows fetched: {len(df_weekly)}")

        # -------------------- APPLY SCANNER LOGIC --------------------
        df_signals = apply_weekly_scanner_logic(df_weekly)
        if df_signals.empty:
            log("⚠ No signals found in the entire backtest period")
            return pd.DataFrame()
        log(f"✅ Total signals detected: {len(df_signals)}")

        # -------------------- EXPORT YEARLY CSV FILES --------------------
        df_signals["year"] = pd.to_datetime(df_signals["date"]).dt.year
        years = df_signals["year"].unique()
        log(f"📁 Exporting signals for years: {sorted(years)}")

        for yr in sorted(years):
            yearly_df = df_signals[df_signals["year"] == yr].drop(columns=["year"])
            yearly_csv_path = os.path.join(base_folder, f"YEARLY_{yr}.csv")
            yearly_df.to_csv(yearly_csv_path, index=False)
            log(f"💾 Exported YEARLY_{yr}.csv | rows={len(yearly_df)}")

        # -------------------- RUN BACKTEST SUMMARY --------------------
        summary_df = backtest_scanners(
            asset_type=asset_type,
            folder_path=base_folder
        )
        log(f"🎯 Backtest summary complete | {len(summary_df)} rows")
        return summary_df

    except Exception as e:
        log(f"❌ Multi-year weekly backtest failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()