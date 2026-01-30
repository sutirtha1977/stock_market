import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from config.db_table import ASSET_TABLE_MAP
from database.connection import get_db_connection, close_db_connection

#################################################################################################
# Fetch weekly base data (row-sequence based, no calendar assumptions)
# OUTPUT CONTRACT:
# - MUST contain `date` as the signal timestamp
#################################################################################################
# def fetch_weekly_base_data(
#     asset_type: str = "india_equity_yahoo",
#     end_date: str | None = None,
#     lookback_days: int = 365
# ) -> pd.DataFrame:

#     if asset_type not in ASSET_TABLE_MAP:
#         raise ValueError(f"Unsupported asset_type: {asset_type}")

#     symbol_table, price_table, indicator_table, _ = ASSET_TABLE_MAP[asset_type]
#     conn = get_db_connection()

#     # -------------------- DATE RANGE --------------------
#     end_date_dt = (
#         datetime.strptime(end_date, "%Y-%m-%d")
#         if end_date else datetime.today()
#     )
#     start_date_dt = end_date_dt - timedelta(days=lookback_days)

#     sql = f"""
#         WITH weekly_base AS (
#             SELECT
#                 d.symbol_id,
#                 s.yahoo_symbol,
#                 d.date AS date,

#                 p.open  AS weekly_open,
#                 p.high  AS weekly_high,
#                 p.low   AS weekly_low,
#                 p.close AS weekly_close,

#                 d.sma_20       AS sma_20,
#                 d.rsi_3        AS rsi_3_weekly,
#                 d.rsi_9        AS rsi_9_weekly,
#                 d.ema_rsi_9_3  AS ema_rsi_9_3_weekly,
#                 d.wma_rsi_9_21 AS wma_rsi_9_21_weekly,

#                 -- sequence-based lookbacks (NO calendar assumptions)
#                 LAG(p.close, 1) OVER w AS close_1w_ago,
#                 LAG(d.sma_20, 2) OVER w AS sma_20_2w_ago,

#                 MIN(p.low) OVER (
#                     PARTITION BY d.symbol_id
#                     ORDER BY d.date
#                     ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
#                 ) AS min_low_4w_ago

#             FROM {indicator_table} d
#             JOIN {price_table} p
#               ON p.symbol_id = d.symbol_id
#              AND p.date = d.date
#              AND p.timeframe = '1wk'
#             JOIN {symbol_table} s
#               ON s.symbol_id = d.symbol_id

#             WHERE d.timeframe = '1wk'
#               AND d.date <= %(end_date)s
#               AND d.date >= %(start_date)s

#             WINDOW w AS (
#                 PARTITION BY d.symbol_id
#                 ORDER BY d.date
#             )
#         )
#         SELECT *
#         FROM weekly_base
#         ORDER BY symbol_id, date;
#     """

#     try:
#         df = pd.read_sql(
#             sql,
#             conn,
#             params={
#                 "start_date": start_date_dt.strftime("%Y-%m-%d"),
#                 "end_date": end_date_dt.strftime("%Y-%m-%d"),
#             }
#         )

#         df["date"] = pd.to_datetime(df["date"])
#         return df

#     finally:
#         close_db_connection(conn)
        
#################################################################################################
# Base data fetcher used by HM scanner
#################################################################################################
def get_base_data(
    asset_type: str = "india_equity_yahoo",
    end_date: str | None = None,
    lookback_days: int = 365
) -> pd.DataFrame:

    if asset_type not in ASSET_TABLE_MAP:
        raise ValueError(f"Unsupported asset_type: {asset_type}")

    symbol_table, price_table, indicator_table, _ = ASSET_TABLE_MAP[asset_type]
    conn = get_db_connection()

    # -------------------- DATE RANGE --------------------
    end_date_dt = (
        datetime.strptime(end_date, "%Y-%m-%d")
        if end_date else datetime.today()
    )
    start_date_dt = end_date_dt - timedelta(days=lookback_days)

    try:
        # ---------------------------------------------------
        # DAILY base data (ONLY what HM needs)
        # ---------------------------------------------------
        daily_sql = f"""
            SELECT
                d.symbol_id,
                s.yahoo_symbol,
                d.date,

                p.close,

                d.rsi_3,
                d.rsi_9,
                d.ema_rsi_9_3,
                d.wma_rsi_9_21

            FROM {indicator_table} d
            JOIN {price_table} p
              ON p.symbol_id = d.symbol_id
             AND p.date = d.date
             AND p.timeframe = '1d'
            JOIN {symbol_table} s
              ON s.symbol_id = d.symbol_id

            WHERE d.timeframe = '1d'
              AND d.date >= %(start_date)s
              AND d.date <= %(end_date)s

            ORDER BY d.symbol_id, d.date
        """

        df = pd.read_sql(
            daily_sql,
            conn,
            params={
                "start_date": start_date_dt.strftime("%Y-%m-%d"),
                "end_date": end_date_dt.strftime("%Y-%m-%d"),
            }
        )

        if df.empty:
            return df

        df["date"] = pd.to_datetime(df["date"])

        # ---------------------------------------------------
        # WEEKLY RSI(3) — last completed week
        # ---------------------------------------------------
        weekly_sql = f"""
            SELECT
                symbol_id,
                date AS weekly_date,
                rsi_3 AS rsi_3_weekly
            FROM {indicator_table}
            WHERE timeframe = '1wk'
              AND date >= %(start_date)s
              AND date <= %(end_date)s
        """

        df_weekly = pd.read_sql(
            weekly_sql,
            conn,
            params={
                "start_date": start_date_dt.strftime("%Y-%m-%d"),
                "end_date": end_date_dt.strftime("%Y-%m-%d"),
            }
        )

        df_weekly["weekly_date"] = pd.to_datetime(df_weekly["weekly_date"])

        df = df.merge(df_weekly, on="symbol_id", how="left")
        df = df[df["weekly_date"] <= df["date"]]
        df = (
            df.sort_values(["symbol_id", "date", "weekly_date"])
              .groupby(["symbol_id", "date"], as_index=False)
              .last()
        )

        # ---------------------------------------------------
        # MONTHLY RSI(3) — last completed month
        # ---------------------------------------------------
        monthly_sql = f"""
            SELECT
                symbol_id,
                date AS monthly_date,
                rsi_3 AS rsi_3_monthly
            FROM {indicator_table}
            WHERE timeframe = '1mo'
              AND date >= %(start_date)s
              AND date <= %(end_date)s
        """

        df_monthly = pd.read_sql(
            monthly_sql,
            conn,
            params={
                "start_date": start_date_dt.strftime("%Y-%m-%d"),
                "end_date": end_date_dt.strftime("%Y-%m-%d"),
            }
        )

        df_monthly["monthly_date"] = pd.to_datetime(df_monthly["monthly_date"])

        df = df.merge(df_monthly, on="symbol_id", how="left")
        df = df[df["monthly_date"] <= df["date"]]
        df = (
            df.sort_values(["symbol_id", "date", "monthly_date"])
              .groupby(["symbol_id", "date"], as_index=False)
              .last()
        )

        return df

    finally:
        close_db_connection(conn)
        
#################################################################################################
# Base data fetcher used by HM scanner
#################################################################################################
def get_daily_data(
    asset_type: str = "india_equity_yahoo",
    end_date: str | None = None,
    lookback_days: int = 365
) -> pd.DataFrame:
    """Fetch daily OHLC for scanner logic (lightweight version)."""

    if asset_type not in ASSET_TABLE_MAP:
        raise ValueError(f"Unsupported asset_type: {asset_type}")

    symbol_table, price_table, indicator_table, _ = ASSET_TABLE_MAP[asset_type]
    conn = get_db_connection()

    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.today()
    start_date_dt = end_date_dt - timedelta(days=lookback_days)

    try:
        sql = f"""
            SELECT
                s.symbol_id,
                s.yahoo_symbol,
                p.date,
                p.open,
                p.low,
                p.close
            FROM {price_table} p
            JOIN {symbol_table} s ON s.symbol_id = p.symbol_id
            WHERE p.timeframe = '1d'
              AND p.date BETWEEN %(start_date)s AND %(end_date)s
            ORDER BY s.symbol_id, p.date
        """

        df = pd.read_sql(
            sql,
            conn,
            params={
                "start_date": start_date_dt.strftime("%Y-%m-%d"),
                "end_date": end_date_dt.strftime("%Y-%m-%d")
            }
        )

        if df.empty:
            return df

        df["date"] = pd.to_datetime(df["date"])
        return df

    finally:
        close_db_connection(conn)