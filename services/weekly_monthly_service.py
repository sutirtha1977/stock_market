from database.connection import get_db_connection, close_db_connection
from psycopg2.extensions import connection
from tqdm import tqdm
import pandas as pd
from datetime import datetime
from config.logger import log
from config.db_table import ASSET_TABLE_MAP
from typing import Optional, List, Tuple
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

#################################################################################################
# Refresh 52-week high/low stats for a given asset type (equity, crypto, forex)
#################################################################################################
def refresh_week52_high_low_stats(asset_key: str) -> None:
    """Refresh 52-week high/low statistics for asset type.
    
    Args:
        asset_key: Key from ASSET_TABLE_MAP (e.g., 'india_equity', 'crypto', 'forex').
    """
    if asset_key not in ASSET_TABLE_MAP:
        log(f"❌ Unknown asset_key: {asset_key}")
        return

    symbol_table: str
    price_table: str
    stats_table: str
    symbol_table, price_table, _, stats_table = ASSET_TABLE_MAP[asset_key]
    col_id: str = "symbol_id"  # all tables use symbol_id

    try:
        conn: connection = get_db_connection()
        cur = conn.cursor()
        log(f"📊 Updating 52W stats for {price_table}")

        # -----------------------------
        # Get all symbols with daily data
        # -----------------------------
        cur.execute(f"""
            SELECT DISTINCT {col_id}
            FROM {price_table}
            WHERE timeframe = '1d'
        """)
        ids: List[int] = [r[0] for r in cur.fetchall()]
        if not ids:
            log(f"⚠ No daily data found in {price_table}, skipping")
            return

        # -----------------------------
        # Fetch 52-week high/low
        # -----------------------------
        placeholders: str = ','.join(['%s'] * len(ids))
        cur.execute(f"""
            SELECT {col_id}, MAX(high), MIN(low)
            FROM {price_table}
            WHERE timeframe = '1d'
              AND {col_id} IN ({placeholders})
              AND date >= CURRENT_DATE - INTERVAL '1 year'
            GROUP BY {col_id}
        """, ids)

        results: List[Tuple] = [(sid, high, low) for sid, high, low in cur.fetchall() if high is not None]
        if not results:
            log(f"⚠ No 52W data found in {price_table}")
            return

        # -----------------------------
        # UPSERT into stats table
        # -----------------------------
        for sid, high52, low52 in results:
            cur.execute(f"""
                INSERT INTO {stats_table} (
                    {col_id}, week52_high, week52_low, as_of_date
                )
                VALUES (%s, %s, %s, CURRENT_DATE)
                ON CONFLICT ({col_id}) DO UPDATE SET
                    week52_high = EXCLUDED.week52_high,
                    week52_low  = EXCLUDED.week52_low,
                    as_of_date  = EXCLUDED.as_of_date
            """, (sid, high52, low52))

        conn.commit()
        log(f"✅ {stats_table}: Updated {len(results)} rows")

    except Exception as e:
        if conn:
            conn.rollback()
        log(f"❌ 52W update failed for {asset_key}: {e}")
        import traceback; traceback.print_exc()

    finally:
        try:
            if cur:
                cur.close()
        except Exception as e:
            log(f"⚠️ Failed to close cursor: {e}")
        if conn:
            close_db_connection(conn)
#################################################################################################
# Refresh 52-week high/low stats for ALL asset types
#################################################################################################
def refresh_all_week52_stats() -> None:
    """Refresh 52-week high/low statistics for all asset types.
    
    Iterates through all configured asset types and updates their
    52-week high/low calculations in the database.
    """
    for asset_key in ASSET_TABLE_MAP.keys():
        refresh_week52_high_low_stats(asset_key)  
        
#################################################################################################
# Generate Weekly and Monthly OHLCV from Daily Data
#################################################################################################    
def generate_higher_timeframes(asset_type: str = "india_equity_nse") -> None:
    """
    Generate weekly and monthly OHLCV from daily data and upsert into the same price table.
    """
    try:
        if asset_type not in ASSET_TABLE_MAP:
            raise ValueError(f"Unsupported asset_type: {asset_type}")

        price_table = ASSET_TABLE_MAP[asset_type][1]

        conn = get_db_connection()
        try:
            # Fetch all symbols with daily data in one go
            cur = conn.cursor()
            cur.execute(f"""
                SELECT DISTINCT symbol_id
                FROM {price_table}
                WHERE timeframe='1d'
            """)
            symbol_ids = [row[0] for row in cur.fetchall()]

            if not symbol_ids:
                log("⚠ No symbols with daily data found.")
                return

            # Process each symbol
            for symbol_id in tqdm(symbol_ids, desc=f"{asset_type} Symbols", ncols=100, leave=False):
                try:
                    df_daily = pd.read_sql(
                        f"""
                        SELECT date, open, high, low, close, volume
                        FROM {price_table}
                        WHERE symbol_id=%s AND timeframe='1d'
                        ORDER BY date
                        """,
                        conn,
                        params=(symbol_id,)
                    )

                    if df_daily.empty:
                        continue

                    df_daily["date"] = pd.to_datetime(df_daily["date"])
                    df_daily = df_daily.set_index("date").sort_index()
                    last_daily_date = df_daily.index.max()

                    # Weekly
                    df_weekly = df_daily.resample("W-FRI").agg({
                        "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"
                    }).dropna()
                    df_weekly = df_weekly[df_weekly.index <= last_daily_date].reset_index()
                    df_weekly["symbol_id"] = symbol_id
                    df_weekly["timeframe"] = "1wk"

                    # Monthly
                    df_monthly = df_daily.resample("M").agg({
                        "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"
                    }).dropna()
                    df_monthly = df_monthly[df_monthly.index <= last_daily_date].reset_index()
                    df_monthly["symbol_id"] = symbol_id
                    df_monthly["timeframe"] = "1mo"

                    # Round numeric columns
                    for df_tf in (df_weekly, df_monthly):
                        if df_tf.empty:
                            continue
                        for col in ["open", "high", "low", "close", "volume"]:
                            df_tf[col] = df_tf[col].round(2)

                        # UPSERT batch
                        rows = [
                            (
                                row.symbol_id,
                                row.timeframe,
                                row.date.date(),
                                row.open,
                                row.high,
                                row.low,
                                row.close,
                                row.volume,
                            )
                            for row in df_tf.itertuples(index=False)
                        ]

                        insert_sql = f"""
                            INSERT INTO {price_table}
                            (symbol_id, timeframe, date, open, high, low, close, volume)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (symbol_id, timeframe, date)
                            DO UPDATE SET
                                open   = EXCLUDED.open,
                                high   = EXCLUDED.high,
                                low    = EXCLUDED.low,
                                close  = EXCLUDED.close,
                                volume = EXCLUDED.volume
                        """
                        cur.executemany(insert_sql, rows)
                        conn.commit()

                except Exception as e:
                    log(f"❌ FAILED processing symbol_id={symbol_id} | {e}")

        finally:
            close_db_connection(conn)

    except Exception as e:
        import traceback
        log(f"❌ CRITICAL FAILURE in generate_higher_timeframes | {e}")
        traceback.print_exc()