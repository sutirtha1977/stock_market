from datetime import datetime
import pandas as pd
import traceback
from pathlib import Path
from typing import Optional, Union
from config.logger import log
from database.connection import get_db_connection, close_db_connection, validate_connection
from psycopg2.extensions import connection
from config.db_table import SYMBOL_SOURCES, ASSET_TABLE_MAP
from config.constants import FREQUENCIES
from services.validation_service import validate_asset_type, validate_symbol_format

#################################################################################################
# Checks if a specific column exists in a given table.
#################################################################################################
def table_has_column(conn: connection, table: str, column: str) -> bool:
    """Check if a column exists in a table."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = %s
              AND column_name = %s
        """, (table, column))
        return cur.fetchone() is not None

#################################################################################################
# Refreshes a single symbol table from a CSV using a shared connection.
#################################################################################################
def refresh_one_symbol_table(
    table_name: str, 
    csv_path: Union[str, Path],
    conn: connection
) -> None:
    """
    Refresh a symbol table using the provided connection. 
    This version does NOT open a new connection internally.
    """
    csv_path = str(csv_path)
    try:
        log(f"🔄 Refreshing {table_name} from {csv_path}")

        # Load CSV
        df: pd.DataFrame = pd.read_csv(csv_path)
        df.columns = [c.lower().strip() for c in df.columns]

        required: set = {"name", "yahoo_symbol", "exchange"}
        if not required.issubset(df.columns):
            raise ValueError(
                f"{csv_path} must have columns {required}, found {set(df.columns)}"
            )

        df = df[list(required)].dropna().drop_duplicates()

        records: list = [
            (
                r["name"].strip(),
                r["yahoo_symbol"].strip().upper(),
                r["exchange"].strip().upper()
            )
            for _, r in df.iterrows()
        ]

        if not records:
            log(f"⚠️ No records in {csv_path}")
            return

        with conn.cursor() as cur:
            # INSERT new symbols
            cur.executemany(f"""
                INSERT INTO {table_name} (name, yahoo_symbol, exchange)
                VALUES (%s, %s, %s)
                ON CONFLICT (yahoo_symbol) DO NOTHING
            """, records)

            # UPDATE missing fields
            cur.executemany(f"""
                UPDATE {table_name}
                SET name = %s, exchange = %s
                WHERE yahoo_symbol = %s
                  AND (name IS NULL OR name = '' OR exchange IS NULL OR exchange = '')
            """, [
                (name, exchange, yahoo_symbol)
                for name, yahoo_symbol, exchange in records
            ])

            # Reactivate if column exists
            if table_has_column(conn, table_name, "is_active"):
                cur.executemany(
                    f"UPDATE {table_name} SET is_active = TRUE WHERE yahoo_symbol = %s",
                    [(r[1],) for r in records]
                )

        conn.commit()
        log(f"✅ {table_name}: {len(records)} symbols refreshed")

    except Exception as e:
        log(f"❌ Error refreshing {table_name}: {e}")
        traceback.print_exc()
        conn.rollback()
        raise

#################################################################################################
# Refresh all symbol tables from configured CSV sources using ONE connection
#################################################################################################
def refresh_symbols() -> None:
    log("🚀 Starting full symbol refresh")
    conn: Optional[connection] = None
    try:
        conn = get_db_connection()
        validate_connection(conn)

        for table, csv_path in SYMBOL_SOURCES:
            try:
                refresh_one_symbol_table(table, csv_path, conn)
            except Exception as e:
                log(f"⚠️ Skipped {table} due to error: {type(e).__name__}: {e}")

        log("🎯 Symbol refresh completed")

    except Exception as e:
        log(f"❌ Full symbol refresh failed: {e}")
        traceback.print_exc()

    finally:
        if conn:
            close_db_connection(conn)

#################################################################################################
# Retrieves symbols from the database based on input criteria.
#################################################################################################
def retrieve_symbols(symbol: str, conn: connection, asset_type: str) -> pd.DataFrame:
    try:
        validate_asset_type(asset_type, context="Retrieve symbols")
        validate_connection(conn)
        
        table = ASSET_TABLE_MAP[asset_type][0]  # symbol table
        select_cols = "symbol_id, name, yahoo_symbol"

        if not symbol or not symbol.strip():
            log("No symbol provided")
            return pd.DataFrame()

        symbol_clean = symbol.strip().upper()

        # --- Fetch all symbols ---
        if symbol_clean == "ALL":
            query = f"SELECT {select_cols} FROM {table} ORDER BY yahoo_symbol"
            df = pd.read_sql(query, conn)
            log(f"Retrieved all symbols | Count: {len(df)}")
            return df

        # --- Parse comma-separated list ---
        symbols_list = [s.strip().upper() for s in symbol.split(",") if s.strip()]
        if not symbols_list:
            log("No valid symbols parsed")
            return pd.DataFrame()

        placeholders = ", ".join(["%s"] * len(symbols_list))
        query = f"""
            SELECT {select_cols}
            FROM {table}
            WHERE yahoo_symbol IN ({placeholders})
              AND is_active = TRUE
            ORDER BY yahoo_symbol
        """
        df = pd.read_sql(query, conn, params=tuple(symbols_list))
        log(f"Retrieved symbols | Count: {len(df)} | Symbols: {symbols_list}")
        return df

    except Exception as e:
        log(f"❌ RETRIEVE SYMBOL FAILED: {e}")
        traceback.print_exc()
        return pd.DataFrame()

#################################################################################################
# Returns the latest trading date for the specified asset type and timeframe.
#################################################################################################
def get_latest_trading_date(asset_type: str, timeframe: str = "1d") -> Optional[datetime]:

    if asset_type not in ASSET_TABLE_MAP:
        raise ValueError(f"Unsupported asset_type: {asset_type}")

    symbol_table = ASSET_TABLE_MAP[asset_type][0]
    price_table  = ASSET_TABLE_MAP[asset_type][1]

    conn: Optional[connection] = None
    try:
        conn = get_db_connection()

        use_is_active = asset_type in {"india_equity", "usa_equity", "india_index", "usa_index"}

        if use_is_active:
            sql = f"""
                SELECT MAX(p.date) AS latest_date
                FROM {price_table} p
                JOIN {symbol_table} s
                  ON p.symbol_id = s.symbol_id
                WHERE p.timeframe = %s
                  AND s.is_active = TRUE
            """
        else:
            sql = f"""
                SELECT MAX(date) AS latest_date
                FROM {price_table}
                WHERE timeframe = %s
            """

        df = pd.read_sql(sql, conn, params=[timeframe])
        latest = df.iloc[0]["latest_date"]

        return latest if latest else None

    except Exception as e:
        log(f"❗ Error fetching latest trading date from {price_table}: {e}")
        traceback.print_exc()
        return None

    finally:
        if conn:
            close_db_connection(conn)

#################################################################################################
# Identifies symbols missing price data across all asset types and timeframes.
#################################################################################################
def find_missing_price_data_symbols_all_assets() -> dict:
    results = {}
    conn: Optional[connection] = None

    try:
        conn = get_db_connection()

        expected_timeframes_sql = (
            "SELECT unnest(ARRAY[%s]) AS timeframe"
            % ",".join(f"'{tf}'" for tf in FREQUENCIES)
        )

        for asset_type, tables in ASSET_TABLE_MAP.items():

            # TEMPORARY SKIP FOR TESTING
            if asset_type in ("india_equity_test",):
                continue

            symbol_table = tables[0]
            price_table = tables[1]

            log(f"🔍 Checking missing price data | asset_type={asset_type}")

            sql = f"""
                WITH expected_timeframes AS (
                    {expected_timeframes_sql}
                )
                SELECT
                    s.symbol_id,
                    s.yahoo_symbol,
                    s.name,
                    tf.timeframe AS missing_timeframe
                FROM {symbol_table} s
                CROSS JOIN expected_timeframes tf
                LEFT JOIN {price_table} p
                       ON p.symbol_id = s.symbol_id
                      AND p.timeframe = tf.timeframe
                WHERE p.symbol_id IS NULL
                ORDER BY s.yahoo_symbol, tf.timeframe
            """

            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    df = pd.DataFrame(rows, columns=columns)

            except Exception as e:
                log(f"❌ Failed to fetch missing symbols for {asset_type}: {e}")
                traceback.print_exc()
                continue

            if df.empty:
                log(f"✅ {asset_type}: No missing symbols")
                continue

            missing_symbols = sorted(df["yahoo_symbol"].unique())
            log(
                f"❌ {asset_type} | Missing symbols ({len(missing_symbols)}): "
                + ",".join(missing_symbols)
            )

            results[asset_type] = df

        return results

    except Exception as e:
        log(f"❌ Error checking missing price data symbols (overall): {e}")
        traceback.print_exc()
        return {}

    finally:
        if conn:
            close_db_connection(conn)