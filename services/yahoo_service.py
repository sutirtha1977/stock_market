import os
import traceback
from datetime import datetime, date, timedelta
from typing import Optional, List, Tuple
import yfinance as yf
import pandas as pd
from tqdm import tqdm
from config.logger import log
from config.paths import YAHOO_DIR
from config.constants import FREQUENCIES
from config.db_table import ASSET_TABLE_MAP
from services.cleanup_service import delete_files_in_folder
from services.validation_service import validate_asset_type
from database.connection import get_db_connection_cm  # context manager version

#################################################################################################
# FETCH SYMBOLS FROM DATABASE
#################################################################################################
def _fetch_symbols_from_db(
    cur,
    symbol_table: str,
    price_table: str,
    timeframe: str,
    symbols: str
) -> List[Tuple[int, str, Optional[date]]]:
    """Fetch symbols and latest date from DB."""
    try:
        base_query = f"""
            SELECT
                s.symbol_id,
                s.yahoo_symbol,
                MAX(p.date) AS latest_date
            FROM {symbol_table} s
            LEFT JOIN {price_table} p
                ON s.symbol_id = p.symbol_id
               AND p.timeframe = %s
            WHERE s.is_active = TRUE
        """
        params = [timeframe]

        if symbols != "ALL":
            symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
            if not symbol_list:
                raise ValueError("Symbol list is empty after cleaning")
            placeholders = ",".join(["%s"] * len(symbol_list))
            base_query += f" AND s.yahoo_symbol IN ({placeholders})"
            params.extend(symbol_list)

        base_query += " GROUP BY s.symbol_id, s.yahoo_symbol ORDER BY s.symbol_id"
        cur.execute(base_query, params)
        rows = cur.fetchall()
        log(f"📌 {len(rows)} symbols loaded for timeframe={timeframe}")
        return rows

    except Exception as e:
        log(f"❌ Failed fetching symbols from DB: {e}", "error")
        traceback.print_exc()
        return []

#################################################################################################
# CALCULATE DATE RANGE FOR DOWNLOAD (TIMEFRAME AWARE)
#################################################################################################
def _calculate_symbol_date_range(
    latest_date: Optional[date],
    timeframe: str
) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Returns (start_date, end_date, skip_flag)
    skip_flag = True if download should be skipped
    """
    today = date.today()

    if timeframe == "1mo":
        first_day_of_month = today.replace(day=1)
        second_day_of_month = first_day_of_month + timedelta(days=1)

        if latest_date is None:  # Case A
            return date(1999, 1, 1).strftime("%Y-%m-%d"), second_day_of_month.strftime("%Y-%m-%d"), False
        if latest_date == first_day_of_month:  # Case B
            return None, None, True
        # Case C
        return first_day_of_month.strftime("%Y-%m-%d"), second_day_of_month.strftime("%Y-%m-%d"), False

    # NORMAL LOGIC for 1d, 1wk, etc.
    end_date = today + timedelta(days=1)
    start_date = (latest_date + timedelta(days=1)) if latest_date else date(1999, 1, 1)
    if start_date >= end_date:
        return None, None, True

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), False

#################################################################################################
# DOWNLOAD YAHOO DATA FOR ONE SYMBOL
#################################################################################################
def _download_symbol_data(
    download_symbol: str,
    start_date: str,
    end_date: str,
    timeframe: str,
    csv_path: str
) -> bool:
    try:
        df = yf.download(
            download_symbol,
            start=start_date,
            end=end_date,
            interval=timeframe,
            auto_adjust=False,
            progress=False
        )
        if df is None or df.empty:
            return False

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)

        df.reset_index(inplace=True)
        df.to_csv(csv_path, index=False)
        return True

    except Exception as e:
        log(f"❌ Download failed: {download_symbol} | {timeframe} | {e}", "error")
        traceback.print_exc()
        return False

#################################################################################################
# DOWNLOAD YAHOO DATA FOR ALL TIMEFRAMES
#################################################################################################
def download_yahoo_data_all_timeframes(asset_type: str, symbols: str = "ALL") -> List[str]:
    failed_symbols: List[str] = []
    validate_asset_type(asset_type, context="Yahoo data download")

    with get_db_connection_cm() as conn:  # context manager ensures no leaks
        with conn.cursor() as cur:
            log(f"🚀 START DOWNLOAD | asset={asset_type} | symbols={symbols}")
            symbol_table, price_table, *_ = ASSET_TABLE_MAP[asset_type]

            for timeframe in FREQUENCIES:
                timeframe_path = os.path.join(YAHOO_DIR, timeframe)
                os.makedirs(timeframe_path, exist_ok=True)

                symbol_rows = _fetch_symbols_from_db(cur, symbol_table, price_table, timeframe, symbols)

                for symbol_id, yahoo_symbol, latest_date in tqdm(symbol_rows, desc=timeframe, ncols=100):
                    start_date, end_date, skip_flag = _calculate_symbol_date_range(latest_date, timeframe)
                    if skip_flag:
                        log(f"⏭️ Skipping {yahoo_symbol} ({timeframe}) | Up-to-date")
                        continue

                    download_symbol = (
                        f"{yahoo_symbol}.NS"
                        if asset_type.startswith("india_equity") and not yahoo_symbol.endswith(".NS")
                        else yahoo_symbol
                    )
                    csv_path = os.path.join(timeframe_path, f"{yahoo_symbol}.csv")

                    success = _download_symbol_data(download_symbol, start_date, end_date, timeframe, csv_path)
                    if not success:
                        log(f"❌ Failed to download: {download_symbol} | {timeframe} | {start_date} -> {end_date}")
                        failed_symbols.append(download_symbol)

    if failed_symbols:
        unique_failed = sorted(set(failed_symbols))
        log(f"❌ Failed symbols ({len(unique_failed)}): {unique_failed[:10]}")
    log(f"🎉 Download complete for {asset_type.upper()}")
    return failed_symbols

#################################################################################################
# IMPORT CSV TO DATABASE
#################################################################################################
def import_yahoo_csv_to_db(asset_type: str = "india_equity", conn=None) -> None:
    validate_asset_type(asset_type, context="Import CSV to database")
    close_conn = False
    try:
        if conn is None:
            from database.connection import get_db_connection
            conn = get_db_connection()
            close_conn = True
        from database.connection import validate_connection
        validate_connection(conn)

        lookup_table: str = ASSET_TABLE_MAP[asset_type][0]
        table_name: str = ASSET_TABLE_MAP[asset_type][1]

        id_col: str = "symbol_id"
        id_lookup_col: str = "yahoo_symbol"
        numeric_cols: list = ["Open", "High", "Low", "Close", "Volume"]

        for timeframe in FREQUENCIES:
            timeframe_path = os.path.join(YAHOO_DIR, timeframe)
            if not os.path.exists(timeframe_path):
                continue
            files = [f for f in os.listdir(timeframe_path) if f.lower().endswith(".csv")]
            if not files:
                continue

            rows_inserted = 0
            for csv_file in tqdm(files, desc=f"{timeframe}", ncols=100):
                csv_path = os.path.join(timeframe_path, csv_file)
                symbol_name = os.path.splitext(csv_file)[0]

                try:
                    with conn.cursor() as cur:
                        # Lookup symbol_id
                        cur.execute(f"SELECT {id_col} FROM {lookup_table} WHERE {id_lookup_col} = %s", (symbol_name,))
                        res = cur.fetchone()
                        if not res:
                            log(f"❌ LOOKUP FAILED | CSV={symbol_name} | table={lookup_table}")
                            continue
                        symbol_id = res[0]

                        # Read CSV
                        try:
                            df = pd.read_csv(csv_path)
                        except Exception as e:
                            log(f"❌ Failed reading CSV {csv_path}: {e}")
                            continue
                        if df.empty:
                            continue
                        df.columns = [c.strip() for c in df.columns]

                        # Convert Date
                        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
                        df = df[df["Date"].notna()]
                        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

                        # Ensure numeric
                        for col in numeric_cols:
                            df[col] = pd.to_numeric(df[col], errors="coerce").round(2) if col in df.columns else None
                        df = df.where(pd.notnull(df), None)

                        # Prepare rows
                        rows = [
                            (
                                symbol_id,
                                timeframe,
                                row["Date"],
                                row.get("Open"),
                                row.get("High"),
                                row.get("Low"),
                                row.get("Close"),
                                row.get("Volume"),
                            )
                            for _, row in df.iterrows()
                        ]
                        if not rows:
                            continue

                        # Insert
                        insert_sql = f"""
                            INSERT INTO {table_name}
                            (symbol_id, timeframe, date, open, high, low, close, volume)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (symbol_id, timeframe, date)
                            DO UPDATE SET
                                open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume
                        """
                        cur.executemany(insert_sql, rows)
                        rows_inserted += len(rows)

                except Exception as e:
                    log(f"❌ FAILED {symbol_name} | {timeframe} | {type(e).__name__}: {e}")
                    traceback.print_exc()

            conn.commit()
            log(f"💾 COMMIT OK | {timeframe} | rows={rows_inserted}")

    finally:
        if close_conn and conn:
            from database.connection import close_db_connection
            close_db_connection(conn)

#################################################################################################
# PRICE DATA INSERTION PIPELINE
#################################################################################################
def insert_yahoo_price_data_pipeline(asset_type, symbols="ALL"):
    failed_symbols: List[str] = []

    log("===== DELETE YAHOO FILES FROM FOLDERS STARTED =====")
    for timeframe in FREQUENCIES:
        delete_files_in_folder(os.path.join(YAHOO_DIR, timeframe))
    log("===== DELETE YAHOO FILES FROM FOLDERS FINISHED =====")

    log("===== YAHOO DOWNLOAD & CSV IMPORT STARTED =====")
    try:
        with get_db_connection_cm() as conn:  # single connection for download + import
            failed_symbols = download_yahoo_data_all_timeframes(asset_type, symbols)
            import_yahoo_csv_to_db(asset_type, conn)
    except Exception as e:
        log(f"❌ ERROR IN PRICE PIPELINE: {e}", "error")
        traceback.print_exc()

    log("===== YAHOO DOWNLOAD & CSV IMPORT FINISHED =====")
    return failed_symbols


#################################################################################################
# CLONE 1-DAY PRICE DATA
#################################################################################################
def clone_data_from_yahoo_to_yahoo_calc() -> dict:
    counts = {
        "source_table": "india_equity_yahoo_price_data",
        "target_table": "india_equity_yahoo_calc_price_data",
        "source_name": "India Equity Yahoo",
        "target_name": "India Equity Yahoo Calc",
        "source_count": 0,
        "target_count": 0,
        "insert_skipped": False
    }

    with get_db_connection_cm() as conn:
        with conn.cursor() as cur:
            log(f"🚀 Checking 1d price data: {counts['source_name']} -> {counts['target_name']}")

            for key, table in [("source_count", counts["source_table"]), ("target_count", counts["target_table"])]:
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE timeframe = '1d'")
                row = cur.fetchone()
                counts[key] = row[0] if row else 0

            if counts["source_count"] == counts["target_count"]:
                counts["insert_skipped"] = True
                log(f"⚠️ Skipping clone. {counts['source_name']} rows = {counts['target_name']} rows = {counts['source_count']}")
            else:
                log(f"⬇️ Cloning 1d price data ({counts['source_name']} rows: {counts['source_count']}, {counts['target_name']} rows: {counts['target_count']})")
                upsert_query = f"""
                    INSERT INTO {counts['target_table']} (
                        symbol_id,timeframe,date,open,high,low,close,volume,delv_pct,is_future
                    )
                    SELECT symbol_id,timeframe,date,open,high,low,close,volume,delv_pct,is_future
                    FROM {counts['source_table']}
                    WHERE timeframe = '1d'
                    ON CONFLICT (symbol_id,timeframe,date)
                    DO UPDATE SET
                        open=EXCLUDED.open,high=EXCLUDED.high,low=EXCLUDED.low,close=EXCLUDED.close,
                        volume=EXCLUDED.volume,delv_pct=EXCLUDED.delv_pct,is_future=EXCLUDED.is_future;
                """
                cur.execute(upsert_query)
                conn.commit()

                cur.execute(f"SELECT COUNT(*) FROM {counts['target_table']} WHERE timeframe = '1d'")
                counts["target_count"] = cur.fetchone()[0]

                log(f"🎉 Clone completed successfully | {counts['source_name']} rows: {counts['source_count']}, {counts['target_name']} rows: {counts['target_count']}")

    return counts