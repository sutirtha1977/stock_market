from rich.table import Table
from rich.console import Console
from datetime import datetime
from database.connection import get_db_connection, close_db_connection, validate_connection
from psycopg2.extensions import connection
from config.db_table import ASSET_TABLE_MAP, ASSET_TYPE_FRIENDLY_MAP
from config.paths import NSE_HOLIDAYS
from config.logger import log
import pandas as pd
from typing import List, Dict, Any

#################################################################################################
# GET LATEST DATES - Returns data as DataFrame (for web display)
#################################################################################################
def get_latest_dates_data() -> pd.DataFrame:
    """Get latest available data dates for all asset types and table types as DataFrame."""
    conn: connection = None
    cur = None
    rows: List[Dict[str, Any]] = []

    try:
        conn = get_db_connection()
        validate_connection(conn)
        cur = conn.cursor()

        for asset_type, (_, price_table, indicator_table, _) in ASSET_TABLE_MAP.items():

            asset_label = ASSET_TYPE_FRIENDLY_MAP.get(asset_type, asset_type.upper())

            for table_name in [price_table, indicator_table]:
                sql: str = f"""
                    SELECT
                        MAX(CASE WHEN timeframe = '1d'  THEN date END)  AS d1,
                        MAX(CASE WHEN timeframe = '1wk' THEN date END) AS d1w,
                        MAX(CASE WHEN timeframe = '1mo' THEN date END) AS d1m
                    FROM {table_name}
                """
                cur.execute(sql)
                r = cur.fetchone()

                d1  = r[0].strftime("%Y-%m-%d") if r and r[0] else "-"
                d1w = r[1].strftime("%Y-%m-%d") if r and r[1] else "-"
                d1m = r[2].strftime("%Y-%m-%d") if r and r[2] else "-"

                rows.append({
                    "Asset Type": asset_label,
                    "Table Name": table_name,
                    "1D": d1,
                    "1WK": d1w,
                    "1MO": d1m
                })

        return pd.DataFrame(rows)

    except Exception as e:
        log(f"❌ Failed to fetch latest dates: {e}")
        return pd.DataFrame()

    finally:
        if cur:
            cur.close()
        if conn:
            close_db_connection(conn)

#################################################################################################
# Reads the NSE holidays CSV (DD/MM/YY format) and inserts/updates the database.
#################################################################################################     
def upsert_nse_holidays() -> None:
    conn: connection = None
    try:
        df = pd.read_csv(NSE_HOLIDAYS)
        if df.empty:
            log("⚠️ NSE holidays CSV is empty")
            return

        required_cols = ["Day", "Date", "Holiday"]
        if not all(col in df.columns for col in required_cols):
            log(f"❌ CSV must contain columns: {required_cols}")
            return

        df["holiday_date"] = pd.to_datetime(df["Date"], format="%d/%m/%y", errors="coerce")
        if df["holiday_date"].isnull().any():
            log("❌ Some dates could not be parsed. Check CSV format.")
            return

        df["day_name"] = df["Day"]
        df["holiday_name"] = df["Holiday"]

        conn = get_db_connection()
        validate_connection(conn)

        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute("""
                    INSERT INTO nse_holidays (holiday_date, day_name, holiday_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (holiday_date)
                    DO UPDATE SET
                        day_name = EXCLUDED.day_name,
                        holiday_name = EXCLUDED.holiday_name
                """, (row["holiday_date"], row["day_name"], row["holiday_name"]))
        conn.commit()
        log(f"✅ NSE holidays loaded successfully ({len(df)} rows)")

    except Exception as e:
        if conn:
            conn.rollback()
        log(f"❌ Failed to load NSE holidays: {e}")
        raise

    finally:
        if conn:
            close_db_connection(conn)

################################################################################################# 
# GET NSE HOLIDAYS FOR CURRENT YEAR
################################################################################################# 
def get_nse_holidays_current_year() -> pd.DataFrame:
    year = datetime.now().year

    query = """
        SELECT
            TO_CHAR(holiday_date, 'dd-Mon-YYYY') AS "Date",
            UPPER(day_name) AS "Day",
            holiday_name AS "Holiday Name"
        FROM nse_holidays
        WHERE EXTRACT(YEAR FROM holiday_date) = %s
        ORDER BY holiday_date;
    """

    conn: connection = None
    try:
        conn = get_db_connection()
        validate_connection(conn)
        return pd.read_sql(query, conn, params=(year,))
    except Exception as e:
        log(f"❌ Failed to load NSE holidays for {year}: {e}")
        return pd.DataFrame(columns=["Date", "Day", "Holiday Name"])
    finally:
        if conn:
            close_db_connection(conn)