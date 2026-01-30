import os
import pandas as pd
from psycopg2.extensions import connection
from config.paths import SCANNER_FOLDER_PLAY
from database.connection import get_db_connection, close_db_connection


def backtest_weekly_signals_inplace(
    base_folder: str,
    price_table: str = "india_equity_test_price_data"
):
    base_folder = os.path.abspath(base_folder)

    csv_files = [
        f for f in os.listdir(base_folder)
        if f.endswith(".csv") and not f.endswith("_trades.csv")
    ]

    if not csv_files:
        return

    conn: connection | None = None

    try:
        conn = get_db_connection()

        for file in csv_files:
            path = os.path.join(base_folder, file)
            df = pd.read_csv(path)

            if df.empty or not {"symbol_id", "date"}.issubset(df.columns):
                continue

            # Prevent double processing
            if "return_pct" in df.columns:
                continue

            df["date"] = pd.to_datetime(df["date"], dayfirst=True)

            symbol_ids = df["symbol_id"].astype(int).unique().tolist()
            min_date = df["date"].min()
            max_date = df["date"].max() + pd.Timedelta(days=10)

            prices = pd.read_sql(
                f"""
                SELECT symbol_id, date, open, close
                FROM {price_table}
                WHERE symbol_id = ANY(%s::INTEGER[])
                  AND timeframe = '1d'
                  AND date BETWEEN %s AND %s
                ORDER BY symbol_id, date
                """,
                conn,
                params=(symbol_ids, min_date, max_date)
            )

            if prices.empty:
                continue

            prices["date"] = pd.to_datetime(prices["date"])

            price_map = {
                sid: grp.reset_index(drop=True)
                for sid, grp in prices.groupby("symbol_id")
            }

            # Prepare empty columns
            df["entry_date"] = pd.NaT
            df["entry_open"] = pd.NA
            df["exit_date"] = pd.NaT
            df["exit_close"] = pd.NA
            df["return_pct"] = pd.NA

            for idx, row in df.iterrows():
                sid = int(row["symbol_id"])
                signal_date = row["date"]

                symbol_prices = price_map.get(sid)
                if symbol_prices is None:
                    continue

                future = symbol_prices[symbol_prices["date"] > signal_date]
                if len(future) < 6:
                    continue

                entry = future.iloc[0]
                exit_row = future.iloc[5]

                ret = (
                    (exit_row["close"] - entry["open"]) / entry["open"]
                ) * 100

                df.at[idx, "entry_date"] = entry["date"]
                df.at[idx, "entry_open"] = entry["open"]
                df.at[idx, "exit_date"] = exit_row["date"]
                df.at[idx, "exit_close"] = exit_row["close"]
                df.at[idx, "return_pct"] = round(ret, 2)

            # 🔁 OVERWRITE SAME FILE
            df.to_csv(path, index=False)

    finally:
        if conn:
            close_db_connection(conn)

def negative_return_ratio_stats_minus_5pct():
    base_folder = os.path.join(SCANNER_FOLDER_PLAY, "india_equity_test")
    file_path = os.path.join(base_folder, "YEARLY_2025_27Jan2026.csv")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)

    # Keep only losing trades within -5% to 0%
    df = df[(df["return_pct"] < 0) & (df["return_pct"] >= -5)].copy()

    if df.empty:
        return {}

    # Calculate ratios
    df["rsi3_by_rsi9"] = df["rsi_3_weekly"] / df["rsi_9_weekly"]
    df["rsi9_by_ema"] = df["rsi_9_weekly"] / df["ema_rsi_9_3_weekly"]
    df["ema_by_wma"] = df["ema_rsi_9_3_weekly"] / df["wma_rsi_9_21_weekly"]

    result = {
        "count": len(df),
        "rsi_3_weekly / rsi_9_weekly": {
            "min": df["rsi3_by_rsi9"].min(),
            "max": df["rsi3_by_rsi9"].max(),
            "mean": df["rsi3_by_rsi9"].mean(),
        },
        "rsi_9_weekly / ema_rsi_9_3_weekly": {
            "min": df["rsi9_by_ema"].min(),
            "max": df["rsi9_by_ema"].max(),
            "mean": df["rsi9_by_ema"].mean(),
        },
        "ema_rsi_9_3_weekly / wma_rsi_9_21_weekly": {
            "min": df["ema_by_wma"].min(),
            "max": df["ema_by_wma"].max(),
            "mean": df["ema_by_wma"].mean(),
        },
        "rsi_9_weekly": {
            "min": df["rsi_9_weekly"].min(),
            "max": df["rsi_9_weekly"].max(),
            "mean": df["rsi_9_weekly"].mean(),
        },
    }

    return result
       
if __name__ == "__main__":
    base_folder = os.path.join(SCANNER_FOLDER_PLAY, "india_equity_test")
    # backtest_weekly_signals_inplace(base_folder)
    stats = negative_return_ratio_stats_minus_5pct()

    print(f"Total trades (-5% to 0%): {stats['count']}\n")

    for k, v in stats.items():
        if k == "count":
            continue
        print(
            f"{k}: "
            f"min={v['min']:.4f}, "
            f"max={v['max']:.4f}, "
            f"mean={v['mean']:.4f}"
        )