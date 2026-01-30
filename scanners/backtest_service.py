import os
import pandas as pd
import traceback
from typing import Optional
from psycopg2.extensions import connection
from database.connection import get_db_connection, close_db_connection
from config.logger import log
from config.db_table import ASSET_TABLE_MAP

#################################################################################################
# Simplified scanner backtest (FILE-WISE SUMMARY ONLY)
# Updated to handle both YEARLY files and WEEKLY_ALL_YEARS bulk file
#################################################################################################
def backtest_scanners(
    asset_type: str = "india_equity_yahoo",
    folder_path: Optional[str] = None
) -> pd.DataFrame:
    
    if not folder_path:
        log("❌ No folder path provided.")
        return pd.DataFrame()

    folder_path = os.path.abspath(folder_path)
    if not os.path.exists(folder_path):
        log(f"❌ Folder does not exist: {folder_path}")
        return pd.DataFrame()

    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    if not csv_files:
        log("❌ No CSV files found.")
        return pd.DataFrame()

    if asset_type not in ASSET_TABLE_MAP:
        log(f"❌ Unsupported asset_type: {asset_type}")
        return pd.DataFrame()

    _, price_table, _, _ = ASSET_TABLE_MAP[asset_type]
    summary_rows = []

    conn: Optional[connection] = None

    try:
        conn = get_db_connection()
        log(f"🔹 Running backtest with trade export for {len(csv_files)} files")

        for file_name in csv_files:
            path = os.path.join(folder_path, file_name)
            log(f"\n📂 Processing {file_name}")

            try:
                df = pd.read_csv(path)
                if df.empty or not {"date", "symbol_id"}.issubset(df.columns):
                    log("⚠ Invalid or empty file, skipping")
                    continue

                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date")

                symbol_ids = [int(x) for x in df["symbol_id"].unique()]
                min_date = df["date"].min().to_pydatetime()
                max_date = (df["date"].max() + pd.Timedelta(days=10)).to_pydatetime()

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
                    log("⚠ No price data found")
                    continue

                prices["date"] = pd.to_datetime(prices["date"])
                prices_map = {
                    int(sid): grp.sort_values("date")
                    for sid, grp in prices.groupby("symbol_id")
                }

                trades = []

                for _, row in df.iterrows():
                    sid = int(row["symbol_id"])
                    symbol_prices = prices_map.get(sid)
                    if symbol_prices is None:
                        continue

                    signal_date = row["date"]
                    entry_df = symbol_prices[symbol_prices["date"] > signal_date]
                    if entry_df.empty:
                        continue

                    entry = entry_df.iloc[0]
                    exit_df = symbol_prices[symbol_prices["date"] >= entry["date"]]
                    if len(exit_df) < 6:
                        continue

                    exit_row = exit_df.iloc[5]
                    ret = ((exit_row["close"] - entry["open"]) / entry["open"]) * 100

                    trades.append({
                        "symbol_id": sid,
                        "entry_date": entry["date"],
                        "entry_open": entry["open"],
                        "exit_date": exit_row["date"],
                        "exit_close": exit_row["close"],
                        "return_pct": ret
                    })

                # Export trades file
                if trades:
                    trades_df = pd.DataFrame(trades)
                    trades_file = os.path.join(folder_path, file_name.replace(".csv", "_trades.csv"))
                    trades_df.to_csv(trades_file, index=False)
                    log(f"✅ Exported trades: {trades_file}")

                    returns_series = trades_df["return_pct"]
                    summary_rows.append({
                        "file": file_name.replace(".csv", ""),
                        "total_trades": len(trades),
                        "win_pct": round((returns_series > 0).mean() * 100, 2),
                        "max_win_pct": round(returns_series.max(), 2),
                        "max_loss_pct": round(returns_series.min(), 2)
                    })
                    log(f"📊 Summary: Trades={len(trades)}, Win%={summary_rows[-1]['win_pct']}")
                else:
                    log("⚠ No trades generated")

            except Exception as e:
                log(f"❌ Error processing {file_name} | {e}")
                traceback.print_exc()

    finally:
        if conn:
            close_db_connection(conn)
            log("🔒 DB connection closed")

    # Export summary
    summary_df = pd.DataFrame(summary_rows)

    if not summary_df.empty:
        # --- Extract year safely ---
        if "scan_date" in df.columns:
            summary_df["year"] = pd.to_datetime(df["scan_date"], errors="coerce").dt.year
        else:
            # fallback for old YEARLY files
            summary_df["year"] = pd.to_numeric(summary_df["file"].str.extract(r'YEARLY_(\d+)')[0], errors='coerce')

        # Sort descending so latest year comes first
        summary_df = summary_df.sort_values("year", ascending=False).drop(columns=["year"]).reset_index(drop=True)
        summary_path = os.path.join(folder_path, "scanner_summary.csv")
        summary_df.to_csv(summary_path, index=False)
        log(f"📊 Summary exported: {summary_path}")
        print(summary_df.to_string(index=False))

    return summary_df