from config.logger import log
from database.connection import get_db_connection, close_db_connection

#################################################################################################
# FUNCTION TO CREATE THE STOCK DATABASE WITH NECESSARY TABLES
#################################################################################################
def create_stock_database(drop_existing=True):
    """
    Create all stock database tables. Drops existing tables/views if drop_existing=True.

    This function ensures symbol tables, price tables, indicator tables, 52-week stats tables,
    and NSE holidays table exist.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # -------------------------------------------------
        # DROP ALL VIEWS AND TABLES
        # -------------------------------------------------
        if drop_existing:
            log("⚠️ Dropping ALL views and tables in database...")
            cur.execute("""
                DO $$
                DECLARE
                    r RECORD;
                BEGIN
                    -- Drop all views first
                    FOR r IN (
                        SELECT table_name
                        FROM information_schema.views
                        WHERE table_schema = 'public'
                    ) LOOP
                        EXECUTE 'DROP VIEW IF EXISTS ' || quote_ident(r.table_name) || ' CASCADE';
                    END LOOP;

                    -- Drop all tables next
                    FOR r IN (
                        SELECT tablename
                        FROM pg_tables
                        WHERE schemaname = 'public'
                    ) LOOP
                        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END $$;
            """)
            log("🗑 All existing views and tables dropped")

        # -------------------------------------------------
        # SYMBOL TABLES
        # -------------------------------------------------
        symbol_tables = [
            "india_equity_symbols",
            "india_index_symbols",
            "usa_equity_symbols",
            "global_index_symbols",
            "commodity_symbols",
            "crypto_symbols",
            "forex_symbols"
        ]

        for table in symbol_tables:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    symbol_id    SERIAL PRIMARY KEY,
                    name         TEXT NOT NULL,
                    yahoo_symbol TEXT UNIQUE NOT NULL,
                    exchange     TEXT,
                    is_active    BOOLEAN DEFAULT TRUE,
                    is_future    BOOLEAN DEFAULT FALSE
                );
            """)
            log(f"🆕 Ensured symbols table: {table}")

        # -------------------------------------------------
        # TABLE CREATORS
        # -------------------------------------------------
        def create_price_table(table_name, symbol_table):
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    symbol_id INTEGER,
                    timeframe TEXT,
                    date DATE,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    delv_pct REAL,
                    is_future BOOLEAN DEFAULT FALSE,
                    PRIMARY KEY (symbol_id, timeframe, date),
                    FOREIGN KEY(symbol_id) REFERENCES {symbol_table}(symbol_id)
                );
            """)

        def create_indicator_table(table_name, symbol_table):
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    symbol_id INTEGER,
                    timeframe TEXT,
                    date DATE,
                    sma_20 REAL,
                    sma_50 REAL,
                    sma_200 REAL,
                    rsi_3 REAL,
                    rsi_9 REAL,
                    rsi_14 REAL,
                    macd REAL,
                    macd_signal REAL,
                    bb_upper REAL,
                    bb_middle REAL,
                    bb_lower REAL,
                    atr_14 REAL,
                    supertrend REAL,
                    supertrend_dir INTEGER,
                    ema_rsi_9_3 REAL,
                    wma_rsi_9_21 REAL,
                    pct_price_change REAL,
                    PRIMARY KEY (symbol_id, timeframe, date),
                    FOREIGN KEY(symbol_id) REFERENCES {symbol_table}(symbol_id)
                );
            """)

        def create_52week_table(table_name, symbol_table):
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    symbol_id INTEGER PRIMARY KEY,
                    week52_high REAL,
                    week52_low REAL,
                    as_of_date DATE,
                    FOREIGN KEY(symbol_id) REFERENCES {symbol_table}(symbol_id)
                );
            """)

        # -------------------------------------------------
        # ASSET TABLE CONFIG
        # -------------------------------------------------
        tables_config = [
            ("india_equity_symbols", "india_equity_yahoo_price_data",       "india_equity_yahoo_indicators",       "india_equity_yahoo_52week_stats"),
            ("india_equity_symbols", "india_equity_yahoo_calc_price_data",  "india_equity_yahoo_calc_indicators",  "india_equity_yahoo_calc_52week_stats"),
            ("india_equity_symbols", "india_equity_test_price_data",        "india_equity_test_indicators",        "india_equity_test_52week_stats"),

            ("india_index_symbols",  "india_index_price_data",              "india_index_indicators",              "india_index_52week_stats"),
            ("usa_equity_symbols",   "usa_equity_price_data",               "usa_equity_indicators",               "usa_equity_52week_stats"),
            ("global_index_symbols", "global_index_price_data",             "global_index_indicators",             "global_index_52week_stats"),
            ("commodity_symbols",    "commodity_price_data",                "commodity_indicators",                "commodity_52week_stats"),
            ("crypto_symbols",       "crypto_price_data",                   "crypto_indicators",                   "crypto_52week_stats"),
            ("forex_symbols",        "forex_price_data",                    "forex_indicators",                    "forex_52week_stats"),
        ]

        for sym_table, price_table, ind_table, stats_table in tables_config:
            create_price_table(price_table, sym_table)
            create_indicator_table(ind_table, sym_table)
            create_52week_table(stats_table, sym_table)
            log(f"✅ Ensured tables for {price_table}")

        # -------------------------------------------------
        # NSE HOLIDAYS TABLE (REFERENCE / CALENDAR)
        # -------------------------------------------------
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nse_holidays (
                holiday_date DATE PRIMARY KEY,
                day_name TEXT NOT NULL,
                holiday_name TEXT NOT NULL
            );
        """)
        log("📅 Ensured table: nse_holidays")

        # -------------------------------------------------
        # COMMIT
        # -------------------------------------------------
        conn.commit()
        log("🎉 PostgreSQL multi-asset database created/updated successfully")

    except Exception as e:
        conn.rollback()
        log(f"❌ DB creation/update failed: {e}")
        raise

    finally:
        # -------------------------------------------------
        # CLOSE CONNECTION PROPERLY
        # -------------------------------------------------
        close_db_connection(conn)