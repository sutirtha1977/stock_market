import psycopg2
from config.db_table import DB_CONFIG

# -------------------------------------------------
# SQL: DROP TABLES (reverse dependency order)
# -------------------------------------------------
DROP_SQL = """
DROP TABLE IF EXISTS fno_option_greeks CASCADE;
DROP TABLE IF EXISTS fno_expiry_oi_snapshot CASCADE;
DROP TABLE IF EXISTS fno_daily_symbol_stats CASCADE;
DROP TABLE IF EXISTS fno_options_price CASCADE;
DROP TABLE IF EXISTS fno_futures_price CASCADE;
DROP TABLE IF EXISTS fno_contracts CASCADE;
DROP TABLE IF EXISTS fno_expiries CASCADE;
DROP TABLE IF EXISTS fno_symbols CASCADE;
DROP TABLE IF EXISTS stg_nse_fo_bhavcopy CASCADE;
"""

# -------------------------------------------------
# SQL: CREATE TABLES
# -------------------------------------------------
CREATE_SQL = """
-- =========================
-- MASTER
-- =========================
CREATE TABLE fno_symbols (
    symbol_id      SERIAL PRIMARY KEY,
    symbol         VARCHAR(30) NOT NULL UNIQUE,
    instrument_type VARCHAR(10) CHECK (instrument_type IN ('INDEX','STOCK')),
    exchange       VARCHAR(10) DEFAULT 'NSE',
    is_active      BOOLEAN DEFAULT TRUE,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fno_expiries (
    expiry_id   SERIAL PRIMARY KEY,
    expiry_date DATE NOT NULL UNIQUE,
    expiry_type VARCHAR(10) CHECK (expiry_type IN ('WEEKLY','MONTHLY')),
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- CONTRACTS
-- =========================
CREATE TABLE fno_contracts (
    contract_id   SERIAL PRIMARY KEY,
    symbol_id     INTEGER NOT NULL REFERENCES fno_symbols(symbol_id),
    segment       VARCHAR(3) CHECK (segment IN ('FUT','OPT')),
    expiry_date   DATE NOT NULL,
    strike_price  NUMERIC(10,2),
    option_type   VARCHAR(2) CHECK (option_type IN ('CE','PE')),
    lot_size      INTEGER,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(symbol_id, segment, expiry_date, strike_price, option_type)
);

-- =========================
-- PRICES
-- =========================
CREATE TABLE fno_futures_price (
    contract_id    INTEGER REFERENCES fno_contracts(contract_id),
    trade_date     DATE,
    open           NUMERIC(10,2),
    high           NUMERIC(10,2),
    low            NUMERIC(10,2),
    close          NUMERIC(10,2),
    settle_price   NUMERIC(10,2),
    volume         BIGINT,
    turnover_lakhs NUMERIC(20,2),   -- 🔥 FIXED
    open_interest  BIGINT,
    change_in_oi   BIGINT,
    PRIMARY KEY(contract_id, trade_date)
);

CREATE TABLE fno_options_price (
    contract_id    INTEGER REFERENCES fno_contracts(contract_id),
    trade_date     DATE,
    open           NUMERIC(10,2),
    high           NUMERIC(10,2),
    low            NUMERIC(10,2),
    close          NUMERIC(10,2),
    settle_price   NUMERIC(10,2),
    volume         BIGINT,
    turnover_lakhs NUMERIC(20,2),   -- 🔥 FIXED
    open_interest  BIGINT,
    change_in_oi   BIGINT,
    PRIMARY KEY(contract_id, trade_date)
);

-- =========================
-- ANALYTICS
-- =========================
CREATE TABLE fno_daily_symbol_stats (
    symbol_id     INTEGER REFERENCES fno_symbols(symbol_id),
    trade_date    DATE,
    fut_oi        BIGINT,
    opt_oi        BIGINT,
    total_oi      BIGINT,
    fut_volume    BIGINT,
    opt_volume    BIGINT,
    pcr           NUMERIC(8,3),
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(symbol_id, trade_date)
);

CREATE TABLE fno_expiry_oi_snapshot (
    symbol_id    INTEGER REFERENCES fno_symbols(symbol_id),
    expiry_date  DATE,
    trade_date   DATE,
    call_oi      BIGINT,
    put_oi       BIGINT,
    net_oi       BIGINT,
    pcr          NUMERIC(8,3),
    PRIMARY KEY(symbol_id, expiry_date, trade_date)
);

-- =========================
-- GREEKS
-- =========================
CREATE TABLE fno_option_greeks (
    contract_id  INTEGER REFERENCES fno_contracts(contract_id),
    trade_date   DATE,
    implied_vol  NUMERIC(8,4),
    delta        NUMERIC(8,4),
    gamma        NUMERIC(8,4),
    theta        NUMERIC(8,4),
    vega         NUMERIC(8,4),
    PRIMARY KEY(contract_id, trade_date)
);

-- =========================
-- STAGING  (OPTION A — UNIQUE CONSTRAINT)
-- =========================
CREATE TABLE stg_nse_fo_bhavcopy (
    trade_date       DATE,
    instrument       VARCHAR(10),
    symbol           VARCHAR(30),
    expiry_date      DATE,
    strike_price     NUMERIC(10,2),
    option_type      VARCHAR(2),
    open_price       NUMERIC(10,2),
    high_price       NUMERIC(10,2),
    low_price        NUMERIC(10,2),
    close_price      NUMERIC(10,2),
    settle_price     NUMERIC(10,2),
    volume           BIGINT,
    turnover_lakhs   NUMERIC(20,2),   -- 🔥 FIXED
    open_interest    BIGINT,
    change_in_oi     BIGINT,
    source_format    VARCHAR(10),
    raw_symbol       VARCHAR(50),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_stg_fno_unique
    UNIQUE (
        trade_date,
        instrument,
        symbol,
        expiry_date,
        strike_price,
        option_type
    )
);
"""

# -------------------------------------------------
# SQL: INDEXES
# -------------------------------------------------
INDEX_SQL = """
-- Regular indexes
CREATE INDEX IF NOT EXISTS idx_fno_contracts_symbol ON fno_contracts(symbol_id);
CREATE INDEX IF NOT EXISTS idx_fno_contracts_expiry ON fno_contracts(expiry_date);

CREATE INDEX IF NOT EXISTS idx_fut_price_date ON fno_futures_price(trade_date);
CREATE INDEX IF NOT EXISTS idx_opt_price_date ON fno_options_price(trade_date);

CREATE INDEX IF NOT EXISTS idx_fut_price_oi ON fno_futures_price(open_interest);
CREATE INDEX IF NOT EXISTS idx_opt_price_oi ON fno_options_price(open_interest);

CREATE INDEX IF NOT EXISTS idx_stg_trade_date ON stg_nse_fo_bhavcopy(trade_date);
CREATE INDEX IF NOT EXISTS idx_stg_symbol ON stg_nse_fo_bhavcopy(symbol);
"""

# -------------------------------------------------
# EXECUTOR
# -------------------------------------------------
def create_fno_tables():
    print("🔌 Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        print("🗑️ Dropping existing tables...")
        cur.execute(DROP_SQL)

        print("🏗️ Creating schema...")
        cur.execute(CREATE_SQL)

        print("⚡ Creating indexes...")
        cur.execute(INDEX_SQL)

        conn.commit()
        print("\n✅ F&O DATABASE SCHEMA CREATED SUCCESSFULLY")

    except Exception as e:
        conn.rollback()
        print("❌ ERROR:", e)

    finally:
        cur.close()
        conn.close()

# -------------------------------------------------
if __name__ == "__main__":
    create_fno_tables()