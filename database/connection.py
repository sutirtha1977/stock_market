import psycopg2
from psycopg2 import pool
from psycopg2.extensions import connection
from typing import Optional, Generator
from contextlib import contextmanager
from config.logger import log
from config.db_table import DB_CONFIG   # expects dict with host, dbname, user, password, port

#################################################################################################
# Connection Validation
#################################################################################################
def validate_connection(conn: Optional[connection]) -> None:
    """Validate database connection is open and usable."""
    if conn is None:
        raise RuntimeError("Database connection not available (None)")
    if hasattr(conn, 'closed') and conn.closed:
        raise RuntimeError("Database connection is closed")

#################################################################################################
# Connection Pool Initialization
#################################################################################################
try:
    CONNECTION_POOL: Optional[pool.SimpleConnectionPool] = pool.SimpleConnectionPool(
        minconn=2,
        maxconn=10,
        host=DB_CONFIG["host"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        port=DB_CONFIG.get("port", 5432),
        connect_timeout=30,
        application_name="stock_market_app"
    )
    log("✅ Database connection pool initialized (2-10 connections)")
except Exception as e:
    log(f"⚠ Connection pool initialization failed, falling back to direct connections: {e}")
    CONNECTION_POOL = None

#################################################################################################
# Get Database Connection
#################################################################################################
def get_db_connection() -> connection:
    """Retrieve a database connection from the pool or create a new direct connection."""
    try:
        if CONNECTION_POOL:
            conn = CONNECTION_POOL.getconn()
            pool_status = (
                f"Pool used: {len(CONNECTION_POOL._used)}, "
                f"Pool free: {len(CONNECTION_POOL._pool)}"
            )
            log(f"📊 Connection retrieved from pool | {pool_status}")
        else:
            conn = psycopg2.connect(
                host=DB_CONFIG["host"],
                dbname=DB_CONFIG["dbname"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
                port=DB_CONFIG.get("port", 5432),
                connect_timeout=30
            )
            log("⚠ Direct connection created (pool not available)")

        # Session-level settings
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '5min';")

        return conn

    except Exception as e:
        log(f"❌ DB CONNECTION FAILED: {e}")
        raise

#################################################################################################
# Return Connection to Pool
#################################################################################################
def return_db_connection(conn: Optional[connection]) -> None:
    """Return connection to pool or close if no pool."""
    try:
        if conn and CONNECTION_POOL:
            CONNECTION_POOL.putconn(conn)
            pool_status = (
                f"Pool used: {len(CONNECTION_POOL._used)}, "
                f"Pool free: {len(CONNECTION_POOL._pool)}"
            )
            log(f"🔄 Connection returned to pool | {pool_status}")
        elif conn:
            conn.close()
            log("🔌 Direct connection closed (no pool)")
    except Exception as e:
        log(f"⚠ Error returning connection: {e}")
        if conn:
            try:
                conn.close()
            except Exception as close_err:
                log(f"⚠ Error closing connection: {type(close_err).__name__}")

#################################################################################################
# Close Database Connection (wrapper)
#################################################################################################
def close_db_connection(conn: Optional[connection]) -> None:
    """Close or return the database connection (calls return_db_connection)."""
    return_db_connection(conn)

#################################################################################################
# Context Manager Support
#################################################################################################
@contextmanager
def get_db_connection_cm() -> Generator[connection, None, None]:
    """Context manager for automatic connection cleanup."""
    conn: Optional[connection] = None
    try:
        conn = get_db_connection()
        yield conn
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            close_db_connection(conn)