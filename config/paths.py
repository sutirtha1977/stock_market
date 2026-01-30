from pathlib import Path

# =========================================================
# BASE PATHS
# =========================================================
BASE_DIR: Path = Path(__file__).parent.parent

# ---------------- Directories ----------------
DATA_DIR: Path = BASE_DIR / "data"

# ---------------- NSE Holiday Lists ----------------
NSE_HOLIDAYS: Path = DATA_DIR / "nse_holiday_lists.csv"

# ---------------- Symbols Directories ----------------
SYMBOLS_DIR: Path = DATA_DIR / "symbols"

# ---------------- Yahoo Directories ----------------
YAHOO_DIR: Path = DATA_DIR / "yahoo"
# ---------------- Misc Directories ----------------
ANALYSIS_FOLDER: Path = DATA_DIR / "analysis"
# ---------------- Scanner Directories ----------------
SCANNER_FOLDER: Path = DATA_DIR / "scanner_results" 
SCANNER_FOLDER_WEEKLY: Path = SCANNER_FOLDER / "weekly"
SCANNER_FOLDER_HM: Path = SCANNER_FOLDER / "HM"
SCANNER_FOLDER_PLAY: Path = SCANNER_FOLDER / "play"
SCANNER_FOLDER_TEST: Path = SCANNER_FOLDER / "test"

# ---------------- Database ----------------
# DB_FILE = BASE_DIR / "db" / "markets.db"

# ---------------- CSV ----------------
INDIA_EQUITY: Path = SYMBOLS_DIR / "india_equity_yahoo_symbols.csv"
USA_EQUITY: Path = SYMBOLS_DIR / "usa_equity_yahoo_symbols.csv"
INDIA_INDEX: Path = SYMBOLS_DIR / "india_index_yahoo_symbols.csv"
USA_INDEX: Path = SYMBOLS_DIR / "usa_index_yahoo_symbols.csv"
GLOBAL_INDEX: Path = SYMBOLS_DIR / "global_index_yahoo_symbols.csv"
COMMODITY_SYMBOLS: Path = SYMBOLS_DIR / "commodity_yahoo_symbols.csv"
CRYPTO_SYMBOLS: Path = SYMBOLS_DIR / "crypto_yahoo_symbols.csv"
FOREX_SYMBOLS: Path = SYMBOLS_DIR / "forex_yahoo_symbols.csv"

# ---------------- Logging ----------------
LOG_FILE: Path = BASE_DIR / "audit_trail.log"

# =========================================================
# Helper Functions
# =========================================================
def ensure_folder(path: Path) -> None:
    """Ensure the folder exists.
    
    Args:
        path: Path object for the directory to create.
    """
    path.mkdir(parents=True, exist_ok=True)

# =========================================================
# Ensure required folders exist
# =========================================================
for p in [
    DATA_DIR,
    SYMBOLS_DIR,
    YAHOO_DIR,
    ANALYSIS_FOLDER,
    SCANNER_FOLDER,
    SCANNER_FOLDER_WEEKLY,
    SCANNER_FOLDER_HM,
    SCANNER_FOLDER_PLAY,
    SCANNER_FOLDER_TEST,
]:
    ensure_folder(p)