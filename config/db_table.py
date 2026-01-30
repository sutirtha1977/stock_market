from config.paths import (
    INDIA_EQUITY,
    USA_EQUITY,
    INDIA_INDEX,
    GLOBAL_INDEX,
    COMMODITY_SYMBOLS,
    CRYPTO_SYMBOLS,
    FOREX_SYMBOLS
)
DB_CONFIG = {
    "host": "localhost",
    "dbname": "market_data",
    "user": "sutirtha",
    "password": "1977",
    "port": 5432
}
# PostgreSQL connection
PG_CONN_STR = "postgresql+psycopg2://sutirtha:1977@localhost:5432/market_data"
SYMBOL_SOURCES = [
    ("india_equity_symbols", INDIA_EQUITY),
    ("usa_equity_symbols", USA_EQUITY),
    ("india_index_symbols",  INDIA_INDEX),
    ("global_index_symbols", GLOBAL_INDEX),
    ("commodity_symbols",    COMMODITY_SYMBOLS),
    ("crypto_symbols",       CRYPTO_SYMBOLS),
    ("forex_symbols",        FOREX_SYMBOLS),
]
ASSET_TABLE_MAP = {
    # -------------------------------
    # INDIA EQUITY (SOURCE-SPECIFIC)
    # -------------------------------
    "india_equity_yahoo": (
        "india_equity_symbols",
        "india_equity_yahoo_price_data",
        "india_equity_yahoo_indicators",
        "india_equity_yahoo_52week_stats",
    ),
    # "india_equity_nse": (
    #     "india_equity_symbols",
    #     "india_equity_nse_price_data",
    #     "india_equity_nse_indicators",
    #     "india_equity_nse_52week_stats",
    # ),
    "india_equity_yahoo_calc": (
        "india_equity_symbols",
        "india_equity_yahoo_calc_price_data",
        "india_equity_yahoo_calc_indicators",
        "india_equity_yahoo_calc_52week_stats",
    ),
    "india_equity_test": (
        "india_equity_symbols",
        "india_equity_test_price_data",
        "india_equity_test_indicators",
        "india_equity_test_52week_stats",
    ),
    # -------------------------------
    # USA EQUITIES
    # -------------------------------
    "usa_equity": (
        "usa_equity_symbols",
        "usa_equity_price_data",
        "usa_equity_indicators",
        "usa_equity_52week_stats",
    ),
    # -------------------------------
    # INDEXES
    # -------------------------------
    "india_index": (
        "india_index_symbols",
        "india_index_price_data",
        "india_index_indicators",
        "india_index_52week_stats",
    ),
    "global_index": (
        "global_index_symbols",
        "global_index_price_data",
        "global_index_indicators",
        "global_index_52week_stats",
    ),
    # -------------------------------
    # OTHER ASSETS
    # -------------------------------
    "commodity": (
        "commodity_symbols",
        "commodity_price_data",
        "commodity_indicators",
        "commodity_52week_stats",
    ),
    "crypto": (
        "crypto_symbols",
        "crypto_price_data",
        "crypto_indicators",
        "crypto_52week_stats",
    ),
    "forex": (
        "forex_symbols",
        "forex_price_data",
        "forex_indicators",
        "forex_52week_stats",
    ),
}
ASSET_TYPE_FRIENDLY_MAP = {
    "india_equity_yahoo": "India Equity Yahoo",
    "india_equity_yahoo_calc": "India Equity Yahoo Calc",
    "india_equity_test": "India Equity Test",
    "usa_equity": "USA Equity Yahoo",
    "india_index": "India Index Yahoo",
    "global_index": "Global Index Yahoo",
    "commodity": "Commodity Yahoo",
    "crypto": "Crypto Yahoo",
    "forex": "Forex Yahoo",
}
ASSET_CHOICE_MAP = {                  
    "1": "india_equity_yahoo",
    "2": "india_equity_yahoo_calc",
    "3": "usa_equity",
    "4": "india_index",
    "5": "global_index",
    "6": "commodity",
    "7": "crypto",
    "8": "forex",
    "0": None,  
}
ASSET_FRIENDLY_NAME = {
    "india_equity_yahoo": "India Equity Yahoo",
    "india_equity_yahoo_calc": "India Equity Yahoo Calc",
    "india_equity_test":   "India Equity Test",
    "usa_equity":         "USA Equity Yahoo",
    "india_index":        "India Index Yahoo",
    "global_index":       "Global Index Yahoo",
    "commodity":          "Commodity Yahoo",
    "crypto":             "Crypto Yahoo",
    "forex":              "Forex Yahoo",
}