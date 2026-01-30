# ---------------- Menu and colors ----------------
MAIN_MENU_ITEMS = [
    ("Database Creation & Refresh Symbols", "[bold green]Enter 1[/bold green]"),
    ("Common Database Operations - Indicators and 52 Week Stats", "[bold green]Enter 2[/bold green]"),
    ("YAHOO Data Manager (Stock / Index / Commodity / Crypto / Forex)", "[bold green]Enter 3[/bold green]"),
    ("NSE Data Manager (India Equity)", "[bold green]Enter 4[/bold green]"),
    ("Test Data Manager (India Equity)", "[bold green]Enter 5[/bold green]"),
    ("Scanners (Stock / Index / Commodity / Crypto / Forex)", "[bold green]Enter 6[/bold green]"),
    # ("FnO Operations", "[bold green]Enter 4[/bold green]"),
    ("[red]Exit Program[/red]", "[bold red]Enter 0[/bold red]"),
]
DATABASE_MENU_ITEMS = [
    ("Create Base Database", "[bold green]Enter 1[/bold green]"),
    ("Insert/Update All Symbols", "[bold green]Enter 2[/bold green]"),
    ("[bold red]Back To Main Menu[/bold red]", "[bold red]Enter 0[/bold red]"),
]
COMMON_MENU_ITEMS = [
    ("Show Latest Dates For Stock / Index / Commodity / Crypto / Forex", "[bold green]Enter 1[/bold green]"),
    ("Display missing Price Data per asset type", "[bold green]Enter 2[/bold green]"),
    ("Update All Indicators", "[bold green]Enter 3[/bold green]"),
    ("Update All 52 Week Stats", "[bold green]Enter 4[/bold green]"),
    ("Update NSE Holiday List", "[bold green]Enter 5[/bold green]"),
    ("[bold red]Back To Main Menu[/bold red]", "[bold red]Enter 0[/bold red]"),
] 
YAHOO_MENU_ITEMS = [
    ("Update Yahoo Price Data based on Asset Type", "[bold green]Enter 1[/bold green]"),
    # ("Update INCREMENTAL Price Data based on Asset Type", "[bold green]Enter 2[/bold green]"),
    ("[bold red]Back To Main Menu[/bold red]", "[bold red]Enter 0[/bold red]"),
]
NSE_MENU_ITEMS = [
    ("Update NSE HISTORICAL Price Data for India Equity", "[bold green]Enter 1[/bold green]"),
    ("Update NSE INCREMENTAL Price Data for India Equity", "[bold green]Enter 2[/bold green]"),
    ("Update Weekly and Monthly Price Data", "[bold green]Enter 3[/bold green]"),
    ("[bold red]Back To Main Menu[/bold red]", "[bold red]Enter 0[/bold red]"),
]
SCANNER_MENU_ITEMS = [
    ("Hilega Milega Scanner", "[bold green]Enter 1[/bold green]"),
    ("Weekly Scanner", "[bold green]Enter 2[/bold green]"),
    ("Scanner Multi Year BackTest", "[bold green]Enter 3[/bold green]"),
    ("[bold red]Back To Main Menu[/bold red]", "[bold red]Enter 0[/bold red]"),
]
FNO_MENU_ITEMS = [
    ("Create FNO Tables", "[bold green]Enter 1[/bold green]"),
    ("Load Historical Data To Staging Tables", "[bold green]Enter 2[/bold green]"),
    ("Coming Soon...", "[bold green]Enter 3[/bold green]"),
    ("[bold red]Back To Main Menu[/bold red]", "[bold red]Enter 0[/bold red]"),
]

# ---------------- Scanner Thresholds ----------------
# Hilega Milega Scanner
HILEGA_MIN_CLOSE_PRICE = 100.0  # Minimum close price threshold
HILEGA_RSI_3_RSI_9_THRESHOLD = 1.15  # RSI(3)/RSI(9) momentum ratio threshold
HILEGA_RSI_9_EMA_THRESHOLD = 1.04  # RSI(9)/EMA strength confirmation ratio
HILEGA_EMA_WMA_THRESHOLD = 1.0  # EMA must be >= WMA
HILEGA_RSI_3_MAX = 60  # RSI(3) overbought level
HILEGA_RSI_3_WEEKLY_MIN = 50  # Weekly RSI(3) minimum threshold
HILEGA_RSI_3_MONTHLY_MIN = 50  # Monthly RSI(3) minimum threshold
HILEGA_PCT_CHANGE_MAX = 5.0  # Maximum % change volatility limit

# Weekly Scanner
WEEKLY_MIN_CLOSE_PRICE = 50.0
WEEKLY_RSI_MIN = 40
WEEKLY_RSI_MAX = 70

# Play Scanner
PLAY_MIN_VOLUME = 100000
PLAY_MIN_ATR = 1.0

# Frequencies ----------------
FREQUENCIES = ["1d", "1wk", "1mo"]

# ---------------- NSE URLs ----------------
NSE_URL_BHAV_DAILY = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{}.csv"
NSE_URL_EQUITY_HIST_BHAV = "https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData"