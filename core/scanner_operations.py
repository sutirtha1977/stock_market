from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from datetime import datetime
from typing import Optional, Dict
import pandas as pd

from config.logger import clear_log
from config.db_table import ASSET_CHOICE_MAP,ASSET_FRIENDLY_NAME
from config.constants import SCANNER_MENU_ITEMS
from scanners.backtest_service import backtest_scanners
from scanners.scanner_HM import run_scanner_hilega_milega
from scanners.scanner_weekly import run_scanner_weekly
from scanners.scanner_play import scanner_backtest_multi_years

console: Console = Console()

# =====================================================================
# MENU DISPLAY
# =====================================================================
def display_menu() -> None:
    """Display the scanner operations menu."""
    table: Table = Table.grid(padding=(0, 3))
    table.add_column("Action", style="bold cyan")
    table.add_column("Press", style="white")

    for opt, action in SCANNER_MENU_ITEMS:
        table.add_row(opt, action)

    panel: Panel = Panel(
        table,
        title="[bold blue]SCANNER OPERATIONS[/bold blue]",
        border_style="bright_blue"
    )
    console.print(panel)
    console.print("\n[bold green]Enter an option and press ENTER:[/bold green] ", end="")

# =====================================================================
# DATAFRAME DISPLAY
# =====================================================================
def print_df_rich(df: pd.DataFrame, max_rows: int = 20) -> None:
    """Display a DataFrame in rich table format.
    
    Args:
        df: DataFrame to display.
        max_rows: Maximum number of rows to show.
    """
    table: Table = Table(show_header=True, header_style="bold magenta")
    for col in df.columns:
        table.add_column(str(col))

    for _, row in df.head(max_rows).iterrows():
        table.add_row(*[str(val) for val in row.values])

    console.print(table)

    if len(df) > max_rows:
        console.print(f"... [bold]{len(df) - max_rows}[/] more rows not shown", style="dim")

# ASSET_CHOICE_MAP = {                  
#     "1": "india_equity_yahoo",
#     "2": "india_equity_nse",
#     "3": "usa_equity",
#     "4": "india_index",
#     "5": "global_index",
#     "6": "commodity",
#     "7": "crypto",
#     "8": "forex",
#     "0": None,  
# }

# ASSET_FRIENDLY_NAME = {
#     "india_equity_yahoo": "YAHOO India Equity",
#     "india_equity_nse":   "NSE India Equity",
#     "usa_equity":         "YAHOO USA Equity",
#     "india_index":        "India Index",
#     "global_index":       "Global Index",
#     "commodity":          "Commodity",
#     "crypto":             "Crypto",
#     "forex":              "Forex",
# }

# =====================================================================
# ASSET SELECTION PROMPT
# =====================================================================
def prompt_asset_type() -> str | None:
    console.print("\n[bold cyan]Select asset type:[/bold cyan]")

    for key, asset in ASSET_CHOICE_MAP.items():
        if key == "0":
            console.print("  [bold]0[/bold] → Back")
        else:
            console.print(
                f"  [bold]{key}[/bold] → {ASSET_FRIENDLY_NAME[asset]}"
            )

    choice = Prompt.ask(
        "👉 Enter choice",
        choices=list(ASSET_CHOICE_MAP.keys())
    )

    return ASSET_CHOICE_MAP[choice]  # None if Back

# =====================================================================
# Helper to prompt a valid date
# =====================================================================
def prompt_date(message: str) -> str:
    """Prompt user for a date in YYYY-MM-DD format.
    
    Args:
        message: Prompt message to display
        
    Returns:
        User-entered date or empty string
    """
    while True:
        user_input = Prompt.ask(message, default="").strip()
        if user_input == "":
            return ""
        try:
            datetime.strptime(user_input, "%Y-%m-%d")
            return user_input
        except ValueError:
            console.print(
                "[bold red]❌ Invalid date format! Use YYYY-MM-DD[/bold red]"
            )

def prompt_year(message: str = "Enter start year") -> int:
    """Prompt user for a valid year.
    
    Args:
        message: Prompt message to display
        
    Returns:
        Valid year as integer
    """
    while True:
        user_input = Prompt.ask(message, default="2026").strip()
        try:
            year = int(user_input)
            if 2000 <= year <= datetime.now().year + 1:
                return year
            console.print(f"[bold red]❌ Year must be between 2000 and {datetime.now().year + 1}[/bold red]")
        except ValueError:
            console.print("[bold red]❌ Year must be a valid integer[/bold red]")

def prompt_lookback(message: str = "Enter lookback count") -> int:
    """Prompt user for a valid lookback count.
    
    Args:
        message: Prompt message to display
        
    Returns:
        Valid lookback count as integer
    """
    while True:
        user_input = Prompt.ask(message, default="15").strip()
        try:
            lookback = int(user_input)
            if 1 <= lookback <= 50:
                return lookback
            console.print("[bold red]❌ Lookback count must be between 1 and 50[/bold red]")
        except ValueError:
            console.print("[bold red]❌ Lookback count must be a valid integer[/bold red]")

# =====================================================================
# SCANNER HANDLER
# =====================================================================
def action_scanner(scanner_type: str) -> None:
    clear_log()

    asset_type = prompt_asset_type()
    if asset_type is None:
        return  # ⬅️ back to scanner menu

    if scanner_type == "HM":
        console.print("[bold yellow]Running Hilega Milega Scanner...[/bold yellow]")
        end_date = prompt_date("Enter end date (YYYY-MM-DD) or press Enter")
        df = run_scanner_hilega_milega(
            end_date=end_date,
            asset_type=asset_type
        )
        print_df_rich(df)

    elif scanner_type == "WEEK":
        console.print("[bold yellow]Running Weekly Scanner...[/bold yellow]")
        end_date = prompt_date("Enter End date (YYYY-MM-DD) or press Enter")
        df = run_scanner_weekly(
            end_date=end_date,
            asset_type=asset_type
        )
        print_df_rich(df)

    elif scanner_type == "PLAY":
        console.print("[bold yellow]Running Multi-Year Scanner...[/bold yellow]")
        user_year = prompt_year("Enter start year")
        lookback_count = prompt_lookback("Enter lookback count")

        scanner_backtest_multi_years(
            start_year=user_year,     # ✅ int
            lookback_years=lookback_count,
            asset_type=asset_type
        )

# =====================================================================
# MAIN LOOP
# =====================================================================
def scanner_manager_user_input() -> None:
    try:
        while True:
            display_menu()
            choice = Prompt.ask("👉").strip()

            if choice in ("0", "q", "quit", "exit"):
                console.print("[bold green]Exiting Scanner Manager...[/bold green]")
                break

            actions = {
                "1": lambda: action_scanner("HM"),
                "2": lambda: action_scanner("WEEK"),
                "3": lambda: action_scanner("PLAY"),
            }

            func = actions.get(choice)
            if func:
                func()
            else:
                console.print("[bold red]❌ Invalid choice![/bold red]")

    except KeyboardInterrupt:
        console.print("\n[bold green]Interrupted by user. Exiting...[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")

# =====================================================================
# ENTRY POINT
# =====================================================================
if __name__ == "__main__":
    scanner_manager_user_input()