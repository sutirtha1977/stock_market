from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from typing import Optional, Dict, Any
import pandas as pd

from config.logger import log, clear_log
from config.constants import COMMON_MENU_ITEMS
from services.utility_service import show_latest_dates, upsert_nse_holidays
from services.indicator_service import refresh_indicators
from services.weekly_monthly_service import refresh_all_week52_stats
from services.symbol_service import find_missing_price_data_symbols_all_assets

console: Console = Console()

# =====================================================================
# MENU DISPLAY
# =====================================================================
def display_menu() -> None:
    """Display the common operations menu."""
    table: Table = Table.grid(padding=(0, 3))
    table.add_column("Action", style="bold cyan")
    table.add_column("Press", style="white")

    # Build rows from constants
    for key, action in COMMON_MENU_ITEMS:
        table.add_row(key, action)

    panel: Panel = Panel(
        table,
        title="[bold blue]COMMON OPERATIONS[/bold blue]",
        border_style="bright_blue"
    )
    console.print(panel)

    # Only ONE instruction line
    console.print("\n[bold green]Enter an option and press ENTER:[/bold green] ", end="")

# =====================================================================
# MENU 1 — Show Latest Dates 
# =====================================================================
def action_show_latest_date() -> None:
    """Show latest dates for all asset types."""
    clear_log()
    show_latest_dates()
# =====================================================================
# MENU 2 — DISPLAY MISSING PRICE DATA
# =====================================================================
def action_display_missing_price_data() -> None:
    """Display missing price data across all assets."""
    clear_log()
    console.print("\n[bold cyan]🔎 Checking Missing Price Data (All Assets)[/bold cyan]\n")

    results: Optional[Dict[str, pd.DataFrame]] = find_missing_price_data_symbols_all_assets()

    if not results:
        console.print("[bold green]✅ No missing price data found across all assets[/bold green]")
        return

    for asset_type, df in results.items():
        console.print(
            f"\n[bold yellow]{asset_type.replace('_', ' ').upper()}[/bold yellow]"
        )

        table: Table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Symbol")
        table.add_column("Name")
        table.add_column("Missing Timeframe")

        for _, row in df.iterrows():
            table.add_row(
                row["yahoo_symbol"],
                row["name"],
                row["missing_timeframe"]
            )

        console.print(table)
# =====================================================================
# MENU 3 — UPDATE INDICATORS
# =====================================================================
def action_update_all_indicators() -> None:
    """Update all technical indicators for all assets."""
    clear_log()
    console.print("[bold green]Update all Indicators Start....[/bold green]")
    refresh_indicators()
    console.print("[bold green]Update all Indicators Finish....[/bold green]")

# =====================================================================
# MENU 4 — UPDATE 52 WEEK STATS
# =====================================================================
def action_update_52week_stats() -> None:
    """Update 52-week statistics for all assets."""
    clear_log()
    console.print("[bold green]Refresh 52 WEEKS Start....[/bold green]")
    refresh_all_week52_stats()
    console.print("[bold green]Refresh 52 WEEKS Finish....[/bold green]")

# =====================================================================
# MENU 5 — UPDATE NSE Holidays
# =====================================================================
def action_update_nse_holidays() -> None:
    clear_log()
    console.print("[bold green]Refresh NSE Holidays Start....[/bold green]")
    upsert_nse_holidays()
    console.print("[bold green]Refresh NSE Holidays Finish....[/bold green]")

# =====================================================================
# MAIN LOOP
# =====================================================================
def common_data_manager_user_input() -> None:
    try:
        while True:
            display_menu()
            choice = Prompt.ask("👉").strip()

            if choice in ("0", "q", "quit", "exit"):
                console.print("[bold green]Exiting Yahoo Data Manager...[/bold green]")
                break

            actions = {
                "1": action_show_latest_date,
                "2": action_display_missing_price_data,
                "3": action_update_all_indicators,
                "4": action_update_52week_stats,
                "5": action_update_nse_holidays,
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
    common_data_manager_user_input()