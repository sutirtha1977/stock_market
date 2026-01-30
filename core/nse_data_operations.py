from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from typing import Optional, Tuple

from config.logger import log, clear_log
from config.constants import NSE_MENU_ITEMS
from services.bhavcopy_service import (
    import_nse_historical_data_to_db, 
    import_nse_incremental_data_to_db
)
from services.weekly_monthly_service import generate_higher_timeframes

console: Console = Console()

# =====================================================================
# MENU DISPLAY
# =====================================================================
def display_menu() -> None:
    """Display the NSE data operations menu."""
    table: Table = Table.grid(padding=(0, 3))
    table.add_column("Action", style="bold cyan")
    table.add_column("Press", style="white")

    # Build rows from constants
    for key, action in NSE_MENU_ITEMS:
        table.add_row(key, action)

    panel: Panel = Panel(
        table,
        title="[bold blue]NSE DATA OPERATIONS[/bold blue]",
        border_style="bright_blue"
    )
    console.print(panel)

    # Only ONE instruction line
    console.print("\n[bold green]Enter an option and press ENTER:[/bold green] ", end="")


# =====================================================================
# ASSET CHOICE → PIPELINE CONFIG
# =====================================================================
ASSET_CHOICE_MAP = {
    "1": ("india_equity_yahoo", False),
    "2": ("usa_equity", False),
    "3": ("india_index", True),
    "4": ("global_index", True),
    "5": ("commodity", False),
    "6": ("crypto", False),
    "7": ("forex", False),
}
# =====================================================================
# MENU 1 — UPDATE INCREMENTAL PRICE DATA
# =====================================================================
def action_update_hist_price_data() -> None:
    clear_log()
    console.print("[bold green]Update India Equity NSE Historical Price Data Start....[/bold green]")
    import_nse_historical_data_to_db(asset_type="india_equity_nse")
    console.print("[bold green]Update India Equity NSE Historical Price Data Finish...[/bold green]")    
# =====================================================================
# MENU 2 — UPDATE INCREMENTAL PRICE DATA
# =====================================================================
def action_update_incr_price_data() -> None:
    clear_log()
    console.print("[bold green]Incremental Price Data Update Start....[/bold green]")
    import_nse_incremental_data_to_db(asset_type="india_equity_nse")
    console.print("[bold green]Incremental Price Data Update Finish...[/bold green]")
# =====================================================================
# MENU 3 — UPDATE Weekly and Monthly PRICE DATA
# =====================================================================
def action_update_weekly_monthly_price_data() -> None:
    clear_log()
    console.print("[bold green]Update Weekly and Monthly Price Data Update Start....[/bold green]")
    generate_higher_timeframes(asset_type="india_equity_nse")
    console.print("[bold green]Update Weekly and Monthly Price Data Update Finish...[/bold green]")
# =====================================================================
# MAIN LOOP
# =====================================================================
def nse_data_manager_user_input() -> None:
    try:
        while True:
            display_menu()
            choice = Prompt.ask("👉").strip()

            if choice in ("0", "q", "quit", "exit"):
                console.print("[bold green]Exiting Yahoo Data Manager...[/bold green]")
                break

            actions = {
                "1": action_update_hist_price_data,
                "2": action_update_incr_price_data,
                "3": action_update_weekly_monthly_price_data,
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
    nse_data_manager_user_input()