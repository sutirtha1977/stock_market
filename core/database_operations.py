from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from typing import Callable, Dict, Optional

from config.logger import log, clear_log
from config.constants import DATABASE_MENU_ITEMS
from database.create_db import create_stock_database
from services.symbol_service import refresh_symbols

console: Console = Console()

# =====================================================================
# MENU DISPLAY
# =====================================================================
def display_menu() -> None:
    """Display the database operations menu."""
    table: Table = Table.grid(padding=(0, 3))
    table.add_column("Action", style="bold cyan")
    table.add_column("Press", style="white")

    for opt, action in DATABASE_MENU_ITEMS:
        table.add_row(opt, action)

    panel: Panel = Panel(
        table,
        title="[bold blue]DATA OPERATIONS[/bold blue]",
        border_style="bright_blue"
    )
    console.print(panel)
    console.print("\n[bold green]Enter an option and press ENTER:[/bold green] ", end="")
# =====================================================================
# Menu 1: Create Database
# =====================================================================
def action_create_db() -> None:
    """Create the stock database."""
    clear_log()
    user_input: str = input("Do you want to create the database? (y/n): ").strip().lower()
    if user_input != "y":
        print("❌ Database creation skipped by user.")
        return

    console.print("[bold green]Database Creation Start...[/bold green]")
    create_stock_database()
    console.print("[bold green]Database Creation Finish...[/bold green]")

# =====================================================================
# Menu 2: Refresh Symbols
# =====================================================================
def action_update_all_symbols() -> None:
    """Update all symbols in the database."""
    clear_log()
    user_input: str = input("Do you want to refresh all symbols? (y/n): ").strip().lower()
    if user_input != "y":
        print("❌ Symbols refresh skipped by user.")
        return

    console.print("[bold green]Symbols Refresh Start...[/bold green]")
    refresh_symbols()
    console.print("[bold green]Symbols Refresh Finish...[/bold green]")
# =====================================================================
# MAIN LOOP
# =====================================================================
def database_manager_user_input() -> None:
    """Main loop for database operations."""
    try:
        while True:
            display_menu()
            choice: str = Prompt.ask("👉").strip()

            if choice in ("0", "q", "quit", "exit"):
                break

            actions: Dict[str, Callable[[], None]] = {
                "1": action_create_db,
                "2": action_update_all_symbols,
            }

            func: Optional[Callable[[], None]] = actions.get(choice)
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
    database_manager_user_input()