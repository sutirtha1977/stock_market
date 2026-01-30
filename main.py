from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt

from config.constants import MAIN_MENU_ITEMS
from config.logger import clear_log
from core.scanner_operations import scanner_manager_user_input
from core.yahoo_data_operations import yahoo_data_manager_user_input
from core.database_operations import database_manager_user_input
# from core.nse_data_operations import nse_data_manager_user_input
from core.common_operations import common_data_manager_user_input
# from core.fno_operations import fno_manager_user_input

console: Console = Console()

# =====================================================================
# MENU DISPLAY  (SIMPLE STRUCTURE)
# =====================================================================
def display_menu() -> None:
    """Display the main menu."""
    table: Table = Table.grid(padding=(0, 3))
    table.add_column("Action", style="bold cyan")
    table.add_column("Press", style="white")

    # Build rows from constants
    for key, action in MAIN_MENU_ITEMS:
        table.add_row(key, action)

    panel: Panel = Panel(
        table,
        title="[bold blue]MAIN MENU[/bold blue]",
        border_style="bright_blue"
    )
    console.print(panel)

    # Only ONE instruction line
    console.print("\n[bold green]Enter an option and press ENTER:[/bold green] ", end="")
# =====================================================================
# MAIN LOOP
# =====================================================================
def start_program() -> None:
    while True:
        display_menu()

        # 👇 keep Prompt.ask minimal so it doesn’t print extra text
        choice = Prompt.ask("👉").strip()

        if choice == "1":
            database_manager_user_input()
            
        elif choice == "2":
            common_data_manager_user_input()
            
        elif choice == "3":
            yahoo_data_manager_user_input()
            
        # elif choice == "4":
        #     nse_data_manager_user_input()

        # elif choice == "5":
        #     # ("Test Data Manager (India Equity)", "[bold green]Enter 4[/bold green]"),
        #     console.print("[bold green]WIP Test Data...[/bold green]")   
    
        elif choice == "6":
            scanner_manager_user_input()   
            
        elif choice in ("0", "clear", "quit", "exit"):
            console.print("[bold green]Exiting...[/bold green]")
            clear_log()
            break

        else:
            console.print("[bold red]❌ Invalid choice![/bold red]")

# -------------------------------------------------
if __name__ == "__main__":
    start_program()
    