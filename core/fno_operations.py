from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt

from config.logger import log, clear_log
from config.constants import FNO_MENU_ITEMS
from database.create_fno_db import create_fno_tables
# from loaders.load_fno_bhavcopy import run_fno_loader

console: Console = Console()

# =====================================================================
# MENU DISPLAY  (SIMPLE STRUCTURE)
# =====================================================================
def display_menu() -> None:
    """Display the FnO (Futures & Options) operations menu."""
    table: Table = Table.grid(padding=(0, 3))
    table.add_column("Action", style="bold cyan")
    table.add_column("Press", style="white")
    # table.add_column("Frequency", justify="center")

    # for opt, action, freq in DATA_MENU_ITEMS:
    #     table.add_row(opt, action, freq)
    for opt, action in FNO_MENU_ITEMS:
        table.add_row(opt, action)
        
    panel: Panel = Panel(
        table,
        title="[bold blue]FUTURE AND OPTIONS[/bold blue]",
        border_style="bright_blue"
    )
    console.print(panel)
    console.print("\n[bold green]Enter an option and press ENTER:[/bold green] ", end="")

# Menu 1
def action_create_fno_db() -> None:
    """Create FnO database tables."""
    clear_log()
    console.print("[bold green]Database Creation Start...[/bold green]")
    create_fno_tables()
    console.print("[bold green]Database Creation Finish...[/bold green]")

# Menu 2
def action_fno_data_to_stage() -> None:
    """Load FnO historical data to staging tables."""
    clear_log()
    console.print("[bold green]Update Historical fno data to staging tables START...[/bold green]")

    # -------------------------------------------------
    # Ask user which folder(s) to run
    # -------------------------------------------------
    console.print("\n[bold cyan]Select data source to load:[/bold cyan]")
    console.print("  [bold]1[/bold] → OLD folder")
    console.print("  [bold]2[/bold] → NEW folder")
    console.print("  [bold]3[/bold] → BOTH folders")

    choice: str = Prompt.ask("👉 Enter choice", choices=["1", "2", "3"], default="3")

    # -------------------------------------------------
    # Call loader with user choice
    # -------------------------------------------------
    if choice == "1":
        run_fno_loader(run_mode="old")
    elif choice == "2":
        run_fno_loader(run_mode="new")
    else:
        run_fno_loader(run_mode="both")

    console.print("[bold green]Update Historical fno data to staging tables FINISH...[/bold green]")

# Menu 3
def action_update() -> None:
    """Placeholder for future FnO update functionality."""
    clear_log()
    console.print("[bold green]Coming soon...[/bold green]")

# =====================================================================
# MAIN LOOP
# =====================================================================
def fno_manager_user_input() -> None:
    """Main loop for FnO operations."""
    try:
        while True:
            display_menu()
            choice: str = Prompt.ask("👉").strip()

            if choice in ("0", "q", "quit", "exit"):
                break

            actions = {
                "1": action_create_fno_db,
                "2": action_fno_data_to_stage,
                "3": action_update,
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
    fno_manager_user_input()