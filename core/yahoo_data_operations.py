from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from typing import Tuple

from config.logger import log, clear_log
from config.constants import YAHOO_MENU_ITEMS
from services.yahoo_service import insert_yahoo_price_data_pipeline

console: Console = Console()

# =====================================================================
# MENU DISPLAY
# =====================================================================
def display_menu() -> None:
    """Display the Yahoo data operations menu."""
    table: Table = Table.grid(padding=(0, 3))
    table.add_column("Action", style="bold cyan")
    table.add_column("Press", style="white")

    # Build rows from constants
    for key, action in YAHOO_MENU_ITEMS:
        table.add_row(key, action)

    panel: Panel = Panel(
        table,
        title="[bold blue]YAHOO DATA OPERATIONS[/bold blue]",
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
# INPUT VALIDATION
# =====================================================================
def validate_symbols_input(symbols: str, asset_type: str) -> tuple[bool, str]:
    """Validate user-provided symbols input.
    
    Args:
        symbols: User input string (ALL or comma-separated list)
        asset_type: Asset type being processed
        
    Returns:
        Tuple of (is_valid: bool, message: str)
    """
    symbols = symbols.strip().upper()
    
    if not symbols:
        return False, "Symbols cannot be empty"
    
    if symbols == "ALL":
        return True, "OK"
    
    # Validate comma-separated list
    symbol_list = [s.strip() for s in symbols.split(",")]
    
    if not symbol_list:
        return False, "No valid symbols found"
    
    # Check each symbol format
    for symbol in symbol_list:
        if not symbol:
            return False, "Empty symbol in list"
        if len(symbol) > 10:
            return False, f"Invalid symbol '{symbol}' (exceeds max length)"
        if not symbol.replace(".", "").replace("-", "").isalnum():
            return False, f"Invalid symbol '{symbol}' (contains invalid characters)"
    
    return True, "OK"

# =====================================================================
# MENU 1 — UPDATE HISTORICAL PRICE DATA
# =====================================================================
def action_update_yahoo_price_data() -> None:

    while True:
        console.print("\n[bold cyan]Select asset type to update:[/bold cyan]")
        console.print("  [bold]1[/bold] → YAHOO India Equity")
        console.print("  [bold]2[/bold] → YAHOO USA Equity")
        console.print("  [bold]3[/bold] → YAHOO India Index")
        console.print("  [bold]4[/bold] → YAHOO Global Index")
        console.print("  [bold]5[/bold] → YAHOO Commodity")
        console.print("  [bold]6[/bold] → YAHOO Crypto")
        console.print("  [bold]7[/bold] → YAHOO Forex")
        console.print("  [bold]0[/bold] → Back to Yahoo Menu")

        choice = Prompt.ask(
            "👉 Enter choice",
            choices=["0","1","2","3","4","5","6","7"]
        )

        # ⬅️ EXIT THIS MENU → BACK TO YAHOO MENU
        if choice == "0":
            return

        asset_type, is_index = ASSET_CHOICE_MAP[choice]

        symbols = "ALL"
        if asset_type in ("india_equity_yahoo", "usa_equity"):
            while True:
                prompt_text = (
                    "Enter symbols (ALL or comma-separated, e.g., RELIANCE, TCS)"
                    if asset_type == "india_equity_yahoo"
                    else "Enter symbols (ALL or comma-separated, e.g., GOOG, AMZN)"
                )
                symbols = Prompt.ask(prompt_text).upper()
                
                # Validate input
                is_valid, message = validate_symbols_input(symbols, asset_type)
                if is_valid:
                    break
                console.print(f"[bold red]❌ {message}[/bold red]")

        console.print(
            f"\n[bold green]{asset_type.replace('_', ' ').upper()} "
            f"Price Data Update Start...[/bold green]"
        )
        
        clear_log()
        
        insert_yahoo_price_data_pipeline(
            asset_type=asset_type,
            symbols=symbols,
            # mode="full",
            is_index=is_index
        )

        console.print(
            f"[bold green]{asset_type.replace('_', ' ').upper()} "
            f"Price Data Update Finish...[/bold green]"
        )
# =====================================================================
# MENU 2 — UPDATE INCREMENTAL PRICE DATA
# =====================================================================
# def action_update_incr_price_data() -> None:
#     clear_log()

#     while True:
#         console.print("\n[bold cyan]Select asset type to update:[/bold cyan]")
#         console.print("  [bold]1[/bold] → YAHOO India Equity")
#         console.print("  [bold]2[/bold] → YAHOO USA Equity")
#         console.print("  [bold]3[/bold] → YAHOO India Index")
#         console.print("  [bold]4[/bold] → YAHOO Global Index")
#         console.print("  [bold]5[/bold] → YAHOO Commodity")
#         console.print("  [bold]6[/bold] → YAHOO Crypto")
#         console.print("  [bold]7[/bold] → YAHOO Forex")
#         console.print("  [bold]0[/bold] → Back to Yahoo Menu")

#         choice = Prompt.ask(
#             "👉 Enter choice",
#             choices=["0","1","2","3","4","5","6","7"]
#         )

#         # ⬅️ EXIT THIS MENU → BACK TO YAHOO MENU
#         if choice == "0":
#             return

#         asset_type, is_index = ASSET_CHOICE_MAP[choice]

#         symbols = "ALL"
#         if asset_type in ("india_equity_yahoo", "usa_equity"):
#             while True:
#                 prompt_text = (
#                     "Enter symbols (ALL or comma-separated, e.g., RELIANCE, TCS)"
#                     if asset_type == "india_equity_yahoo"
#                     else "Enter symbols (ALL or comma-separated, e.g., GOOG, AMZN)"
#                 )
#                 symbols = Prompt.ask(prompt_text).upper()
                
#                 # Validate input
#                 is_valid, message = validate_symbols_input(symbols, asset_type)
#                 if is_valid:
#                     break
#                 console.print(f"[bold red]❌ {message}[/bold red]")

#         console.print(
#             f"\n[bold green]{asset_type.replace('_', ' ').upper()} "
#             f"Price Data Update Start...[/bold green]"
#         )
        
#         clear_log()
        
#         insert_yahoo_price_data_pipeline(
#             asset_type=asset_type,
#             symbols=symbols,
#             mode="incr",
#             is_index=is_index
#         )

#         console.print(
#             f"[bold green]{asset_type.replace('_', ' ').upper()} "
#             f"Price Data Update Finish...[/bold green]"
#         )
# =====================================================================
# MAIN LOOP
# =====================================================================
def yahoo_data_manager_user_input() -> None:
    try:
        while True:
            display_menu()
            choice = Prompt.ask("👉").strip()

            if choice in ("0", "q", "quit", "exit"):
                console.print("[bold green]Exiting Yahoo Data Manager...[/bold green]")
                break

            actions = {
                # "1": display_missing_price_data_symbols,
                "1": action_update_yahoo_price_data,
                # "2": action_update_incr_price_data,
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
    yahoo_data_manager_user_input()