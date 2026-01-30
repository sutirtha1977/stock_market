"""
Streamlit web app for stock market analysis.

Run with: streamlit run app.py
"""

import streamlit as st
import traceback
from datetime import datetime
import pandas as pd
from config.logger import log, clear_log
from config.db_table import ASSET_FRIENDLY_NAME

# Database operations
from database.create_db import create_stock_database
from services.symbol_service import refresh_symbols

# Common operations
from services.indicator_service import refresh_indicators
from services.weekly_monthly_service import (
    refresh_all_week52_stats, 
    generate_higher_timeframes
)
from services.symbol_service import find_missing_price_data_symbols_all_assets

# Yahoo operations
from services.yahoo_service import (
    insert_yahoo_price_data_pipeline, 
    clone_data_from_yahoo_to_yahoo_calc
)   
# Utility operations
from services.utility_service import (
    upsert_nse_holidays,
    get_latest_dates_data, 
    get_nse_holidays_current_year
)
from scanners.scanner_HM import run_scanner_hilega_milega
from scanners.scanner_weekly import run_scanner_weekly
from scanners.scanner_weekly_multi import scanner_backtest_multi_years_weekly
from scanners.scanner_HM_multi import scanner_backtest_multi_years_hm

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
        # if len(symbol) > 10:
        #     return False, f"Invalid symbol '{symbol}' (exceeds max length)"
        if not symbol.replace(".", "").replace("-", "").isalnum():
            return False, f"Invalid symbol '{symbol}' (contains invalid characters)"
    
    return True, "OK"
# =====================================================================
# Streamlit page config
# =====================================================================
st.set_page_config(
    page_title="Stock Market - Database Operations",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)
# =====================================================================
# Custom CSS
# =====================================================================
st.markdown("""
    <style>
        .main-header { font-size: 3rem; color: #1f77b4; margin-bottom: 2rem; }
        .section-header { font-size: 1.5rem; color: #ff7f0e; margin-top: 2rem; margin-bottom: 1rem; }
        .info-box { background-color: #f0f2f6; padding: 1rem; border-radius: 0.5rem; margin: 1rem 0; }
    </style>
""", unsafe_allow_html=True)

# =====================================================================
# Session State Initialization
# =====================================================================
if 'current_operation' not in st.session_state:
    st.session_state.current_operation = None
if 'current_menu' not in st.session_state:
    st.session_state.current_menu = None

def soft_refresh_menu(menu_name: str):
    """Switch menu and clear current operation + UI state"""
    st.session_state.current_menu = menu_name
    st.session_state.current_operation = None

    preserved_keys = {"current_menu", "current_operation"}
    for key in list(st.session_state.keys()):
        if key not in preserved_keys:
            del st.session_state[key]
    st.rerun()

# =====================================================================
# Sidebar rendering
# =====================================================================
def render_sidebar():
    st.sidebar.markdown("# 📊 Stock Market")
    st.sidebar.markdown("---")
    # -------------------------------------------------
    # MAIN MENU BUTTONS
    # -------------------------------------------------
    if st.sidebar.button("📋 Database Operations", key="menu_database", use_container_width=True):
        soft_refresh_menu("database_operations")
    if st.sidebar.button("🛠️ Common Operations", key="menu_common", use_container_width=True):
        soft_refresh_menu("common_operations")
    if st.sidebar.button("🔄 Yahoo Operations", key="menu_yahoo", use_container_width=True):
        soft_refresh_menu("yahoo_operations")
    # if st.sidebar.button("🇮🇳 NSE Operations", key="menu_nse", use_container_width=True):
    #     soft_refresh_menu("nse_operations")
    if st.sidebar.button("🔍 Scanner Operations", key="menu_scanner", use_container_width=True):
        soft_refresh_menu("scanner_operations")
    # # -------------------------------------------------
    # # NSE HOLIDAYS (BOTTOM SECTION)
    # # -------------------------------------------------
    # try:
    #     df = get_nse_holidays_current_year()  # returns ['Date', 'Day', 'Holiday Name']

    #     if df.empty:
    #         st.sidebar.caption("No holidays available")
    #     else:
    #         # Convert the 'Date' string from SQL back to datetime for comparison
    #         df['Date_obj'] = pd.to_datetime(df['Date'], format='%d-%b-%Y').dt.date

    #         today = datetime.today().date()

    #         # Find the next upcoming holiday
    #         upcoming_dates = df.loc[df['Date_obj'] >= today, 'Date_obj']
    #         next_holiday_date = min(upcoming_dates) if not upcoming_dates.empty else None

    #         # Row-wise styling function (applies only to 'Date' column)
    #         def style_date(val):
    #             date_val = datetime.strptime(val, '%d-%b-%Y').date()
    #             if date_val < today:
    #                 return 'color: red'
    #             elif next_holiday_date is not None and date_val == next_holiday_date:
    #                 return 'color: green; font-weight: bold'
    #             else:
    #                 return ''

    #         # Apply styling only to the Date column
    #         styled_df = df.style.applymap(style_date, subset=['Date'])

    #         st.sidebar.dataframe(
    #             styled_df,
    #             hide_index=True,
    #             use_container_width=True,
    #             height=220
    #         )

    # except Exception as e:
    #     st.sidebar.caption(f"Failed to load holidays: {e}")
        
# =====================================================================
# Menu Definitions
# =====================================================================
DATABASE_OPERATIONS = {
    "1": {"title": "Create Database", "description": "Create the stock database from scratch", "icon": "🗄️"},
    "2": {"title": "Refresh Symbols", "description": "Update all symbols in the database", "icon": "🔄"},
}

COMMON_OPERATIONS = {
    "1": {"title": "Show Latest Dates", "description": "Display latest dates for all asset types", "icon": "📅"},
    "2": {"title": "Display Missing Price Data", "description": "Show symbols with missing price data across all assets", "icon": "🔎"},
    "3": {"title": "Update All Indicators", "description": "Refresh technical indicators for all assets", "icon": "📈"},
    "4": {"title": "Update 52 Weeks Stats", "description": "Refresh 52-week statistics for all assets", "icon": "📊"},
    "5": {"title": "Update NSE Holidays", "description": "Refresh NSE holidays in the database", "icon": "📅"},
}

YAHOO_OPERATIONS = {
    "1": {"title": "Update Yahoo Price Data", "description": "Update full/incremental price data for selected assets", "icon": "📊"},
    "2": {"title": "Clone Data to Yahoo Calc", "description": "Clone data to Yahoo Calc Price Data table for Yahoo table for 1d", "icon": "📈"},
    "3": {"title": "Refresh Indicators based on Asset type", "description": "Refresh indicators for a specific asset type", "icon": "📈"},
}

SCANNER_OPERATIONS = {
    "1": {"title": "Hilega Milega Scanner", "description": "Run HM scanner for selected assets", "icon": "🟡"},
    "2": {"title": "Hilega Milega Scanner Backtest", "description": "Run HM scanner for multi year backtest", "icon": "🟣"},
    "3": {"title": "Weekly Scanner", "description": "Run Weekly scanner for selected assets", "icon": "🟢"},
    "4": {"title": "Weekly Scanner Backtest", "description": "Run weekly scanner for multi year backtest", "icon": "🔵"},
}

# =====================================================================
# Main Screens
# =====================================================================
def show_main_screen():
    st.markdown("# 📊 Stock Market System")
    st.markdown("---")
    st.info("👈 Select a menu from the sidebar to begin")

# =====================================================================
# Submenu Renderers
# =====================================================================
def render_operation_submenu(operations: dict, menu_key_prefix: str):
    """Generic function to render submenu cards."""
    st.markdown("### Available Operations:")
    st.markdown("")
    
    for key, op in operations.items():
        col1, col2, col3 = st.columns([0.5, 3, 1])
        with col1: st.markdown(f"## {op['icon']}")
        with col2:
            st.markdown(f"### {op['title']}")
            st.caption(op['description'])
        with col3:
            if st.button("Select", key=f"{menu_key_prefix}_{key}", use_container_width=True):
                st.session_state.current_operation = key
                st.rerun()

def show_database_operations_submenu():
    st.markdown("# 📋 Database Operations")
    st.markdown("---")
    render_operation_submenu(DATABASE_OPERATIONS, "select_db")
    st.markdown("---")

def show_common_operations_submenu():
    st.markdown("# 🛠️ Common Operations")
    st.markdown("---")
    render_operation_submenu(COMMON_OPERATIONS, "select_common")
    st.markdown("---")

def show_yahoo_operations_submenu():
    st.markdown("# 🔄 Yahoo Operations")
    st.markdown("---")
    render_operation_submenu(YAHOO_OPERATIONS, "select_yahoo")
    st.markdown("---")
    
def show_scanner_operations_submenu():
    st.markdown("# 🔍 Scanner Operations")
    st.markdown("---")
    render_operation_submenu(SCANNER_OPERATIONS, "select_scanner")
    st.markdown("---")
    
# =====================================================================
# Database Operations
# =====================================================================
# =====================================================================
# Menu 1: Database Operations
# =====================================================================
def operation_create_database():
    clear_log()
    st.markdown("# 🗄️ Create Database")
    st.markdown("---")
    st.warning("⚠️ This will create a new stock database from scratch")
    confirm = st.radio(
        "Do you want to create the database? (y/n):", 
        ["No", "Yes"], 
        index=0, 
        key="create_db_confirm"
    )

    # Track whether user clicked the confirm button
    create_db_clicked = st.button("✅ Confirm - Create Database", use_container_width=True)

    if create_db_clicked:
        if confirm == "Yes":
            with st.spinner("🔨 Creating database..."):
                try:
                    log("Starting database creation...")
                    create_stock_database()
                    log("Database creation completed successfully")
                    st.success("✅ Database created successfully!")
                except Exception as e:
                    log(f"Database creation error: {str(e)}", "error")
                    st.error(f"❌ Error creating database: {str(e)}")
                    with st.expander("📋 Error Details"):
                        st.code(traceback.format_exc())
        else:
            st.info("❌ Database creation skipped by user.")
    # Back button
    if st.button("⬅️ Back to Database Operations", use_container_width=True):
        st.session_state.current_operation = None
        st.rerun()
# =====================================================================
# Menu 2: Database Operations
# =====================================================================
def operation_refresh_symbols():
    clear_log()
    st.markdown("# 🔄 Refresh Symbols")
    st.markdown("---")
    st.info("ℹ️ This will update all symbols in the database")

    # Radio for user confirmation
    confirm = st.radio(
        "Do you want to refresh all symbols? (y/n):",
        ["No", "Yes"],
        index=0,
        key="refresh_symbols_confirm"
    )

    # Button to confirm action
    refresh_symbols_clicked = st.button("✅ Confirm - Refresh Symbols", use_container_width=True)

    if refresh_symbols_clicked:
        if confirm == "Yes":
            with st.spinner("🔄 Refreshing symbols..."):
                try:
                    log("Starting symbols refresh...")
                    refresh_symbols()
                    log("Symbols refresh completed successfully")
                    st.success("✅ Symbols refreshed successfully!")
                except Exception as e:
                    log(f"Symbols refresh error: {str(e)}", "error")
                    st.error(f"❌ Error refreshing symbols: {str(e)}")
                    with st.expander("📋 Error Details"):
                        st.code(traceback.format_exc())
        else:
            st.info("❌ Symbols refresh skipped by user.")

    # Back button
    if st.button("⬅️ Back to Database Operations", use_container_width=True):
        st.session_state.current_operation = None
        st.rerun()

# =====================================================================
# Common Operations
# =====================================================================
# =====================================================================
# Menu 1: Common Operations
# =====================================================================
def operation_show_latest_dates():
    clear_log()
    st.markdown("# 📅 Show Latest Dates")
    st.markdown("---")

    with st.form(key="latest_dates_form"):
        st.info("ℹ️ Display latest dates for all asset types")
        submit_button = st.form_submit_button(label="✅ Show Latest Dates")

    if submit_button:
        with st.spinner("🔍 Fetching latest dates..."):
            try:
                df = get_latest_dates_data()
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.success("✅ Latest dates displayed successfully!")
            except Exception as e:
                log(f"Show latest dates error: {str(e)}", "error")
                st.error(f"❌ Error showing latest dates: {str(e)}")
                with st.expander("📋 Error Details"):
                    st.code(traceback.format_exc())

    if st.button("⬅️ Back to Common Operations", key="back_common_1", use_container_width=True):
        st.session_state.current_operation = None
        st.rerun()
# =====================================================================
# Menu 2: Common Operations
# =====================================================================
def operation_display_missing_price_data():
    clear_log()
    st.markdown("# 🔎 Display Missing Price Data")
    st.markdown("---")

    with st.form(key="missing_price_form"):
        st.info("ℹ️ Show symbols with missing price data across all assets")
        submit_button = st.form_submit_button(label="✅ Display Missing Price Data")

    if submit_button:
        with st.spinner("🔍 Checking missing price data..."):
            try:
                results = find_missing_price_data_symbols_all_assets()
                if not results:
                    st.success("✅ No missing price data found across all assets!")
                else:
                    st.success("✅ Missing price data analysis completed!")
                    for asset_type, df in results.items():
                        st.subheader(f"{asset_type.replace('_', ' ').upper()}")
                        st.dataframe(df)
            except Exception as e:
                log(f"Display missing price data error: {str(e)}", "error")
                st.error(f"❌ Error displaying missing price data: {str(e)}")
                with st.expander("📋 Error Details"):
                    st.code(traceback.format_exc())

    if st.button("⬅️ Back to Common Operations", key="back_common_2", use_container_width=True):
        st.session_state.current_operation = None
        st.rerun()
# =====================================================================
# Menu 3: Common Operations
# =====================================================================
def operation_update_all_indicators():
    clear_log()
    st.markdown("# 📈 Update All Indicators")
    st.markdown("---")

    with st.form(key="update_indicators_form"):
        st.info("ℹ️ Refresh technical indicators for all assets")
        submit_button = st.form_submit_button(label="✅ Update All Indicators")

    if submit_button:
        with st.spinner("📈 Updating indicators..."):
            try:
                refresh_indicators()
                st.success("✅ All indicators updated successfully!")
            except Exception as e:
                log(f"Update indicators error: {str(e)}", "error")
                st.error(f"❌ Error updating indicators: {str(e)}")
                with st.expander("📋 Error Details"):
                    st.code(traceback.format_exc())

    if st.button("⬅️ Back to Common Operations", key="back_common_3", use_container_width=True):
        st.session_state.current_operation = None
        st.rerun()
# =====================================================================
# Menu 4: Common Operations
# =====================================================================
def operation_update_52_weeks_stats():
    clear_log()
    st.markdown("# 📊 Update 52 Weeks Stats")
    st.markdown("---")

    with st.form(key="update_52weeks_form"):
        st.info("ℹ️ Refresh 52-week statistics for all assets")
        submit_button = st.form_submit_button(label="✅ Update 52 Weeks Stats")

    if submit_button:
        with st.spinner("📊 Updating 52 weeks stats..."):
            try:
                refresh_all_week52_stats()
                st.success("✅ 52 weeks stats updated successfully!")
            except Exception as e:
                log(f"Update 52 weeks stats error: {str(e)}", "error")
                st.error(f"❌ Error updating 52 weeks stats: {str(e)}")
                with st.expander("📋 Error Details"):
                    st.code(traceback.format_exc())

    if st.button("⬅️ Back to Common Operations", key="back_common_4", use_container_width=True):
        st.session_state.current_operation = None
        st.rerun()
# =====================================================================
# Menu 5: Common Operations
# =====================================================================
def operation_update_nse_holidays():
    clear_log()
    st.markdown("# 📅 Update NSE Holidays")
    st.markdown("---")
    st.info("ℹ️ This will update NSE holidays in the database from CSV")

    with st.form(key="update_nse_holidays_form"):
        submit_button = st.form_submit_button(label="✅ Update NSE Holidays")

    if submit_button:
        with st.spinner("📅 Updating NSE holidays..."):
            try:
                # Call your service function
                upsert_nse_holidays()
                st.success("✅ NSE holidays updated successfully!")
            except Exception as e:
                log(f"Update NSE holidays error: {str(e)}", "error")
                st.error(f"❌ Error updating NSE holidays: {str(e)}")
                with st.expander("📋 Error Details"):
                    import traceback
                    st.code(traceback.format_exc())

    if st.button("⬅️ Back to Common Operations", key="back_common_5", use_container_width=True):
        st.session_state.current_operation = None
        st.rerun()   
# =====================================================================
# Yahoo Operations
# =====================================================================
# =====================================================================
# Menu 1: Yahoo Operations
# =====================================================================
def operation_update_yahoo_historical_price_data():
    clear_log()

    st.markdown("# 📊 Update Yahoo Historical Price Data")
    st.markdown("---")

    st.warning("⚠️ This will download and update Yahoo historical price data")

    # -------------------------------------------------
    # 1️⃣ Asset type selection
    # -------------------------------------------------
    asset_choice_map = {
        "India Equity (Yahoo)": ("india_equity_yahoo", False),
        "USA Equity (Yahoo)": ("usa_equity", False),
        "India Index (Yahoo)": ("india_index", True),
        "Global Index (Yahoo)": ("global_index", True),
        "Commodity (Yahoo)": ("commodity", False),
        "Crypto (Yahoo)": ("crypto", False),
        "Forex (Yahoo)": ("forex", False),
    }

    asset_label = st.selectbox(
        "Select asset type",
        options=list(asset_choice_map.keys()),
        index=0
    )

    asset_type, is_index = asset_choice_map[asset_label]

    # -------------------------------------------------
    # 2️⃣ Symbol input (only for equity)
    # -------------------------------------------------
    symbols = "ALL"
    if asset_type in ("india_equity_yahoo", "usa_equity"):
        symbol_placeholder = (
            "ALL or comma-separated (e.g. RELIANCE, TCS)"
            if asset_type == "india_equity_yahoo"
            else "ALL or comma-separated (e.g. GOOG, AMZN)"
        )

        symbols = st.text_input(
            "Enter symbols",
            value="ALL",
            help=symbol_placeholder
        ).upper()

        if symbols:
            is_valid, message = validate_symbols_input(symbols, asset_type)
            if not is_valid:
                st.error(f"❌ {message}")
                return

    # -------------------------------------------------
    # 3️⃣ Execute update
    # -------------------------------------------------
    if st.button("✅ Start Yahoo Historical Data Update", use_container_width=True):
        with st.spinner("📥 Updating Yahoo historical price data..."):
            try:
                # This function should now return a list of failed symbols
                failed_symbols = insert_yahoo_price_data_pipeline(
                    asset_type=asset_type,
                    symbols=symbols
                )

                if failed_symbols:
                    st.warning(
                        f"⚠️ Download completed with failures:\n\n**{', '.join(failed_symbols)}**"
                    )
                else:
                    st.success(
                        f"✅ {asset_label} historical price data updated successfully!"
                    )

            except Exception as e:
                log(f"Yahoo historical price data update error: {str(e)}", "error")
                st.error(f"❌ Error updating data: {str(e)}")

                with st.expander("📋 Error Details"):
                    st.code(traceback.format_exc())

    # -------------------------------------------------
    # ⬅️ Back
    # -------------------------------------------------
    if st.button("⬅️ Back to Yahoo Operations", use_container_width=True):
        st.session_state.current_operation = None
        st.rerun()
# =====================================================================
# Menu 2: Yahoo Operations
# =====================================================================
def operation_clone_data_to_yahoo_calc():
    clear_log()
    st.markdown("# 📈 Clone Data to Yahoo Calc")
    st.markdown("---")
    st.info("ℹ️ This will clone 1-day price data from Yahoo Price Data table to Yahoo Calc Price Data table")

    confirm = st.radio(
        "Do you want to clone data? (y/n):",
        ["No", "Yes"],
        index=0,
        key="clone_yahoo_calc_confirm"
    )
    if st.button("✅ Confirm - Clone Data", use_container_width=True):
        if confirm == "Yes":
            with st.spinner("📥 Cloning data to Yahoo Calc..."):
                try:
                    counts = clone_data_from_yahoo_to_yahoo_calc()

                    if counts["insert_skipped"]:
                        st.info(
                            f"⚠️ Cloning skipped: "
                            f"{counts['source_name']} rows (1d) = {counts['source_count']}, "
                            f"{counts['target_name']} rows (1d) = {counts['target_count']}"
                        )
                    else:
                        st.success("✅ Data cloned successfully!")
                        st.info(f"{counts['source_name']} rows (1d): {counts['source_count']}")
                        st.info(f"{counts['target_name']} rows (1d): {counts['target_count']}")

                except Exception as e:
                    log(f"Clone data error: {str(e)}", "error")
                    st.error(f"❌ Error cloning data: {str(e)}")
                    with st.expander("📋 Error Details"):
                        import traceback
                        st.code(traceback.format_exc())

        else:
            # Only show this if user actually clicked the button and selected "No"
            st.warning("❌ Cloning skipped by user.")

    if st.button("⬅️ Back to Yahoo Operations", use_container_width=True):
        st.session_state.current_operation = None
        st.rerun()
# =====================================================================
# Menu 3: Yahoo Operations
# =====================================================================
def operation_refresh_indicators_by_asset():
    clear_log()
    st.markdown("# 📈 Refresh Indicators by Asset Type")
    st.markdown("---")
    st.info("ℹ️ This will refresh technical indicators for the selected asset type")

    # -------------------------------------------------
    # 1️⃣ Asset type selection
    # -------------------------------------------------
    asset_names = list(ASSET_FRIENDLY_NAME.values())
    asset_choice = st.selectbox(
        "Select Asset Type to refresh indicators",
        options=asset_names
    )
    asset_type = list(ASSET_FRIENDLY_NAME.keys())[asset_names.index(asset_choice)]

    # -------------------------------------------------
    # 2️⃣ Lookback rows input
    # -------------------------------------------------
    lookback_rows = st.number_input(
        "Number of lookback rows for calculation",
        min_value=1,
        max_value=5000,
        value=250
    )

    # -------------------------------------------------
    # 3️⃣ Execute refresh
    # -------------------------------------------------
    if st.button("✅ Refresh Indicators for Selected Asset", use_container_width=True):
        with st.spinner(f"📈 Refreshing indicators for {asset_type}..."):
            try:
                # Call your existing function for just this asset
                refresh_indicators(asset_types=[asset_type], lookback_rows=lookback_rows)
                st.success(f"✅ Indicators refreshed successfully for {asset_type}!")
            except Exception as e:
                log(f"Refresh indicators error: {str(e)}", "error")
                st.error(f"❌ Error refreshing indicators: {str(e)}")
                with st.expander("📋 Error Details"):
                    st.code(traceback.format_exc())

    # -------------------------------------------------
    # Back button
    # -------------------------------------------------
    if st.button("⬅️ Back to Yahoo Operations", use_container_width=True):
        st.session_state.current_operation = None
        st.rerun()
# =====================================================================
# Scanner Operations
# =====================================================================
# =====================================================================
# Menu 1: Scanner Operations
# =====================================================================
def operation_scanner(scanner_type: str):
    st.markdown(f"# {SCANNER_OPERATIONS[scanner_type]['icon']} {SCANNER_OPERATIONS[scanner_type]['title']}")
    st.markdown("---")
    st.info(SCANNER_OPERATIONS[scanner_type]['description'])

    # Asset selection
    asset_names = list(ASSET_FRIENDLY_NAME.values())
    asset_choice = st.selectbox("Select Asset Type", asset_names)
    asset_type = list(ASSET_FRIENDLY_NAME.keys())[asset_names.index(asset_choice)]

    # ---------------- HM & Weekly Scanners ----------------
    if scanner_type in ("1", "3"):
        scan_date = st.date_input("Scan Date (optional)", value=None)
        run_button_text = "✅ Run HM Scanner" if scanner_type == "1" else "✅ Run Weekly Scanner"

        if st.button(run_button_text, use_container_width=True):
            with st.spinner("Running scanner..."):
                try:
                    if scanner_type == "1":
                        df = run_scanner_hilega_milega(
                            scan_date=str(scan_date) if scan_date else datetime.today().strftime("%Y-%m-%d"),
                            asset_type=asset_type
                        )
                    else:
                        df = run_scanner_weekly(
                            scan_date=str(scan_date) if scan_date else datetime.today().strftime("%Y-%m-%d"),
                            asset_type=asset_type
                        )

                    # Show result directly
                    if not df.empty:
                        st.dataframe(df, use_container_width=True)
                    else:
                        st.warning("⚠️ No results found.")
                except Exception as e:
                    st.error(f"❌ Error running scanner: {e}")

    # ---------------- Multi-Year Scanner ----------------
    elif scanner_type in ("2", "4"):
        start_year = st.number_input(
            "Start Year",
            min_value=2000,
            max_value=datetime.now().year,
            value=datetime.now().year
        )
        lookback_years = st.number_input(
            "Lookback Years",
            min_value=1,
            max_value=50,
            value=15
        )

        if st.button("✅ Run Multi-Year Scanner for Backtest", use_container_width=True):
            with st.spinner("Running Multi-Year Scanner..."):
                try:
                    if scanner_type == "2":
                        # Run Hilega–Milega Multi-Year Scanner
                        final_df = scanner_backtest_multi_years_hm(
                            start_year=int(start_year),
                            lookback_years=int(lookback_years),
                            asset_type=asset_type
                        )
                    elif scanner_type == "4":
                        # Run Weekly Multi-Year Scanner
                        final_df = scanner_backtest_multi_years_weekly(
                            start_year=int(start_year),
                            lookback_years=int(lookback_years),
                            asset_type=asset_type
                        )

                    if not final_df.empty:
                        st.dataframe(final_df, use_container_width=True)
                    else:
                        st.warning("⚠️ No signals found across selected years")

                except Exception as e:
                    st.error(f"❌ Error running multi-year scanner: {e}")

    # ---------------- Back Button ----------------
    if st.button("⬅️ Back to Scanner Operations", use_container_width=True):
        st.session_state.current_operation = None
        st.rerun()
        
# =====================================================================
# MAIN APP ENTRY POINT
# =====================================================================
def main():
    """Main app entry point - sidebar menu + content area."""
    # Clear logs at app startup
    clear_log()

    # Always show sidebar menu
    render_sidebar()

    # Main content area based on current menu and operation
    if st.session_state.current_menu is None:
        # Show main welcome screen
        show_main_screen()

    elif st.session_state.current_menu == "database_operations":
        if st.session_state.current_operation is None:
            show_database_operations_submenu()
        elif st.session_state.current_operation == "1":
            operation_create_database()
        elif st.session_state.current_operation == "2":
            operation_refresh_symbols()

    elif st.session_state.current_menu == "common_operations":
        if st.session_state.current_operation is None:
            show_common_operations_submenu()
        elif st.session_state.current_operation == "1":
            operation_show_latest_dates()
        elif st.session_state.current_operation == "2":
            operation_display_missing_price_data()
        elif st.session_state.current_operation == "3":
            operation_update_all_indicators()
        elif st.session_state.current_operation == "4":
            operation_update_52_weeks_stats()
        elif st.session_state.current_operation == "5":
            operation_update_nse_holidays()

    elif st.session_state.current_menu == "yahoo_operations":
        if st.session_state.current_operation is None:
            show_yahoo_operations_submenu()
        elif st.session_state.current_operation == "1":
            operation_update_yahoo_historical_price_data()
        elif st.session_state.current_operation == "2":
            operation_clone_data_to_yahoo_calc()
        elif st.session_state.current_operation == "3":
            operation_refresh_indicators_by_asset()
        
    elif st.session_state.current_menu == "scanner_operations":
        if st.session_state.current_operation is None:
            show_scanner_operations_submenu()
        elif st.session_state.current_operation in ("1", "2", "3"):
            operation_scanner(st.session_state.current_operation)

# =====================================================================
# Run The App 
# =====================================================================
if __name__ == "__main__":
    main()