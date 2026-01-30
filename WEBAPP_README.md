# Stock Market Analyzer - Web App

## 🎉 Features

✅ **Dashboard UI** - Replace terminal menus  
✅ **Sidebar Navigation** - Easy menu access  
✅ **Database Management** - Check status, view stats  
✅ **Data Import** - Yahoo Finance, NSE bhavcopy  
✅ **Scanners** - Run HM, Play, Weekly scanners  
✅ **Settings** - Configure logging, database  
✅ **Real-time Feedback** - Progress indicators, success/error messages  

---

## 🚀 Quick Start

### 1. Install Streamlit
```bash
pip install -r requirements_webapp.txt
```

Or just:
```bash
pip install streamlit
```

### 2. Run the App
```bash
streamlit run app.py
```

This will:
- Start a local web server (typically `http://localhost:8501`)
- Open your default browser automatically
- Show the Stock Market Analyzer interface

### 3. Use the Web Interface
- **Sidebar**: Click menu items to navigate
- **Pages**: Each page has different operations
- **Buttons**: Click to run operations
- **Forms**: Fill in parameters as needed

---

## 📋 Available Pages

### 🏠 Home
- Quick overview of system status
- Fast action buttons
- Available options guide

### 📈 Database
- **Status Tab**: Connection, tables, size
- **Operations Tab**: Check connection, view stats
- **Cleanup Tab**: Clear logs (with confirmation)

### 📥 Import Data
- Select asset type (India Equity, USA Equity, etc.)
- Import CSV files to database
- Export data to CSV

### 🔄 Yahoo Data
- Download historical price data
- Choose between full or incremental download
- Select asset type

### 🇮🇳 NSE Data
- Download NSE bhavcopy files
- Import NSE historical data
- Manage bhavcopy operations

### 🔍 Scanners
- **Run Three Scanner Types**:
  - Hilega-Milega (HM)
  - Play Scanner
  - Weekly Scanner
- Select date range
- Choose asset type
- View signal count

### 📊 Dashboard
- System overview
- Analytics (expandable)
- Performance metrics

### ⚙️ Settings
- **Logging**: Configure log level
- **Database**: View DB configuration
- **Advanced**: Advanced options

---

## 🔌 API Integration

The web app uses your existing backend:
- ✅ All scanner logic unchanged
- ✅ All database operations work
- ✅ All data operations integrated
- ✅ Just a new UI on top

**No backend changes needed!**

---

## 📦 What's Included

**File**: `app.py` (600+ lines)

**Main Components**:
1. Sidebar navigation
2. Home page with quick actions
3. Database management interface
4. Data import/export UI
5. Scanner execution interface
6. Dashboard with metrics
7. Settings configuration
8. Error handling with user feedback

---

## 🎨 UI Features

✅ **Responsive Design** - Works on desktop and tablet  
✅ **Dark Mode Support** - Uses system theme  
✅ **Progress Indicators** - Shows loading state  
✅ **Success/Error Messages** - Clear feedback  
✅ **Tabbed Interface** - Organized sections  
✅ **Forms** - Input parameters easily  
✅ **Metrics** - Display KPIs  

---

## 🔧 Customization

### Change Theme
In Streamlit, go to **Settings** > **Theme** > Choose theme

### Add More Features
Edit `app.py` and add new functions:
```python
def page_custom():
    st.markdown("# Custom Page")
    # Add your content here

# Then add to sidebar and routing
```

### Modify Colors/Styling
Edit the CSS in `page_home()` function

---

## 🌐 Deployment Options

### Option 1: Local Machine (Development)
```bash
streamlit run app.py
```
Access at: `http://localhost:8501`

### Option 2: Streamlit Cloud (Free Hosting)
1. Push to GitHub
2. Go to https://streamlit.io/cloud
3. Deploy from repo
4. Share public URL

### Option 3: Docker Container
```dockerfile
FROM python:3.10
WORKDIR /app
COPY . .
RUN pip install -r requirements_webapp.txt
CMD ["streamlit", "run", "app.py"]
```

### Option 4: Server (AWS, DigitalOcean, etc.)
```bash
# Install on server
pip install -r requirements_webapp.txt

# Run with systemd or supervisor
streamlit run app.py --server.port 8501
```

---

## 📊 Current vs New

**Before (Terminal)**:
```
MAIN MENU
1. Database
2. Import Data
3. Yahoo Data
4. NSE Data
5. Test Data
6. Scanners
0. Exit

Enter an option and press ENTER: _
```

**After (Web App)**:
- Clickable buttons
- Visual cards with metrics
- Tabbed interfaces
- Progress spinners
- Error/success messages
- Date pickers
- Dropdown selectors
- No typing required!

---

## 🐛 Troubleshooting

### "streamlit: command not found"
```bash
pip install streamlit
# or
python -m pip install streamlit
```

### Port already in use
```bash
streamlit run app.py --server.port 8502
# Try different port
```

### Database connection error
- Make sure PostgreSQL is running
- Check `config/db_table.py` for connection settings
- Run: `python -c "from database.connection import get_db_connection; get_db_connection()"`

### Imports not found
```bash
# Make sure you're in the project directory
cd /Users/sutirtha/Desktop/Python_Projects/stock_market
streamlit run app.py
```

---

## 📈 Future Enhancements

- [ ] Add charts (Plotly integration)
- [ ] Real-time data streaming
- [ ] Export reports to PDF
- [ ] User authentication
- [ ] Data visualization dashboard
- [ ] API endpoint for programmatic access
- [ ] Mobile-friendly responsive design
- [ ] Dark mode toggle
- [ ] Data comparison tools
- [ ] Historical analysis

---

## 🎓 Comparison: Terminal vs Web App

| Feature | Terminal | Web App |
|---------|----------|---------|
| User-Friendly | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| Visual Feedback | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| Typing Required | ❌ | ✅ (clicks only) |
| Mobile Support | ❌ | ✅ (responsive) |
| Development Speed | 📅 Weeks | 📅 Hours |
| Customization | Medium | Easy |
| Deployment | Complex | Simple |
| User Base | Tech | Everyone |

---

## 💡 Tips

1. **Keep App Running**: Terminal will show logs in real-time
2. **Reload Code**: Edit `app.py` and refresh browser (auto-reloads)
3. **Session State**: Use `st.session_state` for persistence
4. **Error Handling**: Already included - check browser console
5. **Performance**: Streamlit caches results for faster reruns

---

**Ready to go!** 🚀

Run `streamlit run app.py` and enjoy your web interface!

