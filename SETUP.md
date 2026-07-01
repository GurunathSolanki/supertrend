# Setup Guide: Kite MCP (Free)

The scanner uses the **free** [Kite MCP](https://zerodha.com/z-connect/featured/connect-your-zerodha-account-to-ai-assistants-with-kite-mcp)
server (`https://mcp.kite.trade/mcp`) instead of the paid Kite Connect API.
No API key or access token is needed. You only need a Zerodha trading account.

---

## One-Time Setup

### 1. Install Dependencies

The project uses a Python virtual environment. From the project directory:

```bash
python3 -m venv .venv
.venv/bin/pip install mcp pandas numpy
```

### 2. Run the Scanner

```bash
.venv/bin/python3 scanner.py
```

The first time you run it, the script will:

1. Connect to the Kite MCP server
2. Detect that you're not logged in
3. Print a **login URL**
4. You visit that URL in your browser, log in with your Zerodha credentials, and authorise access
5. Press ENTER in the terminal after the browser shows "Login Successful"

After that, the scanner fetches historical data, computes Supertrend signals, and prints results.

### 3. Session Persistence

The MCP login is tied to a browser session. In practice, the MCP server remembers the
authorisation for a period of time, so you won't need to log in every time. If you get
a "Please log in first" message, just re-run the script and follow the prompt.

---

## Portfolio Stocks

Edit `portfolio.csv` to add/remove stocks:

```csv
Symbol,Broker
TCS,Sharekhan
RELIANCE,Zerodha
INFY,Zerodha
```

Add as many rows as you like. The scanner checks these for SELL signals.

---

## Scanning Other Indices

The Nifty 200 Momentum 30 list is hardcoded in `scanner.py` as
`NIFTY200_MOMENTUM_30`. To scan a different index, edit that list, for example:

```python
NIFTY50 = ["RELIANCE", "TCS", "INFY", "HDFCBANK", ...]
```

Then call `scan_index(session, NIFTY50, ...)` from `main()`.

---

## How It Works

```
scanner.py
  │
  ├── connect to mcp.kite.trade/mcp  (free MCP server)
  ├── login (one-time browser auth)
  ├── for each stock:
  │     ├── search_instruments → get NSE token
  │     └── get_historical_data → daily OHLCV (180 days)
  ├── resample daily → weekly candles
  ├── compute Supertrend (ATR 9, mult 2)
  ├── check last 2 weeks for direction flips
  └── print + save scan_results.txt
```
