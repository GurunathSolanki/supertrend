#!/usr/bin/env python3
"""Weekend Supertrend Scanner for Nifty 200 Momentum 30 & Portfolio Stocks.

Uses the free Kite MCP server (https://mcp.kite.trade/mcp) to fetch OHLCV data,
computes the Supertrend indicator (ATR period=9, multiplier=2), detects BUY/SELL
flips, and prints/saves results.  Designed to be run manually (no cron, no
auto-trading).

Prerequisites:
    pip install mcp pandas numpy

Usage:
    python3 scanner.py

On first run the script will give you a login URL.  Visit it in your browser to
authorise your Zerodha account — subsequent runs reuse the session.
"""

import asyncio
import csv
import json
import os
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from random import uniform

import httpx
import numpy as np
import pandas as pd
from httpx import AsyncHTTPTransport
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

MCP_SERVER_URL = "https://mcp.kite.trade/mcp"



class _ResilientTransport:
    """Wraps httpx.AsyncHTTPTransport to retry on 429 Too Many Requests."""

    def __init__(self, max_retries=5, base_delay=3):
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._inner = AsyncHTTPTransport()

    async def __aenter__(self):
        await self._inner.__aenter__()
        return self

    async def __aexit__(self, *args):
        await self._inner.__aexit__(*args)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        for attempt in range(self._max_retries):
            response = await self._inner.handle_async_request(request)
            if response.status_code != 429:
                return response
            wait = self._base_delay * (attempt + 1)
            print(f"  rate limited, waiting {wait}s (attempt {attempt+1}/{self._max_retries})")
            await asyncio.sleep(wait)
            request = httpx.Request(
                method=request.method,
                url=str(request.url),
                headers=request.headers,
                content=request.content,
            )
        return await self._inner.handle_async_request(request)

# NSE publishes live index constituent CSVs at these URLs.
# These are updated on every index rebalancing.
# NSE live market API index name strings.
# Used with: https://www.nseindia.com/api/equity-stockIndices?index=<name>
# Note: Nifty 200 Momentum 30 is not available via NSE's public API, so we use
# a static list that should be updated quarterly when the index rebalances.
INDEX_MAP = {
    "1": ("Nifty 50",              "NIFTY 50"),
    "2": ("Nifty Next 50",         "NIFTY NEXT 50"),
    "3": ("Nifty Midcap 150",      "NIFTY MIDCAP 150"),
    "4": ("Nifty Smallcap 100",    "NIFTY SMALLCAP 100"),
    "5": ("Nifty 200 Momentum 30", None),  # Static fallback below
}

# Static fallback for Nifty 200 Momentum 30 (as of Jul 2026).
# Update quarterly from https://www.niftyindices.com/reports/factsheets
NIFTY200_MOMENTUM_30_STATIC = [
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "CIPLA", "COALINDIA",
    "DRREDDY", "ETERNAL", "GRASIM", "HINDUNILVR", "ICICIBANK",
    "INDIGO", "JSWSTEEL", "KOTAKBANK", "LT", "MARUTI",
    "MAXHEALTH", "NESTLEIND", "SHRIRAMFIN", "SUNPHARMA", "TITAN",
    "TMPV", "TRENT", "ULTRACEMCO", "WIPRO", "ZYDUSLIFE"
]

SUPERTREND_PERIOD = 9
SUPERTREND_MULTIPLIER = 2
HISTORICAL_DAYS = 180
PORTFOLIO_CSV = "portfolio.csv"

RATE_LIMIT_DELAY = 1.5
CALL_RETRIES = 3

_token_cache = {}


# ── MCP helpers ───────────────────────────────────────────────────────────────

def _find_nse_token(instruments, symbol):
    """Return the NSE cash-market instrument_token for *symbol*, or None."""
    for inst in instruments:
        if (
            inst.get("exchange") == "NSE"
            and inst.get("tradingsymbol") == symbol
            and inst.get("instrument_type") in ("EQ", "")
        ):
            return inst["instrument_token"]
    # Fallback: accept any NSE match
    for inst in instruments:
        if inst.get("exchange") == "NSE" and inst.get("tradingsymbol") == symbol:
            return inst["instrument_token"]
    return None


async def call_tool(session, name, args=None):
    """Call an MCP tool and return the parsed JSON response.

    HTTP-level retry (429) is handled transparently by _ResilientTransport.
    """
    result = await session.call_tool(name, args or {})
    is_error = getattr(result, "isError", False)
    for content in result.content:
        text = getattr(content, "text", "")
        if not text:
            continue
        if is_error:
            raise RuntimeError(f"{name} failed: {text[:500]}")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    return None


async def ensure_authenticated(session):
    """Check if the session is authenticated; if not, prompt the user to log in."""
    try:
        result = await session.call_tool("get_profile", {})
        for content in result.content:
            text = getattr(content, "text", "")
            if text and "log in" not in text.lower():
                return  # already authenticated
    except Exception:
        pass

    # Not authenticated – start login flow
    print("\n=== Kite MCP Login Required ===")
    print("You need to authorise this script with your Zerodha account.")
    print()
    result = await session.call_tool("login", {})
    url = None
    for content in result.content:
        text = getattr(content, "text", "")
        if "http" in text:
            for word in text.split():
                if word.startswith("http"):
                    url = word.rstrip(")")
                    break
    if not url:
        raise RuntimeError("Could not extract login URL from server response.")

    print("1. Click the link below to authorise (or copy-paste if your terminal")
    print("   does not support hyperlinks):")
    # OSC 8 terminal hyperlink — works in GNOME Terminal, Kitty, iTerm2,
    # Windows Terminal, VS Code, etc.  Falls back to plain URL in others.
    print(f"\n   \033]8;;{url}\033\\Click here to log in\033]8;;\033\\\n")
    print(f"   ({url})")
    print("2. Log in with your Zerodha credentials and authorise.")
    print("3. After the 'Login Successful' page, press ENTER here.")
    input("   Press ENTER after logging in...")

    # Verify auth succeeded
    try:
        result = await session.call_tool("get_profile", {})
        for content in result.content:
            if getattr(content, "text", "") and "log in" not in getattr(content, "text", "").lower():
                print("   Login successful!\n")
                return
    except Exception:
        pass
    print("ERROR: Login did not succeed.  Please try again.")
    raise RuntimeError("Login did not succeed.")


# ── Data Access ───────────────────────────────────────────────────────────────

async def resolve_token(session, symbol):
    """Return the NSE instrument token for *symbol*, using a cached lookup."""
    if symbol in _token_cache:
        return _token_cache[symbol]

    await asyncio.sleep(uniform(0.1, RATE_LIMIT_DELAY))

    raw = await call_tool(session, "search_instruments", {"query": f"NSE:{symbol}"})
    if not raw or isinstance(raw, str):
        return None

    token = _find_nse_token(raw, symbol)
    if token:
        _token_cache[symbol] = token
    return token


async def fetch_index_constituents(index_name: str) -> list[str]:
    """Fetch live index constituents from NSE's equity-stockIndices JSON API.

    The API returns each constituent with a 'symbol' field.  The first entry
    is the index itself (e.g. "NIFTY 50") — we skip it and return only the
    stock symbols.

    NSE requires a session cookie obtained by first hitting the homepage.
    Returns an empty list on failure.
    """
    api_url = f"https://www.nseindia.com/api/equity-stockIndices?index={index_name}"
    print(f"  Fetching constituents for '{index_name}' from NSE...", flush=True)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nseindia.com/",
        "Accept-Language": "en-US,en;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
    }
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=30,
            headers=headers,
        ) as client:
            # Establish a session cookie by hitting the main page first.
            await client.get("https://www.nseindia.com/", timeout=20)
            resp = await client.get(api_url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        print(f"  ERROR fetching from NSE API: {exc}")
        return []

    try:
        # 'data' is a list of dicts; first entry is the index row itself.
        rows = data.get("data", [])
        symbols = [
            row["symbol"].strip()
            for row in rows
            if row.get("symbol", "").strip() and row.get("symbol") != index_name
        ]
    except Exception as exc:
        print(f"  ERROR parsing NSE API response: {exc}")
        return []

    symbols = sorted(set(symbols))
    print(f"  Found {len(symbols)} constituents")
    return symbols


async def get_ohlcv(session, symbol, from_date, to_date, interval="day"):
    """Fetch daily OHLCV data for *symbol* via Kite MCP.

    Returns a DataFrame with columns: date, open, high, low, close, volume.
    Returns *None* on failure.
    """
    token = await resolve_token(session, symbol)
    if token is None:
        print(f"  SKIP {symbol}: instrument not found")
        return None

    # Show progress: how many weeks of data we expect
    weeks = (to_date - from_date).days // 7
    print(f"  Fetching {symbol} (last ~{weeks} weeks)...", end=" ", flush=True)

    await asyncio.sleep(uniform(0.1, RATE_LIMIT_DELAY))

    try:
        raw_data = await call_tool(session, "get_historical_data", {
            "instrument_token": token,
            "from_date": from_date.strftime("%Y-%m-%d %H:%M:%S"),
            "to_date": to_date.strftime("%Y-%m-%d %H:%M:%S"),
            "interval": interval,
        })
    except Exception as exc:
        print(f"  ERROR {symbol}: {exc}")
        return None

    if isinstance(raw_data, str):
        if "log in" in raw_data.lower():
            print(f"  SKIP {symbol}: session expired, please log in again")
        else:
            print(f"  SKIP {symbol}: unexpected response: {raw_data[:200]}")
        return None

    if not raw_data:
        print("no data")
        return None
    print(f"{len(raw_data)} days")

    df = pd.DataFrame(raw_data)
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)

    expected = {"date", "open", "high", "low", "close", "volume"}
    missing = expected - set(df.columns)
    if missing:
        print(f"  SKIP {symbol}: missing columns {missing}")
        return None

    df = df[list(expected)]
    return df


def resample_to_weekly(df):
    """Aggregate daily OHLCV into weekly Friday-ending candles.

    Open  → first daily open of the week
    High  → max daily high of the week
    Low   → min daily low of the week
    Close → last daily close of the week
    Volume→ sum of daily volumes
    """
    if df is None or df.empty:
        return None
    df = df.set_index("date")
    weekly = df.resample("W-FRI").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    })
    weekly.dropna(inplace=True)
    return weekly.reset_index()


# ── Indicator Calculations ────────────────────────────────────────────────────

def compute_supertrend(df, period=SUPERTREND_PERIOD, multiplier=SUPERTREND_MULTIPLIER):
    """Compute Supertrend on a DataFrame with columns: high, low, close.

    Matches TradingView's built-in ``ta.supertrend()`` (hl2 source, ATR=9, mult=2):
      - Bands centred on hl2 = (high+low)/2
      - Flip checks use **current** bar's bands (TradingView native logic)
      - Ratchet uses TradingView conditions
      - ATR uses Wilder's RMA smoothing

    Columns added:
        atr                    – Average True Range (Wilder smoothing)
        supertrend             – Supertrend line value
        supertrend_direction   – 1 (bullish/uptrend) or -1 (bearish/downtrend)
    """
    df = df.copy()
    high = df["high"].values.astype(float)
    low = df["low"].values.astype(float)
    close = df["close"].values.astype(float)
    hl2 = (high + low) / 2.0

    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]

    tr = np.maximum(high - low,
                    np.maximum(np.abs(high - prev_close),
                               np.abs(low - prev_close)))

    atr = np.full(len(tr), np.nan)
    atr[period - 1] = np.mean(tr[:period])
    for i in range(period, len(tr)):
        atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

    upper_band = hl2 + multiplier * atr
    lower_band = hl2 - multiplier * atr

    direction = np.full(len(df), np.nan)
    supertrend = np.full(len(df), np.nan)

    for i in range(len(df)):
        if i == 0:
            direction[i] = -1
            supertrend[i] = np.nan
            continue

        # ── Ratchet (TradingView logic; nz(…) == 0 for NaN, like Pine Script) ─
        prev_lb = 0.0 if np.isnan(lower_band[i - 1]) else lower_band[i - 1]
        prev_ub = 0.0 if np.isnan(upper_band[i - 1]) else upper_band[i - 1]
        cur_lb = lower_band[i]
        cur_ub = upper_band[i]

        if not (np.isnan(cur_lb) or (cur_lb > prev_lb or close[i - 1] < prev_lb)):
            lower_band[i] = prev_lb
        if not (np.isnan(cur_ub) or (cur_ub < prev_ub or close[i - 1] > prev_ub)):
            upper_band[i] = prev_ub

        # ── Direction (TradingView logic, mapped to our convention) ────────
        if np.isnan(atr[i - 1]):
            direction[i] = -1
        elif not np.isnan(supertrend[i - 1]) and supertrend[i - 1] == upper_band[i - 1]:
            # Was in downtrend → flip up if close > current upper band
            direction[i] = 1 if close[i] > upper_band[i] else -1
        else:
            # Was in uptrend → flip down if close < current lower band
            direction[i] = -1 if close[i] < lower_band[i] else 1

        supertrend[i] = lower_band[i] if direction[i] == 1 else upper_band[i]

    df["atr"] = atr
    df["supertrend"] = supertrend
    df["supertrend_direction"] = direction
    return df


# ── Signal Detection ──────────────────────────────────────────────────────────

# ── Scanning ──────────────────────────────────────────────────────────────────

async def scan_index(session, symbols, from_date, to_date, lookback_weeks):
    """Scan index constituents for BUY signals."""
    results = []
    print("\n--- Nifty 200 Momentum 30 (BUY scan) ---")
    for sym in symbols:
        df = await get_ohlcv(session, sym, from_date, to_date)
        if df is None:
            continue
        weekly = resample_to_weekly(df)
        if weekly is None or len(weekly) < SUPERTREND_PERIOD:
            continue
        weekly = compute_supertrend(weekly)

        current = weekly["supertrend_direction"].iloc[-1]
        if current != 1:
            continue

        flip_weeks = None
        for i in range(len(weekly) - 1, 0, -1):
            if weekly["supertrend_direction"].iloc[i] != weekly["supertrend_direction"].iloc[i - 1]:
                flip_weeks = len(weekly) - 1 - i
                break

        recent = flip_weeks is not None and flip_weeks < lookback_weeks
        results.append((sym, flip_weeks, recent))
        label = "NEW" if recent else "   "
        age = f"{flip_weeks}w ago" if flip_weeks is not None else "always"
        print(f"  {label} BUY  {sym}  (since {age})")
    return results


async def scan_portfolio(session, symbols, from_date, to_date, lookback_weeks):
    """Scan portfolio stocks for SELL signals."""
    results = []
    print("\n--- Portfolio (SELL scan) ---")
    for sym in symbols:
        df = await get_ohlcv(session, sym, from_date, to_date)
        if df is None:
            continue
        weekly = resample_to_weekly(df)
        if weekly is None or len(weekly) < SUPERTREND_PERIOD:
            continue
        weekly = compute_supertrend(weekly)

        current = weekly["supertrend_direction"].iloc[-1]
        if current != -1:
            continue

        flip_weeks = None
        for i in range(len(weekly) - 1, 0, -1):
            if weekly["supertrend_direction"].iloc[i] != weekly["supertrend_direction"].iloc[i - 1]:
                flip_weeks = len(weekly) - 1 - i
                break

        recent = flip_weeks is not None and flip_weeks < lookback_weeks
        results.append((sym, flip_weeks, recent))
        label = "NEW" if recent else "   "
        age = f"{flip_weeks}w ago" if flip_weeks is not None else "always"
        print(f"  {label} SELL {sym}  (since {age})")
    return results


# ── Reporting ─────────────────────────────────────────────────────────────────

def report_results(buy_signals, sell_signals, lookback_weeks):
    """Print formatted results to console and write ``scan_results.txt``."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    width = 62

    def divider(char="─"):
        return char * width

    def section_header(title):
        return f"  {title}"

    def fmt_row(sym, age_str, badge):
        """Return a fixed-width row: badge | symbol (padded) | age right-aligned."""
        sym_col = f"{badge} {sym}".ljust(28)
        return f"  {sym_col}  {age_str}"

    def build_section(signals, signal_type):
        """Build sorted rows: NEW signals first, then the rest alphabetically."""
        if not signals:
            return ["  (none)"]
        new_rows, old_rows = [], []
        for sym, flip_weeks, recent in sorted(signals, key=lambda x: x[0]):
            if flip_weeks is not None:
                age_str = f"{flip_weeks:>2}w ago"
            else:
                age_str = "always" if signal_type == "BUY" else "always"
            badge = "★ NEW" if recent else "     "
            row = fmt_row(sym, age_str, badge)
            (new_rows if recent else old_rows).append(row)
        return new_rows + old_rows

    # ── console output (with ANSI colour) ────────────────────────────────────
    BOLD  = "\033[1m"
    GREEN = "\033[32m"
    RED   = "\033[31m"
    CYAN  = "\033[36m"
    DIM   = "\033[2m"
    RESET = "\033[0m"

    def coloured_row(row, recent, signal_type):
        if recent:
            colour = GREEN if signal_type == "BUY" else RED
            return f"{colour}{BOLD}{row}{RESET}"
        return f"{DIM}{row}{RESET}"

    console_lines = [
        "",
        f"{BOLD}{CYAN}{'Weekend Scan Results':^{width}}{RESET}",
        f"{CYAN}{divider('═')}{RESET}",
        f"  Scan time : {now}",
        f"  NEW signal: within last {lookback_weeks} week(s)",
        f"{CYAN}{divider()}{RESET}",
        "",
        f"{BOLD}  {'BUY Candidates':}{RESET}",
        f"  {'─'*28}  {'────────'}",
    ]
    for sym, flip_weeks, recent in sorted(buy_signals, key=lambda x: (not x[2], x[0])):
        age_str = f"{flip_weeks:>2}w ago" if flip_weeks is not None else "always "
        badge = "★ NEW" if recent else "     "
        row = fmt_row(sym, age_str, badge)
        console_lines.append(coloured_row(row, recent, "BUY"))
    if not buy_signals:
        console_lines.append("  (none)")

    console_lines += [
        "",
        f"{CYAN}{divider()}{RESET}",
        f"{BOLD}  {'SELL Alerts (Portfolio)':}{RESET}",
        f"  {'─'*28}  {'────────'}",
    ]
    for sym, flip_weeks, recent in sorted(sell_signals, key=lambda x: (not x[2], x[0])):
        age_str = f"{flip_weeks:>2}w ago" if flip_weeks is not None else "always "
        badge = "★ NEW" if recent else "     "
        row = fmt_row(sym, age_str, badge)
        console_lines.append(coloured_row(row, recent, "SELL"))
    if not sell_signals:
        console_lines.append("  (none)")

    console_lines += ["", f"{CYAN}{divider('═')}{RESET}", ""]
    print("\n".join(console_lines))

    # ── plain-text file (no ANSI) ─────────────────────────────────────────────
    file_lines = [
        f"Weekend Scan Results — {now}",
        divider("="),
        f"  Scan time : {now}",
        f"  NEW signal: within last {lookback_weeks} week(s)",
        divider(),
        "",
        "  BUY Candidates",
        f"  {'─'*28}  {'────────'}",
    ]
    file_lines += build_section(buy_signals, "BUY")
    file_lines += [
        "",
        divider(),
        "  SELL Alerts (Portfolio)",
        f"  {'─'*28}  {'────────'}",
    ]
    file_lines += build_section(sell_signals, "SELL")
    file_lines += ["", divider("=")]

    output = "\n".join(file_lines)
    with open("scan_results.txt", "w") as f:
        f.write(output + "\n")
    print(f"  Results saved to {BOLD}scan_results.txt{RESET}\n")



# ── Bootstrap ─────────────────────────────────────────────────────────────────

def load_portfolio_symbols():
    """Read portfolio stock symbols from *portfolio.csv*."""
    try:
        with open(PORTFOLIO_CSV) as f:
            reader = csv.DictReader(f)
            return [row["Symbol"].strip() for row in reader if row.get("Symbol")]
    except FileNotFoundError:
        print(f"[init] {PORTFOLIO_CSV} not found -- creating sample")
        with open(PORTFOLIO_CSV, "w") as f:
            f.write("Symbol,Broker\nTCS,Sharekhan\n")
        return ["TCS"]


async def main():
    # ── Step 1: user input & NSE fetch (no MCP session open yet) ─────────────
    portfolio_symbols = load_portfolio_symbols()
    print(f"Portfolio stocks: {portfolio_symbols}")

    print("\nAvailable indices for scanning:")
    print("1. Nifty 50")
    print("2. Nifty Next 50")
    print("3. Nifty Midcap 150")
    print("4. Nifty Smallcap 100")
    print("5. Nifty 200 Momentum 30")
    print("6. Single Stock")

    choice = input("Enter your choice (1-6): ").strip()

    if choice in INDEX_MAP:
        index_label, nse_index_name = INDEX_MAP[choice]
        if nse_index_name is None:
            # Static fallback for indices not exposed via NSE API
            print(f"\nUsing static list for {index_label} (last updated Jul 2026).")
            index_symbols = NIFTY200_MOMENTUM_30_STATIC
            print(f"Will scan {index_label} ({len(index_symbols)} stocks).")
        else:
            print(f"\nFetching {index_label} constituents from NSE...")
            index_symbols = await fetch_index_constituents(nse_index_name)
            if not index_symbols:
                print(f"ERROR: Could not fetch constituents for {index_label}. Exiting.")
                raise SystemExit(1)
            print(f"Will scan {index_label} ({len(index_symbols)} stocks).")
    elif choice == "6":
        stock_symbol = input("Enter the stock symbol (e.g., TCS): ").strip().upper()
        index_symbols = [stock_symbol]
        index_label = f"Single stock: {stock_symbol}"
    else:
        print("Invalid choice. Defaulting to Nifty 200 Momentum 30.")
        index_label, nse_index_name = INDEX_MAP["5"]
        if nse_index_name is None:
            print(f"Using static list for {index_label} (last updated Jul 2026).")
            index_symbols = NIFTY200_MOMENTUM_30_STATIC
        else:
            index_symbols = await fetch_index_constituents(nse_index_name)
            if not index_symbols:
                print("ERROR: Could not fetch constituents. Exiting.")
                raise SystemExit(1)

    if len(sys.argv) > 1:
        lookback_weeks = int(sys.argv[1])
    else:
        lookback_weeks = int(input(
            "How many weeks back to check for a direction change? "
        ).strip())

    # ── Step 2: connect to Kite MCP and scan ─────────────────────────────────
    print(f"\nConnecting to Kite MCP ({MCP_SERVER_URL}) ...")
    transport = _ResilientTransport(max_retries=6, base_delay=5)
    async with httpx.AsyncClient(transport=transport) as http_client:
        async with streamable_http_client(MCP_SERVER_URL, http_client=http_client) as (read, write, get_sid):
            try:
                async with ClientSession(read, write) as session:
                    init = await session.initialize()
                    print(f"Connected -- {init.serverInfo.name} v{init.serverInfo.version}")

                    await ensure_authenticated(session)

                    to_date = datetime.now()
                    from_date = to_date - timedelta(days=HISTORICAL_DAYS)

                    buy = await scan_index(session, index_symbols, from_date, to_date, lookback_weeks)
                    sell = await scan_portfolio(session, portfolio_symbols, from_date, to_date, lookback_weeks)
                    report_results(buy, sell, lookback_weeks)
            except (Exception, KeyboardInterrupt) as exc:
                traceback.print_exc()
                raise SystemExit(1) from exc


if __name__ == "__main__":
    asyncio.run(main())
