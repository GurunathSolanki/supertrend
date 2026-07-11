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
"ABB",
"ADANIENSOL",
"ADANIGREEN",
"ADANIPOWER",
"ABCAPITAL",
"BSE",
"BHARATFORG",
"BHEL",
"CGPOWER",
"CUMMINSIND",
"FEDERALBNK",
"GVT&D",
"GLENMARK",
"HINDALCO",
"POWERINDIA",
"KEI",
"LTF",
"LAURUSLABS",
"MCX",
"NTPC",
"NATIONALUM",
"POLYCAB",
"MOTHERSON",
"SHRIRAMFIN",
"SOLARINDS",
"SAIL",
"TATASTEEL",
"TORNTPHARM",
"VEDL",
"IDEA"
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
    """Fetch live index constituents from NSE's official CSV lists on archives.nseindia.com."""
    csv_urls = {
        "NIFTY 50": "https://archives.nseindia.com/content/indices/ind_nifty50list.csv",
        "NIFTY NEXT 50": "https://archives.nseindia.com/content/indices/ind_niftynext50list.csv",
        "NIFTY MIDCAP 150": "https://archives.nseindia.com/content/indices/ind_niftymidcap150list.csv",
        "NIFTY SMALLCAP 100": "https://archives.nseindia.com/content/indices/ind_niftysmallcap100list.csv",
    }
    
    url = csv_urls.get(index_name)
    if not url:
        print(f"  ERROR: No CSV URL mapped for index '{index_name}'")
        return []

    print(f"  Fetching constituents for '{index_name}' from NSE...", flush=True)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Accept": "text/csv,text/plain,application/csv",
    }
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=30,
            headers=headers,
        ) as client:
            resp = await client.get(url, timeout=30)
        resp.raise_for_status()
        
        import csv
        reader = csv.DictReader(resp.text.splitlines())
        symbol_col = None
        if reader.fieldnames:
            for field in reader.fieldnames:
                if field.strip().lower() == "symbol":
                    symbol_col = field
                    break
        
        if not symbol_col:
            print("  ERROR: Could not find 'Symbol' column in CSV")
            return []
            
        symbols = []
        for row in reader:
            sym = row.get(symbol_col)
            if sym:
                symbols.append(sym.strip())
                
    except Exception as exc:
        print(f"  ERROR fetching/parsing index CSV from NSE: {exc}")
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

        # Calculate Volume Ratio (current week volume vs 10-week average volume prior to current week)
        volume_ratio = 1.0
        if len(weekly) >= 2:
            prev_volumes = weekly["volume"].iloc[:-1]
            avg_vol = prev_volumes.tail(10).mean()
            if avg_vol > 0:
                volume_ratio = weekly["volume"].iloc[-1] / avg_vol

        # Calculate Distance to Supertrend support in %
        current_close = weekly["close"].iloc[-1]
        st_val = weekly["supertrend"].iloc[-1]
        distance_to_st = 0.0
        if st_val > 0:
            distance_to_st = ((current_close - st_val) / st_val) * 100

        # Calculate Recommendation Score
        base_score = 50.0
        new_bonus = 30.0 if recent else 0.0
        vol_bonus = min(volume_ratio * 5.0, 25.0)
        dist_penalty = min(max(distance_to_st, 0.0) * 1.5, 25.0)
        score = base_score + new_bonus + vol_bonus - dist_penalty

        results.append((sym, flip_weeks, recent, volume_ratio, distance_to_st, score))
        label = "NEW" if recent else "   "
        age = f"{flip_weeks}w ago" if flip_weeks is not None else "always"
        print(f"  {label} BUY  {sym}  (since {age}) | Vol Ratio: {volume_ratio:.2f}x | Dist: {distance_to_st:.1f}% | Score: {score:.1f}")
    return results


async def scan_portfolio(session, symbols, from_date, to_date, lookback_weeks):
    """Scan portfolio stocks for SELL signals."""
    results = []
    print("\n--- Portfolio (SELL scan) ---")
    
    # Check if symbols is a dictionary of symbol -> broker
    if isinstance(symbols, dict):
        items = list(symbols.items())
    else:
        items = [(sym, "") for sym in symbols]

    for sym, broker in items:
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
        results.append((sym, flip_weeks, recent, broker))
        label = "NEW" if recent else "   "
        age = f"{flip_weeks}w ago" if flip_weeks is not None else "always"
        broker_str = f" ({broker})" if broker else ""
        print(f"  {label} SELL {sym}{broker_str}  (since {age})")
    return results


# ── Reporting ─────────────────────────────────────────────────────────────────

def report_results_html(buy_signals, sell_signals, lookback_weeks, index_label, filename_prefix):
    """Write results to a beautifully designed, responsive HTML report file."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # Sort signals: buys by score descending (extended metrics), else NEW-first then alpha
    if buy_signals and len(buy_signals[0]) >= 6:
        sorted_buys = sorted(buy_signals, key=lambda x: -x[5])
    else:
        sorted_buys = sorted(buy_signals, key=lambda x: (not x[2], x[0]))
    sorted_sells = sorted(sell_signals, key=lambda x: (not x[2], x[0]))

    # Generate lists of items
    buy_rows_html = ""
    if not sorted_buys:
        buy_rows_html = '<div class="no-signals">No BUY signals found.</div>'
    else:
        for item in sorted_buys:
            sym, flip_weeks, recent = item[0], item[1], item[2]
            vol_ratio = item[3] if len(item) > 3 else 1.0
            dist = item[4] if len(item) > 4 else 0.0
            score = item[5] if len(item) > 5 else 0.0

            badge_class = "badge badge-new" if recent else "badge badge-old"
            badge_text = "★ NEW" if recent else "ACTIVE"
            age_str = f"{flip_weeks}w ago" if flip_weeks is not None else "always"
            
            metrics_html = ""
            if len(item) >= 6:
                metrics_html = f"""
                <div style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 0.25rem;">
                    Score: <strong style="color: var(--primary);">{score:.1f}</strong> | 
                    Vol: <strong>{vol_ratio:.1f}x</strong> | 
                    Dist: <strong>{dist:.1f}%</strong>
                </div>
                """

            buy_rows_html += f"""
            <div class="signal-card {'card-new' if recent else ''}">
                <div class="symbol-section">
                    <span class="{badge_class}">{badge_text}</span>
                    <div style="display: flex; flex-direction: column;">
                        <div style="display: flex; align-items: center; gap: 0.5rem;">
                            <span class="symbol-name">{sym}</span>
                            <button class="copy-btn" onclick="copySymbol(this, '{sym}')" title="Copy symbol">⎘</button>
                        </div>
                        {metrics_html}
                    </div>
                </div>
                <div class="age-info">
                    <span class="age-label">Trend flip:</span>
                    <span class="age-value">{age_str}</span>
                </div>
            </div>
            """

    sell_rows_html = ""
    if not sorted_sells:
        sell_rows_html = '<div class="no-signals">No SELL signals found.</div>'
    else:
        for item in sorted_sells:
            sym, flip_weeks, recent = item[0], item[1], item[2]
            broker = item[3] if len(item) > 3 else ""
            badge_class = "badge badge-sell-new" if recent else "badge badge-sell-old"
            badge_text = "★ NEW" if recent else "ACTIVE"
            age_str = f"{flip_weeks}w ago" if flip_weeks is not None else "always"
            
            broker_html = f'<div class="broker-info">Broker: <span>{broker}</span></div>' if broker else ''
            
            sell_rows_html += f"""
            <div class="signal-card {'card-sell-new' if recent else ''}">
                <div class="symbol-section">
                    <span class="{badge_class}">{badge_text}</span>
                    <div style="display: flex; flex-direction: column;">
                        <div style="display: flex; align-items: center; gap: 0.5rem;">
                            <span class="symbol-name">{sym}</span>
                            <button class="copy-btn" onclick="copySymbol(this, '{sym}')" title="Copy symbol">⎘</button>
                        </div>
                        {broker_html}
                    </div>
                </div>
                <div class="age-info">
                    <span class="age-label">Trend flip:</span>
                    <span class="age-value">{age_str}</span>
                </div>
            </div>
            """

    # Generate top recommended setups
    has_extended_metrics = len(buy_signals) > 0 and len(buy_signals[0]) >= 6
    recommendations_html = ""
    if has_extended_metrics:
        # Sort by score descending
        ranked_buys = sorted(buy_signals, key=lambda x: x[5], reverse=True)
        top_recommendations = [b for b in ranked_buys if b[2]] # Preferred NEW breakouts
        if not top_recommendations:
            top_recommendations = ranked_buys[:3]
        else:
            top_recommendations = top_recommendations[:3]

        if top_recommendations:
            recommendations_html = """
            <section class="section-panel recommendation-panel" style="grid-column: 1 / -1; margin-bottom: 2rem; background: linear-gradient(135deg, rgba(88, 166, 255, 0.15) 0%, rgba(63, 185, 80, 0.1) 100%); border-color: rgba(88, 166, 255, 0.3);">
                <h2 class="section-title" style="color: #58a6ff;">
                    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/></svg>
                    Top Recommended Setups (Ranked)
                </h2>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 1.5rem; margin-top: 1rem;">
            """
            for idx, item in enumerate(top_recommendations):
                sym, flip_weeks, recent, vol_ratio, dist, score = item
                rank = idx + 1
                age_str = f"{flip_weeks}w ago" if flip_weeks is not None else "always"
                badge = "★ NEW BREAKOUT" if recent else "ACTIVE UPTREND"
                badge_style = "background: var(--success); color: #04270d;" if recent else "background: rgba(88, 166, 255, 0.15); color: var(--primary);"
                
                recommendations_html += f"""
                    <div style="background: rgba(22, 27, 34, 0.6); border: 1px solid var(--border-color); border-radius: 12px; padding: 1.25rem; position: relative; overflow: hidden; display: flex; flex-direction: column; justify-content: space-between;">
                        <div style="position: absolute; top: 0; right: 0; background: var(--primary); color: #0d1117; font-weight: 800; padding: 0.25rem 0.75rem; border-bottom-left-radius: 12px; font-size: 0.85rem;">Rank #{rank}</div>
                        <div>
                            <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.75rem;">
                                <span style="font-size: 0.75rem; font-weight: 700; padding: 0.15rem 0.5rem; border-radius: 4px; {badge_style}">{badge}</span>
                                <span style="font-weight: 800; font-size: 1.25rem; letter-spacing: 0.5px;">{sym}</span>
                                <button class="copy-btn" onclick="copySymbol(this, '{sym}')" title="Copy symbol">⎘</button>
                            </div>
                            <div style="display: flex; flex-direction: column; gap: 0.4rem; font-size: 0.85rem; color: var(--text-secondary); margin-bottom: 1rem;">
                                <div style="display: flex; justify-content: space-between;"><span>Setup Score:</span><strong style="color: var(--text-primary);">{score:.1f}/100</strong></div>
                                <div style="display: flex; justify-content: space-between;"><span>Volume Ratio:</span><strong style="color: var(--text-primary);">{vol_ratio:.2f}x</strong></div>
                                <div style="display: flex; justify-content: space-between;"><span>Distance to ST Support:</span><strong style="color: var(--text-primary);">{dist:.1f}%</strong></div>
                                <div style="display: flex; justify-content: space-between;"><span>Trend Flip:</span><strong style="color: var(--text-primary);">{age_str}</strong></div>
                            </div>
                        </div>
                        <div style="background: rgba(88, 166, 255, 0.1); border-radius: 6px; padding: 0.5rem; text-align: center; font-size: 0.75rem; color: var(--primary); font-weight: 600;">
                            {"High volume breakout setup with tight stop-loss!" if (vol_ratio > 1.5 and dist < 8) else "Stable uptrend continuation setup."}
                        </div>
                    </div>
                """
            recommendations_html += """
                </div>
            </section>
            """

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Weekend Supertrend Scan - {index_label}</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&family=Plus+Jakarta+Sans:wght@300;400;500;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg-color: #0d1117;
            --card-bg: rgba(22, 27, 34, 0.7);
            --border-color: rgba(48, 54, 61, 0.6);
            --text-primary: #f0f6fc;
            --text-secondary: #8b949e;
            --primary: #58a6ff;
            --success: #3fb950;
            --success-glow: rgba(63, 185, 80, 0.15);
            --danger: #f85149;
            --danger-glow: rgba(248, 81, 73, 0.15);
            --glass-border: rgba(255, 255, 255, 0.08);
        }}

        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}

        body {{
            background: radial-gradient(circle at 50% 0%, #161b22 0%, #0d1117 100%);
            color: var(--text-primary);
            font-family: 'Plus Jakarta Sans', sans-serif;
            min-height: 100vh;
            padding: 2rem 1.5rem;
            line-height: 1.5;
        }}

        .container {{
            max-width: 1100px;
            margin: 0 auto;
        }}

        header {{
            text-align: center;
            margin-bottom: 3rem;
            animation: fadeIn 0.8s ease-out;
        }}

        h1 {{
            font-family: 'Outfit', sans-serif;
            font-size: 2.5rem;
            font-weight: 800;
            background: linear-gradient(135deg, #ffffff 0%, #a5b4fc 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 0.5rem;
            letter-spacing: -0.5px;
        }}

        .meta-info {{
            display: flex;
            justify-content: center;
            gap: 1.5rem;
            font-size: 0.9rem;
            color: var(--text-secondary);
            margin-top: 1rem;
            flex-wrap: wrap;
        }}

        .meta-item {{
            background: var(--card-bg);
            padding: 0.4rem 1rem;
            border-radius: 50px;
            border: 1px solid var(--border-color);
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}

        .meta-item span {{
            color: var(--primary);
            font-weight: 600;
        }}

        .grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 2rem;
            align-items: start;
        }}

        @media (max-width: 768px) {{
            .grid {{
                grid-template-columns: 1fr;
            }}
        }}

        .section-panel {{
            background: var(--card-bg);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            border-radius: 20px;
            border: 1px solid var(--glass-border);
            padding: 1.75rem;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }}

        .section-panel:hover {{
            transform: translateY(-2px);
            box-shadow: 0 15px 35px rgba(0, 0, 0, 0.4);
        }}

        .section-title {{
            font-family: 'Outfit', sans-serif;
            font-size: 1.5rem;
            font-weight: 700;
            margin-bottom: 1.5rem;
            display: flex;
            align-items: center;
            gap: 0.75rem;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 0.75rem;
        }}

        .buy-title {{
            color: #4ade80;
        }}

        .sell-title {{
            color: #f87171;
        }}

        .signals-list {{
            display: flex;
            flex-direction: column;
            gap: 0.75rem;
        }}

        .signal-card {{
            background: rgba(30, 41, 59, 0.4);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 1rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            transition: all 0.2s ease;
        }}

        .signal-card:hover {{
            background: rgba(30, 41, 59, 0.7);
            border-color: var(--primary);
            transform: translateX(3px);
        }}

        .card-new {{
            border-color: rgba(63, 185, 80, 0.4);
            background: var(--success-glow);
        }}

        .card-new:hover {{
            border-color: var(--success);
            background: rgba(63, 185, 80, 0.25);
        }}

        .card-sell-new {{
            border-color: rgba(248, 81, 73, 0.4);
            background: var(--danger-glow);
        }}

        .card-sell-new:hover {{
            border-color: var(--danger);
            background: rgba(248, 81, 73, 0.25);
        }}

        .symbol-section {{
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }}

        .symbol-name {{
            font-weight: 700;
            font-size: 1.1rem;
            letter-spacing: 0.5px;
        }}

        .badge {{
            font-size: 0.75rem;
            font-weight: 700;
            padding: 0.2rem 0.6rem;
            border-radius: 4px;
            letter-spacing: 0.5px;
        }}

        .badge-new {{
            background: var(--success);
            color: #04270d;
        }}

        .badge-old {{
            background: rgba(88, 166, 255, 0.15);
            color: var(--primary);
            border: 1px solid rgba(88, 166, 255, 0.3);
        }}

        .badge-sell-new {{
            background: var(--danger);
            color: #310705;
        }}

        .badge-sell-old {{
            background: rgba(139, 148, 158, 0.15);
            color: var(--text-secondary);
            border: 1px solid var(--border-color);
        }}

        .broker-info {{
            font-size: 0.75rem;
            color: var(--text-secondary);
            margin-top: 0.1rem;
        }}

        .broker-info span {{
            color: var(--primary);
            font-weight: 600;
        }}

        .age-info {{
            text-align: right;
        }}

        .age-label {{
            font-size: 0.75rem;
            color: var(--text-secondary);
            display: block;
        }}

        .age-value {{
            font-size: 0.9rem;
            font-weight: 600;
        }}

        .no-signals {{
            text-align: center;
            color: var(--text-secondary);
            padding: 2rem;
            font-style: italic;
            border: 1px dashed var(--border-color);
            border-radius: 12px;
        }}

        .copy-btn {{
            background: none;
            border: 1px solid var(--border-color);
            border-radius: 6px;
            color: var(--text-secondary);
            cursor: pointer;
            padding: 0.2rem 0.45rem;
            font-size: 0.75rem;
            line-height: 1;
            transition: all 0.15s ease;
            flex-shrink: 0;
        }}

        .copy-btn:hover {{
            background: rgba(88, 166, 255, 0.12);
            border-color: var(--primary);
            color: var(--primary);
        }}

        .copy-btn.copied {{
            background: rgba(63, 185, 80, 0.15);
            border-color: var(--success);
            color: var(--success);
        }}

        footer {{
            text-align: center;
            margin-top: 4rem;
            color: var(--text-secondary);
            font-size: 0.85rem;
            border-top: 1px solid var(--border-color);
            padding-top: 1.5rem;
        }}

        @keyframes fadeIn {{
            from {{ opacity: 0; transform: translateY(-10px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>Weekend Supertrend Scan</h1>
            <div class="meta-info">
                <div class="meta-item">Index/Stock: <span>{index_label}</span></div>
                <div class="meta-item">Scan Time: <span>{now}</span></div>
                <div class="meta-item">Lookback: <span>{lookback_weeks} week(s)</span></div>
                <div class="meta-item">ATR: <span>{SUPERTREND_PERIOD} (Mult: {SUPERTREND_MULTIPLIER})</span></div>
            </div>
        </header>

        <main class="grid">
            {recommendations_html}
            <!-- BUY Signals -->
            <section class="section-panel">
                <h2 class="section-title buy-title">
                    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M12 5v14M5 12h14"/></svg>
                    BUY Candidates (Uptrend)
                </h2>
                <div class="signals-list">
                    {buy_rows_html}
                </div>
            </section>

            <!-- SELL Signals -->
            <section class="section-panel">
                <h2 class="section-title sell-title">
                    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M5 12h14"/></svg>
                    SELL Alerts (Portfolio Downtrend)
                </h2>
                <div class="signals-list">
                    {sell_rows_html}
                </div>
            </section>
        </main>

        <footer>
            <p>Generated dynamically using Kite MCP | Supertrend (ATR={SUPERTREND_PERIOD}, Multiplier={SUPERTREND_MULTIPLIER})</p>
            <p style="margin-top: 0.5rem; font-size: 0.75rem; opacity: 0.7;">This report is for educational purposes only.</p>
        </footer>
    </div>
    <script>
        function copySymbol(btn, symbol) {{
            navigator.clipboard.writeText(symbol).then(function() {{
                btn.textContent = '✓';
                btn.classList.add('copied');
                setTimeout(function() {{
                    btn.textContent = '⎘';
                    btn.classList.remove('copied');
                }}, 1500);
            }}).catch(function() {{
                // Fallback for older browsers
                var el = document.createElement('textarea');
                el.value = symbol;
                el.style.position = 'fixed';
                el.style.opacity = '0';
                document.body.appendChild(el);
                el.select();
                document.execCommand('copy');
                document.body.removeChild(el);
                btn.textContent = '✓';
                btn.classList.add('copied');
                setTimeout(function() {{
                    btn.textContent = '⎘';
                    btn.classList.remove('copied');
                }}, 1500);
            }});
        }}
    </script>
</body>
</html>
"""
    filename = f"{filename_prefix}.html"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"\n  Beautiful HTML report saved/overwritten: \033[1m{filename}\033[0m")


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
        for item in sorted(signals, key=lambda x: x[0]):
            sym = item[0]
            flip_weeks = item[1]
            recent = item[2]
            broker = item[3] if len(item) > 3 else ""
            broker_str = f" ({broker})" if broker else ""
            if flip_weeks is not None:
                age_str = f"{flip_weeks:>2}w ago"
            else:
                age_str = "always"
            badge = "★ NEW" if recent else "     "
            row = fmt_row(f"{sym}{broker_str}", age_str, badge)
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

    # Generate console recommendations
    recommendations_console = []
    has_extended_metrics = len(buy_signals) > 0 and len(buy_signals[0]) >= 6
    if has_extended_metrics:
        # Sort by score descending
        ranked_buys = sorted(buy_signals, key=lambda x: x[5], reverse=True)
        top_recommendations = [b for b in ranked_buys if b[2]] # Preferred NEW breakouts
        if not top_recommendations:
            top_recommendations = ranked_buys[:3]
        else:
            top_recommendations = top_recommendations[:3]
        
        if top_recommendations:
            recommendations_console += [
                f"{CYAN}{divider('═')}{RESET}",
                f"{BOLD}{GREEN}  ★★★ TOP RECOMMENDED BUY SETUPS (Ranked) ★★★{RESET}",
                f"  {'─'*58}",
            ]
            for idx, item in enumerate(top_recommendations):
                sym, flip_weeks, recent, vol_ratio, dist, score = item
                note = "NEW Breakout" if recent else "Active Trend"
                recommendations_console.append(
                    f"  #{idx+1} {BOLD}{sym:<8}{RESET} | Score: {score:>5.1f}/100 | Vol: {vol_ratio:>4.2f}x | Dist: {dist:>4.1f}% | {note}"
                )
            recommendations_console += [""]

    console_lines = [
        "",
        f"{BOLD}{CYAN}{'Weekend Scan Results':^{width}}{RESET}",
        f"{CYAN}{divider('═')}{RESET}",
        f"  Scan time : {now}",
        f"  NEW signal: within last {lookback_weeks} week(s)",
        f"{CYAN}{divider()}{RESET}",
        "",
    ]
    if recommendations_console:
        console_lines += recommendations_console

    console_lines += [
        f"{BOLD}  {'BUY Candidates':}{RESET}",
        f"  {'─'*28}  {'────────'}",
    ]
    for item in sorted(buy_signals, key=lambda x: (not x[2], x[0])):
        sym = item[0]
        flip_weeks = item[1]
        recent = item[2]
        broker = item[3] if len(item) > 3 else ""
        broker_str = f" ({broker})" if broker else ""
        age_str = f"{flip_weeks:>2}w ago" if flip_weeks is not None else "always "
        badge = "★ NEW" if recent else "     "
        row = fmt_row(f"{sym}{broker_str}", age_str, badge)
        console_lines.append(coloured_row(row, recent, "BUY"))
    if not buy_signals:
        console_lines.append("  (none)")

    console_lines += [
        "",
        f"{CYAN}{divider()}{RESET}",
        f"{BOLD}  {'SELL Alerts (Portfolio)':}{RESET}",
        f"  {'─'*28}  {'────────'}",
    ]
    for item in sorted(sell_signals, key=lambda x: (not x[2], x[0])):
        sym = item[0]
        flip_weeks = item[1]
        recent = item[2]
        broker = item[3] if len(item) > 3 else ""
        broker_str = f" ({broker})" if broker else ""
        age_str = f"{flip_weeks:>2}w ago" if flip_weeks is not None else "always "
        badge = "★ NEW" if recent else "     "
        row = fmt_row(f"{sym}{broker_str}", age_str, badge)
        console_lines.append(coloured_row(row, recent, "SELL"))
    if not sell_signals:
        console_lines.append("  (none)")

    console_lines += ["", f"{CYAN}{divider('═')}{RESET}", ""]
    print("\n".join(console_lines))

    # ── plain-text file (no ANSI) ─────────────────────────────────────────────
    # Generate file recommendations
    recommendations_file = []
    if has_extended_metrics and top_recommendations:
        recommendations_file += [
            "  ★★★ TOP RECOMMENDED BUY SETUPS (Ranked) ★★★",
            "  " + "─"*58,
        ]
        for idx, item in enumerate(top_recommendations):
            sym, flip_weeks, recent, vol_ratio, dist, score = item
            note = "NEW Breakout" if recent else "Active Trend"
            recommendations_file.append(
                f"  #{idx+1} {sym:<8} | Score: {score:>5.1f}/100 | Vol: {vol_ratio:>4.2f}x | Dist: {dist:>4.1f}% | {note}"
            )
        recommendations_file += ["", divider(), ""]

    file_lines = [
        f"Weekend Scan Results — {now}",
        divider("="),
        f"  Scan time : {now}",
        f"  NEW signal: within last {lookback_weeks} week(s)",
        divider(),
        "",
    ]
    if recommendations_file:
        file_lines += recommendations_file

    file_lines += [
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
    """Read portfolio stock symbols from *portfolio.csv*.
    Returns a dict of Symbol -> Broker mapping.
    """
    try:
        with open(PORTFOLIO_CSV) as f:
            reader = csv.DictReader(f)
            portfolio = {}
            for row in reader:
                symbol = row.get("Symbol")
                if symbol:
                    # Clean symbol (handling non-breaking spaces like \xa0)
                    sym_clean = symbol.replace('\xa0', '').strip()
                    broker = row.get("Broker", "")
                    broker_clean = broker.replace('\xa0', '').strip() if broker else ""
                    portfolio[sym_clean] = broker_clean
            return portfolio
    except FileNotFoundError:
        print(f"[init] {PORTFOLIO_CSV} not found -- creating sample")
        with open(PORTFOLIO_CSV, "w") as f:
            f.write("Symbol,Broker\nTCS,Sharekhan\n")
        return {"TCS": "Sharekhan"}


SCAN_ALL_OPTIONS = ["1", "2", "3", "4", "5"]

FILENAME_PREFIX_MAP = {
    "1": "nifty_50",
    "2": "nifty_next_50",
    "3": "nifty_midcap_150",
    "4": "nifty_smallcap_100",
    "5": "nifty_200_momentum_30",
    "6": "portfolio",
    "7": "single_stock",
}


async def resolve_index_symbols(choice: str) -> tuple[str, list[str]]:
    """Resolve the index label and symbol list for a given menu choice (1-5).

    Returns (index_label, symbols).  Exits on unrecoverable error.
    """
    index_label, nse_index_name = INDEX_MAP[choice]
    if nse_index_name is None:
        print(f"\nUsing static list for {index_label} (last updated Jul 2026).")
        symbols = NIFTY200_MOMENTUM_30_STATIC
    else:
        print(f"\nFetching {index_label} constituents from NSE...")
        symbols = await fetch_index_constituents(nse_index_name)
        if not symbols:
            print(f"ERROR: Could not fetch constituents for {index_label}. Skipping.")
            return index_label, []
    print(f"Will scan {index_label} ({len(symbols)} stocks).")
    return index_label, symbols


async def run_single_scan(session, choice, index_label, index_symbols, portfolio_map,
                          from_date, to_date, lookback_weeks, filename_prefix):
    """Run one scan (index BUY or portfolio SELL) and write its HTML report."""
    is_portfolio_scan = (choice == "6")
    if is_portfolio_scan:
        buy = []
        sell = await scan_portfolio(session, portfolio_map, from_date, to_date, lookback_weeks)
    else:
        buy = await scan_index(session, index_symbols, from_date, to_date, lookback_weeks)
        sell = []

    report_results(buy, sell, lookback_weeks)
    report_results_html(buy, sell, lookback_weeks, index_label, filename_prefix)


async def main():
    # ── Step 1: user input & NSE fetch (no MCP session open yet) ─────────────
    portfolio_map = load_portfolio_symbols()
    portfolio_symbols = list(portfolio_map.keys())
    print(f"Portfolio stocks: {portfolio_symbols}")

    print("\nAvailable options for scanning:")
    print("1. Nifty 50")
    print("2. Nifty Next 50")
    print("3. Nifty Midcap 150")
    print("4. Nifty Smallcap 100")
    print("5. Nifty 200 Momentum 30")
    print("6. Portfolio (from portfolio.csv)")
    print("7. Single Stock")
    print("8. Scan All  (options 1-5, generates 5 HTML reports)")

    choice = input("Enter your choice (1-8): ").strip()

    if len(sys.argv) > 1:
        lookback_weeks = int(sys.argv[1])
    else:
        lookback_weeks = int(input(
            "How many weeks back to check for a direction change? "
        ).strip())

    # ── Scan All path ─────────────────────────────────────────────────────────
    if choice == "8":
        # Resolve all index symbol lists up-front (no MCP needed for this)
        scan_jobs = []  # list of (choice, index_label, symbols, filename_prefix)
        for opt in SCAN_ALL_OPTIONS:
            index_label, symbols = await resolve_index_symbols(opt)
            if symbols:
                scan_jobs.append((opt, index_label, symbols, FILENAME_PREFIX_MAP[opt]))

        if not scan_jobs:
            print("ERROR: Could not resolve any index constituents. Exiting.")
            raise SystemExit(1)

        total = len(scan_jobs)
        print(f"\nScan All: will process {total} index/indices → {total} HTML report(s).")

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

                        for idx, (opt, index_label, symbols, filename_prefix) in enumerate(scan_jobs, 1):
                            print(f"\n{'='*62}")
                            print(f"  [{idx}/{total}] Scanning {index_label} ...")
                            print(f"{'='*62}")
                            await run_single_scan(
                                session, opt, index_label, symbols,
                                portfolio_map, from_date, to_date,
                                lookback_weeks, filename_prefix,
                            )

                        print(f"\n{'='*62}")
                        print(f"  Scan All complete. {total} HTML report(s) saved.")
                        print(f"{'='*62}\n")
                except (Exception, KeyboardInterrupt) as exc:
                    traceback.print_exc()
                    raise SystemExit(1) from exc
        return

    # ── Single scan path (choices 1-7) ───────────────────────────────────────
    filename_prefix = FILENAME_PREFIX_MAP.get(choice, "nifty_200_momentum_30")
    is_portfolio_scan = (choice == "6")

    if is_portfolio_scan:
        index_symbols = []
        index_label = "Portfolio"
    elif choice in INDEX_MAP:
        index_label, symbols = await resolve_index_symbols(choice)
        index_symbols = symbols
        if not index_symbols:
            raise SystemExit(1)
    elif choice == "7":
        stock_symbol = input("Enter the stock symbol (e.g., TCS): ").strip().upper()
        index_symbols = [stock_symbol]
        index_label = f"Single stock: {stock_symbol}"
        filename_prefix = f"single_stock_{stock_symbol.lower()}"
    else:
        print("Invalid choice. Defaulting to Nifty 200 Momentum 30.")
        index_label, symbols = await resolve_index_symbols("5")
        index_symbols = symbols
        if not index_symbols:
            raise SystemExit(1)

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

                    await run_single_scan(
                        session, choice, index_label, index_symbols,
                        portfolio_map, from_date, to_date,
                        lookback_weeks, filename_prefix,
                    )
            except (Exception, KeyboardInterrupt) as exc:
                traceback.print_exc()
                raise SystemExit(1) from exc


if __name__ == "__main__":
    asyncio.run(main())
