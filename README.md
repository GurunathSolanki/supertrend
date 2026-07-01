# Weekend Supertrend Scanner

A Python script for scanning NSE indices and portfolio stocks using the Supertrend indicator (ATR period=9, multiplier=2) to identify potential BUY and SELL signals on weekly timeframes.

## Features

- **Dynamic Index Constituents**: Fetches live constituent lists for Nifty 50, Nifty Next 50, Nifty Midcap 150, and Nifty Smallcap 100 from NSE
- **Supertrend Analysis**: Computes TradingView-compatible Supertrend on weekly candles
- **Signal Detection**: Identifies recent trend flips (configurable lookback period)
- **Dual Scanning**: BUY signals for index constituents, SELL alerts for portfolio stocks
- **Rich Terminal Output**: Color-coded results with NEW signals highlighted
- **Kite Integration**: Uses [Kite MCP](https://mcp.kite.trade/mcp) for historical OHLCV data

## Prerequisites

- Python 3.10+
- A Zerodha account (free to create)
- Dependencies: `mcp`, `pandas`, `numpy`, `httpx`

## Installation

```bash
git clone https://github.com/GurunathSolanki/supertrend.git
cd supertrend
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install mcp pandas numpy httpx
```

See [SETUP.md](SETUP.md) for detailed setup instructions.

## Usage

```bash
python3 scanner.py
```

On first run, you'll be prompted to authorize the script with your Zerodha account (one-time). Then:

1. Select an index or single stock to scan
2. Specify lookback period for NEW signals (e.g., 3 weeks)
3. Wait for data fetch and analysis

Results are printed to console and saved to `scan_results.txt`.

## Portfolio Configuration

Create a `portfolio.csv` file to track your holdings:

```csv
Symbol,Broker
TCS,Sharekhan
INFY,Zerodha
RELIANCE,ICICI
```

The script will scan these for SELL signals.

## How It Works

1. **Data Fetch**: Downloads 180 days of daily OHLCV via Kite MCP
2. **Weekly Resampling**: Aggregates to Friday-ending weekly candles
3. **Supertrend Calculation**: Computes indicator using Wilder's ATR smoothing
4. **Signal Detection**: Identifies trend direction flips within lookback period
5. **Reporting**: Displays BUY candidates and SELL alerts with flip age

## Index Support

| Index | Fetch Method |
|-------|-------------|
| Nifty 50 | NSE API (live) |
| Nifty Next 50 | NSE API (live) |
| Nifty Midcap 150 | NSE API (live) |
| Nifty Smallcap 100 | NSE API (live) |
| Nifty 200 Momentum 30 | Static list (updated quarterly) |

## Configuration

Edit constants in `scanner.py`:

- `SUPERTREND_PERIOD`: ATR period (default: 9)
- `SUPERTREND_MULTIPLIER`: ATR multiplier (default: 2)
- `HISTORICAL_DAYS`: Data lookback (default: 180)
- `RATE_LIMIT_DELAY`: Delay between API calls (default: 1.5s)

## Output Format

```
                     Weekend Scan Results                     
══════════════════════════════════════════════════════════════
  Scan time : 2026-07-01 18:00
  NEW signal: within last 3 week(s)
──────────────────────────────────────────────────────────────

  BUY Candidates
  ────────────────────────────  ────────
  ★ NEW DRREDDY                  1w ago   (bold green)
  ★ NEW MARUTI                   0w ago   (bold green)
        ADANIENT                11w ago   (dimmed)
        TITAN                   12w ago   (dimmed)
```

## License

MIT License - see LICENSE file for details

## Disclaimer

This tool is for educational and informational purposes only. It does not constitute financial advice. Trading and investing involve risk. Always do your own research and consult a qualified financial advisor before making investment decisions.

## Credits

- [Kite MCP](https://mcp.kite.trade/mcp) by Zerodha for market data API
- Supertrend indicator logic matches TradingView's native implementation
