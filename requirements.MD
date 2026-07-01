You are an expert Python developer. Build me a complete Python script called `scanner.py` that runs locally on my machine when I execute it manually (no cron jobs, no automation). The script should integrate with Zerodha’s Kite MCP server and perform the following tasks:

1. **Data Access**
   - Connect to Kite MCP using my API key and access token.
   - Fetch OHLCV (Open, High, Low, Close, Volume) data for:
     a) Nifty 200 Momentum 30 constituents (hardcode the list initially).
     b) My personal portfolio stocks (read from a CSV file called `portfolio.csv`).

2. **Weekly Candle Aggregation**
   - Fetch daily OHLCV data from Kite MCP.
   - Resample the data into **weekly candles**:
     - Open = first daily open of the week
     - High = max daily high of the week
     - Low = min daily low of the week
     - Close = last daily close of the week
     - Volume = sum of daily volumes
   - Use these weekly candles for all indicator calculations.

3. **Indicator Calculation**
   - Use `pandas-ta` (preferred) or `TA-Lib` to compute the **Supertrend indicator** with:
     - ATR period = 9
     - Multiplier = 2
   - For each stock, detect if the latest Supertrend signal has flipped:
     - **BUY**: Supertrend turned bullish this week or last week.
     - **SELL**: For portfolio stocks, Supertrend turned bearish.

4. **Signal Detection Logic**
   - Compare the last two weekly rows of data to determine if a flip occurred.
   - If Supertrend line crosses above price → SELL signal.
   - If Supertrend line crosses below price → BUY signal.

5. **Output**
   - Print results clearly in the console.
   - Example:
     ```
     Weekend Scan Results:
     BUY Candidates (Nifty 200 Momentum 30):
     - ABC Ltd (Supertrend flipped to Buy this week)

     SELL Alerts (Portfolio):
     - XYZ Ltd (Supertrend flipped to Sell in your portfolio)
     ```
   - Also save results to a text file `scan_results.txt`.

6. **Simplifications**
   - No auto-trading. I will always execute orders manually.
   - No cron jobs. I will run the script myself on weekends.
   - Keep the code modular: separate functions for fetching data, resampling to weekly, computing Supertrend, checking signals, and reporting.

7. **Future Expansion**
   - Allow easy extension to other indices (Nifty 50, Nifty 100).
   - Allow easy addition/removal of portfolio stocks by editing `portfolio.csv`.

Deliverables:
- A complete Python script (`scanner.py`) with clear comments.
- Functions:
  - `get_ohlcv(symbol, from_date, to_date, interval)`
  - `resample_to_weekly(df)`
  - `compute_supertrend(df)`
  - `check_signal(df)`
  - `scan_index(stocks)`
  - `scan_portfolio(stocks)`
  - `main()`
- Use `requests` or KiteConnect SDK for MCP integration.
- Ensure the script runs end-to-end when executed manually.
