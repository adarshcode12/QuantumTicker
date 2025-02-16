import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
from config import FETCH_DATE, NIFTY50_SYMBOLS, OUTPUT_DIR

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_full_day_data(ticker, fetch_date):
    """Fetch full-day 1-minute interval stock data and save it as CSV."""
    stock = yf.Ticker(ticker)
    selected_date = datetime.strptime(fetch_date, "%Y-%m-%d")

    start_date = selected_date.strftime("%Y-%m-%d")
    end_date = (selected_date + timedelta(days=1)).strftime("%Y-%m-%d")  # Next day for full session

    try:
        # Fetch stock data
        data = stock.history(start=start_date, end=end_date, interval="1m")
        data.reset_index(inplace=True)

        # Ensure correct date filter
        if "Datetime" in data.columns:
            data = data[data["Datetime"].dt.date == selected_date.date()]

        if data.empty:
            print(f"⚠️ No data available for {ticker} on {fetch_date}")
            return

        # Convert date format to YYYYMMDD
        formatted_date = selected_date.strftime("%Y-%m-%d")
        
        # Save data to CSV with YYYYMMDD format
        csv_filename = os.path.join(OUTPUT_DIR, f"{ticker.replace('.NS', '')}_{formatted_date}.csv")
        data.to_csv(csv_filename, index=False)
        print(f"✅ Saved: {csv_filename}")

    except Exception as e:
        print(f"❌ Error fetching {ticker}: {e}")

def main():
    """Main function to fetch and save Nifty 50 stock data."""
    for symbol in NIFTY50_SYMBOLS:
        fetch_full_day_data(symbol, FETCH_DATE)

    print("✅ All data fetching completed.")

if __name__ == "__main__":
    main()
