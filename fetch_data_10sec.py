import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

def fetch_intraday_data(ticker, interval="1m"):
    """
    Fetches intraday stock data from Yahoo Finance.

    Parameters:
        ticker (str): The stock ticker symbol (e.g., "RELIANCE.NS").
        interval (str): The interval for historical data (default: "1m").

    Returns:
        pd.DataFrame: DataFrame containing stock data with a 'Datetime' column.
    """
    print(f"📡 Fetching {interval} data for {ticker}...")
    
    # Get data for the current day
    today = datetime.today().strftime("%Y-%m-%d")
    data = yf.download(ticker, start=today, interval=interval, progress=False)
    
    if data.empty:
        print("❌ No data found. Ensure the market is open.")
        return None

    # Reset index to make 'Datetime' a column
    data.reset_index(inplace=True)
    
    # Ensure 'Datetime' column is in datetime format
    data["Datetime"] = pd.to_datetime(data["Datetime"])

    return data

def convert_to_10s_ticks(data):
    """
    Converts 1-minute tick data to 10-second tick data using interpolation.

    Parameters:
        data (pd.DataFrame): DataFrame with 1-minute interval stock data.

    Returns:
        pd.DataFrame: DataFrame with 10-second interval stock data.
    """
    print("🔄 Converting to 10-second tick data...")

    # Ensure 'Datetime' is in datetime format and remove timezone if any
    data["Datetime"] = pd.to_datetime(data["Datetime"]).dt.tz_localize(None)

    new_rows = []

    for i in range(len(data) - 1):
        current_row = data.iloc[i]
        next_row = data.iloc[i + 1]

        current_time = current_row["Datetime"]
        next_time = next_row["Datetime"]

        # Ensure the timestamps are single values
        if isinstance(current_time, pd.Series):
            current_time = current_time.iloc[0]
        if isinstance(next_time, pd.Series):
            next_time = next_time.iloc[0]

        # Generate 10-second interval timestamps
        time_intervals = pd.date_range(start=current_time, end=next_time - pd.Timedelta(seconds=10), freq="10s")

        for j, t in enumerate(time_intervals):
            fraction = (j + 1) / len(time_intervals)

            interpolated_open = (1 - fraction) * current_row["Open"] + fraction * next_row["Open"]
            interpolated_high = (1 - fraction) * current_row["High"] + fraction * next_row["High"]
            interpolated_low = (1 - fraction) * current_row["Low"] + fraction * next_row["Low"]
            interpolated_close = (1 - fraction) * current_row["Close"] + fraction * next_row["Close"]
            interpolated_volume = (current_row["Volume"] + next_row["Volume"]) / 6  # Spread across 6 new ticks

            new_rows.append([t, interpolated_open, interpolated_high, interpolated_low, interpolated_close, interpolated_volume])

    new_df = pd.DataFrame(new_rows, columns=["Datetime", "Open", "High", "Low", "Close", "Volume"])
    return new_df

def main():
    ticker = "RELIANCE.NS"
    
    # Fetch 1-minute data
    data = fetch_intraday_data(ticker)
    if data is None:
        return

    # Convert to 10-second tick data
    converted_data = convert_to_10s_ticks(data)

    # Save the output as CSV
    output_file = f"{ticker.replace('.NS', '')}_{datetime.today().strftime('%Y-%m-%d')}.csv"
    converted_data.to_csv(output_file, index=False)
    
    print(f"✅ 10-second tick data saved successfully: {output_file}")

if __name__ == "__main__":
    main()
