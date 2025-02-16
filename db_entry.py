import psycopg2
import glob
import os
from datetime import datetime

# Database connection details
DB_NAME = "stock_data"
DB_USER = "postgres"
DB_PASSWORD = "root"
DB_HOST = "localhost"
DB_PORT = "5432"

# Function to insert a CSV file path into the database
def insert_csv_to_db(file_path):
    # Extract filename
    filename = os.path.basename(file_path)
    parts = filename.replace(".csv", "").split("_")

    if len(parts) < 2:
        print(f"Skipping file {filename} (invalid format)")
        return

    symbol = parts[0]  # Example: AAPL_20240215.csv → symbol = AAPL
    date = parts[1]  # Example: AAPL_20240215.csv → date = 20240215

    # Convert date to YYYY-MM-DD format
    try:
        formatted_date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        print(f"Skipping file {filename} (invalid date format: {date})")
        return

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    cur = conn.cursor()

    # Insert file path into database
    cur.execute("""
        INSERT INTO stock_files (symbol, date, csv_path)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol, date) DO UPDATE SET csv_path = EXCLUDED.csv_path;
    """, (symbol, formatted_date, file_path))

    # Commit and close connection
    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted: {filename} → {file_path}")

# Get all CSV files from the directory
csv_files = glob.glob("/Users/adarshkumar/Documents/PROJECTS/db_entry/symbol_data/*.csv")

for file in csv_files:
    insert_csv_to_db(file)

print("All file paths inserted successfully.")
