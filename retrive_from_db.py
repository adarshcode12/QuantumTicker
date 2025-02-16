import psycopg2
import os

# Database connection details
DB_CONFIG = {
    "dbname": "stock_data",
    "user": "postgres",
    "password": "root",
    "host": "localhost",
    "port": "5432"
}

def retrieve_csv(symbol, date, output_dir):
    """Retrieve CSV from the database and save it locally."""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # SQL Query to retrieve the CSV file
        query = """
        SELECT csv_data FROM stock_files 
        WHERE symbol = %s AND date = %s
        """
        cur.execute(query, (symbol, date))
        result = cur.fetchone()

        if result is None:
            print(f"No data found for Symbol: {symbol} on Date: {date}")
        else:
            # Convert binary CSV data to file
            csv_data = result[0]
            filename = f"{symbol}_{date}.csv"
            file_path = os.path.join(output_dir, filename)

            # Write to file
            with open(file_path, "wb") as f:
                f.write(csv_data)

            print(f"✅ CSV saved at: {file_path}")

        # Close connection
        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")

# Example Usage
if __name__ == "__main__":
    symbol = "TATACONSUM"   # Example symbol
    date = "2025-02-03"   # Example date
    output_dir = "/Users/adarshkumar/Documents/PROJECTS/db_entry/retrieve_symbol_data/"  # Change path as needed

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    retrieve_csv(symbol, date, output_dir)
