import psycopg2
from config.db_config import DB_CONFIG

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def recreate_individual_tables():
    conn = get_db_connection()
    cur = conn.cursor()

    # List of tickers to recreate
    tickers = ['TSLA', 'AAPL']

    for ticker in tickers:
        print(f"Recreating table for {ticker}...")
        try:
            # Step 1: Create the table if it doesn't exist
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{ticker}" (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP NOT NULL UNIQUE,
                    open_price DECIMAL(18, 6),
                    high_price DECIMAL(18, 6),
                    low_price DECIMAL(18, 6),
                    close_price DECIMAL(18, 6),
                    volume BIGINT,
                    turnover DECIMAL(18, 6)
                );
            """)
            # Create index on timestamp
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS "idx_{ticker}_timestamp" ON "{ticker}" (timestamp);
            """)
            print(f"Table {ticker} created or already exists.")

            # Step 2: Insert data from stock_daily into the respective table
            cur.execute(f"""
                INSERT INTO "{ticker}" (timestamp, open_price, high_price, low_price, close_price, volume, turnover)
                SELECT 
                    timestamp,
                    open::decimal(18,6),
                    high::decimal(18,6),
                    low::decimal(18,6),
                    close::decimal(18,6),
                    volume,
                    turnover::decimal(18,6)
                FROM stock_daily
                WHERE ticker = '{ticker}'
                ON CONFLICT (timestamp) DO NOTHING;
            """)
            print(f"Successfully populated {ticker}")

        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            conn.rollback()
            continue

    conn.commit()
    cur.close()
    conn.close()
    print("Table recreation completed.")

if __name__ == "__main__":
    recreate_individual_tables()