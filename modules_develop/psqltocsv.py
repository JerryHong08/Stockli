# this code is used to connect to PostgreSQL database and export data to CSV file
import psycopg2
import os
import csv

def get_project_root():
    current_dir = os.path.abspath(__file__)
    while not os.path.exists(os.path.join(current_dir, "README.md")):  # 假设根目录有 README.md
        current_dir = os.path.dirname(current_dir)
        if current_dir == os.path.dirname(current_dir):  # 到达文件系统根目录
            raise Exception("无法找到项目根目录")
    return current_dir

# 项目根目录
BASE_DIR = get_project_root()


db_config = {
    "dbname" : os.getenv("DB_NAME", "Stock"),
    "user" : os.getenv("DB_USER", "postgres"),
    "password" : "hgl084877",
    "host" : "localhost",
    "port" : os.getenv("DB_PORT", "5432")
    }
ticker = 'AAPL'  # 替换为你要查询的股票代码
output_csv = os.path.join(BASE_DIR, 'outputs/stock_csv_data')

try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = cursor.fetchall()
    
    cursor.execute(f"SELECT * FROM stock_daily WHERE ticker = '{ticker}'")
    stock_data = cursor.fetchall()
    
    print(stock_data)
    
    # Ensure the output directory exists
    os.makedirs(output_csv, exist_ok=True)

    # Define the output file path
    output_file = os.path.join(output_csv, f"{ticker}_stock_data.csv")

    # Write the stock data to a CSV file
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        # Write the column headers
        column_names = [desc[0] for desc in cursor.description]
        writer.writerow(column_names)
        
        # Write the data rows
        writer.writerows(stock_data)

    print(f"Data for ticker {ticker} has been saved to {output_file}")
    conn.commit()
except psycopg2.Error as e:
    print(f"Database error: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    print("Database connection closed.")
    