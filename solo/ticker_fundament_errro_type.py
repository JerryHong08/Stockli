import psycopg2
import csv
from config.db_config import DB_CONFIG  # 数据库配置
from config.paths import ERRORstock_PATH  # 错误日志路径

# 数据库连接函数
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# 从数据库查询 ticker 的 type 和 primary_exchange
def fetch_ticker_details(tickers):
    conn = get_db_connection()
    cursor = conn.cursor()
    # 使用 IN 子句批量查询
    query = """
        SELECT ticker, type, primary_exchange
        FROM tickers_fundamental
        WHERE ticker IN %s
    """
    cursor.execute(query, (tuple(tickers),))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    # 转换为字典，便于后续匹配
    ticker_details = {row[0]: (row[1], row[2]) for row in results}
    return ticker_details

# 读取 error_log.csv 并添加 type 和 primary_exchange
def enrich_error_log():
    # 读取原始 error_log.csv
    with open(ERRORstock_PATH, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        tickers = [row['Ticker'] for row in rows]
    
    print(f"Found {len(tickers)} failed tickers in error_log.csv")
    
    # 从数据库查询 ticker 详情
    ticker_details = fetch_ticker_details(tickers)
    print(f"Retrieved details for {len(ticker_details)} tickers from database")
    
    # 添加新列
    for row in rows:
        ticker = row['Ticker']
        if ticker in ticker_details:
            row['Type'] = ticker_details[ticker][0] or 'N/A'  # 如果 type 为 NULL，用 'N/A'
            row['Primary Exchange'] = ticker_details[ticker][1] or 'N/A'  # 如果为空，用 'N/A'
        else:
            row['Type'] = 'Not Found'
            row['Primary Exchange'] = 'Not Found'
    
    # 写入新的 CSV 文件
    output_path = ERRORstock_PATH.replace('error_log.csv', 'error_log_enriched.csv')
    with open(output_path, mode='w', newline='', encoding='utf-8') as f:
        fieldnames = ['Ticker', 'Error Message', 'Timestamp', 'Type', 'Primary Exchange']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Enriched error log saved to: {output_path}")

# 主函数
def main():
    print("Starting to enrich error_log.csv with type and primary_exchange...")
    enrich_error_log()

if __name__ == "__main__":
    main()