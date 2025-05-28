import sys
import os
# 添加项目根目录到 sys.path，确保可以导入 src 包
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import psycopg2
import requests
import time
from datetime import datetime
from src.config.db_config import DB_CONFIG

# 数据库连接函数
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# 将 tickers 数据插入数据库
def insert_tickers_to_db(tickers):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 自动创建 tickers_fundamental 表（如果不存在）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tickers_fundamental (
            ticker TEXT PRIMARY KEY,
            name TEXT,
            type TEXT,
            active BOOLEAN,
            primary_exchange TEXT,
            last_updated_utc TIMESTAMP
        );
    """)

    insert_query = """
        INSERT INTO tickers_fundamental (ticker, name, type, active, primary_exchange, last_updated_utc)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker) DO UPDATE
        SET name = EXCLUDED.name,
            type = EXCLUDED.type,
            active = EXCLUDED.active,
            primary_exchange = EXCLUDED.primary_exchange,
            last_updated_utc = EXCLUDED.last_updated_utc;
    """
    
    
    
    data_to_insert = [
        (
            ticker["ticker"],
            ticker.get("name", ""),  # 使用 .get() 提供默认值
            ticker.get("type", None),  # 如果 type 缺失，设为 None
            ticker.get("active", False),
            ticker.get("primary_exchange", ""),
            ticker.get("last_updated_utc", None)
        )
        for ticker in tickers
    ]
    
    cursor.executemany(insert_query, data_to_insert)
    conn.commit()
    print(f"Inserted/Updated {len(data_to_insert)} tickers into the database.")
    
    cursor.close()
    conn.close()

# 获取所有 Polygon.io tickers 数据，带重试机制
def fetch_tickers_from_polygon(polygon_api_key, max_retries=3):
    base_url = "https://api.polygon.io/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "true",
        "order": "asc",
        "limit": 1000,
        "sort": "ticker",
        "apiKey": polygon_api_key
    }
    
    all_tickers = []
    page_count = 0
    url = base_url
    
    while True:
        retries = 0
        while retries < max_retries:
            response = None
            try:
                print(f"Fetching page {page_count + 1} at {datetime.now()} from {url}")
                response = requests.get(
                    url,
                    params=params if page_count == 0 else None,
                    timeout=30  # 设置 30 秒超时
                )
                response.raise_for_status()
                
                data = response.json()
                page_tickers = data.get("results", [])
                all_tickers.extend(page_tickers)
                page_count += 1
                
                print(f"Page {page_count}: Fetched {len(page_tickers)} tickers.")
                print(f"Total tickers so far: {len(all_tickers)}")
                
                # return all_tickers 
                next_url = data.get("next_url")
                if next_url:
                    url = f"{next_url}&apiKey={polygon_api_key}"
                    print(f"Next URL: {url}")
                else:
                    print("No more pages to fetch.")
                    return all_tickers
                
                print("Sleeping for 12 seconds to respect API rate limit...")
                time.sleep(12)
                break  # 成功后跳出重试循环
            
            except requests.exceptions.RequestException as e:
                retries += 1
                if response and response.status_code == 429:
                    print(f"Rate limit exceeded (429 error) at {datetime.now()}. Waiting 120 seconds...")
                    time.sleep(61)
                elif isinstance(e, requests.exceptions.SSLError):
                    print(f"SSL error: {e}. Retrying {retries}/{max_retries} after 30 seconds...")
                    time.sleep(30)
                else:
                    print(f"Request error: {e}. Retrying {retries}/{max_retries} after 30 seconds...")
                    time.sleep(30)
                if retries == max_retries:
                    print(f"Max retries ({max_retries}) exceeded. Stopping.")
                    return all_tickers

# 主函数
def main():
    polygon_api_key = os.getenv("POLYGON_API_KEY")
    print("Starting to fetch all tickers from Polygon.io using HTTP...")
    
    tickers = fetch_tickers_from_polygon(polygon_api_key)
    print(f"Total tickers fetched: {len(tickers)}")
    print(f"First ticker: {tickers[0] if tickers else 'None'}")
    
    # if tickers:
    #     insert_tickers_to_db(tickers)
    # else:
    #     print("No tickers to insert.")

if __name__ == "__main__":
    main()