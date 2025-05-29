import sys
import os
# 添加项目根目录到 sys.path，确保可以导入 src 包
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.time_teller import update_limit_date
import psycopg2
import requests
import time
from datetime import datetime
from config.db_config import DB_CONFIG
from longport.openapi import QuoteContext, Config, Period, AdjustType
from database.db_operations import clean_symbol_for_postgres,save_to_table
from database.db_connection import get_engine

# 数据库连接函数
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# 将 tickers 数据插入数据库
def insert_tickers_to_tickers_fundamental(tickers):
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
            ticker[0],  # ticker
            ticker[1],  # name
            ticker[2],  # type
            ticker[3],  # active
            ticker[4],  # primary_exchange
            ticker[5],  # last_updated_utc
        )
        for ticker in tickers
    ]
    
    cursor.executemany(insert_query, data_to_insert)
    conn.commit()
    print(f"Inserted/Updated {len(data_to_insert)} tickers into the database.")
    
    cursor.close()
    conn.close()

# 获取所有 Polygon.io tickers 数据，带重试机制
def fetch_tickers_from_polygon(polygon_api_key, last_updated_time, max_retries=3):
    
    base_url = "https://api.polygon.io/vX/reference/ipos"
    params = {
        "order": "desc",
        "limit": 1000,
        "sort": "listing_date",
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
                
                need_next_page = False
                
                if page_tickers:
                    # 找到当前页最后一个ticker的listing_date
                    last_ticker = page_tickers[-1]
                    listing_date = last_ticker.get("listing_date")
                    print(f"Last ticker's listing_date: {listing_date}")
                    if listing_date is not None and listing_date > last_updated_time:
                        print(f"Continuing to next page.")
                        need_next_page = True
                    
                if need_next_page:
                    next_url = data.get("next_url")
                else:
                    print("No more pages to fetch based on listing_date condition.")
                    next_url = []
                    
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


def get_last_updated_utc():
    """获取 ticker 的 last_updated_utc 时间戳"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT last_updated_utc FROM tickers_fundamental order by last_updated_utc desc limit 1")
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if result:
        return result[0] 
    else:
        return None

def fetch_data_from_longprot_to_stock_daily(tickers):
    config = Config.from_env()
    ctx = QuoteContext(config=config)
    for ticker in tickers:
        symbol = ticker[0]
        ticker_type = ticker[2]
        primary_exchange = ticker[4]
        listing_date = ticker[5]
        cleaned_symbol = clean_symbol_for_postgres(symbol, ticker_type, primary_exchange)
        latched_days = (datetime.strptime(update_limit_date().strftime("%Y-%m-%d"), "%Y-%m-%d") - datetime.strptime(listing_date, "%Y-%m-%d")).days + 1
        print(f"{symbol}'s listing date : {listing_date} Latched time : {latched_days}")
        # resp = ctx.candlesticks(f"{cleaned_symbol}.US", Period.Day, latched_days, AdjustType.ForwardAdjust)
        # print(resp)

        # if not resp:
        #     print("No data returned from LongPort API.")
        #     return
        
        # engine = get_engine()
        # save_to_table(resp, cleaned_symbol, engine)  # ticker 而非 table_name
        
    
# 主函数
def ipo_incremental_update():
    polygon_api_key = os.getenv("POLYGON_API_KEY")
    
    last_updated_time = get_last_updated_utc().strftime("%Y-%m-%d")
    limit_date = update_limit_date().strftime("%Y-%m-%d")
    
    print(f"Last updated UTC: {last_updated_time}")
    
    print("Starting to fetch all tickers from Polygon.io using HTTP...")
    ipo_tickers = fetch_tickers_from_polygon(polygon_api_key, last_updated_time)
    
    # 只保留有 listing_date 且大于 上次更新日期 的
    ipo_filtered = [
        t for t in ipo_tickers
        if "listing_date" in t and t["listing_date"] > last_updated_time and t["listing_date"] <= limit_date
    ]
    ipo_future_filtered = [
        t for t in ipo_tickers
        if "listing_date" in t and t["listing_date"] > limit_date
    ]
    
    for t in ipo_filtered:
        print(t["ticker"],t["listing_date"])

    for t in ipo_future_filtered:
        print(t["ticker"], t["listing_date"], "Future IPO")
        
        
    tickers = [
        (
            t.get("ticker", ""),
            t.get("issuer_name", ""),
            t.get("security_type", ""),
            t.get("active", True),
            t.get("primary_exchange", ""),
            t.get("listing_date")
        )
        for t in ipo_filtered
    ]
    
    if tickers:
        # print(f"Total tickers to insert: {tickers}")
        # insert_tickers_to_tickers_fundamental(tickers)
        fetch_data_from_longprot_to_stock_daily(tickers)


if __name__ == "__main__":
    ipo_incremental_update()