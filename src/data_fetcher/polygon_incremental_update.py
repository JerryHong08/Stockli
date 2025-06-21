import sys
import os
# 添加项目根目录到 sys.path，确保可以导入 src 包
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.utils.time_teller import get_latest_date_from_longport
import psycopg2
import requests
import time
from datetime import datetime, timedelta, date
from src.config.db_config import DB_CONFIG
from longport.openapi import QuoteContext, Config, Period, AdjustType
from src.database.db_operations import clean_symbol_for_postgres,save_to_table
from src.database.db_connection import get_engine
from src.utils.logger import setup_logger
import traceback
from collections import defaultdict
from .incremental_ms import revese_all_histroical_before_ms


logger = setup_logger("ipo&delisted incremental_update")
polygon_api_key = os.getenv("POLYGON_API_KEY")

# 数据库连接函数
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# 将 tickers 数据插入数据库
def insert_tickers_to_tickers_fundamental(ipo_filtered_tickers):
    conn = get_db_connection()
    try:
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
        
        cursor.executemany(insert_query, ipo_filtered_tickers)
        conn.commit()
        print(f"Inserted/Updated {len(ipo_filtered_tickers)} tickers into the database.")
    
    except Exception as e:    
        conn.rollback()
        print(f'Error ocurred, rolled back: {e}')
        raise
    finally:
        cursor.close()
        conn.close()
        
def insert_ms_to_stock_splits(splits_tickers):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # 自动创建 stock_splits 表（如果不存在）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_splits (
                id TEXT PRIMARY KEY,
                ticker TEXT,
                execution_date DATE,
                split_from FLOAT,
                split_to FLOAT
            );
        """)

        insert_query = """
            INSERT INTO stock_splits (id, ticker, execution_date, split_from, split_to)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """
        
        data_to_insert = [
            (
                ticker.get("id"), 
                ticker.get("ticker"), 
                ticker.get("execution_date"),
                ticker.get("split_from"),
                ticker.get("split_to")
            )
            for ticker in splits_tickers if ticker.get("ticker") and ticker.get("execution_date") and ticker.get("split_from") and ticker.get("split_to")
        ]
        
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f"Inserted {len(data_to_insert)} stock splits into the database.")
    
    except Exception as e:    
        conn.rollback()
        print(f'Error ocurred, rolled back: {e}')
        raise
    finally:
        cursor.close()
        conn.close()

# 获取 tickers_fundamental里最新的last_updated_utc 时间戳
def get_last_tickers_fundamental_updated_utc():
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

# 获取 stock_splits 表里最新的 execution_date 时间戳
def get_last_stock_splits_updated_utc():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT execution_date FROM stock_splits order by execution_date desc limit 1")
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if result:
        return result[0]
    else:
        return None
    

def fetch_data_from_longprot_to_stock_daily(ipo_filtered_tickers):
    config = Config.from_env()
    ctx = QuoteContext(config=config)
    for ticker in ipo_filtered_tickers:
        symbol = ticker[0]
        ticker_type = ticker[2]
        primary_exchange = ticker[4]
        listing_date = ticker[5]
        try:
            cleaned_symbol = clean_symbol_for_postgres(symbol, ticker_type, primary_exchange)
            latched_days = (datetime.strptime(get_latest_date_from_longport().strftime("%Y-%m-%d"), "%Y-%m-%d") - datetime.strptime(listing_date, "%Y-%m-%d")).days + 1
            print(f"{symbol}'s listing date : {listing_date} Latched time : {latched_days}")
            resp = ctx.candlesticks(f"{cleaned_symbol}.US", Period.Day, latched_days, AdjustType.ForwardAdjust)
            if not resp:
                logger.error(f"{cleaned_symbol}数据从longport获取失败，可能是因为没有数据或API错误。")
                continue
            engine = get_engine()
            save_to_table(resp, cleaned_symbol, engine)
        except Exception as e:
            logger.error(f"{symbol} 获取数据异常: {e}")
            continue
        
    
# 获取所有 Polygon.io tickers 数据，带重试机制
def fetch_ipo_tickers_from_polygon(polygon_api_key, last_updated_time, max_retries=3):
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
                    print(f"Last updated UTC: {last_updated_time}")
                    if listing_date is not None and listing_date > last_updated_time:
                        print(f"Continuing to next page.")
                        need_next_page = True
                    
                if need_next_page:
                    next_url = data.get("next_url")
                    url = f"{next_url}&apiKey={polygon_api_key}"
                    print(f"Next URL: {url}")  
                else:
                    print("No more pages to fetch based on listing_date condition.")
                    next_url = []
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

# 获取截至上次更新的所有退市股票数据
# 通过 Polygon.io API 获取退市股票数据，带重试机制
def fetch_delisted_tickers_from_polygon(polygon_api_key, last_updated_time, max_retries=3):
    base_url = "https://api.polygon.io/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "False",
        "order": "desc",
        "limit": 1000,
        "sort": "delisted_utc",
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
                    delisted_utc = last_ticker.get("delisted_utc")
                    print(f"Last ticker's listing_date: {delisted_utc}")
                    print(f"Last updated UTC: {last_updated_time}")
                    if delisted_utc is not None and delisted_utc > last_updated_time:
                        print(f"Continuing to next page.")
                        need_next_page = True
                    
                if need_next_page:
                    next_url = data.get("next_url")
                    url = f"{next_url}&apiKey={polygon_api_key}"
                    print(f"Next URL: {url}")  
                else:
                    print("No more pages to fetch based on delisted_utc condition.")
                    next_url = []
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

# 增量更新退市股票数据
def delisted_incremental_update(limit_date):
    # last_updated_time = get_last_tickers_fundamental_updated_utc().strftime("%Y-%m-%d")
    conn = get_db_connection()
    cursor = conn.cursor()
    # 获取 tickers_fundamental 表中最新 active 为 NULL也就是待观察的ticker 的 last_updated_utc 时间戳 ‘YYYY-MM-DD HH:MM:SS’
    cursor.execute("SELECT last_updated_utc FROM tickers_fundamental WHERE active IS NULL ORDER BY last_updated_utc DESC LIMIT 1")
    
    #转化为“YYYY-MM-DD"格式
    row = cursor.fetchone()
    if row and row[0]:
        last_updated_time = row[0].strftime("%Y-%m-%d")
    else:
        # 没有数据时给一个默认值，比如 '1970-01-01'
        last_updated_time = '2025-03-08'
    print(f"Last updated time for delisted tickers: {last_updated_time}")
    
    delisted_tickers = fetch_delisted_tickers_from_polygon(polygon_api_key, last_updated_time)
    # 只保留有 listing_date 且大于 上次更新日期 的
    delisted_filtered = [
        t for t in delisted_tickers
        if "delisted_utc" in t and t["delisted_utc"] > last_updated_time and t["delisted_utc"] <= limit_date
    ]
    print(f"Total delisted tickers fetched: {len(delisted_filtered)}")
    # for t in delisted_filtered:
    #     print(t["ticker"], t["delisted_utc"], end=' ')
    
    return delisted_filtered

# 检测退市股票是否仍然活跃
def detect_delisted_tickers(tickers):
    days_to_delist_count = defaultdict(list)
    before_nominate = defaultdict(list)
    after_nominate = defaultdict(list)
    config = Config.from_env()
    ctx = QuoteContext(config=config)
    active = 0
    delisted = 0
    for ticker in tickers:
        symbol = ticker["ticker"]
        # print(ticker["market"])
        try:
            cleaned_symbol = clean_symbol_for_postgres(symbol, ticker["type"], ticker["primary_exchange"])
            resp = ctx.candlesticks(f"{cleaned_symbol}.US", Period.Day, 1000, AdjustType.ForwardAdjust)
            # print(resp)
            if resp and hasattr(resp[0], "timestamp"):
                # 假设timestamp为字符串或datetime对象
                ts = resp[-1].timestamp
                if isinstance(ts, datetime):
                    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    ts_str = str(ts)
                ts_cmp = ts_str.replace("T", " ").replace("Z", "")
                print(f"Ticker: {cleaned_symbol}, delisted_utc: {ticker['delisted_utc']}, timestamp: {ts_cmp}")
                if ts_cmp == "2025-06-20 12:00:00":
                    if not hasattr(detect_delisted_tickers, "splits_to_active_tickers"):
                        detect_delisted_tickers.splits_to_active_tickers = set()
                    if not hasattr(detect_delisted_tickers, "renew_to_active_tickers"):
                        detect_delisted_tickers.renew_to_active_tickers = set()
                    #检测是否还能获取到退市日期前的K线数据
                    delisted_utc_str = ticker['delisted_utc'].replace('Z', '')
                    delisted_utc_dt = datetime.strptime(delisted_utc_str, "%Y-%m-%dT%H:%M:%S")
                    try:
                        found = False
                        for candle in resp:
                            ts = getattr(candle, "timestamp", None)
                            # print(f"Ticker: {cleaned_symbol}, delisted_utc: {ticker['delisted_utc']}, {ts}")
                            if ts:
                                if isinstance(ts, datetime):
                                    ts_dt = ts
                                else:
                                    ts_dt = datetime.strptime(ts.replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")
                                # 判断是否有timestamp早于delisted_utc_dt至少3天
                                if ts_dt <= delisted_utc_dt - timedelta(days=3):
                                    detect_delisted_tickers.splits_to_active_tickers.add((symbol, ticker["delisted_utc"]))
                                    print(f"Ticker {cleaned_symbol} is still active after delisting on {ticker['delisted_utc']}.")
                                    found = True
                                    break
                        if not found:                
                            detect_delisted_tickers.renew_to_active_tickers.add((symbol, ticker["delisted_utc"]))
                            print(f"Ticker {cleaned_symbol} has renewed trading after delisting on {ticker['delisted_utc']}.")
                    except Exception as e:
                        print(f"Error fetching historical data for {cleaned_symbol}: {e}")
                        continue
                else:
                    if not hasattr(detect_delisted_tickers, "delisted_tickers"):
                        detect_delisted_tickers.delisted_tickers = set()
                    detect_delisted_tickers.delisted_tickers.add((symbol, ticker["delisted_utc"]))
                    ts_cmp_dt = datetime.strptime(ts_cmp, "%Y-%m-%d %H:%M:%S")
                    delisted_utc_str = ticker["delisted_utc"].replace("Z", "").replace("T", " ")
                    delisted_utc_dt = datetime.strptime(delisted_utc_str, "%Y-%m-%d %H:%M:%S")   
                    
                    diff_days = abs((delisted_utc_dt - ts_cmp_dt).days)
                    days_to_delist_count[diff_days].append(symbol)
                    
                    if ts_cmp_dt < delisted_utc_dt:
                        before_nominate[diff_days].append(symbol)
                        # print(f"Ticker: {symbol}, {diff_days}天前退市")
                    else:
                        after_nominate[diff_days].append(symbol)
                        # print(f"Ticker: {symbol}, {diff_days}天后退市")
            print(f"Checking {cleaned_symbol}...")
            
        except Exception as e:
                # logger.error(f"{symbol} 获取数据异常: {e}\n{traceback.format_exc()}")
                print(f"Ticker {cleaned_symbol} is probably just delisted forever.")
                detect_delisted_tickers.delisted_tickers.add((symbol))
                continue
    print("\n=== 距离提名前退市天数统计 ===")
    for days in sorted(before_nominate.keys()):
        print(f"{days}天前退市：{len(before_nominate[days])}只股票，代码：{before_nominate[days]}")

    print("\n=== 距离提名后退市天数统计 ===")
    for days in sorted(after_nominate.keys()):
        print(f"{days}天后退市：{len(after_nominate[days])}只股票，代码：{after_nominate[days]}")
    if hasattr(detect_delisted_tickers, "splits_to_active_tickers"):
        print(f'进行股票拆分后继续交易股票{len(detect_delisted_tickers.splits_to_active_tickers)}个：{detect_delisted_tickers.splits_to_active_tickers}')
    if hasattr(detect_delisted_tickers, "renew_to_active_tickers"):
        print(f'重新作为新ticker继续交易的股票{len(detect_delisted_tickers.renew_to_active_tickers)}个：{detect_delisted_tickers.renew_to_active_tickers}')
    if hasattr(detect_delisted_tickers, "delisted_tickers"):
        print(f'检测到被退市股票{len(detect_delisted_tickers.delisted_tickers)}个: {detect_delisted_tickers.delisted_tickers}')
    else:
        print('没有检测到退市股票')
        print(sorted(detect_delisted_tickers.still_active_tickers, key=lambda x: x[1]))

def fetch_ms_tickers_from_polygon(max_retries=3):
    base_url = "https://api.polygon.io/v3/reference/splits"
    params = {
        "order": "desc",
        "limit": 1000,
        "sort": "execution_date",
        "apiKey": polygon_api_key
    }
    
    all_tickers = []
    page_count = 0
    url = base_url

    last_ms_ticker_date = get_last_stock_splits_updated_utc().strftime("%Y-%m-%d")
    # print(f"Last MS ticker date in database: {last_ms_ticker_date}")
    
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
                
                if page_tickers:
                        # 找到当前页最后一个ticker的execution date
                        last_ticker = page_tickers[-1]
                        execution_date = last_ticker.get("execution_date")
                        print(f"Last ticker's excution date: {execution_date}")

                # 如果没有上一次更新，则初始化更新翻页直到最后一页
                if last_ms_ticker_date is None:    
                    next_url = data.get("next_url")
                    
                    if next_url:
                        print(f"Next URL: {url}")  
                        url = f"{next_url}&apiKey={polygon_api_key}"
                    else:
                        print("No more pages to fetch.")
                        return all_tickers                          
                # 如果有上一次更新，则增量更新
                else:
                    need_next_page = False
                    
                    print(f"Last updated UTC: {last_ms_ticker_date}")
                    if execution_date is not None and execution_date > last_ms_ticker_date:
                        print(f"Continuing to next page.")
                        need_next_page = True
                        
                    if need_next_page:
                        next_url = data.get("next_url")
                        url = f"{next_url}&apiKey={polygon_api_key}"
                        print(f"Next URL: {url}")  
                    else:
                        print("No more pages to fetch based on delisted_utc condition.")
                        next_url = []
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
    
def delisted_confirm(new_tickers=None):
    """
    确认退市股票的状态，并更新数据库
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""select ticker, last_updated_utc from tickers_fundamental where active is null""")
    pending_tickers = cursor.fetchall()
    
    combined_tickers = []
    seen = set()
    
    if new_tickers:
        for t in new_tickers:
            if t['ticker'] not in seen:
                combined_tickers.append((t['ticker'], t['delisted_utc']))
                seen.add(t['ticker'])

    for row in pending_tickers:
        ticker, delisted_utc = row
        if ticker not in seen:
            combined_tickers.append((ticker, delisted_utc))
            seen.add(ticker)
    
    for symbol,delisted_utc in combined_tickers:
        print(f"Confirming delisted status for {symbol} with delisted_utc: {delisted_utc}")
        
        # 检查是否在 stock_splits 中
        cursor.execute("SELECT COUNT(*) FROM stock_splits WHERE ticker = %s AND execution_date = %s", (symbol, delisted_utc))
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"{symbol} is in stock_splits, marking as active.")
            # do nothing, as it is still active
        else:
            print(f"{symbol} is not in stock_splits, checking if it is still active.")
            cursor.execute("SELECT COUNT(*) FROM stock_daily WHERE ticker = %s AND timestamp >= %s", (symbol, delisted_utc))
            count = cursor.fetchone()[0]
            if count > 0:
                # 判断longport是否还能返回delisted_utc之前的K线数据
                past_data=check_ticker_past(symbol, delisted_utc,cursor)
                # 如果没有过去的数据，则标记为非活跃
                if not past_data:
                    print(f"{symbol} has no historical data before delisted_utc, marking as OTC.")
                    cursor.execute("UPDATE tickers_fundamental SET primary_exchange = 'OTCP' WHERE ticker = %s", (symbol,))
                # 如果有过去的数据，则继续判断其是否处于标记观察状态
                else:
                    # 判断是否之前就被标记为观察状态
                    cursor.execute("select active from tickers_fundamental where ticker = %s", (symbol,))
                    active_status = cursor.fetchone()[0]
                    # 如果 active 是 null，说明需要下次再检查
                    if active_status is None:
                        continue    
                    # 如果 active 不是 null，说明需要更新其状态为观察状态，同时将 last_updated_utc 更新为 delisted_utc
                    else:
                        print(f"{symbol} has historical data after delisted_utc, marking as active.")
                        # set active to null, meaning it needs to be checked next time
                        cursor.execute("UPDATE tickers_fundamental SET active = null WHERE ticker = %s", (symbol,))
                        cursor.execute("UPDATE tickers_fundamental SET last_updated_utc = %s WHERE ticker = %s", (delisted_utc, symbol))
            else:
                print(f"{symbol} has no data after delisted_utc, marking as inactive.")
                cursor.execute("UPDATE tickers_fundamental SET active = FALSE WHERE ticker = %s", (symbol,))
    
    conn.commit()
    cursor.close()
    conn.close()
    
def check_ticker_past(symbol, delisted_utc, cursor):
    """
    检查 ticker 在 delisted_utc 之前是否有历史数据
    """
    config = Config.from_env()
    ctx = QuoteContext(config=config)

    cursor.execute("SELECT primary_exchange, type FROM tickers_fundamental WHERE ticker = %s", (symbol,))
    result = cursor.fetchone()
    
    if not result:
        print(f"[WARN] ticker {symbol} not found in tickers_fundamental.")
        logger.warning(f"Ticker {symbol} not found in tickers_fundamental.")
        return False
    primary_exchange, ticker_type = result
    
    try:
        cleaned_symbol = clean_symbol_for_postgres(symbol, ticker_type, primary_exchange)
        resp = ctx.candlesticks(f"{cleaned_symbol}.US", Period.Day, 1000, AdjustType.ForwardAdjust)
        delisted_utc_str = delisted_utc.replace('Z', '')
        delisted_utc_dt = datetime.strptime(delisted_utc_str, "%Y-%m-%dT%H:%M:%S")
        if resp and hasattr(resp[0], "timestamp"):
            found = False
            for candle in resp:
                ts = getattr(candle, "timestamp", None)
                # print(f"Ticker: {cleaned_symbol}, delisted_utc: {ticker['delisted_utc']}, {ts}")
                if ts:
                    if isinstance(ts, datetime):
                        ts_dt = ts
                    else:
                        ts_dt = datetime.strptime(ts.replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")
                    # 判断是否有timestamp早于delisted_utc_dt至少3天
                    if ts_dt <= delisted_utc_dt - timedelta(days=3):
                        # print(f"Ticker {cleaned_symbol} is still active after delisting on {delisted_utc}.")
                        found = True
            if not found:          
                print(f"Ticker {cleaned_symbol} has renewed trading after delisted.")
                return False
            else:
                # print(f"Ticker {cleaned_symbol} has historical data before delisted.")
                return True
    except Exception as e:
        print(f"Error checking past data for {cleaned_symbol}: {e}")
        return False
            
# 1.       
def process_ms(limit_date):
    
    # ms_tickers为增量更新获取ms股票数据
    ms_tickers = fetch_ms_tickers_from_polygon()
    
    # 只保留有 listing_date 且大于 上次更新日期 的
    ms_update_utc = get_last_stock_splits_updated_utc().strftime("%Y-%m-%d")
    
    ms_filtered = [
        t for t in ms_tickers
        if "execution_date" in t and t["execution_date"] > ms_update_utc and t["execution_date"] <= limit_date
    ]
    
    insert_ms_to_stock_splits(ms_filtered) # 将 MS 数据插入数据库stock_splits表
    
    # ms_future_filtered = [
    #     t for t in ms_tickers
    #     if "execution_date" in t and t["execution_date"] > limit_date
    # ]
    
    # print(f"Total ms tickers fetched: {converted_tickers_info}")
    # print(f"Total ms tickers fetched: {len(ms_filtered)}")
    # print(f"Total future ms tickers fetched: {len(ms_future_filtered)}")
# 2.
def process_delisted(limit_date):
    # 获取截至上一更新日期到今天最新的退市股票的列表
    delisted_tickers = delisted_incremental_update(limit_date)
    
    # 检测从polygon.io API获取到的delisted_tickers股票的具体退市状态
    ###### detect_delisted_tickers(delisted_tickers)
    
    # 标记检测退市股票状态并更新active状态到tickers_fundamental数据库
    delisted_confirm(delisted_tickers)    
# 3.stock_daily

# 4.
def process_delisted_reverse(ms_filtered):
    cutoff_date = date(2025, 6, 19)  # 设置时间门槛

    converted_tickers_info = [
        (
            item['ticker'],
            execution_date,
            float(item['split_from']),
            float(item['split_to'])
        )
        for item in ms_filtered
        if (execution_date := datetime.strptime(item['execution_date'], "%Y-%m-%d").date()) >= cutoff_date
    ]

    revese_all_histroical_before_ms(converted_tickers_info)
# 4.最后一步 更新IPO数据 #### ipo_incremental_update()
def ipo_incremental_update(limit_date):
    
    last_updated_time = get_last_tickers_fundamental_updated_utc().strftime("%Y-%m-%d")
    
    print("Starting to fetch all ipo tickers from Polygon.io using HTTP...")
    ipo_tickers = fetch_ipo_tickers_from_polygon(polygon_api_key, last_updated_time)
    
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
        print(t["ticker"], t["listing_date"], end=' ')

    for t in ipo_future_filtered:
        print(t["ticker"], t["listing_date"], "Future IPO")
        
        
    # 过滤掉不需要的类型
    EXCLUDED_TYPES = ("FUND", "INDEX", "PFD", "RIGHT", "SP", "UNIT", "WARRANT")
    ipo_filtered_tickers = [
        (
            t.get("ticker", ""),
            t.get("issuer_name", ""),
            t.get("security_type", ""),
            t.get("active", True),
            t.get("primary_exchange", ""),
            t.get("listing_date")
        )
        for t in ipo_filtered
        if t.get("security_type", "") not in EXCLUDED_TYPES
    ]
    
    if ipo_filtered_tickers:
        # print(f"Total tickers to insert: {tickers}")
        insert_tickers_to_tickers_fundamental(ipo_filtered_tickers) # 将 IPO 数据插入数据库tickers_fundamental表
        fetch_data_from_longprot_to_stock_daily(ipo_filtered_tickers) # 从 LongPort 获取 IPO 数据并保存到 stock_daily 表


# if __name__ == "__main__":
    
    # 1. 更新获取stock_splits
    # 2. 获取delisted, 标记到tickers_fundamental表的delisted_statue为on,then check if it's on stock_splits, if yes: off, else:on
    # 3. 单独run statue is on tickers, 尝试获取其上一交易日数据, 若无,则标记成OTC, 若有,则继续为on
    # 4. 更新stock_incremental_update, run everystock, if failed ,check if its statue is on, if yes: delisted