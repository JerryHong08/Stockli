# this file is for ms feature add in.
# 1. I need to check if the stock had a merge/splits betweeen 2024-12-01(not include) and 2025-6-19(not include) also in stock_daily.
# 2. if not, just skip. if so, catch all since 2024-12-01 to today and replace them in stock_daily.
# 3. then cut from 2024-12-01 to the latest ms date, transform stock_daily price and volume with the splits ratio.
# 4. transform every price dates before ms date with the splits ratio

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import psycopg2
import requests
import time
from datetime import datetime
from src.config.db_config import DB_CONFIG
from longport.openapi import QuoteContext, Config, OpenApiException
from src.data_fetcher.batch_fetcher import BatchDataFetcher
from src.database.db_connection import get_engine
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import QThread

app = QApplication(sys.argv)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# 获取2024-12-01到今天，2025-06-19的所有ms的tickery and splits ratio
def fetch_ms_tickers(start_timestamp, end_timestamp):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ss.ticker, ss.execution_date, ss.split_from, ss.split_to
        FROM stock_splits ss
        WHERE ss.execution_date BETWEEN %s AND %s
          AND ss.ticker IN (
              SELECT DISTINCT sd.ticker
              FROM stock_daily sd
          )
        ORDER BY ss.execution_date ASC;
    """, (start_timestamp,end_timestamp))
    result = cursor.fetchall()
    cursor.close()
    conn.close()    
    return result

# 删去stock_daily里从2024-12-01到今天
def delete_data(tickers):
    if not tickers:
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    # 使用参数化防止 SQL 注入
    sql = f"""
        DELETE FROM stock_daily
        WHERE ticker = ANY(%s) AND timestamp >= %s;
    """
    cursor.execute(sql, (tickers, '2024-12-01'))

    print(f"已删除 {cursor.rowcount} 行数据。")
    conn.commit()
    cursor.close()
    conn.close()


def fetch_last_201_rows(ticker):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, timestamp, open, high, low, close, volume
        FROM stock_daily
        WHERE ticker = %s
        ORDER BY timestamp DESC
        LIMIT 201;
    """, (ticker,))

    rows = cursor.fetchall()  # 每行为一个元组
    cursor.close()
    conn.close()
    return rows

def adjust_ticker_split(ticker, execution_date, split_from, split_to):
    ratio = split_to / split_from  # 比如 1 / 15
    rows = fetch_last_201_rows(ticker)

    # 只调整 execution_date 之前的行
    execution_date_dt = datetime.combine(execution_date, datetime.min.time())
    to_update = [row for row in rows if row[1] < execution_date_dt]

    print(to_update)
    
    conn = get_db_connection()
    cursor = conn.cursor()

    for row in to_update:
        row_id, ts, o, h, l, c, v = row

        new_o = round(o * ratio, 3)
        new_h = round(h * ratio, 3)
        new_l = round(l * ratio, 3)
        new_c = round(c * ratio, 3)
        new_v = int(round(v / ratio))  # volume 反向变换，split_from 较大则 volume 应变大
        print(new_o)
        cursor.execute("""
            UPDATE stock_daily
            SET open = %s, high = %s, low = %s, close = %s, volume = %s
            WHERE id = %s;
        """, (new_o, new_h, new_l, new_c, new_v, row_id))

    conn.commit()
    cursor.close()
    conn.close()

def update_stock_daily_with_ms_ratio(tickers_info):
    for ticker, execution_date, split_from, split_to in tickers_info:
        print(f"调整 {ticker} 的拆股数据（{split_from} -> {split_to}）...")
        adjust_ticker_split(ticker, execution_date, split_from, split_to)

def fetch_all_rows(ticker):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, timestamp, open, high, low, close, volume
        FROM stock_daily
        WHERE ticker = %s
        ORDER BY timestamp ASC;
    """, (ticker,))
    
    rows = cursor.fetchall()
    
    cursor.close()
    conn.close()

    return rows  # 每行是一个 tuple: (id, timestamp, open, high, low, close, volume)

def reverse_historical(ticker, execution_date, split_from, split_to):
    ratio = split_to / split_from  # 比如 1 / 15
    rows = fetch_all_rows(ticker)

    # 只调整 execution_date 之前的行
    execution_date_dt = datetime.combine(execution_date, datetime.min.time())
    to_update = [row for row in rows if row[1] < execution_date_dt]
    
    conn = get_db_connection()
    cursor = conn.cursor()

    for row in to_update:
        row_id, ts, o, h, l, c, v = row

        new_o = round(o / ratio, 3)
        new_h = round(h / ratio, 3)
        new_l = round(l / ratio, 3)
        new_c = round(c / ratio, 3)
        new_v = int(round(v * ratio))  # volume 反向变换，split_from 较大则 volume 应变大
        # print(new_o,ts)
        cursor.execute("""
            UPDATE stock_daily
            SET open = %s, high = %s, low = %s, close = %s, volume = %s
            WHERE id = %s;
        """, (new_o, new_h, new_l, new_c, new_v, row_id))

    conn.commit()
    cursor.close()
    conn.close()

def revese_all_histroical_before_ms(tickers_info):
    for ticker, execution_date, split_from, split_to in tickers_info:
        print(f"调整 {ticker} 的拆股数据（{split_from} -> {split_to}）...")
        reverse_historical(ticker, execution_date, split_from, split_to)

if __name__== "__main__":
    
    # 获取哪些进行过ms
    tickers_info = fetch_ms_tickers('2025-06-19','2025-06-20')
    tickers = list({row[0] for row in tickers_info})
    print(tickers_info)
    # 删去这些ticker从24-12-01到今天的数据
    # delete_data(tickers)
    
    # 重新获取从24-12-01到今天的数据
    # fetcher = BatchDataFetcher(tickers)
    # fetcher.start()
    # fetcher.wait()  # 等待线程执行完，防止提前销毁
    
    ## 这里有个bug 是重新获取数据是覆盖了从24-08-29到25-06-18，最大total为201行数据
    
    #将所有这些ticker在stock_daily的行数按照timestamp往回201行，每一行的ochlv的数值都按照split_from和split_to进行转化
    # update_stock_daily_with_ms_ratio(tickers_info)
    
    # 将所有历史数据都reverse回来
    ##### revese_all_histroical_before_ms(tickers_info)