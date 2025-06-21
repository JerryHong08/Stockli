# this file is for ms incremental update.

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

# 获取时间区间内的ticker以及其splits ratio
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
    tickers_info = fetch_ms_tickers('2024-12-01','2025-03-01')
    tickers = list({row[0] for row in tickers_info})
    print(tickers_info)
    
    # 将所有历史数据都reverse回来
    ##### revese_all_histroical_before_ms(tickers_info)
    # incremental update stock ms into stock_splits
    # incremental update stock_daily. write down the failed
    # 
    