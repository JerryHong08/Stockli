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

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# 获取2024-12-01到今天，2025-06-19的所有ms的tickery and splits ratio
def fetch_ms_tickers():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ss.ticker, ss.execution_date, ss.split_from, ss.split_to
        FROM stock_splits ss
        WHERE ss.execution_date BETWEEN '2024-12-01' AND '2025-06-19'
          AND ss.ticker IN (
              SELECT DISTINCT sd.ticker
              FROM stock_daily sd
          );
    """)
    result = cursor.fetchall()
    cursor.close()
    conn.close()    
    return result

# 删去stock_daily里从2024-12-01到今天
def delete_data():
    return

# 重新获取从24-12-01到今天的数据
def insert_new_data():
    stock_symbols = fetch_ms_tickers()
    BatchDataFetcher(stock_symbols)
    return

def update_stock_daily_with_ms_ratio():
    engine = get_engine
    return

if __name__== "__main__":
    print(fetch_ms_tickers())