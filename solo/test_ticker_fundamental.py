import psycopg2
from longport.openapi import QuoteContext, Config, Period, AdjustType, OpenApiException
from config.db_config import DB_CONFIG  # 数据库配置
from config.paths import ERRORstock_PATH  # 错误日志路径
import csv
from datetime import datetime

# 数据库连接函数
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# 从数据库获取所有 ticker
def fetch_tickers_from_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ticker FROM tickers_fundamental WHERE active = true")
    tickers = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    print(f"Fetched {len(tickers)} tickers from database")
    return tickers

# 测试 LongPort API 并记录失败的 ticker
def test_longport_api(tickers):
    config = Config.from_env()  # 从环境变量加载 LongPort 配置
    ctx = QuoteContext(config)
    
    success_count = 0
    failed_tickers = []  # 存储失败的 ticker 和错误信息
    
    for ticker in tickers:
        symbol = f"{ticker}.US"  # 添加 .US 后缀
        try:
            # 测试获取 1 天日线 K 线数据
            resp = ctx.candlesticks(symbol, Period.Day, 1, AdjustType.ForwardAdjust)
            print(f"Success: {symbol} - Retrieved {len(resp)} candlesticks")
            success_count += 1
        except OpenApiException as e:
            error_msg = f"OpenApiException: {e}"
            print(f"Failed: {symbol} - {error_msg}")
            failed_tickers.append((ticker, error_msg))
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            print(f"Failed: {symbol} - {error_msg}")
            failed_tickers.append((ticker, error_msg))
    
    # 打印总结
    print(f"\nSummary:")
    print(f"Total tickers tested: {len(tickers)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {len(failed_tickers)}")
    
    # 将失败的 ticker 写入 CSV
    if failed_tickers:
        with open(ERRORstock_PATH, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Ticker", "Error Message", "Timestamp"])  # CSV 表头
            for ticker, error in failed_tickers:
                writer.writerow([ticker, error, datetime.now().isoformat()])
        print(f"Failed tickers logged to: {ERRORstock_PATH}")

# 主函数
def main():
    print("Starting LongPort API test with tickers from database...")
    tickers = fetch_tickers_from_db()
    if not tickers:
        print("No tickers found in database. Exiting.")
        return
    test_longport_api(tickers)

if __name__ == "__main__":
    main()