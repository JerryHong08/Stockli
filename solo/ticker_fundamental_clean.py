# 用于实验测试确认清洗逻辑
import psycopg2
import csv
from longport.openapi import QuoteContext, Config, Period, AdjustType, OpenApiException
from src.config.db_config import DB_CONFIG  # 数据库配置
from src.config.paths import ERRORstock_PATH  # 错误日志路径
from datetime import datetime

config = Config.from_env()  # 从环境变量加载 LongPort 配置
ctx = QuoteContext(config)
    
# 数据库连接函数
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# 检查数据库中是否存在某个 ticker
def check_ticker_exists(ticker):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM tickers_fundamental WHERE ticker = %s", (ticker,))
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count > 0

# 测试 LongPort API 是否能获取数据
def test_ticker_api(ticker):
    try:
        resp = ctx.candlesticks(ticker, Period.Day, 1, AdjustType.ForwardAdjust)
        return True, len(resp)  # 返回成功状态和数据条数
    except OpenApiException as e:
        return False, f"OpenApiException: {e}"
    except Exception as e:
        return False, f"Unexpected error: {e}"

# 清洗函数（添加 .US）
def clean_ticker(ticker, ticker_type, primary_exchange):
    cleaned_ticker = ticker
    
    # 规则 1：如果交易所类型是 XNAS，删除 "."
    if primary_exchange == "XNAS":
        cleaned_ticker = cleaned_ticker.replace(".", "")
        
    # 规则 2：如果类型是 WARRANT
    if ticker_type == "WARRANT":
        # 先测试原始 ticker 是否有效
        success, _ = test_ticker_api(cleaned_ticker)
        if success:
            cleaned_ticker = cleaned_ticker  # 如果有效，不做变化
        # 如果失败，执行后续逻辑
        else:
            if ".WS" in cleaned_ticker:
                base_ticker = cleaned_ticker.split(".WS")[0]  # 提取 .WS 前的部分
                if check_ticker_exists(base_ticker):
                    # 检查 .WS 后是否还有 "."
                    if ".WS." in cleaned_ticker:
                        # 替换 .WS 和其后的 . 为 +
                        cleaned_ticker = cleaned_ticker.replace(".WS.", "+")
                    else:
                        # 仅替换 .WS 为 +
                        cleaned_ticker = cleaned_ticker.replace(".WS", "+")
                else:
                    # 如果 base_ticker 不存在，删除 .WS 及之后的内容
                    cleaned_ticker = cleaned_ticker.split(".WS")[0]
    
    # 规则 3：如果类型是 PFD 且包含小写 "p"，替换为 "-"
    if ticker_type in ("PFD", "SP") and "p" in cleaned_ticker.lower():
        cleaned_ticker = cleaned_ticker.replace("p", "-")
        
    # 规则 4：如果类型是 RIGHT 且包含小写 "r"，替换为 ".RT"
    if ticker_type == "RIGHT" and "r" in cleaned_ticker.lower():
        cleaned_ticker = cleaned_ticker.replace("r", ".RT")
    
    # 添加 .US 后缀
    cleaned_ticker = f"{cleaned_ticker}.US"
    
    return cleaned_ticker

# 从 error_log_enriched.csv 获取失败的 ticker
def get_failed_tickers():
    error_log_path = ERRORstock_PATH.replace('error_log.csv', 'error_log_enriched.csv')
    with open(error_log_path, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return {row['Ticker'] for row in reader}

# 获取成功的 ticker 列表
def fetch_successful_tickers():
    failed_tickers = get_failed_tickers()
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ticker, type, primary_exchange 
        FROM tickers_fundamental 
        WHERE active = true 
        AND ticker NOT IN %s
    """, (tuple(failed_tickers),))
    successful_tickers = [(row[0], row[1], row[2]) for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    print(f"Found {len(successful_tickers)} successful tickers")
    return successful_tickers

# 测试 LongPort API 并记录失败的 ticker
def test_longport_api(tickers_info):
    config = Config.from_env()
    ctx = QuoteContext(config)
    
    success_count = 0
    failed_tickers = []
    
    for ticker, original_ticker, ticker_type, primary_exchange in tickers_info:
        try:
            resp = ctx.candlesticks(ticker, Period.Day, 1, AdjustType.ForwardAdjust)
            print(f"Success: {ticker} - Retrieved {len(resp)} candlesticks")
            success_count += 1
        except OpenApiException as e:
            error_msg = f"OpenApiException: {e}"
            print(f"Failed: {ticker} - {error_msg}")
            failed_tickers.append((original_ticker, ticker, ticker_type, primary_exchange, error_msg))
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            print(f"Failed: {ticker} - {error_msg}")
            failed_tickers.append((original_ticker, ticker, ticker_type, primary_exchange, error_msg))
    
    print(f"\nSummary:")
    print(f"Total tickers tested: {len(tickers_info)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {len(failed_tickers)}")
    
    return failed_tickers

# 主处理函数
def process_tickers():
    # 读取 error_log_enriched.csv
    error_log_path = ERRORstock_PATH.replace('error_log.csv', 'error_log_enriched.csv')
    with open(error_log_path, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        error_rows = list(reader)
    
    # 清洗失败的 ticker 并准备测试
    tickers_to_test = []
    for row in error_rows:
        original_ticker = row['Ticker']
        ticker_type = row['Type']
        primary_exchange = row['Primary Exchange']
        cleaned_ticker = clean_ticker(original_ticker, ticker_type, primary_exchange)
        tickers_to_test.append((cleaned_ticker, original_ticker, ticker_type, primary_exchange))
    
    # 测试清洗后的 ticker 并记录失败结果
    failed_tickers = test_longport_api(tickers_to_test)
    
    # 写入测试失败的 ticker
    if failed_tickers:
        error_output_path = ERRORstock_PATH.replace('error_log.csv', 'error_log_enriched_errorout.csv')
        with open(error_output_path, mode='w', newline='', encoding='utf-8') as f:
            fieldnames = ['Original Ticker', 'Cleaned Ticker', 'Type', 'Primary Exchange', 'Error Message', 'Timestamp']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for original_ticker, cleaned_ticker, ticker_type, primary_exchange, error_msg in failed_tickers:
                writer.writerow({
                    'Original Ticker': original_ticker,
                    'Cleaned Ticker': cleaned_ticker,
                    'Type': ticker_type,
                    'Primary Exchange': primary_exchange,
                    'Error Message': error_msg,
                    'Timestamp': datetime.now().isoformat()
                })
        print(f"Failed tickers after cleaning saved to: {error_output_path}")
    
    # 检查成功的 ticker
    successful_tickers = fetch_successful_tickers()
    cleaned_success_rows = []
    for ticker, ticker_type, primary_exchange in successful_tickers:
        cleaned_ticker = clean_ticker(ticker, ticker_type, primary_exchange)
        if cleaned_ticker != f"{ticker}.US":  # 如果清洗后有变化（排除仅添加 .US 的情况），记录下来
            cleaned_success_rows.append({
                'Ticker': ticker,
                'Type': ticker_type,
                'Primary Exchange': primary_exchange,
                'Cleaned Ticker': cleaned_ticker
            })
    
    # 写入符合清洗规则的成功 ticker
    if cleaned_success_rows:
        success_output_path = ERRORstock_PATH.replace('error_log.csv', 'success_log_enriched.csv')
        with open(success_output_path, mode='w', newline='', encoding='utf-8') as f:
            fieldnames = ['Ticker', 'Type', 'Primary Exchange', 'Cleaned Ticker']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(cleaned_success_rows)
        print(f"Successful tickers matching cleaning rules saved to: {success_output_path}")
    else:
        print("No successful tickers matched the cleaning rules beyond adding .US.")

# 主函数
def main():
    print("Starting ticker cleaning and testing process...")
    process_tickers()

if __name__ == "__main__":
    main()