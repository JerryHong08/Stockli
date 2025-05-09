import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from utils.logger import setup_logger
from database.db_connection import DatabaseConnectionError
import pytz
from config.db_config import DB_CONFIG  # 数据库配置
import psycopg2
from longport.openapi import QuoteContext, Config, Period, AdjustType, OpenApiException

logger = setup_logger("db_operations")
ny_tz = pytz.timezone('America/New_York')

# def clean_symbol_for_postgres(symbol):
#     """规范化ticker name"""
#     if not symbol:
#         raise ValueError("股票代码不能为空")
#     cleaned_symbol = symbol.strip()
#     cleaned_symbol = cleaned_symbol.replace(" ", "")
#     cleaned_symbol = cleaned_symbol.replace("^", "-").replace("/", ".")
#     if not cleaned_symbol:
#         raise ValueError(f"清洗后的表名为空: {symbol}")
#     return cleaned_symbol

# LongPort API 配置
config = Config.from_env()  # 从环境变量加载 LongPort 配置
ctx = QuoteContext(config)

# 数据库连接函数
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# 测试 LongPort API 是否能获取数据
def test_ticker_api(ticker):
    try:
        resp = ctx.candlesticks(f"{ticker}.US", Period.Day, 1, AdjustType.ForwardAdjust)
        return True, len(resp)  # 返回成功状态和数据条数
    except OpenApiException as e:
        return False, f"OpenApiException: {e}"
    except Exception as e:
        return False, f"Unexpected error: {e}"
    
# 检查数据库中是否存在某个 ticker
def check_ticker_exists(ticker):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM tickers_fundamental WHERE ticker = %s", (ticker,))
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count > 0

def clean_symbol_for_postgres(ticker, ticker_type, primary_exchange):
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
    
    return cleaned_ticker

def fetch_data_from_db(ticker, engine, limit=None):
    """从 stock_daily 表读取指定 ticker 的数据，返回 DataFrame"""
    try:
        query = """
        SELECT timestamp, open, high, low, close, volume
        FROM stock_daily
        WHERE ticker = :ticker
        ORDER BY timestamp DESC
        """
        if limit is not None:
            query += f" LIMIT {limit}"
        query += ";"
        df = pd.read_sql_query(
            text(query),
            engine,
            params={"ticker": ticker}
        )
        df.rename(
            columns={
                "timestamp": "Date",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
            },
            inplace=True,
        )
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.sort_values(by="Date")
        if not df.empty:
            logger.info(df.iloc[-1])
        return df
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

def fetch_table_names(engine):
    """获取 stock_daily 中的所有 ticker"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT DISTINCT ticker
                FROM stock_daily
            """))
            return [row[0] for row in result.fetchall()]
    except SQLAlchemyError as e:
        logger.error(f"Error fetching tickers: {e}")
        return []

def get_latest_timestamp(ticker, engine):
    """获取 stock_daily 中指定 ticker 的最新时间戳"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT timestamp 
                FROM stock_daily
                WHERE ticker = :ticker
                ORDER BY timestamp DESC
                LIMIT 1
            """), {"ticker": ticker})
            row = result.fetchone()
            return row[0] if row else None
    except SQLAlchemyError as e:
        logger.error(f"获取 {ticker} 最新日期失败: {e}")
        return None

def should_skip_save(api_data, ticker, engine):
    """判断是否需要跳过保存"""
    if not api_data:
        return True
    
    api_latest = max(candlestick.timestamp for candlestick in api_data)
    api_latest_ny = api_latest.astimezone(ny_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    
    db_latest = get_latest_timestamp(ticker, engine)
    if db_latest is None:
        return False
    
    db_latest_ny = db_latest.astimezone(ny_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    return api_latest_ny.date() == db_latest_ny.date()

def save_to_table(data, ticker, engine, batch_size=1000):
    """将数据保存到 stock_daily，按 ticker 和 timestamp 处理"""
    if should_skip_save(data, ticker, engine):
        logger.info(f"跳过保存 {ticker}，数据已是最新")
        return
    
    logger.debug(f"开始保存数据到 stock_daily (ticker: {ticker})")
    try:
        values = [
            (
                ticker,
                candlestick.timestamp.astimezone(ny_tz).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None),
                float(candlestick.open),
                float(candlestick.high),
                float(candlestick.low),
                float(candlestick.close),
                candlestick.volume,
                float(candlestick.turnover)
            )
            for candlestick in data
        ]

        total_inserted = 0
        with engine.connect() as conn:
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                conn.execute(
                    text("""
                        INSERT INTO stock_daily 
                        (ticker, timestamp, open, high, low, close, volume, turnover)
                        VALUES (:ticker, :timestamp, :open, :high, :low, :close, :volume, :turnover)
                        ON CONFLICT (ticker, timestamp) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            turnover = EXCLUDED.turnover
                    """),
                    [{"ticker": row[0], "timestamp": row[1], "open": row[2], "high": row[3], 
                      "low": row[4], "close": row[5], "volume": row[6], "turnover": row[7]} 
                     for row in batch]
                )
                conn.commit()
                total_inserted += len(batch)
                logger.info(f"Inserted/Updated {len(batch)} records for {ticker} (total: {total_inserted})")
    except SQLAlchemyError as e:
        logger.error(f"保存数据失败 (ticker: {ticker}): {e}")
        raise DatabaseConnectionError(f"保存数据失败: {e}")