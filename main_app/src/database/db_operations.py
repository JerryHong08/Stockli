import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from utils.logger import setup_logger
from database.db_connection import DatabaseConnectionError
import pytz

logger = setup_logger("db_operations")
ny_tz = pytz.timezone('America/New_York')

def clean_symbol_for_postgres(symbol):
    """规范化ticker name"""
    if not symbol:
        raise ValueError("股票代码不能为空")
    cleaned_symbol = symbol.strip()
    cleaned_symbol = cleaned_symbol.replace(" ", "")
    cleaned_symbol = cleaned_symbol.replace("^", "-").replace("/", ".")
    if not cleaned_symbol:
        raise ValueError(f"清洗后的表名为空: {symbol}")
    return cleaned_symbol

def fetch_data_from_db(ticker, engine, limit=None):
    """从 stock_daily 表读取指定 ticker 的数据，返回 DataFrame"""
    ticker = clean_symbol_for_postgres(ticker)
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
    ticker = clean_symbol_for_postgres(ticker)
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
    ticker = clean_symbol_for_postgres(ticker)
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
    ticker = clean_symbol_for_postgres(ticker)
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