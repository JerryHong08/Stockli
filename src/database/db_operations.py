import pandas as pd
from sqlalchemy import MetaData, Table, text, select, DDL
from functools import lru_cache
import pytz
from utils.logger import setup_logger
import logging
from sqlalchemy.exc import SQLAlchemyError
from database.db_connection import DatabaseConnectionError

# At the top of the file, modify the logger initialization
try:
    logger = setup_logger("db_operations")
except Exception as e:
    print(f"Warning: Could not set up logger: {e}")
    logger = logging.getLogger("db_operations")
    logger.addHandler(logging.StreamHandler())

def fetch_data_from_db(table_name, engine, limit=None):
    """
    从数据库中读取指定表的数据，并返回一个 Pandas DataFrame。
    """
    try:
        query = f"""
        SELECT timestamp, open_price, high_price, low_price, close_price, volume
        FROM "{table_name}"
        ORDER BY timestamp DESC
        """
        if limit is not None:
            query += f" LIMIT {limit}"
        query += ";"
        df = pd.read_sql_query(query, engine)
        df.rename(
            columns={
                "timestamp": "Date",
                "open_price": "Open",
                "high_price": "High",
                "low_price": "Low",
                "close_price": "Close",
                "volume": "Volume",
            },
            inplace=True,
        )
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.sort_values(by="Date")  # 按日期升序排列
        logger.info(df.iloc[-1])  # Log the latest column of the DataFrame
        return df
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

def fetch_table_names(engine):
    """
    获取数据库中的所有表名。
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
            """))
            return [row[0] for row in result.fetchall()]
    except SQLAlchemyError as e:
        logger.error(f"Error fetching table names: {e}")
        return []

def create_table_if_not_exists(engine, table_name):
    """动态创建表，确保表结构符合要求。"""
    try:
        # 清洗表名
        table_name = clean_symbol_for_postgres(table_name)
        
        # 创建一个有效的索引名（移除空格和特殊字符）
        index_name = f"idx_{table_name.replace(' ', '_')}_timestamp"

        create_table_ddl = DDL(f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL UNIQUE,
                open_price DECIMAL(18, 6),
                high_price DECIMAL(18, 6),
                low_price DECIMAL(18, 6),
                close_price DECIMAL(18, 6),
                volume BIGINT,
                turnover DECIMAL(18, 6)
            );
            CREATE INDEX IF NOT EXISTS {index_name} ON "{table_name}"(timestamp);
        """)

        with engine.connect() as conn:
            conn.execute(create_table_ddl)
            conn.commit()
            print(f"Table '{table_name}' is ready.")
            
    except SQLAlchemyError as e:
        print(f"创建表失败: {e}")
        raise DatabaseConnectionError(f"创建表失败: {e}")

def clean_symbol_for_postgres(symbol):
    """清洗股票代码中的特殊符号，用于 PostgreSQL 表名。"""
    if not symbol:
        raise ValueError("股票代码不能为空")

    # 去除前后空格
    cleaned_symbol = symbol.strip()
    
    # 将中间的空格替换为下划线
    cleaned_symbol = cleaned_symbol.replace(" ", "_")
    
    # 将其他特殊字符替换为下划线
    cleaned_symbol = cleaned_symbol.replace("^", "_").replace("/", "_").replace("-", "_")
    
    # 如果清洗后的表名为空，抛出异常
    if not cleaned_symbol:
        raise ValueError(f"清洗后的表名为空: {symbol}")

    return cleaned_symbol

def save_to_table(data, table_name, engine, batch_size=1000):
    """
    将数据保存到 PostgreSQL 数据库的指定表中。

    :param data: 要保存的数据
    :param table_name: 表名
    :param engine: SQLAlchemy 引擎
    :param batch_size: 批量插入的大小，默认1000条
    """
    try:
        # 清洗表名
        table_name = clean_symbol_for_postgres(table_name)

        # 确保表存在
        create_table_if_not_exists(engine, table_name)

        # 设置纽约时区
        ny_tz = pytz.timezone('America/New_York')

        # 数据转换为元组
        values = [
            (
                candlestick.timestamp.astimezone(ny_tz).replace(tzinfo=None),  # 转换为纽约时间并去除时区信息
                float(candlestick.open),
                float(candlestick.high),
                float(candlestick.low),
                float(candlestick.close),
                candlestick.volume,
                float(candlestick.turnover)
            )
            for candlestick in data
        ]

        # 分批次插入数据
        total_inserted = 0
        with engine.connect() as conn:
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                conn.execute(
                    text(f"""
                        INSERT INTO "{table_name}" 
                        (timestamp, open_price, high_price, low_price, close_price, volume, turnover)
                        VALUES :values
                        ON CONFLICT (timestamp) DO NOTHING
                    """), 
                    [{"values": row} for row in batch]
                )
                conn.commit()
                total_inserted += len(batch)
                logger.info(f"Inserted {len(batch)} records to table '{table_name}' (total: {total_inserted})")

    except ValueError as e:
        logger.error(f"表名无效: {e}")
        raise DatabaseConnectionError(f"表名无效: {e}")
    except SQLAlchemyError as e:
        logger.error(f"保存数据失败: {e}")
        raise DatabaseConnectionError(f"保存数据失败: {e}")
