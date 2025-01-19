import pandas as pd
from sqlalchemy import MetaData, Table, text, select, DDL
from functools import lru_cache
import pytz
ny_tz = pytz.timezone('America/New_York')  # 定义纽约时区
from utils.logger import setup_logger
import logging
from sqlalchemy.exc import SQLAlchemyError
from database.db_connection import DatabaseConnectionError

# Initialize logger
logger = setup_logger("db_operations")

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
        logger.error(f"Error fetching data from{table_name}: {e}")
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
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # 清洗表名
            table_name = clean_symbol_for_postgres(table_name)
            index_name = f'idx_{table_name}_timestamp'

            with engine.connect() as conn:
                # 设置事务隔离级别为SERIALIZABLE
                conn.execute(text("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"))
                
                # 开始事务
                try:
                    # 创建表
                    conn.execute(text(f"""
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
                    """))
                    
                    # 检查索引是否存在
                    result = conn.execute(text(f"""
                        SELECT 1
                        FROM pg_indexes
                        WHERE tablename = :table_name
                        AND indexname = :index_name
                    """), {"table_name": table_name, "index_name": index_name})
                    
                    # 如果索引不存在则创建
                    if not result.fetchone():
                        conn.execute(text(f"""
                            CREATE INDEX "{index_name}" ON "{table_name}"(timestamp);
                        """))
                        logger.debug(f"新创建表： '{table_name}' ")
                    
                    # 提交事务
                    conn.commit()
                    return
                except Exception as e:
                    # 回滚事务
                    conn.rollback()
                    raise e
                    
        except SQLAlchemyError as e:
            retry_count += 1
            if retry_count == max_retries:
                print(f"创建表失败: {e}")
                raise DatabaseConnectionError(f"创建表失败: {e}")
            continue

def clean_symbol_for_postgres(symbol):
    """清洗股票代码中的特殊符号，用于 PostgreSQL 表名。"""
    if not symbol:
        raise ValueError("股票代码不能为空")

    # 去除前后空格
    cleaned_symbol = symbol.strip()
    
    # 将中间的空格替换为空
    cleaned_symbol = cleaned_symbol.replace(" ", "")
    
    # 将特殊字符替换为指定字符
    cleaned_symbol = cleaned_symbol.replace("^", "-").replace("/", ".")
    
    # 如果清洗后的表名为空，抛出异常
    if not cleaned_symbol:
        raise ValueError(f"清洗后的表名为空: {symbol}")

    return cleaned_symbol

def clean_duplicate_timestamps(table_name, engine):
    """
    清理重复的时间戳数据，删除所有时间戳不是00:00:00的数据
    """
    try:
        # 清洗表名
        cleaned_table_name = clean_symbol_for_postgres(table_name)
        with engine.connect() as conn:
            # 开始事务
            conn.execute(text("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"))
            try:
                 # 先查询有多少条记录需要删除
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM "{cleaned_table_name}"
                    WHERE EXTRACT(HOUR FROM timestamp) != 0
                    OR EXTRACT(MINUTE FROM timestamp) != 0
                    OR EXTRACT(SECOND FROM timestamp) != 0;
                """))
                count = result.fetchone()[0]
                
                if count > 0:
                    # 删除所有时间戳不是00:00:00的数据
                    conn.execute(text(f"""
                        DELETE FROM "{cleaned_table_name}"
                        WHERE EXTRACT(HOUR FROM timestamp) != 0
                        OR EXTRACT(MINUTE FROM timestamp) != 0
                        OR EXTRACT(SECOND FROM timestamp) != 0;
                    """))
                    conn.commit()
                    logger.info(f"Cleaned {count} duplicate timestamps in table '{table_name}'")
                # 如果没有需要删除的记录，则不记录日志
            except Exception as e:
                conn.rollback()
                logger.error(f"Error cleaning timestamps: {e}")
                raise DatabaseConnectionError(f"Error cleaning timestamps: {e}")
                
    except SQLAlchemyError as e:
        logger.error(f"Database error while cleaning timestamps: {e}")
        raise DatabaseConnectionError(f"Database error while cleaning timestamps: {e}")


def get_latest_timestamp(table_name, engine):
    """获取表中最新数据的日期"""
    try:
        # 清洗表名
        cleaned_table_name = clean_symbol_for_postgres(table_name)
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT timestamp 
                FROM "{cleaned_table_name}"
                ORDER BY timestamp DESC
                LIMIT 1
            """))
            row = result.fetchone()
            return row[0] if row else None
    except SQLAlchemyError as e:
        logger.error(f"获取最新日期失败: {e}")
        return None

def should_skip_save(api_data, table_name, engine):
    """判断是否需要跳过保存"""
    if not api_data:
        return True
        
    # 清洗表名
    cleaned_table_name = clean_symbol_for_postgres(table_name)
        
    # 获取API数据的最新日期（转换为纽约时区）
    api_latest = max(candlestick.timestamp for candlestick in api_data)
    api_latest_ny = api_latest.astimezone(pytz.timezone('America/New_York'))
    
    # 获取数据库最新日期
    db_latest = get_latest_timestamp(cleaned_table_name, engine)
    if db_latest is None:
        return False
        
    # 转换为纽约时区比较
    db_latest_ny = db_latest.astimezone(pytz.timezone('America/New_York'))
    
    return api_latest_ny.date() == db_latest_ny.date()

def save_to_table(data, table_name, engine, batch_size=1000):
    """
    将数据保存到 PostgreSQL 数据库的指定表中。

    :param data: 要保存的数据
    :param table_name: 表名
    :param engine: SQLAlchemy 引擎
    :param batch_size: 批量插入的大小，默认1000条
    """
    # 检查是否需要跳过保存
    if should_skip_save(data, table_name, engine):
        logger.info(f"跳过保存 {table_name}，数据已是最新")
        return
        
    # 添加调试日志
    logger.debug(f"开始保存数据到表 {table_name}")
    try:
        # 清洗表名
        table_name = clean_symbol_for_postgres(table_name)

        # 确保表存在
        create_table_if_not_exists(engine, table_name)
        
        # 清理旧的时间戳数据
        clean_duplicate_timestamps(table_name, engine)

        # 数据转换为元组
        values = [
            (
                candlestick.timestamp.astimezone(pytz.timezone('America/New_York')).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None),  # 转换为纽约时间并统一为00:00:00
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
                        ON CONFLICT (timestamp) DO UPDATE SET
                            open_price = EXCLUDED.open_price,
                            high_price = EXCLUDED.high_price,
                            low_price = EXCLUDED.low_price,
                            close_price = EXCLUDED.close_price,
                            volume = EXCLUDED.volume,
                            turnover = EXCLUDED.turnover
                    """), 
                    [{"values": row} for row in batch]
                )
                conn.commit()
                total_inserted += len(batch)
                logger.info(f"Inserted/Updated {len(batch)} records to table '{table_name}' (total: {total_inserted})")

    except ValueError as e:
        logger.error(f"表名无效: {e}")
        raise DatabaseConnectionError(f"表名无效: {e}")
    except SQLAlchemyError as e:
        logger.error(f"保存数据失败: {e}")
        raise DatabaseConnectionError(f"保存数据失败: {e}")
