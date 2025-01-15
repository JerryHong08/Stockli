import psycopg2
import pandas as pd
from sqlalchemy import MetaData, Table
from functools import lru_cache
from psycopg2.extras import execute_values
import pytz
from utils.logger import setup_logger
import logging

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

def fetch_table_names(db_config):
    """
    获取数据库中的所有表名。
    """
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(
            """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        """
        )
        table_names = [row[0] for row in cursor.fetchall()]
        conn.close()
        return table_names
    except Exception as e:
        print(f"Error fetching table names: {e}")
        logger.error(f"Error fetching table names: {e}")
        return []

def create_table_if_not_exists(conn, table_name):
    """动态创建表，确保表结构符合要求。"""
    try:
        # 清洗表名
        table_name = clean_symbol_for_postgres(table_name)
        
        # 创建一个有效的索引名（移除空格和特殊字符）
        index_name = f"idx_{table_name.replace(' ', '_')}_timestamp"

        with conn.cursor() as cursor:
            create_table_query = f"""
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
            """
            cursor.execute(create_table_query)
            conn.commit()
            print(f"Table '{table_name}' is ready.")
            
    except Exception as e:
        print(f"创建表失败: {e}")
        conn.rollback()  # 发生错误时回滚事务
        raise

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

def save_to_table(data, table_name, db_config):
    """
    将数据保存到 PostgreSQL 数据库的指定表中。

    :param data: 要保存的数据
    :param table_name: 表名
    :param db_config: 数据库连接配置（字典）
    """
    try:
        # 清洗表名
        table_name = clean_symbol_for_postgres(table_name)

        conn = psycopg2.connect(**db_config)
        if not conn:
            raise Exception("数据库连接失败")

        try:
            # 确保表存在
            create_table_if_not_exists(conn, table_name)

            cursor = conn.cursor()
            # 设置纽约时区
            ny_tz = pytz.timezone('America/New_York')

            # 在 SQL 语句中使用双引号括起表名
            insert_query = f"""
            INSERT INTO "{table_name}" (timestamp, open_price, high_price, low_price, close_price, volume, turnover)
            VALUES %s
            ON CONFLICT (timestamp) DO NOTHING;  -- 避免重复插入
            """

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

            # 执行批量插入
            execute_values(cursor, insert_query, values)
            conn.commit()
            logger.info(f"Saved {len(values)} records to table '{table_name}'.")
            print(f"Saved {len(values)} records to table '{table_name}'.")

        except Exception as e:
            logger.error(f"保存数据到数据库失败: {e}")
            raise Exception(f"保存数据到数据库失败: {e}")
        finally:
            conn.close()

    except ValueError as e:
        logger.error(f"表名无效: {e}")
        print(f"表名无效: {e}")
    except Exception as e:
        logger.error(f"保存数据失败: {e}")
        print(f"保存数据失败: {e}")