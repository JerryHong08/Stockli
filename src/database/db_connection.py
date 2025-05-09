from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import time
import logging
from typing import Optional
from config.db_config import DB_CONFIG
from sqlalchemy.sql import text
from functools import lru_cache

# 配置日志
logger = logging.getLogger("db_connection")
logger.setLevel(logging.INFO)

class DatabaseConnectionError(Exception):
    """自定义数据库连接异常"""
    pass

@lru_cache(maxsize=1)
def get_engine(max_retries: int = 5, retry_delay: float = 2.0) -> Optional[create_engine]:
    """
    获取数据库引擎，支持重试机制
    
    Args:
        max_retries: 最大重试次数
        retry_delay: 重试间隔时间（秒）
        
    Returns:
        SQLAlchemy引擎对象，如果连接失败则返回None
    """
    # 创建连接字符串
    connection_string = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    
    # 配置连接池
    pool_config = {
        "pool_size": 10,
        "max_overflow": 20,
        "pool_timeout": 30,
        "pool_recycle": 3600,
        "pool_pre_ping": True
    }
    
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                connect_args={
                    "connect_timeout": 10,
                    "keepalives": 1,
                    "keepalives_idle": 30,
                    "keepalives_interval": 10,
                    "keepalives_count": 5
                },
                **pool_config
            )
            
            # 测试连接
            if not check_connection(engine):
                raise OperationalError("Connection test failed", None, None)
                
            logger.info("数据库连接成功")
            print("数据库连接成功")
            return engine
            
        except (SQLAlchemyError, OperationalError) as e:
            last_exception = e
            logger.warning(f"数据库连接失败，尝试 {attempt + 1}/{max_retries}...")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))  # 指数退避
                continue
                
    error_msg = f"数据库连接失败，重试{max_retries}次后仍然无法连接。最后错误信息: {str(last_exception)}"
    logger.error(error_msg)
    raise DatabaseConnectionError(error_msg)

def check_connection(engine) -> bool:
    """检查数据库连接是否有效"""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except SQLAlchemyError as e:
        logger.warning(f"数据库连接检查失败: {str(e)}")
        return False

def get_db_session(engine):
    """创建并返回数据库会话"""
    from sqlalchemy.orm import sessionmaker
    try:
        Session = sessionmaker(bind=engine)
        return Session()
    except SQLAlchemyError as e:
        logger.error(f"创建数据库会话失败: {e}")
        raise DatabaseConnectionError(f"创建数据库会话失败: {e}")
