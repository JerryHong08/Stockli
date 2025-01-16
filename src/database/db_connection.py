from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError
import time
from typing import Optional
from config.db_config import DB_CONFIG
from sqlalchemy.sql import text

class DatabaseConnectionError(Exception):
    """自定义数据库连接异常"""
    pass

def get_engine(max_retries: int = 3, retry_delay: float = 1.0) -> Optional[create_engine]:
    """
    获取数据库引擎，支持重试机制
    
    Args:
        max_retries: 最大重试次数
        retry_delay: 重试间隔时间（秒）
        
    Returns:
        SQLAlchemy引擎对象，如果连接失败则返回None
    """
    connection_string = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    # 连接池配置
    pool_config = {
        "poolclass": QueuePool,
        "pool_size": DB_CONFIG.get('pool_size', 5),
        "max_overflow": DB_CONFIG.get('max_overflow', 10),
        "pool_timeout": DB_CONFIG.get('pool_timeout', 30),
        "pool_recycle": DB_CONFIG.get('pool_recycle', 3600),
    }
    
    engine = None
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            engine = create_engine(connection_string, **pool_config)
            
            # 测试连接
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
            return engine
            
        except SQLAlchemyError as e:
            last_exception = e
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
                
    raise DatabaseConnectionError(
        f"数据库连接失败，重试{max_retries}次后仍然无法连接。最后错误信息: {str(last_exception)}"
    )

def check_connection(engine) -> bool:
    """检查数据库连接是否有效"""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except SQLAlchemyError:
        return False
