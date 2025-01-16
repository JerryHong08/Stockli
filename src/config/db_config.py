import os
from typing import Dict, Any

# 从环境变量获取数据库配置，如果不存在则使用默认值
DB_CONFIG: Dict[str, Any] = {
    "dbname": os.getenv("DB_NAME", "Stock"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "12138"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    # 连接池配置
    "pool_size": int(os.getenv("DB_POOL_SIZE", 5)),
    "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", 10)),
    "pool_timeout": int(os.getenv("DB_POOL_TIMEOUT", 30)),
    "pool_recycle": int(os.getenv("DB_POOL_RECYCLE", 3600)),
}

def update_db_config(new_config: Dict[str, Any]) -> None:
    """更新数据库配置"""
    global DB_CONFIG
    DB_CONFIG.update(new_config)
