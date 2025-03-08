import os
from typing import Dict, Any

# 从环境变量获取数据库配置，如果不存在则使用默认值
DB_CONFIG: Dict[str, Any] = {
    "dbname": os.getenv("DB_NAME", "Stock"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": "hgl084877",
    # "password": os.getenv("DB_PASSWORD", "12138"),
    #"host": "120.55.182.153",  # 直接指定云服务器的公有 IP
    "host": "localhost",
    "port": os.getenv("DB_PORT", "5432")
}

def update_db_config(new_config: Dict[str, Any]) -> None:
    """更新数据库配置"""
    global DB_CONFIG
    DB_CONFIG.update(new_config)
