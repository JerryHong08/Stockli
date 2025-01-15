from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

DB_CONFIG = {
    "dbname": "Stock",
    "user": "postgres",
    "password": "12138",
    "host": "localhost",
    "port": "5432",
}

def get_engine():
    engine = create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}",
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
    )
    try:
        with engine.connect() as conn:
            print("数据库连接成功！")
        return engine
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return None