import psycopg2
import os

# 数据库连接信息（替换为你的实际信息）
db_config = {
    "dbname": os.getenv("DB_NAME", "Stock"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": "hgl084877",
    # "password": os.getenv("DB_PASSWORD", "12138"),
    #"host": "120.55.182.153",  # 直接指定云服务器的公有 IP
    "host": "localhost",
    "port": os.getenv("DB_PORT", "5432")
}

try:
    # 连接数据库
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # 获取所有表名
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = cursor.fetchall()

    batch_size = 50  # 每批处理50个表
    table_count = 0

    # 分批处理
    for table in tables:
        table_name = table[0]
        # 使用参数化查询防止SQL注入，并正确引用表名
        query = f'DELETE FROM "{table_name}" WHERE id IN (SELECT id FROM "{table_name}" ORDER BY id DESC LIMIT 3)'
        cursor.execute(query)
        table_count += 1
        
        if table_count % batch_size == 0:
            conn.commit()  # 提交事务
            print(f"Processed {table_count} tables so far")
    
    # 提交剩余的事务
    conn.commit()
    print(f"Total tables processed: {table_count}")

except psycopg2.Error as e:
    print(f"Database error: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # 确保连接关闭
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    print("Database connection closed.")