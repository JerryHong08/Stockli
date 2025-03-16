## get increased_update stock merge and split data
import requests
import csv
from datetime import datetime, timedelta
import time

# 配置
API_KEY = "BCBarFeCkIC70Wzfjp3AgjqFw42moGbU"  # 替换为你的 Polygon.io API Key
START_DATE = "2023-06-12"         # 开始日期
END_DATE = "2025-03-07"           # 结束日期（当前日期，可调整）
OUTPUT_CSV = "stock_splits.csv"   # 输出 CSV 文件名
LIMIT = 1000                        # 每页返回的最大结果数

# 将日期字符串转换为 datetime 对象
start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
end_date = datetime.strptime(END_DATE, "%Y-%m-%d")

# API 请求函数
def fetch_splits(date_str):
    url = f"https://api.polygon.io/v3/reference/splits?execution_date={date_str}&limit={LIMIT}&apiKey={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # 检查请求是否成功
        data = response.json()
        if data["status"] == "OK":
            return data.get("results", [])
        else:
            print(f"API Error on {date_str}: {data.get('status')}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Request failed on {date_str}: {e}")
        return []

# 初始化 CSV 文件，写入表头
def init_csv():
    with open(OUTPUT_CSV, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["execution_date", "id", "split_from", "split_to", "ticker"])
        writer.writeheader()

# 将数据追加到 CSV 文件
def append_to_csv(splits):
    with open(OUTPUT_CSV, mode="a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["execution_date", "id", "split_from", "split_to", "ticker"])
        for split in splits:
            writer.writerow(split)

# 主程序：遍历日期并获取数据
def main():
    # 初始化 CSV 文件
    init_csv()
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Fetching splits for {date_str}...")
        
        # 获取当天的拆分数据
        splits = fetch_splits(date_str)
        if splits:
            append_to_csv(splits)
            print(f"Saved {len(splits)} splits for {date_str}")
        else:
            print(f"No splits found for {date_str}")
        
        # 移动到下一天
        current_date += timedelta(days=1)
        
        # 避免触发 API 频率限制（Polygon.io 免费计划有限制）
        time.sleep(1)  # 每秒 1 次请求，调整根据你的计划

if __name__ == "__main__":
    main()