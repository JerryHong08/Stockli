## ## get historical stock merge and split data
# this request does not need VPN
#1 run redis on windows
#2 run ProxyPool/run.py on windows
#3 run this code
import requests
import csv
import time
from datetime import datetime

BASE_URL = "https://www.tipranks.com/api/stocks/splits"
OUTPUT_CSV = "outputs/tipranks_stock_splits.csv"
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,zh-TW;q=0.6",
    "referer": "https://www.tipranks.com/calendars/stock-splits/historical",
    "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Google Chrome\";v=\"133\", \"Chromium\";v=\"133\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest"
}
PARAMS = {
    "sortDir": 1,
    "pageSize": 50,
    "country": "us",
    "method": "stockSplit",
    "isFuture": "false"
}
CUTOFF_DATE = datetime(2023, 6, 11)
proxypool_url = 'http://127.0.0.1:5555/random'

def get_proxy():
    try:
        proxy = requests.get(proxypool_url, timeout=5).text.strip()
        return {'http': f"http://{proxy}", 'https': f"http://{proxy}"}
    except Exception as e:
        print(f"Failed to get proxy from pool: {e}")
        return None

def init_csv():
    with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Date", "Symbol", "Company", "Type", "Split Ratio"])

def append_to_csv(rows):
    with open(OUTPUT_CSV, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerows(rows)

def fetch_page(page_num, proxies):
    params = PARAMS.copy()
    params["page"] = page_num
    print(f"Using proxy: {proxies}")
    try:
        response = requests.get(BASE_URL, headers=HEADERS, params=params, proxies=proxies, verify=False, timeout=10)
        print(f"Page {page_num} - Status Code: {response.status_code}")
        print(f"Response Text: {response.text[:500]}")
        json_data = response.json()
        # 检查是否是有效数据
        if "data" in json_data:
            return json_data
        else:
            print(f"Page {page_num} returned invalid JSON (no 'data' field)")
            return None
    except (requests.exceptions.RequestException, ValueError) as e:
        print(f"Failed to fetch page {page_num}: {e}")
        return None

def parse_data(json_data):
    if not json_data or "data" not in json_data:
        return [], False
    
    rows = []
    should_stop = False
    for item in json_data["data"]:
        date_str = item["effectiveDate"].split("T")[0]
        date = datetime.strptime(date_str, "%Y-%m-%d")
        
        if date <= CUTOFF_DATE:
            should_stop = True
            break
        
        symbol = item["ticker"]
        company = item["companyName"]
        split_type = item["type"]
        ratio = item["splitRatioText"]
        rows.append([date_str, symbol, company, split_type, ratio])
    
    return rows, should_stop

def main():
    init_csv()
    total_records = 8326
    records_per_page = 50
    total_pages = (total_records + records_per_page - 1) // records_per_page
    page = 1
    current_proxy = get_proxy()  # 初始化代理
    
    while page <= total_pages:
        print(f"Fetching page {page} of {total_pages}...")
        json_data = fetch_page(page, current_proxy)
        
        if json_data:
            rows, should_stop = parse_data(json_data)
            if rows:
                append_to_csv(rows)
                print(f"Saved {len(rows)} records from page {page}")
            else:
                print(f"No data found on page {page}")
            
            if should_stop:
                print(f"Reached cutoff date {CUTOFF_DATE.strftime('%Y-%m-%d')}, stopping.")
                break
        else:
            print(f"Switching proxy due to failure on page {page}")
            current_proxy = get_proxy()  # 失败时切换代理
            if current_proxy is None:
                print("No valid proxy available, stopping.")
                break
            continue  # 重试当前页，不增加 page
        
        page += 1
        time.sleep(2)

if __name__ == "__main__":
    main()