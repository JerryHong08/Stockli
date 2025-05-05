import requests
from bs4 import BeautifulSoup
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

def get_linkedin_job_count(company_name):
    url = f"https://www.linkedin.com/jobs/{company_name}-jobs-worldwide"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    # 配置重试策略
    session = requests.Session()
    retry_strategy = Retry(
        total=3,  # 最大重试次数
        backoff_factor=1,  # 每次重试之间的延迟（秒），1 表示 1s, 2s, 4s
        status_forcelist=[500, 502, 503, 504]  # 重试的 HTTP 状态码
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    
    # 发送请求
    try:
        response = session.get(url, headers=headers, timeout=10)  # 设置超时
        response.raise_for_status()  # 检查状态码
    except requests.exceptions.RequestException as e:
        print(f"无法访问 {url}，错误: {e}")
        return None
    
    # 解析 HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # 查找公司职位数
    filter_values = soup.find_all("div", class_="filter-values-container__filter-value")
    for value in filter_values:
        label = value.find("label")
        if label and company_name.lower() in label.text.lower():
            print(f"原始文本: {label.text}")
            match = re.search(r'\(([\d,]+)\)', label.text)
            if match:
                job_count = int(match.group(1).replace(",", ""))
                return job_count
            else:
                print(f"无法从 {label.text} 中提取职位数量")
                return None
    
    print(f"未找到 {company_name} 的职位数据")
    return None

# 测试代码
companies = ["tesla"]
for company in companies:
    job_count = get_linkedin_job_count(company)
    print(f"{company} 的职位数量: {job_count}")
    time.sleep(2)  # 添加 2 秒延迟
