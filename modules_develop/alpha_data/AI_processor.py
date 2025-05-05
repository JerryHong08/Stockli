import os
import json
from openai import OpenAI

# DeepSeek 配置
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_MODEL = "deepseek-chat"

# SiliconFlow 配置
SILICONFLOW_API_KEY = os.getenv("SiliconFlow_apikey")
SILICONFLOW_BASE_URL = "https://api.siliconflow.cn/v1"
SILICONFLOW_MODEL = "deepseek-ai/DeepSeek-V2.5"

# 初始化客户端
def initialize_client(botselect):
    """根据 botselect 初始化对应的 OpenAI 客户端"""
    if botselect == "deepseek":
        api_key = os.getenv("DEEPSEEK_API_KEY")
        if not api_key:
            raise ValueError("DEEPSEEK_API_KEY environment variable is not set")
        return OpenAI(api_key=api_key, base_url=DEEPSEEK_BASE_URL), DEEPSEEK_MODEL
    elif botselect == "siliconflow":
        api_key = os.getenv("SiliconFlow_apikey")
        if not api_key:
            raise ValueError("SiliconFlow_apikey environment variable is not set")
        return OpenAI(api_key=api_key, base_url=SILICONFLOW_BASE_URL), SILICONFLOW_MODEL
    else:
        raise ValueError(f"Unsupported botselect: {botselect}")

# tickertoname 的系统提示
TICKER_SYSTEM_PROMPT = """
You are an expert in converting stock tickers to company names suitable for LinkedIn job search URLs. Your task is to take a stock ticker (e.g., "AMD") and generate a company name format that matches how it appears in LinkedIn job search URLs (e.g., "advanced-micro-devices"). Output a JSON object with a single key "company_name" and the formatted company name as a string. Do not include additional text or explanations outside the JSON.

Rules:
1. Convert the ticker to a lowercase, hyphen-separated format that aligns with LinkedIn URL conventions (e.g., "AMD" -> "advanced-micro-devices", "MSFT" -> "microsoft").
2. Use your knowledge of company names associated with tickers to ensure accuracy.
3. If the ticker is ambiguous or unknown, return an empty string in the "company_name" field.
Examples:
- Ticker: "AMD" -> {"company_name": "advanced-micro-devices"}
- Ticker: "MSFT" -> {"company_name": "microsoft"}
- Ticker: "GOOGL" -> {"company_name": "google"}
"""

# is_relevant 的系统提示
RELEVANT_SYSTEM_PROMPT = """
You are an expert in determining whether a company name matches a target company for job search purposes. Your task is to take a company name from LinkedIn (e.g., "Amazon Web Services (AWS)") and a target company name (e.g., "amazon"), then decide if they refer to the same entity. Output a JSON object with a single key "is_relevant" and a value of 1 (yes) or 0 (no). Do not include additional text or explanations outside the JSON.

Rules:
1. Return 1 if the company name clearly belongs to the target company or its subsidiaries (e.g., "Amazon Web Services (AWS)" for "amazon").
2. Return 0 if the company name is unrelated or only tangentially connected (e.g., "iStore South Africa" for "apple").
3. Ignore case and minor formatting differences.
4. Be strict: only return 1 if the match is unambiguous.
Examples:
- Company: "Amazon Web Services (AWS)", Target: "amazon" -> {"is_relevant": 1}
- Company: "iStore South Africa", Target: "apple" -> {"is_relevant": 0}
- Company: "Microsoft Corporation", Target: "microsoft" -> {"is_relevant": 1}
- Company: "Google Ireland", Target: "google" -> {"is_relevant": 1}
"""

def tickertoname(ticker, primary_bot="siliconflow"):
    """
    根据股票 ticker 生成 LinkedIn 搜索格式的公司名称。
    返回格式化的 company_name 字符串。
    """
    bots = ["deepseek", "siliconflow"]
    secondary_bot = [b for b in bots if b != primary_bot][0]  # 备用 bot
    
    # 优先使用 primary_bot
    client, model = initialize_client(primary_bot)
    user_prompt = f"Ticker: \"{ticker}\""
    messages = [
        {"role": "system", "content": TICKER_SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt}
    ]

    try:
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            response_format={'type': 'json_object'}
        )
        result = json.loads(response.choices[0].message.content)
        print(f"{primary_bot} 输出: {ticker} -> {result}")
        if "company_name" not in result:
            print(f"错误：{primary_bot} 返回的 JSON 缺少 'company_name' 键: {result}")
            return ""
        return result["company_name"]
    except Exception as e:
        print(f"{primary_bot} API 调用失败：{str(e)}")
        if "429" in str(e):  # 速率限制
            print(f"{primary_bot} 触发速率限制，切换到 {secondary_bot}")
            client, model = initialize_client(secondary_bot)
            try:
                response = client.chat.completions.create(
                    model=model,
                    messages=messages,
                    response_format={'type': 'json_object'}
                )
                result = json.loads(response.choices[0].message.content)
                print(f"{secondary_bot} 输出: {ticker} -> {result}")
                if "company_name" not in result:
                    print(f"错误：{secondary_bot} 返回的 JSON 缺少 'company_name' 键: {result}")
                    return ""
                return result["company_name"]
            except Exception as e2:
                print(f"{secondary_bot} API 调用失败：{str(e2)}")
                return ""
        return ""  # 默认返回空字符串

def is_relevant(company_name, target_company, primary_bot="siliconflow"):
    """
    判断 LinkedIn 页面中的公司名是否与目标公司相关。
    返回 1（相关）或 0（不相关）。
    """
    bots = ["deepseek", "siliconflow"]
    secondary_bot = [b for b in bots if b != primary_bot][0]  # 备用 bot
    
    # 优先使用 primary_bot
    client, model = initialize_client(primary_bot)
    user_prompt = f"Company: \"{company_name}\", Target: \"{target_company}\""
    messages = [
        {"role": "system", "content": RELEVANT_SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt}
    ]

    try:
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            response_format={'type': 'json_object'}
        )
        result = json.loads(response.choices[0].message.content)
        print(f"{primary_bot} 输出: {company_name} vs {target_company} -> {result}")
        if "is_relevant" not in result:
            print(f"错误：{primary_bot} 返回的 JSON 缺少 'is_relevant' 键: {result}")
            return 0
        return result["is_relevant"]
    except Exception as e:
        print(f"{primary_bot} API 调用失败：{str(e)}")
        if "429" in str(e):  # 速率限制
            print(f"{primary_bot} 触发速率限制，切换到 {secondary_bot}")
            client, model = initialize_client(secondary_bot)
            try:
                response = client.chat.completions.create(
                    model=model,
                    messages=messages,
                    response_format={'type': 'json_object'}
                )
                result = json.loads(response.choices[0].message.content)
                print(f"{secondary_bot} 输出: {company_name} vs {target_company} -> {result}")
                if "is_relevant" not in result:
                    print(f"错误：{secondary_bot} 返回的 JSON 缺少 'is_relevant' 键: {result}")
                    return 0
                return result["is_relevant"]
            except Exception as e2:
                print(f"{secondary_bot} API 调用失败：{str(e2)}")
                return 0
        return 0  # 默认返回 0

# 测试代码
if __name__ == "__main__":
    # 测试 tickertoname
    print("测试 tickertoname:")
    print(tickertoname("AMD"))
    print(tickertoname("MSFT"))
    
    # 测试 is_relevant
    print("\n测试 is_relevant:")
    print(is_relevant("Amazon Web Services (AWS)", "amazon"))
    print(is_relevant("iStore South Africa", "apple"))