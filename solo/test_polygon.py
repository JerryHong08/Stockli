from polygon import RESTClient

api_key = "BCBarFeCkIC70Wzfjp3AgjqFw42moGbU"
client = RESTClient(api_key)

tickers = client.list_tickers(
    market="stocks",
    active=True,  # 修改为布尔值
    order="asc",
    limit=100,  # 修改为整数
    sort="ticker"
)

for t in tickers:
    # If t is a dictionary
    if isinstance(t, dict):
        print(t.get("ticker"), t.get("name"))
    # If t is bytes, decode and print
    elif isinstance(t, bytes):
        print(t.decode())
    else:
        print(t)