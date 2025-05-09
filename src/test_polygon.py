from polygon import RESTClient

api_key = "BCBarFeCkIC70Wzfjp3AgjqFw42moGbU"
client = RESTClient(api_key)

tickers = client.list_tickers(
    market="stocks",
    active="true",
    order="asc",
    limit="100",
    sort="ticker"
)

for t in tickers:
    print(t.ticker, t.name)