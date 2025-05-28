import yfinance as yf

print("yfinance version:", yf.__version__)
# 测试 yfinance 是否正常工作
ticker = yf.Ticker("AAPL")
data = ticker.history(period="1d")
print(data)