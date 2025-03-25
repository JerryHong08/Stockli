from longport.openapi import QuoteContext, Config, Period, AdjustType, OpenApiException

config = Config.from_env()
ctx = QuoteContext(config)

try:
    resp = ctx.candlesticks("BAC-B.US", Period.Day, 1, AdjustType.ForwardAdjust)
    print(resp)
except OpenApiException as e:
    print(f"OpenApiException: {e}")
    print("Please check your API access and the symbol used.")
    
    
# import yfinance as yf
# # Define the ticker and create a Ticker object
# ticker = "AAPL"
# stock = yf.Ticker(ticker)
# # Fetch basic stock information
# stock_info = stock.info
# print(stock_info)