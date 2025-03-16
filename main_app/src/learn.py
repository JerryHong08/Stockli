from longport.openapi import QuoteContext, Config, Period, AdjustType, OpenApiException

config = Config.from_env()
ctx = QuoteContext(config)

try:
    resp = ctx.candlesticks("TSLA", Period.Day, 1000, AdjustType.ForwardAdjust)
    print(resp)
except OpenApiException as e:
    print(f"OpenApiException: {e}")
    print("Please check your API access and the symbol used.")