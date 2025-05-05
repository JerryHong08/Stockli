from longport.openapi import QuoteContext, Config, Period, AdjustType, OpenApiException
from datetime import datetime
from pytz import timezone
    
# et_timezone = timezone('US/Eastern')
# current_time = datetime.now(et_timezone)
# formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

def get_latest_date_from_longport():
    config = Config.from_env()
    ctx = QuoteContext(config)

    resp = ctx.candlesticks("NVDA.US", Period.Day, 2, AdjustType.ForwardAdjust)
    if not resp:
        return None
    current_time = datetime.now(timezone('US/Eastern'))
    print(f"ET Time: {current_time}")
    # 检查当前时间是否在交易时间内
    if current_time.hour >= 9 and current_time.hour < 16 and current_time.day == resp[0].timestamp.day:
        # 如果当前时间日子与longport api日子相同且正处于交易时间内，返回前一交易日日期
        return datetime.combine(resp[1].timestamp.date(), datetime.min.time())
    else:
        # 如果不在交易时间内，返回最新的日期
        return datetime.combine(resp[0].timestamp.date(), datetime.min.time())


print(get_latest_date_from_longport())
# import yfinance as yf
# # Define the ticker and create a Ticker object
# ticker = "AAPL"
# stock = yf.Ticker(ticker)
# # Fetch basic stock information
# stock_info = stock.info
# print(stock_info)