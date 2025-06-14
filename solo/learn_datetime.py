from datetime import datetime, timedelta, date
from pytz import timezone
from longport.openapi import Config, QuoteContext, Market

def datetime_processor():
    # 获取当前 ET 时间
    et_now = datetime.now(timezone('US/Eastern'))
    today = et_now.date()

    # 初始化 LongPort API
    config = Config.from_env()
    ctx = QuoteContext(config)

    # 获取最近30天的美股交易日
    start_date = today - timedelta(days=30)
    resp_tradingdays = ctx.trading_days(Market.US, start_date, today)
    
    # 交易日列表解析        
    trade_days = [
        d if isinstance(d, date) else datetime.strptime(d, "%Y-%B-%d").date()
        for d in resp_tradingdays.trading_days
    ]
    trade_days.sort()
    
    print(f"最近30天的交易日: {trade_days}")
    
        

if __name__ == "__main__":
    datetime_processor() 