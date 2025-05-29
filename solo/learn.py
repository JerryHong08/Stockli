
from longport.openapi import QuoteContext, Config, Market
from datetime import datetime, timedelta, date
from pytz import timezone

def update_limit_date():
        # 首先获取当前时间
        # 再判断今天是否是trade day
        # 如果是周末或节假日，则返回上一个交易日的日期
        # 如果不是周末或节假日，再通过longport判断此时此刻是不是盘中交易时间
        # 如果是盘中交易时间则返回上一个交易日的日期
        # 如果不是盘中交易时间则返回longport当前最新数据日期
    
        # 获取当前 ET 时间
        et_now = datetime.now(timezone('US/Eastern'))
        today = et_now.date()

        # 初始化 LongPort API
        config = Config.from_env()
        ctx = QuoteContext(config)

        # 获取最近30天的美股交易日
        start_date = today - timedelta(days=30)
        resp = ctx.trading_days(Market.US, start_date, today)
        print(f"Recent trading days: {resp}")
        
        
        
        
def main():
    update_limit_date()
    
if __name__ == "__main__":
    main()