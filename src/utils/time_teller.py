from longport.openapi import QuoteContext, Config, Market, Period, AdjustType
from datetime import datetime, timedelta, date, time
from pytz import timezone
from utils.logger import setup_logger

logger = setup_logger("main_logic")

def get_latest_date_from_longport(ctx):
        try:
            resp = ctx.candlesticks("NVDA.US", Period.Day, 2, AdjustType.ForwardAdjust)
            if not resp:
                return None
            market_open = time(9, 30)
            market_close = time(16, 0)
            current_time = datetime.now(timezone('US/Eastern'))
            now_time = current_time.time()
            # 检查当前时间是否在交易时间内
            if (current_time.year == resp[1].timestamp.year and 
                current_time.month == resp[1].timestamp.month and 
                current_time.day == resp[1].timestamp.day and 
                market_open <= now_time < market_close):
                # 如果当前时间年月日与longport api相同且正处于交易时间内，返回前一交易日日期
                print(f"今天是交易日，且当前时间在交易时间内: ")
                return datetime.combine(resp[0].timestamp.date(), datetime.min.time())
            else:
                print(f"今天是交易日，但不在交易时间内: ")
                if now_time < market_open:
                    print(f"当前时间在盘前: ")
                    # 如果当前时间在盘前，返回前一交易日日期
                    print(f"longport api data: {datetime.combine(resp[0].timestamp.date(), datetime.min.time())}")
                    return datetime.combine(resp[0].timestamp.date(), datetime.min.time())
                else:
                    print(f"当前时间在盘后: ")
                    # 如果当前时间在盘后，返回最新数据日期
                    print(f"longport api data: {datetime.combine(resp[1].timestamp.date(), datetime.min.time())}")
                    return datetime.combine(resp[1].timestamp.date(), datetime.min.time())
        except Exception as e:
            logger.error(f"获取 Longport 最新日期失败: {e}")
            return None
def update_limit_date(): # 返回格式为 YYYY-MM-DD HH:MM:SS
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
        
        # 交易日列表解析        
        trade_days = [
            d if isinstance(d, date) else datetime.strptime(d, "%Y-%B-%d").date()
            for d in resp.trading_days
        ]
        trade_days.sort()

        # 判断今天是否是交易日
        if today not in trade_days:
            # 如果不是交易日，返回上一个交易日
            prev_trade_day = max([d for d in trade_days if d < today])
            limit_date = prev_trade_day.strftime("%Y-%m-%d")
            print(f"今天不是交易日，返回上一个交易日: {limit_date}")
            return limit_date

        # 如果是交易日，判断是否盘中
        latest_date = get_latest_date_from_longport(ctx)
        if not latest_date:
            print("无法获取 LongPort 最新日期")
            return
        
        # 复用 get_latest_date_from_longport 的逻辑
        # 如果盘中，返回前一交易日，否则返回最新日期
        limit_date = latest_date
        return limit_date