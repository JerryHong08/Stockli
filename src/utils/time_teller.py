from longport.openapi import QuoteContext, Config, Market, Period, AdjustType, TradeSession
from datetime import datetime, timedelta, date, time
from pytz import timezone
from utils.logger import setup_logger

logger = setup_logger("main_logic")


def get_latest_date_from_longport(): # 返回格式为 YYYY-MM-DD HH:MM:SS
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
    resp_tradingdays = ctx.trading_days(Market.US, start_date, today)
    
    # 交易日列表解析        
    trade_days = [
        d if isinstance(d, date) else datetime.strptime(d, "%Y-%B-%d").date()
        for d in resp_tradingdays.trading_days
    ]
    trade_days.sort()

    # 判断今天是否是交易日
    if today not in trade_days:
        # 如果不是交易日，返回上一个交易日
        prev_trade_day = max([d for d in trade_days if d < today])
        limit_date = prev_trade_day.strftime("%Y-%m-%d")
        print(f"今天不是交易日，返回上一个交易日: {limit_date}")
        return limit_date

    # 今天是交易日，判断今天是否在交易时间内
    resp_trading_session = ctx.trading_session()
    # print(resp_trading_session)
    
    # 只取美股（US）市场的 session
    us_sessions = None
    for market_session in resp_trading_session:
        if getattr(market_session, "market", None) == Market.US:
            us_sessions = getattr(market_session, "trade_sessions", [])
            break
    
    # if us_sessions:
    #     # print(f"找到美股（US）市场的交易时段信息: {us_sessions}")
    # if not us_sessions:
    #     # print("未找到美股（US）市场的交易时段信息！")
    #     return

    # 解析所有阶段时间段
    session_times = []
    for session in us_sessions:
        begin_time = session.begin_time  # 已经是 time 类型
        end_time = session.end_time      # 已经是 time 类型
        stage = session.trade_session
        session_times.append((begin_time, end_time, stage))

    # 当前时间
    current_time = datetime.now(timezone('US/Eastern')).time()

    # 判断当前属于哪个阶段
    current_stage = None
    for begin, end, stage in session_times:
        if begin <= current_time < end:
            current_stage = stage
            break

    print(f"当前阶段: {current_stage}")
    
    
    resp = ctx.candlesticks("NVDA.US", Period.Day, 2, AdjustType.ForwardAdjust)
    if not resp:
        return None

    # 根据当前时间和交易时段判断返回日期
    if current_stage == TradeSession.Pre:
            # print(f"今日为交易日，且目前在盘前时间，返回longport最新交易日同时也是上一交易日数据 : ")
            return datetime.combine(resp[1].timestamp.date(), datetime.min.time())
    elif current_stage == TradeSession.Intraday:
            # print(f"今日为交易日，且目前在盘中时间，返回上一longport交易日同时也是上一交易日数据: ")
            return datetime.combine(resp[0].timestamp.date(), datetime.min.time())
    elif current_stage == TradeSession.Post:
            # print(f"今日为交易日，且目前在盘后时间，返回longport最新交易日数据同时也是当日： ")
            return datetime.combine(resp[1].timestamp.date(), datetime.min.time())
    else:
        # Overnight: 盘后结束到下一个盘前开始。无论是美东晚8点后：返回当日；美东早4点，返回前一交易日
        # print("当前为overnight（夜间）时段，返回longport最新交易日数据:")
        return datetime.combine(resp[1].timestamp.date(), datetime.min.time())