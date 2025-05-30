
from longport.openapi import QuoteContext, Config, Market
from datetime import datetime, timedelta, date, time
from pytz import timezone


def get_latest_date_from_longport():
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

        resp_trading_session = ctx.trading_session()
        print(resp_trading_session)
        
        # 只取美股（US）市场的 session
        us_sessions = None
        for market_session in resp_trading_session:
            if getattr(market_session, "market", None) == Market.US:
                us_sessions = getattr(market_session, "trade_sessions", [])
                break
        
        if us_sessions:
            print(f"找到美股（US）市场的交易时段信息: {us_sessions}")
        if not us_sessions:
            print("未找到美股（US）市场的交易时段信息！")
            return

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
        
        
        
def main():
    get_latest_date_from_longport()
    
if __name__ == "__main__":
    main()