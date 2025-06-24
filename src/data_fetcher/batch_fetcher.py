# 执行获取股票数据
from PySide6.QtCore import QThread, Signal
from longport.openapi import QuoteContext, Config, Period, AdjustType, OpenApiException
import os
import time
from datetime import datetime
from src.database.db_operations import save_to_table, fetch_table_names, clean_symbol_for_postgres
from src.utils.logger import setup_logger
from src.config.paths import ERRORstock_PATH
from src.database.db_connection import get_engine
from sqlalchemy.sql import text
from src.config.db_config import DB_CONFIG
import psycopg2
from pytz import timezone
from src.utils.time_teller import get_latest_date_from_longport

logger = setup_logger("batch_fetcher")

# 数据库连接函数
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

class BatchDataFetcher(QThread):
    progress_updated = Signal(dict)
    fetch_complete = Signal(str)
    error_occurred = Signal(str)

    def __init__(self, stock_symbols):
        super().__init__()
        self.stock_symbols = stock_symbols
        self.error_log_path = ERRORstock_PATH
        self.start_time = None

    def run(self):
        try:
            self.start_time = time.time()
            config = Config.from_env()
            ctx = QuoteContext(config)
            self.error_count = 0

            # 获取 LongPort 最新数据日期,同时检查是否在交易时间内
            latest_date = get_latest_date_from_longport()
            if not latest_date:
                logger.error("无法从 Longport 获取最新数据日期")
                return

            # 获取所有 ticker 的最新日期
            ticker_latest_dates = self.get_ticker_latest_dates_from_db()
            ticker_details = self.fetch_ticker_details(self.stock_symbols)
            
            self.incremental_update(ctx, latest_date, ticker_latest_dates, ticker_details)

        except Exception as e:
            logger.error(f"批量获取数据失败: {e}")
            print(e)

    # 新增：批量获取每个 ticker 的最新日期
    def get_ticker_latest_dates_from_db(self):
        try:
            engine = get_engine()
            if engine is None:
                logger.error("数据库引擎未初始化，无法获取每个 ticker 的最新日期")
                return {}
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT ticker, MAX(timestamp) as latest_date
                    FROM stock_daily
                    GROUP BY ticker
                """))
                return {row[0]: row[1] for row in result.fetchall()}
        except Exception as e:
            logger.error(f"获取每个 ticker 的最新日期失败: {e}")
            return {}
                
    # 从数据库查询 ticker 的 type 和 primary_exchange
    def fetch_ticker_details(self, tickers):
        conn = get_db_connection()
        cursor = conn.cursor()
        # 确保 tickers 中的每个元素都是字符串
        tickers = [str(ticker) for ticker in tickers]
        # 使用 IN 子句批量查询
        query = """
            SELECT ticker, type, primary_exchange
            FROM tickers_fundamental
            WHERE ticker IN %s
        """
        cursor.execute(query, (tuple(tickers),))
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # 转换为字典，便于后续匹配
        ticker_details = {row[0]: (row[1], row[2]) for row in results}
        return ticker_details

    # 修改：incremental_update 根据每个 ticker 的最新日期决定更新
    def incremental_update(self, ctx, latest_date, ticker_latest_dates, ticker_details):
        total = len(self.stock_symbols)
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            for i, symbol in enumerate(self.stock_symbols, 1):
                try:
                    if symbol not in ticker_details:
                        logger.warning(f"No details found for {symbol}")
                        continue

                    ticker_type, primary_exchange = ticker_details[symbol]
                    cleaned_symbol = clean_symbol_for_postgres(symbol, ticker_type, primary_exchange)
                    db_latest_date = ticker_latest_dates.get(cleaned_symbol)
                    
                    # 如果数据库中没有数据或 LongPort 数据更新，则更新
                    if db_latest_date is None or latest_date > db_latest_date:
                        delta_days = (latest_date - (db_latest_date or datetime.min)).days
                        if delta_days <= 0:
                            continue

                        self.progress_updated.emit({
                            'current': i,
                            'total': total,
                            'start_time': self.start_time,
                            'message': f"正在更新 {symbol} 的增量数据（最新至 {db_latest_date or '无数据'}）..."
                        })
                        print(f"Fetching data for {cleaned_symbol} with delta_days={delta_days}")
                        engine = get_engine()
                        resp = ctx.candlesticks(f"{cleaned_symbol}.US", Period.Day, delta_days, AdjustType.ForwardAdjust)                        
                        if not resp or not hasattr(resp[0], "timestamp"):
                            logger.warning(f"No data returned for {cleaned_symbol}")
                            continue

                        # 有数据，处理 timestamp
                        ts = resp[-1].timestamp
                        if isinstance(ts, datetime):
                            ts_cmp = ts.strftime("%Y-%m-%d")
                        else:
                            ts_clean = ts.replace("T", " ").replace("Z", "")
                            ts_cmp = datetime.strptime(ts_clean, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")

                        print(f"Ticker: {cleaned_symbol}, lates_longport_data_timestamp: {ts_cmp}")

                        # 判断是否能获取正常更新时间的数据
                        if ts_cmp != latest_date.strftime("%Y-%m-%d"):
                            cursor.execute("SELECT active FROM tickers_fundamental WHERE ticker = %s", (symbol,))
                            active_status = cursor.fetchone()[0]
                            if active_status is None:
                                logger.warning(f"{cleaned_symbol} 在 tickers_fundamental 中没有 active 状态，无法判断是否退市")
                                cursor.execute("UPDATE tickers_fundamental SET active = FALSE WHERE ticker = %s", (symbol,))
                                conn.commit()
                                continue

                        # 其余情况都保存数据
                        save_to_table(resp, cleaned_symbol, engine)
                    else:
                        logger.debug(f"{cleaned_symbol} 数据已是最新，无需更新")
                except Exception as e:
                    self.error_count += 1
                    error_message = f"更新 {symbol} 数据失败: {str(e)}"
                    logger.error(error_message)
                    with open(self.error_log_path, mode="a", newline="", encoding="utf-8") as file:
                        file.write(f"{symbol},{error_message}\n")
        finally:
            cursor.close()
            conn.close()
        table_count = self.get_table_count_from_db()
        self.fetch_complete.emit(f"最新最全数据，当前有 {table_count} 个股票截至 {latest_date} 的数据")
        self.progress_updated.emit({
                            'current': total,
                            'total': total,
                            'start_time': self.start_time,
                            'message': f"最新最全数据，当前有 {table_count} 个股票截至 {latest_date} 的数据"
                        })

    def get_latest_date_from_db(self):
        try:
            engine = get_engine()
            if engine is None:
                logger.error("数据库引擎未初始化，无法获取数据库最新日期")
                return None
            with engine.connect() as conn:
                result = conn.execute(text("SELECT MAX(timestamp) FROM stock_daily"))
                return result.fetchone()[0]
        except Exception as e:
            logger.error(f"获取数据库最新日期失败: {e}")
            return None

    def get_table_count_from_db(self):
        try:
            engine = get_engine()
            if engine is None:
                logger.error("数据库引擎未初始化，无法获取 ticker 数量")
                return 0
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(DISTINCT ticker) FROM stock_daily"))
                return result.fetchone()[0]
        except Exception as e:
            logger.error(f"获取数据库 ticker 数量失败: {e}")
            return 0

