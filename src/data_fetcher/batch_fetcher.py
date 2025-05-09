# 执行获取股票数据
from PyQt6.QtCore import QThread, pyqtSignal
from PyQt6.QtCore import QThread, pyqtSignal
from longport.openapi import QuoteContext, Config, Period, AdjustType, OpenApiException
import os
import time
from datetime import datetime
from database.db_operations import save_to_table, fetch_table_names, clean_symbol_for_postgres
from utils.logger import setup_logger
from config.paths import ERRORstock_PATH
from database.db_connection import get_engine
from sqlalchemy.sql import text
from config.db_config import DB_CONFIG
import psycopg2
from pytz import timezone

logger = setup_logger("batch_fetcher")

# 数据库连接函数
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

class BatchDataFetcher(QThread):
    progress_updated = pyqtSignal(dict)
    fetch_complete = pyqtSignal(str)
    error_occurred = pyqtSignal(str)

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

            if not os.path.exists(self.error_log_path):
                with open(self.error_log_path, mode="w", newline="", encoding="utf-8") as file:
                    file.write("Symbol,Error\n")

            self.check_new_stocks(ctx)
            
            latest_date = self.get_latest_date_from_longport(ctx)
            if not latest_date:
                logger.error("无法从 Longport 获取最新数据日期")
                return

            # 获取所有 ticker 的最新日期
            ticker_latest_dates = self.get_ticker_latest_dates_from_db()
            ticker_details = self.fetch_ticker_details(self.stock_symbols)

            # 检查哪些 ticker 需要更新
            needs_update = False
            for symbol in self.stock_symbols:
                if symbol not in ticker_details:
                    continue
                cleaned_symbol = clean_symbol_for_postgres(symbol, *ticker_details[symbol])
                db_latest_date = ticker_latest_dates.get(cleaned_symbol)
                if db_latest_date is None or latest_date > db_latest_date:
                    needs_update = True
                    break

            if needs_update:
                self.progress_updated.emit({
                    'message': f"检测到需要更新的数据，LongPort 最新日期为 {latest_date}",
                    'start_time': self.start_time
                })
                self.incremental_update(ctx, latest_date, ticker_latest_dates, ticker_details)
            else:
                table_count = self.get_table_count_from_db()
                self.fetch_complete.emit(f"最新最全数据，当前有 {table_count} 个股票截至 {latest_date} 的数据")

        except Exception as e:
            logger.error(f"批量获取数据失败: {e}")
            print(e)

    # 新增：批量获取每个 ticker 的最新日期
    def get_ticker_latest_dates_from_db(self):
        try:
            engine = get_engine()
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
                    logger.debug(f"Fetching data for {cleaned_symbol} with delta_days={delta_days}")
                    resp = ctx.candlesticks(f"{cleaned_symbol}.US", Period.Day, delta_days, AdjustType.ForwardAdjust)
                    if not resp:
                        logger.warning(f"No data returned for {cleaned_symbol}")
                        continue

                    engine = get_engine()
                    save_to_table(resp, cleaned_symbol, engine)
                else:
                    logger.debug(f"{cleaned_symbol} 数据已是最新，无需更新")
            except Exception as e:
                self.error_count += 1
                error_message = f"更新 {symbol} 数据失败: {str(e)}"
                logger.error(error_message)
                with open(self.error_log_path, mode="a", newline="", encoding="utf-8") as file:
                    file.write(f"{symbol},{error_message}\n")

    def check_new_stocks(self, ctx):
        engine = get_engine()
        db_tickers = set(ticker for ticker in fetch_table_names(engine))  # 清洗已有 ticker
        total = len(self.stock_symbols)
        new_stocks_found = False
        ticker_details = self.fetch_ticker_details(self.stock_symbols)
        
        for i, symbol in enumerate(self.stock_symbols, 1):
            
            if symbol not in ticker_details:
                logger.warning(f"No details found for {symbol}")
                continue

            ticker_type, primary_exchange = ticker_details[symbol]
            cleaned_symbol = clean_symbol_for_postgres(symbol, ticker_type, primary_exchange)
            
            if cleaned_symbol not in db_tickers:
                new_stocks_found = True
                try:
                    
                    self.progress_updated.emit({
                        'current': i,
                        'total': total,
                        'start_time': self.start_time,
                        'message': f"检测到新股票 {symbol}，正在获取数据..."
                    })
                    logger.debug(f"NEW TICKER: Fetching data for {cleaned_symbol} with 1000 days")
                    resp = ctx.candlesticks(f"{cleaned_symbol}.US", Period.Day, 1000, AdjustType.ForwardAdjust)
                    if not resp:
                        logger.warning(f"No data returned for {cleaned_symbol}")
                        continue
                    engine = get_engine()
                    save_to_table(resp, cleaned_symbol, engine)
                    logger.info(f"成功获取新股票 {symbol} 的数据并保存")
                except Exception as e:
                    error_message = str(e)
                    logger.error(f"获取新股票 {symbol} 数据失败: {error_message}")
                    # self.error_occurred.emit(f"获取新股票 {symbol} 数据失败: {error_message}")
                    with open(self.error_log_path, mode="a", newline="", encoding="utf-8") as file:
                        file.write(f"{symbol},{error_message}\n")

        if not new_stocks_found:
            self.progress_updated.emit({
                'message': "没有检测到新股票",
                'start_time': self.start_time
            })

    # need to add a feature that determines whether now is a trading time
    # def is_trading_time(self):
    #     now = datetime.now()
    #     # 假设交易时间为周一到周五的 9:30 到 16:00
    #     if now.weekday() < 5 and now.hour >= 9 and now.hour < 16:
    #         return True
    #     return False
    
    def get_latest_date_from_longport(self, ctx):
        try:
            resp = ctx.candlesticks("NVDA.US", Period.Day, 2, AdjustType.ForwardAdjust)
            if not resp:
                return None
            current_time = datetime.now(timezone('US/Eastern'))
            # 检查当前时间是否在交易时间内
            if (current_time.year == resp[0].timestamp.year and 
                current_time.month == resp[0].timestamp.month and 
                current_time.day == resp[0].timestamp.day and 
                current_time.hour >= 9 and current_time.hour < 16):
                # 如果当前时间年月日与longport api相同且正处于交易时间内，返回前一交易日日期
                return datetime.combine(resp[0].timestamp.date(), datetime.min.time())
            else:
                # 如果不在交易时间内，返回最新的日期
                return datetime.combine(resp[1].timestamp.date(), datetime.min.time())
        except Exception as e:
            logger.error(f"获取 Longport 最新日期失败: {e}")
            return None

    def get_latest_date_from_db(self):
        try:
            engine = get_engine()
            with engine.connect() as conn:
                result = conn.execute(text("SELECT MAX(timestamp) FROM stock_daily"))
                return result.fetchone()[0]
        except Exception as e:
            logger.error(f"获取数据库最新日期失败: {e}")
            return None

    def get_table_count_from_db(self):
        try:
            engine = get_engine()
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(DISTINCT ticker) FROM stock_daily"))
                return result.fetchone()[0]
        except Exception as e:
            logger.error(f"获取数据库 ticker 数量失败: {e}")
            return 0