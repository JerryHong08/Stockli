from PyQt5.QtCore import QThread, pyqtSignal
from longport.openapi import QuoteContext, Config, Period, AdjustType
import os
import time
from datetime import datetime
from database.db_operations import save_to_table, fetch_table_names, clean_symbol_for_postgres
from utils.logger import setup_logger
from config.paths import ERRORstock_PATH
from database.db_connection import get_engine
from sqlalchemy.sql import text

logger = setup_logger("batch_fetcher")

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
                self.error_occurred.emit("无法从 Longport 获取最新数据日期")
                return

            db_latest_date = self.get_latest_date_from_db()
            if not db_latest_date:
                self.error_occurred.emit("无法从数据库获取最新数据日期")
                return

            if latest_date > db_latest_date:
                self.progress_updated.emit({
                    'message': f"检测到新数据，最新日期为 {latest_date}，数据库最新日期为 {db_latest_date}",
                    'start_time': self.start_time
                })
                self.incremental_update(ctx, latest_date, db_latest_date)
            else:
                table_count = self.get_table_count_from_db()
                self.fetch_complete.emit(f"最新最全数据，当前有 {table_count} 个股票截至 {latest_date} 的数据")

        except Exception as e:
            self.error_occurred.emit(f"批量获取数据失败: {e}")

    def incremental_update(self, ctx, latest_date, db_latest_date):
        total = len(self.stock_symbols)
        delta_days = (latest_date - db_latest_date).days
        if delta_days <= 0:
            return

        for i, symbol in enumerate(self.stock_symbols, 1):
            try:
                cleaned_symbol = clean_symbol_for_postgres(symbol)
                self.progress_updated.emit({
                    'current': i,
                    'total': total,
                    'start_time': self.start_time,
                    'message': f"正在更新 {symbol} 的增量数据..."
                })
                stock_symbol_with_suffix = f"{cleaned_symbol}.US"
                logger.debug(f"Fetching data for {stock_symbol_with_suffix} with delta_days={delta_days}")
                resp = ctx.candlesticks(stock_symbol_with_suffix, Period.Day, delta_days, AdjustType.ForwardAdjust)
                if not resp:
                    logger.warning(f"No data returned for {stock_symbol_with_suffix}")
                    continue

                engine = get_engine()
                save_to_table(resp, cleaned_symbol, engine)
            except Exception as e:
                self.error_count += 1
                error_message = f"更新 {symbol} 数据失败: {str(e)}"
                logger.error(error_message)
                if self.error_count % 10 == 0:
                    self.error_occurred.emit(f"已遇到 {self.error_count} 个错误，最新错误：{error_message}")
                with open(self.error_log_path, mode="a", newline="", encoding="utf-8") as file:
                    file.write(f"{symbol},{error_message}\n")

    def check_new_stocks(self, ctx):
        engine = get_engine()
        db_tickers = set(clean_symbol_for_postgres(ticker) for ticker in fetch_table_names(engine))  # 清洗已有 ticker
        total = len(self.stock_symbols)
        new_stocks_found = False

        for i, symbol in enumerate(self.stock_symbols, 1):
            symbol = str(symbol).strip()
            cleaned_symbol = clean_symbol_for_postgres(symbol)
            if cleaned_symbol not in db_tickers:
                new_stocks_found = True
                try:
                    self.progress_updated.emit({
                        'current': i,
                        'total': total,
                        'start_time': self.start_time,
                        'message': f"检测到新股票 {symbol}，正在获取数据..."
                    })
                    stock_symbol_with_suffix = f"{cleaned_symbol}.US"
                    logger.debug(f"Fetching data for {stock_symbol_with_suffix} with 1000 days")
                    resp = ctx.candlesticks(stock_symbol_with_suffix, Period.Day, 1000, AdjustType.ForwardAdjust)
                    if not resp:
                        logger.warning(f"No data returned for {stock_symbol_with_suffix}")
                        continue
                    engine = get_engine()
                    save_to_table(resp, cleaned_symbol, engine)
                    logger.info(f"成功获取新股票 {symbol} 的数据并保存")
                except Exception as e:
                    error_message = str(e)
                    self.error_occurred.emit(f"获取新股票 {symbol} 数据失败: {error_message}")
                    with open(self.error_log_path, mode="a", newline="", encoding="utf-8") as file:
                        file.write(f"{symbol},{error_message}\n")

        if not new_stocks_found:
            self.progress_updated.emit({
                'message': "没有检测到新股票",
                'start_time': self.start_time
            })

    def get_latest_date_from_longport(self, ctx):
        try:
            resp = ctx.candlesticks("NVDA.US", Period.Day, 1, AdjustType.ForwardAdjust)
            if not resp:
                return None
            return datetime.combine(resp[0].timestamp.date(), datetime.min.time())
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