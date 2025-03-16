from PyQt5.QtCore import QThread, pyqtSignal
from longport.openapi import QuoteContext, Config, Period, AdjustType
import pandas as pd
import psycopg2
import csv
import os
import time
import pytz
from pytz import timezone
from datetime import datetime
from database.db_operations import save_to_table, fetch_table_names
from utils.logger import setup_logger
from config.paths import ERRORstock_PATH
from database.db_connection import get_engine
from database.db_operations import clean_symbol_for_postgres

logger = setup_logger("batch_fetcher")

class BatchDataFetcher(QThread):
    progress_updated = pyqtSignal(dict)  # 信号：更新进度
    fetch_complete = pyqtSignal(str)  # 信号：批量获取完成
    error_occurred = pyqtSignal(str)  # 信号：发生错误

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
            self.error_count = 0  # 初始化错误计数器

            # 初始化错误日志文件
            if not os.path.exists(self.error_log_path):
                with open(self.error_log_path, mode="w", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)
                    writer.writerow(["Symbol", "Error"])

            # 判断是否有新股票
            # self.check_new_stocks(ctx)
            
            # 判断是否是盘中交易时间
            current_time = datetime.now()
            current_time_et = current_time.astimezone(pytz.timezone('America/New_York'))
            if current_time_et.hour < 9 or current_time_et.hour >= 16 or (current_time_et.hour == 9 and current_time_et.minute < 30):
                # 继续执行获取数据
                print(f"{current_time_et.hour}:{current_time_et.minute}现在不在盘中交易时间，可以执行获取数据")
                self.progress_updated.emit({
                    'message': "现在不在盘中交易时间，可以执行获取数据",
                    'start_time': self.start_time
                })
            else:    
                self.error_occurred.emit("正处于盘中交易时间，无法获取数据")
                return

            # 获取最新数据日期
            latest_date = self.get_latest_date_from_longport(ctx)
            if not latest_date:
                error_msg = "无法从 Longport 获取最新数据日期"
                logger.error(error_msg)
                self.error_occurred.emit(error_msg)
                return

            # 获取数据库中第一个表的最新日期
            db_latest_date = self.get_latest_date_from_db()
            if not db_latest_date:
                self.error_occurred.emit("无法从数据库获取最新数据日期")
                return
            
            # 判断是否需要增量更新
            if latest_date > db_latest_date:
                self.progress_updated.emit({
                    'message': f"检测到新数据，最新日期为 {latest_date}，数据库最新日期为 {db_latest_date}",
                    'start_time': self.start_time
                })
                self.incremental_update(ctx, latest_date, db_latest_date)
            else:
                self.progress_updated.emit({
                    'message': "数据库数据已是最新",
                    'start_time': self.start_time
                })
                # 获取数据库中实际存在的表数量
                table_count = self.get_table_count_from_db()
                self.fetch_complete.emit(f"最新最全数据，当前有 {table_count} 个股票截至 {latest_date} 的数据")

        except Exception as e:
            self.error_occurred.emit(f"批量获取数据失败: {e}")

    def incremental_update(self, ctx, latest_date, db_latest_date):
        """增量更新数据"""
        try:
            total = len(self.stock_symbols)
            delta_days = (latest_date - db_latest_date).days
            if delta_days <= 0:
                return

            for i, symbol in enumerate(self.stock_symbols, 1):
                try:
                    self.progress_updated.emit({
                        'current': i,
                        'total': total,
                        'start_time': self.start_time,
                        'message': f"正在更新 {symbol} 的增量数据..."
                    })
                    
                    stock_symbol_with_suffix = f"{self.clean_symbol_for_api(symbol)}.US"
                    resp = ctx.candlesticks(stock_symbol_with_suffix, Period.Day, delta_days, AdjustType.ForwardAdjust)
                    if not resp:
                        continue

                    table_name = symbol
                    engine = get_engine()
                    save_to_table(resp, table_name, engine)

                except Exception as e:
                    self.error_count += 1
                    error_message = f"更新 {symbol} 数据失败: {str(e)}"
                    logger.error(error_message)
                    
                    # 每10个错误弹窗一次
                    if self.error_count % 10 == 0:
                        self.error_occurred.emit(f"已遇到 {self.error_count} 个错误，最新错误：{error_message}")
                    
                    with open(self.error_log_path, mode="a", newline="", encoding="utf-8") as file:
                        writer = csv.writer(file)
                        writer.writerow([symbol, error_message])

        except Exception as e:
            self.error_occurred.emit(f"增量更新失败: {e}")

    def check_new_stocks(self, ctx):
        """检查是否有新股票"""
        try:
            engine = get_engine()
            db_tables = fetch_table_names(engine)
            total = len(self.stock_symbols)
            new_stocks_found = False

            for i, symbol in enumerate(self.stock_symbols, 1):
                symbol = str(symbol).strip()
                table_name = clean_symbol_for_postgres(symbol)
                
                if table_name not in db_tables:
                    new_stocks_found = True
                    try:
                        self.progress_updated.emit({
                            'current': i,
                            'total': total,
                            'start_time': self.start_time,
                            'message': f"检测到新股票 {symbol}，正在获取数据..."
                        })
                        
                        stock_symbol_with_suffix = f"{self.clean_symbol_for_api(symbol)}.US"
                        resp = ctx.candlesticks(stock_symbol_with_suffix, Period.Day, 1000, AdjustType.ForwardAdjust)
                        if not resp:
                            continue
                        engine = get_engine()
                        save_to_table(resp, table_name, engine)
                        logger.info(f"成功获取新股票 {symbol} 的数据并保存到数据库")

                    except Exception as e:
                        error_message = str(e)
                        self.error_occurred.emit(f"获取新股票 {symbol} 数据失败: {error_message}")
                        logger.error(f"获取新股票 {symbol} 数据失败: {error_message}")
                        with open(self.error_log_path, mode="a", newline="", encoding="utf-8") as file:
                            writer = csv.writer(file)
                            writer.writerow([symbol, error_message])

            if not new_stocks_found:
                self.progress_updated.emit({
                    'message': "没有检测到新股票",
                    'start_time': self.start_time
                })

        except Exception as e:
            self.error_occurred.emit(f"检查新股票失败: {e}")
            logger.error(f"检查新股票失败: {e}")

    def get_latest_date_from_longport(self, ctx):
        """从 Longport 获取最新数据日期"""
        try:
            resp = ctx.candlesticks("NVDA.US", Period.Day, 1, AdjustType.ForwardAdjust)
            if not resp:
                return None
            latest_date = datetime.combine(resp[0].timestamp.date(), datetime.min.time())
            return latest_date
        except Exception as e:
            print(f"获取最新数据日期失败: {e}")
            return None

    def get_latest_date_from_db(self):
        """从数据库中获取最后一个表的最新日期"""
        try:
            engine = get_engine()
            conn = engine.raw_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name DESC LIMIT 1;")
            table_name = cursor.fetchone()[0]
            cursor.execute(f'SELECT MAX(timestamp) FROM "{table_name}";')
            db_latest_date = cursor.fetchone()[0]
            conn.close()
            return db_latest_date
        except Exception as e:
            print(f"获取数据库最新日期失败: {e}")
            return None

    def get_table_count_from_db(self):
        """从数据库中获取表的数量"""
        try:
            engine = get_engine()
            conn = engine.raw_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public';")
            table_count = cursor.fetchone()[0]
            conn.close()
            return table_count
        except Exception as e:
            print(f"获取数据库表数量失败: {e}")
            return 0

    def clean_symbol_for_api(self, symbol):
        """清洗股票代码中的特殊符号，用于 Longport API"""
        cleaned_symbol = symbol.replace(" ", "")
        cleaned_symbol = cleaned_symbol.replace("^", "-").replace("/", ".")
        return cleaned_symbol
