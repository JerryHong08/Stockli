from PyQt5.QtCore import QThread, pyqtSignal
from longport.openapi import QuoteContext, Config, Period, AdjustType
import pandas as pd
import psycopg2
import csv
import os
from datetime import datetime
from database.db_operations import save_to_table, fetch_table_names
from utils.logger import setup_logger
from config.paths import ERRORstock_PATH
from database.db_connection import DB_CONFIG  # 导入数据库配置
from database.db_operations import clean_symbol_for_postgres

# 初始化日志
logger = setup_logger("batch_fetcher")

class BatchDataFetcher(QThread):
    progress_updated = pyqtSignal(str)  # 信号：更新进度
    fetch_complete = pyqtSignal(str)  # 信号：批量获取完成
    error_occurred = pyqtSignal(str)  # 信号：发生错误

    def __init__(self, stock_symbols):
        super().__init__()
        self.stock_symbols = stock_symbols
        self.error_log_path = ERRORstock_PATH  # 错误日志文件路径

    def get_table_count_from_db(self):
        """从数据库中获取表的数量"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public';")
            table_count = cursor.fetchone()[0]
            conn.close()
            return table_count
        except Exception as e:
            print(f"获取数据库表数量失败: {e}")
            return 0
        
    def run(self):
        try:
            config = Config.from_env()
            ctx = QuoteContext(config)

            # 初始化错误日志文件
            if not os.path.exists(self.error_log_path):
                with open(self.error_log_path, mode="w", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)
                    writer.writerow(["Symbol", "Error"])  # 写入表头

            # 获取最新数据日期
            latest_date = self.get_latest_date_from_longport(ctx)
            if not latest_date:
                self.error_occurred.emit("无法从 Longport 获取最新数据日期")
                return

            # 获取数据库中第一个表的最新日期
            db_latest_date = self.get_latest_date_from_db()
            if not db_latest_date:
                self.error_occurred.emit("无法从数据库获取最新数据日期")
                return

            # 判断是否需要增量更新
            if latest_date > db_latest_date:
                self.progress_updated.emit(f"检测到新数据，最新日期为 {latest_date}，数据库最新日期为 {db_latest_date}")
                self.incremental_update(ctx, latest_date, db_latest_date)
            else:
                self.progress_updated.emit("数据库数据已是最新，检查是否有新股票...")
                self.check_new_stocks(ctx)
                
             # 获取数据库中实际存在的表数量
            table_count = self.get_table_count_from_db()
        
            # 批量获取完成
            self.fetch_complete.emit(f"最新最全数据，当前有 {table_count} 个股票截至 {latest_date} 的数据")

        except Exception as e:
            self.error_occurred.emit(f"批量获取数据失败: {e}")

    def get_latest_date_from_longport(self, ctx):
        """从 Longport 获取最新数据日期"""
        try:
            resp = ctx.candlesticks("NVDA.US", Period.Day, 1, AdjustType.ForwardAdjust)
            if not resp:
                return None
            # 将 datetime.date 转换为 datetime.datetime
            latest_date = datetime.combine(resp[0].timestamp.date(), datetime.min.time())
            return latest_date
        except Exception as e:
            print(f"获取最新数据日期失败: {e}")
            return None

    def get_latest_date_from_db(self):
        """从数据库中获取最后一个表的最新日期"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
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

    def incremental_update(self, ctx, latest_date, db_latest_date):
        """增量更新数据"""
        try:
            # 计算时间差（天数）
            delta_days = (latest_date - db_latest_date).days
            if delta_days <= 0:
                return

            # 遍历股票代码，获取增量数据
            for symbol in self.stock_symbols:
                try:
                    self.progress_updated.emit(f"正在更新 {symbol} 的增量数据...")
                    stock_symbol_with_suffix = f"{self.clean_symbol_for_api(symbol)}.US"
                    resp = ctx.candlesticks(stock_symbol_with_suffix, Period.Day, delta_days, AdjustType.ForwardAdjust)
                    if not resp:
                        continue

                    # 保存增量数据到数据库
                    table_name = symbol  # 表名不包含 ".US"
                    save_to_table(resp, table_name, DB_CONFIG)

                except Exception as e:
                    # 捕获错误并记录到 CSV 文件
                    error_message = str(e)
                    self.error_occurred.emit(f"更新 {symbol} 数据失败: {error_message}")

                    # 将错误信息写入 CSV 文件
                    with open(self.error_log_path, mode="a", newline="", encoding="utf-8") as file:
                        writer = csv.writer(file)
                        writer.writerow([symbol, error_message])

        except Exception as e:
            self.error_occurred.emit(f"增量更新失败: {e}")

    def check_new_stocks(self, ctx):
        """检查是否有新股票"""
        try:
            # 获取数据库中的所有表名
            db_tables = fetch_table_names(DB_CONFIG)
            new_stocks_found = False  # 标记是否找到新股票

            # 遍历股票代码，检查是否有新股票
            for symbol in self.stock_symbols:
                # 确保 symbol 是字符串类型
                symbol = str(symbol).strip()  # 添加类型转换
                
                table_name = clean_symbol_for_postgres(symbol)
                if table_name not in db_tables:
                    new_stocks_found = True  # 找到新股票
                    try:
                        self.progress_updated.emit(f"检测到新股票 {symbol}，正在获取数据...")
                        stock_symbol_with_suffix = f"{self.clean_symbol_for_api(symbol)}.US"
                        resp = ctx.candlesticks(stock_symbol_with_suffix, Period.Day, 1000, AdjustType.ForwardAdjust)
                        if not resp:
                            continue

                        # 保存新股票数据到数据库
                        save_to_table(resp, table_name, DB_CONFIG)
                        logger.info(f"成功获取新股票 {symbol} 的数据并保存到数据库")

                    except Exception as e:
                        error_message = str(e)
                        self.error_occurred.emit(f"获取新股票 {symbol} 数据失败: {error_message}")
                        logger.error(f"获取新股票 {symbol} 数据失败: {error_message}")

                        # 将错误信息写入 CSV 文件
                        with open(self.error_log_path, mode="a", newline="", encoding="utf-8") as file:
                            writer = csv.writer(file)
                            writer.writerow([symbol, error_message])

            if not new_stocks_found:
                self.progress_updated.emit("没有检测到新股票")

        except Exception as e:
            self.error_occurred.emit(f"检查新股票失败: {e}")
            logger.error(f"检查新股票失败: {e}")

    def clean_symbol_for_api(self, symbol):
        """清洗股票代码中的特殊符号，用于 Longport API"""
        # 去除空格
        cleaned_symbol = symbol.replace(" ", "")
        # 将 ^ 替换为 -，将 / 替换为 .
        cleaned_symbol = cleaned_symbol.replace("^", "-").replace("/", ".")
        return cleaned_symbol