import time
from PyQt6.QtCore import QThread, pyqtSignal, QStringListModel
from PyQt6.QtWidgets import QMessageBox, QToolTip, QWidget, QLabel, QHBoxLayout
import pandas as pd
import os
import sys
from database.db_operations import fetch_table_names
from data_fetcher.data_fetcher import DataFetcher
from data_fetcher.batch_fetcher import BatchDataFetcher
from data_fetcher.data_loader import DataLoader
from config.paths import STOCK_LIST_PATH
from data_visualization.candlestick_plot import plot_candlestick
from database.db_connection import get_engine, check_connection, DatabaseConnectionError
from config.db_config import DB_CONFIG
import yfinance as yf
import psycopg2
import csv
from config.paths import ERRORstock_PATH  # 错误日志路径

# 数据库连接函数
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# 从数据库获取所有 ticker
def fetch_tickers_from_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ticker FROM tickers_fundamental WHERE active = true")
    tickers = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    print(f"Fetched {len(tickers)} tickers from database")
    return tickers

class MainWindowLogic:
    def __init__(self, ui):
        self.ui = ui
        self.data_cache = {}
        self.all_stock_symbols = []
        self.current_fetcher = None
        try:
            self.engine = get_engine()
            if not check_connection(self.engine):
                raise DatabaseConnectionError("数据库连接测试失败")
        except DatabaseConnectionError as e:
            QMessageBox.critical(self.ui, "错误", f"无法连接到数据库: {str(e)}")
            sys.exit(1)
            
        self.connect_signals()
        self.update_stock_selector()

    def connect_signals(self):
        self.ui.data_fetch_tab.batch_fetch_button.clicked.connect(self.batch_fetch_stocks)
        self.ui.visualization_tab.search_button.clicked.connect(self.confirm_search)
        self.ui.visualization_tab.load_button.clicked.connect(self.load_stock_data)
        self.ui.visualization_tab.hover_toggle.stateChanged.connect(self.toggle_hover_display)
        self.ui.visualization_tab.search_box.textChanged.connect(self.filter_stock_selector)
        
    # 数据规范化
    def format_value(self, value):
        if isinstance(value, (int, float)):  # 检查输入是否为整数或浮点数
            if abs(value) >= 1_000_000_000:  # 如果绝对值大于等于十亿
                return f"{value/1_000_000_000:.2f}B"  # 格式化为以“B”为单位的字符串，保留两位小数
            elif abs(value) >= 1_000_000:  # 如果绝对值大于等于一百万
                return f"{value/1_000_000:.2f}M"  # 格式化为以“M”为单位的字符串，保留两位小数
            elif abs(value) >= 1_000:  # 如果绝对值大于等于一千
                return f"{value/1_000:.2f}K"  # 格式化为以“K”为单位的字符串，保留两位小数
        return str(value)  # 如果不是数字类型或不满足上述条件，直接返回字符串形式
    
    # 如果搜索框内容有改变，过滤股票选择器下拉框里的股票代码
    def filter_stock_selector(self):
        search_text = self.ui.visualization_tab.search_box.text().strip().lower()
        if not search_text:
            self.ui.visualization_tab.stock_selector.clear()
            self.ui.visualization_tab.stock_selector.addItems(self.all_stock_symbols)
            return
        filtered_symbols = [symbol for symbol in self.all_stock_symbols if search_text in symbol.lower()]
        self.ui.visualization_tab.stock_selector.clear()
        self.ui.visualization_tab.stock_selector.addItems(filtered_symbols)

    # 更新股票选择器下拉框里的股票代码
    def update_stock_selector(self):
        self.all_stock_symbols = fetch_table_names(self.engine)
        self.ui.visualization_tab.stock_selector.clear()
        self.ui.visualization_tab.stock_selector.addItems(self.all_stock_symbols)
        model = QStringListModel()
        model.setStringList(self.all_stock_symbols)
        self.ui.visualization_tab.completer.setModel(model)
    
    # 蜡烛图页面，搜索按钮点击后，确认搜索
    def confirm_search(self):
        search_text = self.ui.visualization_tab.search_box.text().strip().upper()
        if search_text:
            exact_matches = [symbol for symbol in self.all_stock_symbols if symbol.upper() == search_text]
            if not exact_matches:
                partial_matches = [symbol for symbol in self.all_stock_symbols if search_text in symbol.upper()]
            if exact_matches:
                self.ui.visualization_tab.stock_selector.setCurrentText(exact_matches[0])
            elif partial_matches:
                self.ui.visualization_tab.stock_selector.setCurrentText(partial_matches[0])
            else:
                QMessageBox.warning(self.ui, "未找到", f"未找到匹配的股票代码: {search_text}")

    # 鼠标悬浮提示
    def toggle_hover_display(self):
        if self.ui.visualization_tab.hover_toggle.isChecked():
            self.enable_hover = True
        else:
            self.enable_hover = False
            self.ui.visualization_tab.main_plot.setTitle("")
            QToolTip.hideText()
        if hasattr(self, 'current_df'):
            self.update_plot(self.current_df)
            
    # 批量获取股票数据        
    def batch_fetch_stocks(self):
        try:
            # 从数据库获取 ticker 列表
            stock_symbols = fetch_tickers_from_db()
            if not stock_symbols:
                QMessageBox.warning(self.ui, "错误", "数据库中没有找到有效的股票代码")
                return

            # 读取 error_log_enriched_errorout.csv 中的错误 ticker
            error_log_path = ERRORstock_PATH.replace('error_log.csv', 'error_log_enriched_errorout.csv')
            error_tickers = set()
            try:
                with open(error_log_path, mode='r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    # 确保文件有 'Original Ticker' 列
                    if 'Original Ticker' not in reader.fieldnames:
                        print(f"警告: {error_log_path} 中缺少 'Original Ticker' 列，跳过错误 ticker 过滤")
                    else:
                        error_tickers = {row['Original Ticker'] for row in reader}
                        print(f"Loaded {len(error_tickers)} error tickers from {error_log_path}")
            except FileNotFoundError:
                print(f"错误日志文件 {error_log_path} 不存在，跳过错误 ticker 过滤")
            except Exception as e:
                print(f"读取错误日志文件失败: {e}")

            # 从 stock_symbols 中去除错误 ticker
            original_count = len(stock_symbols)
            stock_symbols = [symbol for symbol in stock_symbols if symbol not in error_tickers]
            filtered_count = len(stock_symbols)
            print(f"Filtered out {original_count - filtered_count} error tickers, remaining: {filtered_count}")

            # 检查过滤后的列表是否为空
            if not stock_symbols:
                QMessageBox.warning(self.ui, "错误", "过滤掉错误 ticker 后没有剩余的股票代码")
                return
            self.ui.data_fetch_tab.batch_fetch_button.setEnabled(False)
            self.batch_fetcher = BatchDataFetcher(stock_symbols)
            self.batch_fetcher.progress_updated.connect(self.update_progress)
            self.batch_fetcher.fetch_complete.connect(self.on_batch_fetch_complete)
            self.batch_fetcher.error_occurred.connect(self.show_error)
            self.batch_fetcher.start()
        except Exception as e:
            self.show_error(f"从数据库获取股票代码失败: {e}")
            print(e)

    # 更新进度条
    def update_progress(self, progress_data):
        current = progress_data.get('current', 0)
        total = progress_data.get('total', 1)
        start_time = progress_data.get('start_time')
        self.ui.data_fetch_tab.progress_bar.setMaximum(total)
        self.ui.data_fetch_tab.progress_bar.setValue(current)
        if start_time:
            elapsed = time.time() - start_time
            remaining = (elapsed / current) * (total - current) if current > 0 else 0
            elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))
            remaining_str = time.strftime("%H:%M:%S", time.gmtime(remaining))
            self.ui.data_fetch_tab.progress_info.setText(
                f"进度: {current}/{total} ({current/total*100:.1f}%) | "
                f"已用: {elapsed_str} | 剩余: {remaining_str}"
            )

    # 批量获取完成
    def on_batch_fetch_complete(self, message):
        self.ui.data_fetch_tab.batch_fetch_button.setEnabled(True)
        QMessageBox.information(self.ui, "完成", message)
        self.update_stock_selector()

    # 数据获取完成
    def on_fetch_complete(self, ticker):
        QMessageBox.information(self.ui, "成功", f"股票 {ticker} 的数据已更新。")
        self.update_stock_selector()

    def load_stock_data(self):
        ticker = self.ui.visualization_tab.stock_selector.currentText()
        period = int(self.ui.visualization_tab.period_selector.currentText())
        limit = period if period != 0 else None
        if ticker in self.data_cache:
            cached_data = self.data_cache[ticker]
            if limit is None or len(cached_data) >= limit:
                df = cached_data.tail(limit) if limit is not None else cached_data
                self.update_plot(df)
                return
        self.loader = DataLoader(ticker, self.engine, limit)
        self.loader.data_loaded.connect(self.on_data_loaded)
        self.loader.error_occurred.connect(self.show_error)
        self.loader.start()

    def on_data_loaded(self, df):
        ticker = self.ui.visualization_tab.stock_selector.currentText()
        self.data_cache[ticker] = df
        period = int(self.ui.visualization_tab.period_selector.currentText())
        if period != 0:
            df = df.tail(period)
        self.update_plot(df)

    def update_plot(self, df):
        self.ui.visualization_tab.main_plot.clear()
        self.ui.visualization_tab.volume_plot.clear()
        self.current_df = df
        enable_hover = self.ui.visualization_tab.hover_toggle.isChecked()
        plot_candlestick(
            self.ui.visualization_tab.main_plot,
            self.ui.visualization_tab.volume_plot,
            df,
            enable_hover
        )

    def show_error(self, message):
        QMessageBox.warning(self.ui, "错误", message)

    def save_db_config(self):
        db_config = {
            "host": self.ui.settings_tab.db_host_input.text(),
            "port": self.ui.settings_tab.db_port_input.text(),
            "user": self.ui.settings_tab.db_user_input.text(),
            "password": self.ui.settings_tab.db_password_input.text(),
            "dbname": self.ui.settings_tab.db_name_input.text(),
        }
        if not all(db_config.values()):
            QMessageBox.warning(self.ui, "错误", "请填写所有数据库配置项")
            return
        try:
            global DB_CONFIG
            DB_CONFIG.update(db_config)
            QMessageBox.information(self.ui, "成功", "数据库配置已保存")
        except Exception as e:
            QMessageBox.warning(self.ui, "错误", f"保存配置失败: {str(e)}")