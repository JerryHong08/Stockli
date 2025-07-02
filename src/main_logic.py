import time
from PySide6.QtCore import QThread, QStringListModel
from PySide6.QtWidgets import QMessageBox, QToolTip, QWidget, QLabel, QHBoxLayout
import pandas as pd
import os
import sys
from src.database.db_operations import fetch_table_names
from src.utils.time_teller import get_latest_date_from_longport
from src.data_fetcher.batch_fetcher import BatchDataFetcher
from src.data_fetcher.data_loader import DataLoader
from src.data_fetcher.polygon_incremental_update import ipo_incremental_update, process_delisted, process_delisted_reverse, DelistedProcessThread, MsProcessThread
from src.config.paths import STOCK_LIST_PATH
from src.data_visualization.candlestick_plot import plot_candlestick, plot_volume, plot_obv
from src.database.db_connection import get_engine, check_connection, DatabaseConnectionError
from src.config.db_config import DB_CONFIG
import yfinance as yf
import psycopg2
from src.utils.logger import setup_logger
import csv
import pyqtgraph as pg
import numpy as np
from src.config.paths import ERRORstock_PATH  # 错误日志路径

# 数据库连接函数
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

class MainWindowLogic:
    def __init__(self, ui):
        self.ui = ui
        self.data_cache = {}
        self.all_stock_symbols = []
        self.current_fetcher = None
        self.batch_fetcher = None
        self.loader = None
        try:
            self.engine = get_engine()
            if not check_connection(self.engine):
                raise DatabaseConnectionError("数据库连接测试失败")
        except DatabaseConnectionError as e:
            QMessageBox.critical(self.ui, "错误", f"无法连接到数据库: {str(e)}")
            sys.exit(1)
            
        self.connect_signals()
        self.update_stock_selector()
        self.ui.data_fetch_tab.limit_time_label.setText(f"获取数据的截至日期为：{get_latest_date_from_longport().strftime('%Y-%m-%d')}")
        

    # 连接信号和槽
    def connect_signals(self):
        self.ui.data_fetch_tab.batch_fetch_button.clicked.connect(self.batch_fetch_stocks)
        self.ui.visualization_tab.subplot_selector.itemSelectionChanged.connect(self.on_subplot_selection_changed)
        self.ui.visualization_tab.search_button.clicked.connect(self.confirm_search)
        self.ui.visualization_tab.load_button.clicked.connect(self.load_stock_data)
        self.ui.visualization_tab.hover_toggle.stateChanged.connect(self.toggle_hover_display)
        self.ui.visualization_tab.search_box.textChanged.connect(self.filter_stock_selector)
        
    
    # 关闭窗口时清理资源
    def cleanup(self):
        """清理线程和资源"""
        print("Cleaning up MainWindowLogic...")
        # 终止 batch_fetcher
        if self.batch_fetcher and self.batch_fetcher.isRunning():
            print("Terminating batch_fetcher thread...")
            self.batch_fetcher.terminate()
            self.batch_fetcher.wait(1000)  # 等待最多1秒
            self.batch_fetcher = None
        # 终止 loader
        if self.loader and self.loader.isRunning():
            print("Terminating loader thread...")
            self.loader.terminate()
            self.loader.wait(1000)
            self.loader = None
        # 清理数据库连接
        if hasattr(self, 'engine'):
            if self.engine is not None:
                self.engine.dispose()
                print("Database engine disposed")

    # 数据规范化
    def format_value(self, value):
        if isinstance(value, (int, float)):
            if abs(value) >= 1_000_000_000:
                return f"{value/1_000_000_000:.2f}B"
            elif abs(value) >= 1_000_000:
                return f"{value/1_000_000:.2f}M"
            elif abs(value) >= 1_000:
                return f"{value/1_000:.2f}K"
        return str(value)
    
        
    # 清理股票选择器
    def filter_stock_selector(self):
        search_text = self.ui.visualization_tab.search_box.text().strip().lower()
        if not search_text:
            self.ui.visualization_tab.stock_selector.clear()
            self.ui.visualization_tab.stock_selector.addItems(self.all_stock_symbols)
            return
        # 完全匹配
        exact_matches = [symbol for symbol in self.all_stock_symbols if symbol.lower() == search_text]
        # 前缀匹配（但排除完全匹配）
        prefix_matches = [symbol for symbol in self.all_stock_symbols if symbol.lower().startswith(search_text) and symbol.lower() != search_text]
        # 包含匹配（但排除前两种）
        contains_matches = [symbol for symbol in self.all_stock_symbols if search_text in symbol.lower() and not symbol.lower().startswith(search_text)]

        filtered_symbols = exact_matches + prefix_matches + contains_matches
        self.ui.visualization_tab.stock_selector.clear()
        self.ui.visualization_tab.stock_selector.addItems(filtered_symbols)
    
    # 更新股票选择器
    def update_stock_selector(self):
        self.all_stock_symbols = fetch_table_names(self.engine)
        self.ui.visualization_tab.stock_selector.clear()
        self.ui.visualization_tab.stock_selector.addItems(self.all_stock_symbols)
        model = QStringListModel()
        model.setStringList(self.all_stock_symbols)
        self.ui.visualization_tab.completer.setModel(model)
        # 手动设置 QCompleter 的 popup 样式
        popup = self.ui.visualization_tab.completer.popup()
        popup.setStyleSheet("""
            QAbstractItemView {
                background-color: #ffffff;
                border: 1px solid #ccc;
                border-radius: 5px;
                color: #222;
                selection-background-color: #579190;
                selection-color: white;
            }
        """)
        
    # 搜索文字框
    def confirm_search(self):
        search_text = self.ui.visualization_tab.search_box.text().strip().upper() # 转换为大写
        if search_text:
            exact_matches = [symbol for symbol in self.all_stock_symbols if symbol.upper() == search_text] # 完全匹配
            if not exact_matches:
                partial_matches = [symbol for symbol in self.all_stock_symbols if search_text in symbol.upper()] # 部分匹配
            if exact_matches:
                self.ui.visualization_tab.stock_selector.setCurrentText(exact_matches[0]) # 将搜索框的内容设置为匹配的股票代码
            elif partial_matches:
                self.ui.visualization_tab.stock_selector.setCurrentText(partial_matches[0])
            else:
                QMessageBox.warning(self.ui, "未找到", f"未找到匹配的股票代码: {search_text}")

    # 切换鼠标悬停显示
    def toggle_hover_display(self):
        if self.ui.visualization_tab.hover_toggle.isChecked():
            self.enable_hover = True
        else:
            self.enable_hover = False
            self.ui.visualization_tab.main_plot.setTitle("")
            QToolTip.hideText()
        if hasattr(self, 'current_df') and self.current_df is not None:
            self.plot_main_chart(self.current_df, auto_range=False)
    
    # 批量获取股票数据
    def batch_fetch_stocks(self): 
        try:
            stock_symbols = fetch_tickers_from_db() # 从tickers_fundamental数据库获取active为true或null的股票
            if not stock_symbols:
                QMessageBox.warning(self.ui, "错误", "数据库中没有找到有效的股票代码")
                return
            error_log_path = ERRORstock_PATH
            error_tickers = set()
            try:
                with open(error_log_path, mode='r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    if not reader.fieldnames or 'Original Ticker' not in reader.fieldnames:
                        print(f"警告: {error_log_path} 中缺少 'Original Ticker' 列，跳过错误 ticker 过滤")
                    else:
                        error_tickers = {row['Original Ticker'] for row in reader}
                        print(f"Loaded {len(error_tickers)} error tickers from {error_log_path}")
            except FileNotFoundError:
                print(f"错误日志文件 {error_log_path} 不存在，跳过错误 ticker 过滤")
            except Exception as e:
                print(f"读取错误日志文件失败: {e}")

            original_count = len(stock_symbols)
            self.stock_symbols = [symbol for symbol in stock_symbols if symbol not in error_tickers]
            filtered_count = len(stock_symbols)
            print(f"Filtered out {original_count - filtered_count} error tickers, remaining: {filtered_count}")

            if not stock_symbols:
                QMessageBox.warning(self.ui, "错误", "过滤掉错误 ticker 后没有剩余的股票代码")
                return
            self.ui.data_fetch_tab.batch_fetch_button.setEnabled(False)
            
            self.limit_date = get_latest_date_from_longport().strftime("%Y-%m-%d")
            print(f"Limit date for fetching IPO and delisted tickers: {self.limit_date}")
            
            # Step 1: Run MS thread
            self.ms_thread = MsProcessThread(self.limit_date)
            self.ms_thread.finished_with_result.connect(self.on_ms_finished)
            self.ms_thread.start()
        except Exception as e:
            self.show_error(f"从数据库获取股票代码失败: {e}")
            print(e)
            
    def on_ms_finished(self, ms_filtered):
        # print("=== on_ms_finished 被调用 ===")
        # print(f"接收到的 ms_filtered 类型: {type(ms_filtered)}")
        # print(f"接收到的 ms_filtered 长度: {len(ms_filtered) if hasattr(ms_filtered, '__len__') else 'N/A'}")
        self.ms_filtered = ms_filtered
        # 安全关闭 ms_thread
        self.ms_thread.quit()
        self.ms_thread.wait()
        self.ms_thread = None
        # Step 2: Run delisted thread
        self.delisted_thread = DelistedProcessThread(self.limit_date)
        self.delisted_thread.finished.connect(self.on_delisted_finished)
        self.delisted_thread.start()
    
    def on_delisted_finished(self):
        # 安全关闭 delisted_thread
        self.delisted_thread.quit()
        self.delisted_thread.wait()
        self.delisted_thread = None
        # Step 3: Now run batch fetcher
        self.batch_fetcher = BatchDataFetcher(self.stock_symbols)
        self.batch_fetcher.progress_updated.connect(self.update_progress)
        self.batch_fetcher.fetch_complete.connect(self.on_batch_fetch_complete)
        self.batch_fetcher.error_occurred.connect(self.show_error)
        self.batch_fetcher.start()
    
    # 批量获取完成
    def on_batch_fetch_complete(self, message):
        self.ui.data_fetch_tab.batch_fetch_button.setEnabled(True)
        QMessageBox.information(self.ui, "完成", message)    
        process_delisted_reverse(self.ms_filtered)
        ipo_incremental_update(self.limit_date)
        self.update_stock_selector()
    
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

    # 加载图表按钮    
    def load_stock_data(self):
        ticker = self.ui.visualization_tab.stock_selector.currentText()
        period = int(self.ui.visualization_tab.period_selector.currentText())
        limit = period if period != 0 else None
        if ticker in self.data_cache:
            cached_data = self.data_cache[ticker]
            if limit is None or len(cached_data) >= limit:
                df = cached_data.tail(limit) if limit is not None else cached_data
                self.plot_main_chart(df)
                self.plot_subplots(df)
                return
        self.loader = DataLoader(ticker, self.engine, limit)
        self.loader.data_loaded.connect(self.on_data_loaded)
        self.loader.error_occurred.connect(self.show_error)
        self.loader.start()

    # 图表加载
    def on_data_loaded(self, df):
        ticker = self.ui.visualization_tab.stock_selector.currentText()
        self.data_cache[ticker] = df
        period = int(self.ui.visualization_tab.period_selector.currentText())
        if period != 0:
            df = df.tail(period)
        self.plot_main_chart(df)
        self.plot_subplots(df)
        
    
    def plot_main_chart(self, df, auto_range=True):
        self.ui.visualization_tab.main_plot.clear()
        self.current_df = df
        enable_hover = self.ui.visualization_tab.hover_toggle.isChecked()
        print("toggle hover display:", enable_hover)
        plot_candlestick(self.ui.visualization_tab.main_plot, df, enable_hover, auto_range)
        axis = pg.DateAxisItem(orientation='bottom')
        self.ui.visualization_tab.main_plot.setAxisItems({'bottom': axis})
        x = np.arange(len(df))
        ticks = [(x[i], df["Date"].iloc[i].strftime('%Y%m%d')) for i in range(0, len(df), 5)]
        axis.setTicks([ticks])
        
    def plot_subplots(self, df):
        splitter = self.ui.visualization_tab.splitter
        # 移除除main_plot外的所有widget
        while splitter.count() > 1:
            widget = splitter.widget(1)
            widget.setParent(None)
            widget.deleteLater()
        # 获取选择的subplot
        selected_items = self.ui.visualization_tab.subplot_selector.selectedItems()
        selected_subplots = [item.text() for item in selected_items]
        for subplot_type in selected_subplots:
            subplot = pg.PlotWidget()
            if subplot_type == "Volume":
                plot_volume(subplot, df)
            elif subplot_type == "OBV":
                plot_obv(subplot, df)
            subplot.setXLink(self.ui.visualization_tab.main_plot)
            splitter.addWidget(subplot)

    def on_subplot_selection_changed(self):
        if hasattr(self, 'current_df') and self.current_df is not None:
            self.plot_subplots(self.current_df)

    # 显示错误消息
    def show_error(self, message):
        QMessageBox.warning(self.ui, "错误", message)
        
# 从数据库获取所有 ticker
def fetch_tickers_from_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ticker FROM tickers_fundamental WHERE active is not False")
    tickers = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    print(f"Fetched {len(tickers)} tickers from database")
    return tickers
