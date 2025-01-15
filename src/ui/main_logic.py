from PyQt5.QtCore import QThread, pyqtSignal, QStringListModel
from PyQt5.QtWidgets import (
    QMessageBox, QToolTip, QWidget, QLabel, QHBoxLayout)
import pandas as pd
import os
import sys
from database.db_operations import fetch_table_names
from data_fetcher.data_fetcher import DataFetcher
from data_fetcher.batch_fetcher import BatchDataFetcher
from data_fetcher.data_loader import DataLoader
from config.paths import STOCK_LIST_PATH
from data_visualization.candlestick_plot import plot_candlestick
from database.db_connection import DB_CONFIG, get_engine
import yfinance as yf

class MainWindowLogic:
    def __init__(self, ui):
        self.ui = ui
        self.data_cache = {}
        self.all_stock_symbols = []
        # 初始化 SQLAlchemy 引擎
        self.engine = get_engine()
        if self.engine is None:
            QMessageBox.critical(self.ui, "错误", "无法连接到数据库，请检查数据库配置。")
            sys.exit(1)
            
        self.connect_signals()
        self.update_stock_selector()
        
    def connect_signals(self):
        """连接信号和槽"""
        self.ui.batch_fetch_button.clicked.connect(self.batch_fetch_data)
        self.ui.fetch_button.clicked.connect(self.fetch_data)
        self.ui.search_button.clicked.connect(self.confirm_search)
        self.ui.load_button.clicked.connect(self.load_and_plot_data)
        self.ui.hover_toggle.stateChanged.connect(self.mouse_hover)
        self.ui.save_db_config_button.clicked.connect(self.save_db_config)
        self.ui.search_box.textChanged.connect(self.filter_stock_selector)
        self.ui.random_stock.clicked.connect(self.on_button_click)

    def format_value(self, value):
        """格式化数值显示"""
        if isinstance(value, (int, float)):
            if abs(value) >= 1_000_000_000:
                return f"{value/1_000_000_000:.2f}B"
            elif abs(value) >= 1_000_000:
                return f"{value/1_000_000:.2f}M"
            elif abs(value) >= 1_000:
                return f"{value/1_000:.2f}K"
        return str(value)

    def on_button_click(self):
        try:
            # 从数据库随机获取一个股票代码
            table_names = fetch_table_names(DB_CONFIG)
            if not table_names:
                QMessageBox.warning(self.ui, "错误", "数据库中没有股票数据")
                return
            
            import random
            random_stock = random.choice(table_names)
            
            # 获取股票信息
            ticker = yf.Ticker(random_stock)
            info = ticker.get_info()
            
            # 清空之前的显示内容
            for i in reversed(range(self.ui.stock_info_layout.count())): 
                self.ui.stock_info_layout.itemAt(i).widget().setParent(None)
            
            # 创建信息显示组件
            def add_info_row(label, value):
                row = QWidget()
                row_layout = QHBoxLayout()
                row_layout.setContentsMargins(0, 5, 0, 5)
                
                label_widget = QLabel(f"<b>{label}:</b>")
                label_widget.setStyleSheet("font-size: 14px; color: #333;")
                
                value_widget = QLabel(str(value))
                value_widget.setStyleSheet("font-size: 14px; color: #555;")
                value_widget.setWordWrap(True)
                
                row_layout.addWidget(label_widget)
                row_layout.addWidget(value_widget)
                row.setLayout(row_layout)
                self.ui.stock_info_layout.addWidget(row)
            
            # 添加基本信息
            add_info_row("股票代码", random_stock)
            add_info_row("公司名称", info.get('longName', 'N/A'))
            add_info_row("当前价格", f"${self.format_value(info.get('currentPrice', 'N/A'))}")
            add_info_row("市值", f"${self.format_value(info.get('marketCap', 'N/A'))}")
            add_info_row("行业", info.get('industry', 'N/A'))
            
            # 添加关键指标
            add_info_row("52周最高", f"${self.format_value(info.get('fiftyTwoWeekHigh', 'N/A'))}")
            add_info_row("52周最低", f"${self.format_value(info.get('fiftyTwoWeekLow', 'N/A'))}")
            add_info_row("市盈率", info.get('trailingPE', 'N/A'))
            add_info_row("股息率", f"{float(info.get('dividendYield', 0))*100:.2f}%" if info.get('dividendYield') else 'N/A')
            
            # 添加官网链接
            website = info.get('website', 'N/A')
            if website != 'N/A':
                website_link = QLabel(f'<a href="{website}">{website}</a>')
                website_link.setOpenExternalLinks(True)
                website_link.setStyleSheet("font-size: 14px; color: #1a73e8;")
                self.ui.stock_info_layout.addWidget(QLabel("<b>官网:</b>"))
                self.ui.stock_info_layout.addWidget(website_link)
            
            # 添加公司简介
            summary = info.get('longBusinessSummary', 'N/A')
            if summary != 'N/A':
                summary_label = QLabel("<b>公司简介:</b>")
                summary_label.setStyleSheet("font-size: 14px; color: #333; margin-top: 15px;")
                self.ui.stock_info_layout.addWidget(summary_label)
                
                summary_text = QLabel(summary)
                summary_text.setStyleSheet("font-size: 14px; color: #555;")
                summary_text.setWordWrap(True)
                self.ui.stock_info_layout.addWidget(summary_text)
            
        except Exception as e:
            QMessageBox.warning(self.ui, "错误", f"获取股票信息失败: {str(e)}")


    def update_stock_selector(self):
        """更新股票代码选择器"""
        self.all_stock_symbols = fetch_table_names(DB_CONFIG)
        self.ui.stock_selector.clear()
        self.ui.stock_selector.addItems(self.all_stock_symbols)
        
        # Update completer with new model
        model = QStringListModel()
        model.setStringList(self.all_stock_symbols)
        self.ui.completer.setModel(model)

    def filter_stock_selector(self):
        """根据搜索框内容过滤股票代码选择器"""
        search_text = self.ui.search_box.text().strip().lower()
        self.ui.stock_selector.clear()
        filtered_symbols = [symbol for symbol in self.all_stock_symbols if search_text in symbol.lower()]
        self.ui.stock_selector.addItems(filtered_symbols)

    def confirm_search(self):
        """确认搜索"""
        search_text = self.ui.search_box.text().strip().upper()  # 转换为大写
        if search_text:
            # 优先精确匹配
            exact_matches = [symbol for symbol in self.all_stock_symbols 
                        if symbol.upper() == search_text]
            
            # 如果没有精确匹配，再找包含匹配
            if not exact_matches:
                partial_matches = [symbol for symbol in self.all_stock_symbols 
                                if search_text in symbol.upper()]
            
            # 如果有匹配结果
            if exact_matches:
                self.ui.stock_selector.setCurrentText(exact_matches[0])
            elif partial_matches:
                self.ui.stock_selector.setCurrentText(partial_matches[0])
            else:
                QMessageBox.warning(self.ui, "未找到", f"未找到匹配的股票代码: {search_text}")
    def mouse_hover(self):
        """根据复选框状态启用或禁用鼠标悬停功能"""
        if self.ui.hover_toggle.isChecked():
            self.enable_hover = True
        else:
            self.enable_hover = False
            self.ui.main_plot.setTitle("")
            QToolTip.hideText()

        if hasattr(self, 'current_df'):
            self.update_plot(self.current_df)

    def batch_fetch_data(self):
        """批量获取数据"""
        try:
            if not os.path.exists(STOCK_LIST_PATH):
                QMessageBox.warning(self.ui, "错误", f"未找到文件: {STOCK_LIST_PATH}")
                return

            df = pd.read_csv(STOCK_LIST_PATH)
            if "Symbol" not in df.columns:
                QMessageBox.warning(self.ui, "错误", "CSV 文件中缺少 'Symbol' 列")
                return

            stock_symbols = df["Symbol"].tolist()
            if not stock_symbols:
                QMessageBox.warning(self.ui, "错误", "CSV 文件中没有股票代码")
                return

            self.ui.batch_fetch_button.setEnabled(False)
            self.batch_fetcher = BatchDataFetcher(stock_symbols)
            self.batch_fetcher.progress_updated.connect(self.update_progress)
            self.batch_fetcher.fetch_complete.connect(self.on_batch_fetch_complete)
            self.batch_fetcher.error_occurred.connect(self.show_error)
            self.batch_fetcher.start()

        except Exception as e:
            self.show_error(f"读取 CSV 文件失败: {e}")

    def update_progress(self, message):
        """更新进度信息"""
        self.ui.statusBar().showMessage(message)

    def on_batch_fetch_complete(self, message):
        """批量获取完成后的回调"""
        self.ui.batch_fetch_button.setEnabled(True)
        QMessageBox.information(self.ui, "完成", message)
        self.update_stock_selector()

    def fetch_data(self):
        """获取数据"""
        stock_symbol = self.ui.stock_input.text().strip()
        if not stock_symbol:
            QMessageBox.warning(self.ui, "错误", "请输入股票代码！")
            return

        self.fetcher = DataFetcher(stock_symbol)
        self.fetcher.fetch_complete.connect(self.on_fetch_complete)
        self.fetcher.error_occurred.connect(self.show_error)
        self.fetcher.start()

    def on_fetch_complete(self, table_name):
        """数据获取完成后的回调"""
        QMessageBox.information(self.ui, "成功", f"数据已成功存储到表 '{table_name}'。")
        self.update_stock_selector()

    def load_and_plot_data(self):
        """加载数据并绘制图表"""
        table_name = self.ui.stock_selector.currentText()
        period = int(self.ui.period_selector.currentText())
        limit = period if period != 0 else None

        if table_name in self.data_cache:
            cached_data = self.data_cache[table_name]
            if limit is None or len(cached_data) >= limit:
                df = cached_data.tail(limit) if limit is not None else cached_data
                self.update_plot(df)
                return

        self.loader = DataLoader(table_name, self.engine, limit)
        self.loader.data_loaded.connect(self.on_data_loaded)
        self.loader.error_occurred.connect(self.show_error)
        self.loader.start()

    def on_data_loaded(self, df):
        """数据加载完成后的回调"""
        table_name = self.ui.stock_selector.currentText()
        self.data_cache[table_name] = df
        period = int(self.ui.period_selector.currentText())
        if period != 0:
            df = df.tail(period)
        self.update_plot(df)

    def update_plot(self, df):
        """更新图表"""
        self.ui.main_plot.clear()
        self.ui.volume_plot.clear()
        self.current_df = df
        enable_hover = self.ui.hover_toggle.isChecked()
        plot_candlestick(self.ui.main_plot, self.ui.volume_plot, df, enable_hover)

    def show_error(self, message):
        """显示错误信息"""
        QMessageBox.warning(self.ui, "错误", message)

    def save_db_config(self):
        """保存数据库配置"""
        db_config = {
            "host": self.ui.db_host_input.text(),
            "port": self.ui.db_port_input.text(),
            "user": self.ui.db_user_input.text(),
            "password": self.ui.db_password_input.text(),
            "dbname": self.ui.db_name_input.text(),
        }
        
        # Validate the inputs
        if not all(db_config.values()):
            QMessageBox.warning(self.ui, "错误", "请填写所有数据库配置项")
            return
        
        try:
            # Here you can add code to save the configuration to a file
            # or update your DB_CONFIG
            global DB_CONFIG
            DB_CONFIG.update(db_config)
            QMessageBox.information(self.ui, "成功", "数据库配置已保存")
        except Exception as e:
            QMessageBox.warning(self.ui, "错误", f"保存配置失败: {str(e)}")




