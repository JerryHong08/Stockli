from PyQt5.QtWidgets import (
    QMainWindow, QVBoxLayout, QWidget, QLabel, QPushButton, QMessageBox,
    QSplitter, QLineEdit, QFrame, QGridLayout, QCompleter, QTabWidget,
    QComboBox, QCheckBox, QStackedLayout, QAbstractButton,QScrollArea
)
from PyQt5.QtGui import QIcon
from PyQt5.QtCore import Qt
import pyqtgraph as pg
from .main_logic import MainWindowLogic

class MainWindowUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("股票数据可视化")
        self.setGeometry(100, 100, 800, 600)
        
        # 设置全局样式表
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f5f5f5;
            }
            QPushButton {
                background-color: #C0C0C0;
                color: white;
                font-size: 14px;
                padding: 10px;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #808080;
            }
            QLabel {
                font-size: 14px;
                color: #333;
            }
            QLineEdit, QComboBox {
                padding: 8px;
                font-size: 14px;
                border: 1px solid #ccc;
                border-radius: 5px;
            }
            QLineEdit:focus, QComboBox:focus {
                border: 1px solid #4CAF50;
            }
            QFrame {
                border: 1px solid #ccc;
            }
            QTabWidget::pane {
                border: 1px solid #ccc;
                padding: 10px;
            }
            QTabBar::tab {
                background: #e0e0e0;
                padding: 10px;
                border: 1px solid #ccc;
                border-bottom: none;
                border-top-left-radius: 5px;
                border-top-right-radius: 5px;
            }
            QTabBar::tab:selected {
                background: #fff;
                border-bottom: 2px solid #579190;
            }
        """)
        
        # 初始化 UI
        self.init_ui()
        
        # 初始化 logic（在 UI 初始化之后）
        self.logic = MainWindowLogic(self)
        
        # 连接信号（在 logic 初始化之后）
        self.connect_signals()

    def connect_signals(self):
        """连接所有信号"""
        self.search_box.textChanged.connect(self.logic.filter_stock_selector)

    def init_ui(self):
        
        """初始化UI界面"""
        self.layout = QVBoxLayout()

        # 创建选项卡
        self.tabs = QTabWidget()
        self.layout.addWidget(self.tabs)

        # 数据获取选项卡
        self.data_fetch_tab = QWidget()
        self.init_data_fetch_tab()
        self.tabs.addTab(self.data_fetch_tab, "数据获取")

        # 数据可视化选项卡
        self.data_visualization_tab = QWidget()
        self.init_data_visualization_tab()
        self.tabs.addTab(self.data_visualization_tab, "数据可视化")
        
        # 每日随机推荐股
        self.random_stock_info = QWidget()
        self.init_random_stock_info_tab()
        self.tabs.addTab(self.random_stock_info, "随机股票生成")

        # 设置选项卡
        self.settings_tab = QWidget()
        self.init_settings_tab()
        self.tabs.addTab(self.settings_tab, "设置")
        
        

        # 设置主窗口的中心部件
        container = QWidget()
        container.setLayout(self.layout)
        self.setCentralWidget(container)

    def init_data_fetch_tab(self):
        """初始化数据获取选项卡"""
        layout = QGridLayout()
        layout.setSpacing(10)
        layout.setContentsMargins(20, 20, 20, 20)

        # 批量获取数据按钮
        self.batch_fetch_button = QPushButton("一次性获取列表中的股票数据")
        self.batch_fetch_button.setFixedHeight(40)
        layout.addWidget(self.batch_fetch_button, 0, 0, 1, 2)

        # 添加分割线
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setFrameShadow(QFrame.Sunken)
        layout.addWidget(line, 1, 0, 1, 2)

        # 输入股票代码部分
        self.input_label = QLabel("输入股票代码（如 SMCI）：")
        self.stock_input = QLineEdit()
        self.stock_input.setPlaceholderText("请输入股票代码...")
        layout.addWidget(self.input_label, 2, 0)
        layout.addWidget(self.stock_input, 2, 1)

        # 获取数据按钮
        self.fetch_button = QPushButton("获取数据并存储到数据库")
        self.fetch_button.setFixedHeight(40)
        layout.addWidget(self.fetch_button, 3, 0, 1, 2)

        # 设置布局
        self.data_fetch_tab.setLayout(layout)

    def init_data_visualization_tab(self):
        """初始化数据可视化选项卡"""
        layout = QGridLayout()
        layout.setSpacing(10)
        layout.setContentsMargins(20, 20, 20, 20)

        # 搜索框和确认按钮
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("输入股票代码或名称进行搜索...")
        layout.addWidget(self.search_box, 0, 0)

        self.search_button = QPushButton("确认搜索")
        layout.addWidget(self.search_button, 0, 1)
        
        # 股票代码选择器
        self.stock_selector = QComboBox()
        layout.addWidget(self.stock_selector, 1, 0, 1, 2)

        # 周期选择器和加载按钮
        self.period_label = QLabel("选择查看最近N个时间周期：")
        layout.addWidget(self.period_label, 2, 0)

        self.period_selector = QComboBox()
        self.period_selector.addItems(["50", "200", "500", "1000"])
        layout.addWidget(self.period_selector, 2, 1)

        self.load_button = QPushButton("加载数据并绘制蜡烛图")
        layout.addWidget(self.load_button, 3, 0, 1, 2)

        # 鼠标悬停显示开关
        self.hover_toggle = QCheckBox("启用鼠标悬停显示")
        layout.addWidget(self.hover_toggle, 4, 0, 1, 2)

        # 使用 QSplitter 分割主图和成交量图
        self.splitter = QSplitter(Qt.Vertical)
        self.main_plot = pg.PlotWidget()
        self.volume_plot = pg.PlotWidget()
        
        # 设置图表样式
        self.set_plot_style(self.main_plot)
        self.set_plot_style(self.volume_plot)
        
        self.splitter.addWidget(self.main_plot)
        self.splitter.addWidget(self.volume_plot)
        layout.addWidget(self.splitter, 5, 0, 1, 2)

        # 设置自动补全pyp
        self.completer = QCompleter([])
        self.completer.setCaseSensitivity(False)
        self.search_box.setCompleter(self.completer)
        
        # Remove the signal connection from here
        # self.search_box.textChanged.connect(self.logic.filter_stock_selector)
        
        # 设置布局
        self.data_visualization_tab.setLayout(layout)

    def set_plot_style(self, plot_widget):
        """设置图表样式"""
        plot_widget.setBackground("w")  # 设置背景为白色
        plot_widget.showGrid(x=True, y=True, alpha=0.3)  # 显示网格线
        plot_widget.setLabel("left", "Price" if plot_widget == self.main_plot else "Volume")  # 设置Y轴标签
        plot_widget.setLabel("bottom", "Date")  # 设置X轴标签
        
    def init_random_stock_info_tab(self):
        """初始化每日股票推荐选项卡"""
        layout = QVBoxLayout()
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # 创建按钮
        self.random_stock = QPushButton("🎲 随机股票")
        self.random_stock.setFixedHeight(40)
        self.random_stock.setStyleSheet("""
            QPushButton {
                background-color: #579190;
                color: white;
                border-radius: 5px;
                font-size: 16px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #4a7a79;
            }
        """)
        
        # 信息显示区域
        self.stock_info_scroll = QScrollArea()
        self.stock_info_widget = QWidget()
        self.stock_info_layout = QVBoxLayout()
        self.stock_info_widget.setLayout(self.stock_info_layout)
        self.stock_info_scroll.setWidget(self.stock_info_widget)
        self.stock_info_scroll.setWidgetResizable(True)
        self.stock_info_scroll.setStyleSheet("""
            QScrollArea {
                border: 1px solid #ddd;
                border-radius: 5px;
                background-color: #f9f9f9;
            }
        """)
        
        # 添加分隔线
        separator = QFrame()
        separator.setFrameShape(QFrame.HLine)
        separator.setFrameShadow(QFrame.Sunken)
        separator.setStyleSheet("color: #ddd;")
        
        # 添加到布局中
        layout.addWidget(self.random_stock)
        layout.addWidget(separator)
        layout.addWidget(self.stock_info_scroll)
        
        self.random_stock_info.setLayout(layout)
    
    
    def init_settings_tab(self):
        """初始化设置选项卡"""
        layout = QVBoxLayout()

        # 数据库配置
        self.db_config_label = QLabel("数据库配置：")
        layout.addWidget(self.db_config_label)

        self.db_host_input = QLineEdit()
        self.db_host_input.setPlaceholderText("数据库主机")
        layout.addWidget(self.db_host_input)

        self.db_port_input = QLineEdit()
        self.db_port_input.setPlaceholderText("数据库端口")
        layout.addWidget(self.db_port_input)

        self.db_user_input = QLineEdit()
        self.db_user_input.setPlaceholderText("数据库用户")
        layout.addWidget(self.db_user_input)

        self.db_password_input = QLineEdit()
        self.db_password_input.setPlaceholderText("数据库密码")
        self.db_password_input.setEchoMode(QLineEdit.Password)
        layout.addWidget(self.db_password_input)

        self.db_name_input = QLineEdit()
        self.db_name_input.setPlaceholderText("数据库名称")
        layout.addWidget(self.db_name_input)

        self.save_db_config_button = QPushButton("保存数据库配置")
        layout.addWidget(self.save_db_config_button)

        self.settings_tab.setLayout(layout)
