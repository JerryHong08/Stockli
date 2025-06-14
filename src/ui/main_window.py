from PySide6.QtWidgets import QMainWindow, QVBoxLayout, QWidget, QTabWidget
from PySide6.QtGui import QIcon
from PySide6.QtCore import Qt
from .components.data_fetch_tab import DataFetchTab
from .components.visualization_tab import VisualizationTab
from .components.nlp_stock_screener import nlp_screener
from .main_logic import MainWindowLogic
import os
from config.paths import ICON_PATH

class MainWindowUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("股票数据可视化工具")
        self.setGeometry(100, 100, 800, 600)
        
        # 设置窗口图标
        icon_path = ICON_PATH
        self.setWindowIcon(QIcon(icon_path))
        
        # 初始化UI
        self.init_ui()
        
        # 初始化logic
        self.logic = MainWindowLogic(self)
        
        # 加载样式表
        self.load_stylesheet()
        
        # 连接信号
        self.connect_signals()
        
        # 默认最大化显示
        self.showMaximized()

    # 加载样式表
    def load_stylesheet(self):
        """从外部文件加载样式表"""
        try:
            stylesheet_path = os.path.join(os.path.dirname(__file__), '..', 'styles', 'main.qss')
            with open(stylesheet_path, "r", encoding="utf-8") as f:
                self.setStyleSheet(f.read())
                print("样式表加载成功")
        except FileNotFoundError:
            print("样式表文件未找到，使用默认样式")
        except Exception as e:
            print(f"加载样式表失败: {e}")

    def connect_signals(self):
        """连接所有信号"""
        self.visualization_tab.search_box.textChanged.connect(
            self.logic.filter_stock_selector
        )

    def init_ui(self):
        """初始化UI界面"""
        self.main_layout = QVBoxLayout()
        
        # 创建选项卡
        self.tabs = QTabWidget()
        self.main_layout.addWidget(self.tabs)

        # 初始化各个选项卡
        self.data_fetch_tab = DataFetchTab()
        self.visualization_tab = VisualizationTab()
        self.nlp_stock_screener = nlp_screener()
        
        # 添加选项卡
        self.tabs.addTab(self.visualization_tab, "数据可视化")
        self.tabs.addTab(self.data_fetch_tab, "数据获取")
        self.tabs.addTab(self.nlp_stock_screener, "AI智能股票筛选器")

        # 设置主窗口的中心部件
        container = QWidget()
        container.setLayout(self.main_layout)
        self.setCentralWidget(container)