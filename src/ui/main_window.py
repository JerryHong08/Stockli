from PyQt5.QtWidgets import QMainWindow, QVBoxLayout, QWidget, QTabWidget
from PyQt5.QtGui import QIcon
from PyQt5.QtCore import Qt
from .components.data_fetch_tab import DataFetchTab
from .components.visualization_tab import VisualizationTab
from .components.settings_tab import SettingsTab
from .components.random_stock_tab import RandomStockTab
from .main_logic import MainWindowLogic
import os

class MainWindowUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("股票数据可视化工具")
        self.setGeometry(100, 100, 800, 600)
        
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

    def load_stylesheet(self):
        """从外部文件加载样式表"""
        try:
            stylesheet_path = os.path.join(os.path.dirname(__file__), '..', 'styles', 'main.qss')
            with open(stylesheet_path, "r", encoding="utf-8") as f:
                self.setStyleSheet(f.read())
        except FileNotFoundError:
            print("样式表文件未找到，使用默认样式")
        except Exception as e:
            print(f"加载样式表失败: {e}")

    def connect_signals(self):
        """连接所有信号"""
        
        # 连接可视化选项卡信号
        self.visualization_tab.search_box.textChanged.connect(
            self.logic.filter_stock_selector
        )

    def init_ui(self):
        """初始化UI界面"""
        self.layout = QVBoxLayout()
        
        # 创建选项卡
        self.tabs = QTabWidget()
        self.layout.addWidget(self.tabs)

        # 初始化各个选项卡
        self.data_fetch_tab = DataFetchTab()
        self.visualization_tab = VisualizationTab()
        self.random_stock_tab = RandomStockTab()
        self.settings_tab = SettingsTab()

        # 添加选项卡
        self.tabs.addTab(self.data_fetch_tab, "数据获取")
        self.tabs.addTab(self.visualization_tab, "数据可视化")
        self.tabs.addTab(self.random_stock_tab, "随机股票生成")
        self.tabs.addTab(self.settings_tab, "设置")

        # 设置主窗口的中心部件
        container = QWidget()
        container.setLayout(self.layout)
        self.setCentralWidget(container)
