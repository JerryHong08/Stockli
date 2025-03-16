from PyQt5.QtWidgets import (
    QWidget, QGridLayout, QLabel, QLineEdit, 
    QPushButton, QComboBox, QCheckBox, QFrame,
    QSplitter, QCompleter
)
from PyQt5.QtCore import Qt
import pyqtgraph as pg

class VisualizationTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        """初始化数据可视化选项卡"""
        layout = QGridLayout()
        layout.setSpacing(10)
        layout.setContentsMargins(20, 20, 20, 20)

        # 紧凑布局容器
        control_container = QWidget()
        control_layout = QGridLayout()
        control_layout.setSpacing(5)
        control_layout.setContentsMargins(0, 0, 0, 0)
        
        # 搜索框和确认按钮
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("搜索股票...")
        self.search_box.setFixedHeight(30)
        control_layout.addWidget(self.search_box, 0, 0, 1, 2)

        self.search_button = QPushButton("搜索")
        self.search_button.setFixedHeight(30)
        control_layout.addWidget(self.search_button, 0, 2)
        
        # 股票代码选择器
        self.stock_selector = QComboBox()
        self.stock_selector.setFixedHeight(30)
        control_layout.addWidget(self.stock_selector, 1, 0, 1, 2)

        # 周期选择器和加载按钮
        self.period_label = QLabel("周期:")
        self.period_label.setFixedHeight(30)
        control_layout.addWidget(self.period_label, 1, 2)

        self.period_selector = QComboBox()
        self.period_selector.addItems(["50", "200", "500", "1000"])
        self.period_selector.setFixedHeight(30)
        control_layout.addWidget(self.period_selector, 1, 3)

        self.load_button = QPushButton("加载")
        self.load_button.setFixedHeight(30)
        control_layout.addWidget(self.load_button, 1, 4)

        control_container.setLayout(control_layout)
        layout.addWidget(control_container, 0, 0, 1, 2)

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

        # 初始化自动补全
        self.completer = QCompleter([])
        self.completer.setCaseSensitivity(False)
        self.search_box.setCompleter(self.completer)

        self.setLayout(layout)

    def set_plot_style(self, plot_widget):
        """设置图表样式"""
        plot_widget.setBackground("w")
        plot_widget.showGrid(x=True, y=True, alpha=0.3)
        plot_widget.setLabel("left", "Price" if plot_widget == self.main_plot else "Volume")
        plot_widget.setLabel("bottom", "Date")
