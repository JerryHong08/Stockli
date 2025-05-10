from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QPushButton, 
    QLabel, QScrollArea, QFrame
)

class RandomStockTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        """初始化随机股票推荐选项卡"""
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
        
        self.setLayout(layout)
