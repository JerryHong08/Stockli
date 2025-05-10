from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, 
    QLineEdit, QPushButton
)

class ScreenerTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        """股票筛选器选项卡"""
        layout = QVBoxLayout()
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)

        # Screener配置
        self.sreener = QLabel("筛选器页面：")
        layout.addWidget(self.sreener)
        
        self.confirm_search = QPushButton("确认筛选")
        layout.addWidget(self.confirm_search)
        
        self.setLayout(layout)