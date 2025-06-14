from PySide6.QtWidgets import (
    QWidget, QGridLayout, QLabel, QLineEdit, 
    QPushButton, QComboBox, QCheckBox, QFrame,
    QSplitter, QCompleter, QListWidget, QHBoxLayout, QAbstractItemView
)
from PySide6.QtCore import Qt
import pyqtgraph as pg
import numpy as np

class nlp_screener(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        """Initialize the data visualization tab"""
        layout = QGridLayout()
        layout.setSpacing(10)
        layout.setContentsMargins(20, 20, 20, 20)

        # Compact control container
        control_container = QWidget()
        control_layout = QGridLayout()
        control_layout.setSpacing(5)
        control_layout.setContentsMargins(0, 0, 0, 0) 
        
        # 文字输入框
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("描述你的策略想法...") 
        self.search_box.setFixedHeight(40)
        control_layout.addWidget(self.search_box, 0, 0, 1, 2)

        # 确认搜索按钮
        self.search_button = QPushButton("AI解析")
        self.search_button.setFixedHeight(40)
        control_layout.addWidget(self.search_button, 0, 2)

        # control_container.setLayout(control_layout)
        # layout.addWidget(control_container, 0, 0, 1, 2)
        
        self.setLayout(control_layout)