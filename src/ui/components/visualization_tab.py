from PySide6.QtWidgets import (
    QWidget, QGridLayout, QLabel, QLineEdit, 
    QPushButton, QComboBox, QCheckBox, QFrame,
    QSplitter, QCompleter, QListWidget, QHBoxLayout, QAbstractItemView
)
from PySide6.QtCore import Qt
import pyqtgraph as pg
import numpy as np

# # 禁用 pyqtgraph 的 OpenGL 和多线程
# pg.setConfigOption('useOpenGL', False)
# pg.setConfigOption('enableExperimental', False)

class VisualizationTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.main_plot = None
        self.bg_color = 'w'  # Default white background
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
        
        # Search box and button
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Search stocks...") 
        self.search_box.setFixedHeight(40)
        control_layout.addWidget(self.search_box, 0, 0, 1, 2)

        self.search_button = QPushButton("Search")
        self.search_button.setFixedHeight(40)
        control_layout.addWidget(self.search_button, 0, 2)
        
        # Stock selector
        self.stock_selector = QComboBox()
        self.stock_selector.setFixedHeight(40)
        control_layout.addWidget(self.stock_selector, 1, 0, 1, 2)

        # Period selector and load button
        self.period_label = QLabel("Period:")
        self.period_label.setFixedHeight(40)
        control_layout.addWidget(self.period_label, 1, 2)

        self.period_selector = QComboBox()
        self.period_selector.addItems(["50", "200", "500", "1000"])
        self.period_selector.setFixedHeight(40)
        control_layout.addWidget(self.period_selector, 1, 3)

        self.load_button = QPushButton("Load")
        self.load_button.setFixedHeight(40)
        control_layout.addWidget(self.load_button, 1, 4)

        control_container.setLayout(control_layout)
        layout.addWidget(control_container, 0, 0, 1, 2)

        # Horizontal layout for subplot_selector, hover_toggle, and bg_toggle
        options_layout = QHBoxLayout()

        # Subplot selector
        self.subplot_selector = QListWidget()
        self.subplot_selector.setSelectionMode(QAbstractItemView.MultiSelection)
        self.subplot_selector.addItems(["Volume", "OBV"])
        self.subplot_selector.item(0).setSelected(True)
        self.subplot_selector.setMaximumHeight(80)  # Limit width to keep it compact
        options_layout.addWidget(self.subplot_selector)

        # Hover toggle
        self.hover_toggle = QCheckBox("Enable Mouse Hover")
        options_layout.addWidget(self.hover_toggle)

        # Background toggle
        self.bg_toggle = QCheckBox("Switch Background Color")
        options_layout.addWidget(self.bg_toggle)
        options_layout.addStretch()  # Push items to the left

        # Add options_layout to the main layout
        layout.addLayout(options_layout, 4, 0, 1, 2)

        # Splitter for main_plot and sub_plots
        self.splitter = QSplitter(Qt.Orientation.Vertical)
        self.main_plot = pg.PlotWidget()
        self.set_plot_style(self.main_plot)
        self.splitter.addWidget(self.main_plot)
        layout.addWidget(self.splitter, 5, 0, 1, 2)

        # Initialize autocompletion
        self.completer = QCompleter([])
        self.completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.search_box.setCompleter(self.completer)
        
        self.setLayout(layout)


    def set_plot_style(self, plot_widget):
        """Set plot styles"""
        plot_widget.setBackground(self.bg_color)
        plot_widget.showGrid(x=True, y=True, alpha=0.3)
        plot_widget.setLabel("left", "Price" if plot_widget == self.main_plot else "Value")
        plot_widget.setLabel("bottom", "Date")