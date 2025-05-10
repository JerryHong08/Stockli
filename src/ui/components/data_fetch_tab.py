from datetime import datetime
from pytz import timezone
from PySide6.QtCore import QTimer

from PySide6.QtWidgets import (
    QWidget, QGridLayout, QPushButton, QProgressBar, 
    QLabel, QLineEdit, QFrame
)

class DataFetchTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        """初始化数据获取选项卡界面"""
        layout = QGridLayout()
        layout.setSpacing(15)  # 增加间距
        layout.setContentsMargins(20, 20, 20, 20)

        # 实时更新ET时间
        self.task_timer_label = QLabel()
        layout.addWidget(self.task_timer_label, 0, 0, 1, 2)
        
        def update_et_time():
            et_timezone = timezone('US/Eastern')
            current_time = datetime.now(et_timezone)
            formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
            self.task_timer_label.setText(f"ET Time: {formatted_time}")

        # 使用定时器每秒更新时间
        timer = QTimer(self)
        timer.timeout.connect(update_et_time)
        timer.start(1000)

        # 进度信息
        self.progress_info = QLabel("准备中...")
        layout.addWidget(self.progress_info, 2, 0, 1, 2)

        # 进度条
        self.progress_bar = QProgressBar()
        self.progress_bar.setFixedHeight(30)
        self.progress_bar.setTextVisible(True)
        layout.addWidget(self.progress_bar, 3, 0, 1, 2)
        
        # 批量获取数据按钮
        self.batch_fetch_button = QPushButton("一次性获取列表中的股票数据")
        self.batch_fetch_button.setFixedHeight(40)
        layout.addWidget(self.batch_fetch_button, 4, 0, 1, 2)

        self.setLayout(layout)
