from PyQt5.QtWidgets import (
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

        # 任务倒计时
        self.task_timer_label = QLabel("任务倒计时: 00:00")
        layout.addWidget(self.task_timer_label, 0, 0, 1, 2)

        # 添加分割线
        line1 = QFrame()
        line1.setFrameShape(QFrame.HLine)
        line1.setFrameShadow(QFrame.Sunken)
        layout.addWidget(line1, 1, 0, 1, 2)

        # 批量获取数据按钮
        self.batch_fetch_button = QPushButton("一次性获取列表中的股票数据")
        self.batch_fetch_button.setFixedHeight(40)
        layout.addWidget(self.batch_fetch_button, 2, 0, 1, 2)

        # 进度条
        self.progress_bar = QProgressBar()
        self.progress_bar.setFixedHeight(20)
        self.progress_bar.setTextVisible(True)
        layout.addWidget(self.progress_bar, 3, 0, 1, 2)

        # 进度信息
        self.progress_info = QLabel("准备中...")
        layout.addWidget(self.progress_info, 4, 0, 1, 2)

        # 添加分割线
        line2 = QFrame()
        line2.setFrameShape(QFrame.HLine)
        line2.setFrameShadow(QFrame.Sunken)
        layout.addWidget(line2, 5, 0, 1, 2)

        # 输入股票代码部分
        self.input_label = QLabel("输入股票代码（如 SMCI):")
        layout.addWidget(self.input_label, 6, 0)
        
        self.stock_input = QLineEdit()
        self.stock_input.setPlaceholderText("请输入股票代码...")
        layout.addWidget(self.stock_input, 6, 1)

        # 获取数据按钮
        self.fetch_button = QPushButton("获取数据并存储到数据库")
        self.fetch_button.setFixedHeight(40)
        layout.addWidget(self.fetch_button, 7, 0, 1, 2)

        self.setLayout(layout)
