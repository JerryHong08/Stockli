from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, 
    QLineEdit, QPushButton
)

class SettingsTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        """初始化设置选项卡"""
        layout = QVBoxLayout()
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)

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

        self.setLayout(layout)
