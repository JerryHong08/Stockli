from PySide6.QtWidgets import (
    QWidget, QGridLayout, QLabel, QLineEdit, 
    QPushButton, QComboBox, QCheckBox, QFrame,
    QSplitter, QCompleter, QListWidget, QHBoxLayout, QAbstractItemView, QTextEdit,QVBoxLayout
)
from PySide6.QtCore import Qt, Slot
import pyqtgraph as pg
import numpy as np
from agents.longport_mcp_agent import LongportMcpAgentWrapper
import asyncio

class nlp_screener(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()
        self.agent = LongportMcpAgentWrapper()  # 只初始化一次

    def init_ui(self):
        # 聊天内容显示区
        self.chat_display = QTextEdit()
        self.chat_display.setReadOnly(True)

        # 输入区
        self.mcp_input_frame = QLineEdit()
        self.mcp_input_frame.setPlaceholderText("请输入你的问题...")
        self.mcp_send_messgage = QPushButton("发送")
        
        self.clear_button = QPushButton("清空对话")
        
        # 布局
        input_layout = QHBoxLayout()
        input_layout.addWidget(self.mcp_input_frame)
        input_layout.addWidget(self.mcp_send_messgage)

        main_layout = QVBoxLayout()
        main_layout.addWidget(self.chat_display)
        main_layout.addLayout(input_layout)
        main_layout.addWidget(self.clear_button)
        self.setLayout(main_layout)

        # 聊天历史
        self.chat_history = []
        
        # 绑定事件
        self.mcp_send_messgage.clicked.connect(self.on_send_clicked)
        self.mcp_input_frame.returnPressed.connect(self.on_send_clicked)
        self.clear_button.clicked.connect(self.on_clear_clicked)

    @Slot()
    def on_send_clicked(self):
        user_text = self.mcp_input_frame.text().strip()
        if not user_text:
            return
        self.append_message("你", user_text)
        self.mcp_input_frame.clear()
        # 启动异步任务
        asyncio.create_task(self.get_ai_reply(user_text))

    async def get_ai_reply(self, user_text):
        ai_reply = await self.agent.longport_mcp_ask(user_text)
        if ai_reply:
            self.append_message("AI", ai_reply)

    def append_message(self, who, msg):
        self.chat_history.append((who, msg))
        self.chat_display.append(f"<b>{who}：</b> {msg}")
        
    def on_clear_clicked(self):
        self.chat_display.clear()
        self.chat_history.clear()
        self.agent = LongportMcpAgentWrapper()  # 重新初始化，清空上下文