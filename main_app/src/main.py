import sys
from PyQt5.QtWidgets import QApplication
from ui.main_window import MainWindowUI
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QIcon
import os

# 获取图标的相对路径并转换为绝对路径
icon_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'resources', 'icons', 'refresh_icon.png'))
print("Icon path:", icon_path)

def main():
    QApplication.setAttribute(Qt.AA_EnableHighDpiScaling)
    QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps)
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon(icon_path))  # 设置任务栏图标
    window = MainWindowUI()
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()