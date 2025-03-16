import sys
from PyQt5.QtWidgets import QApplication
from ui.main_window import MainWindowUI
from PyQt5.QtCore import Qt  # Add this import

def main():
    QApplication.setAttribute(Qt.AA_EnableHighDpiScaling)
    QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps)
    app = QApplication(sys.argv)
    window = MainWindowUI()
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()