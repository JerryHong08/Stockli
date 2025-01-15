import sys
from PyQt5.QtWidgets import QApplication
#from ui.main_window3 import MainWindow
from ui.main_window import MainWindowUI

def main():
    app = QApplication(sys.argv)
    window = MainWindowUI()
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()