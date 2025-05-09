import sys
import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from PyQt5.QtWidgets import QApplication
from PyQt5.QtGui import QIcon
from PyQt5.QtCore import Qt
from ui.main_window import MainWindowUI
import subprocess

# 图标路径
icon_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'resources', 'icons', 'refresh_icon.png'))

class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, restart_func):
        self.restart_func = restart_func

    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('.py'):
            print(f"Detected change in {event.src_path}, restarting...")
            self.restart_func()

def run_app():
    """运行 PyQt5 应用程序"""
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon(icon_path))
    window = MainWindowUI()
    window.show()
    # Store app and window globally for cleanup
    run_app.app = app
    run_app.window = window
    sys.exit(app.exec_())

def main():
    QApplication.setAttribute(Qt.AA_EnableHighDpiScaling)
    QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps)

    def restart():
        """重启程序，确保关闭旧窗口"""
        # Close the existing window and quit the application
        if hasattr(run_app, 'window') and run_app.window:
            run_app.window.close()  # Close the MainWindow
        if hasattr(run_app, 'app') and run_app.app:
            run_app.app.quit()  # Quit the QApplication
        # Start a new process
        subprocess.Popen([sys.executable, __file__])
        sys.exit(0)

    # 监控文件变化
    event_handler = FileChangeHandler(restart)
    observer = Observer()
    observer.schedule(event_handler, path='.', recursive=True)
    observer.start()

    try:
        run_app()
    finally:
        observer.stop()
        observer.join()

if __name__ == "__main__":
    main()