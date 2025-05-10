import sys
import os
import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from PySide6.QtWidgets import QApplication
from PySide6.QtGui import QIcon
from PySide6.QtCore import Qt
from ui.main_window import MainWindowUI

# 图标路径
icon_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'resources', 'icons', 'refresh_icon.png'))

class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, restart_func):
        self.restart_func = restart_func
        self.last_modified = 0
        self.debounce_interval = 1.0  # 防抖间隔1秒

    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('.py'):
            current_time = time.time()
            if current_time - self.last_modified > self.debounce_interval:
                print(f"Detected change in {event.src_path}, restarting...")
                self.last_modified = current_time
                self.restart_func()

def run_app():
    """运行 PySide6 应用程序"""
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon(icon_path))
    window = MainWindowUI()
    window.showMaximized()
    run_app.app = app
    run_app.window = window
    sys.exit(app.exec())

def main():
    def restart():
        """重启程序，确保关闭旧窗口"""
        try:
            if hasattr(run_app, 'window') and run_app.window:
                print("Attempting to close window...")
                # 断开信号
                try:
                    run_app.window.visualization_tab.search_box.textChanged.disconnect()
                    print("Disconnected visualization_tab.search_box.textChanged")
                except Exception as e:
                    print(f"Error disconnecting signals: {e}")
                # 清理 logic 资源（包括线程）
                try:
                    run_app.window.logic.cleanup()
                    print("Cleaned up MainWindowLogic resources")
                except Exception as e:
                    print(f"Error cleaning up MainWindowLogic: {e}")
                try:
                    run_app.window.hide()
                    print("Window hidden")
                    run_app.window.centralWidget().deleteLater()
                    print("Central widget deleted")
                except Exception as e:
                    print(f"Error deleting GUI resources: {e}")
            if hasattr(run_app, 'app') and run_app.app:
                print("Attempting to quit application...")
                # run_app.app.processEvents()
                # run_app.app.quit()
                print("Application quit")
                for _ in range(5):
                    run_app.app.processEvents()
                    time.sleep(0.05)
                print("Window closed")
        except Exception as e:
            print(f"Error during cleanup: {e}")
        # 启动新进程
        try:
            subprocess.Popen([sys.executable, __file__])
            print("New process started")
        except Exception as e:
            print(f"Error starting new process: {e}")
        # 强制退出
        os._exit(0)

    # 监控文件变化
    event_handler = FileChangeHandler(restart)
    observer = Observer()
    observer.schedule(event_handler, path='.', recursive=True)
    observer.start()

    try:
        run_app()
    except Exception as e:
        print(f"Error in run_app: {e}")
    finally:
        observer.stop()
        observer.join()

if __name__ == "__main__":
    main()