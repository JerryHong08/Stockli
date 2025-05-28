import sys
import os
import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from PySide6.QtWidgets import QApplication
from PySide6.QtGui import QIcon
from PySide6.QtCore import Qt
from typing import Optional
from ui.main_window import MainWindowUI

# 图标路径
icon_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'resources', 'icons', 'refresh_icon.png'))

class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, restart_func):
        self.restart_func = restart_func
        self.last_modified = 0
        self.debounce_interval = 1.0  # 防抖间隔1秒

    def on_modified(self, event):
        # 1. watchdog 检测到文件被修改，自动调用这个方法
        # 2. 判断是不是文件且是 .py 文件
        if not event.is_directory and str(event.src_path).endswith('.py'):
            # 3. 防抖处理，避免重复触发
            current_time = time.time()
            if current_time - self.last_modified > self.debounce_interval:
                print(f"Detected change in {event.src_path}, restarting...")
                self.last_modified = current_time
                self.restart_func()

class AppState:
    app: Optional[QApplication] = None
    window: Optional[MainWindowUI] = None

def run_app():
    """运行 PySide6 应用程序"""
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon(icon_path))
    window = MainWindowUI()
    window.showMaximized()
    AppState.app = app
    AppState.window = window
    sys.exit(app.exec())

def main():
    def restart():
        """重启程序，确保关闭旧窗口"""
        try:
            if AppState.window:
                print("Attempting to close window...")
                # 断开信号
                try:
                    AppState.window.visualization_tab.search_box.textChanged.disconnect()
                    print("Disconnected visualization_tab.search_box.textChanged")
                except Exception as e:
                    print(f"Error disconnecting signals: {e}")
                # 清理 logic 资源（包括线程）
                try:
                    AppState.window.logic.cleanup()
                    print("Cleaned up MainWindowLogic resources")
                except Exception as e:
                    print(f"Error cleaning up MainWindowLogic: {e}")
                try:
                    AppState.window.hide()
                    print("Window hidden")
                    AppState.window.centralWidget().deleteLater()
                    print("Central widget deleted")
                except Exception as e:
                    print(f"Error deleting GUI resources: {e}")
            if AppState.app:
                print("Attempting to quit application...")
                # AppState.app.processEvents()
                # AppState.app.quit()
                print("Application quit")
                for _ in range(5):
                    AppState.app.processEvents()
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
    # 1. 创建事件处理器
    event_handler = FileChangeHandler(restart)

    # 2. 创建 Observer 并绑定事件处理器
    observer = Observer()
    observer.schedule(event_handler, path='.', recursive=True)

    # 3. 启动 Observer（开始自动检测变化）
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