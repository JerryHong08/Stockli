from PyQt5.QtCore import QThread, pyqtSignal
import pandas as pd
from database.db_operations import fetch_data_from_db

class DataLoader(QThread):
    """
    数据加载线程，用于从数据库中加载数据。
    """
    data_loaded = pyqtSignal(pd.DataFrame)  # 信号：数据加载完成
    error_occurred = pyqtSignal(str)  # 信号：发生错误

    def __init__(self, table_name, engine, limit=None):
        """
        初始化数据加载线程。

        :param table_name: 表名
        :param engine: SQLAlchemy 引擎
        :param limit: 加载数据的条数（可选）
        """
        super().__init__()
        self.table_name = table_name
        self.engine = engine
        self.limit = limit

    def run(self):
        """
        线程运行方法，从数据库中加载数据。
        """
        try:
            # 从数据库加载数据
            df = fetch_data_from_db(self.table_name, self.engine, self.limit)
            if df.empty:
                self.error_occurred.emit("数据框为空，无法绘制图表。")
            else:
                self.data_loaded.emit(df)  # 发送信号，传递加载的数据
        except Exception as e:
            self.error_occurred.emit(f"数据加载失败: {e}")