from PySide6.QtCore import QThread, Signal
import pandas as pd
from database.db_operations import fetch_data_from_db

class DataLoader(QThread):
    data_loaded = Signal(pd.DataFrame)
    error_occurred = Signal(str)

    def __init__(self, ticker, engine, limit=None):
        super().__init__()
        self.ticker = ticker
        self.engine = engine
        self.limit = limit

    def run(self):
        try:
            df = fetch_data_from_db(self.ticker, self.engine, self.limit)
            if df.empty:
                self.error_occurred.emit(f"数据框为空，无法绘制图表 (ticker: {self.ticker})")
            else:
                self.data_loaded.emit(df)
        except Exception as e:
            self.error_occurred.emit(f"数据加载失败: {e}")