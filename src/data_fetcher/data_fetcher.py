from PyQt5.QtCore import QThread, pyqtSignal
from longport.openapi import QuoteContext, Config, Period, AdjustType
from database.db_operations import save_to_table
from database.db_connection import get_engine  # 导入数据库引擎

class DataFetcher(QThread):
    fetch_complete = pyqtSignal(str)  # 信号：数据获取完成
    error_occurred = pyqtSignal(str)  # 信号：发生错误

    def __init__(self, stock_symbol):
        super().__init__()
        self.stock_symbol = stock_symbol

    def run(self):
        try:
            # 从 longport 获取数据
            config = Config.from_env()
            ctx = QuoteContext(config)
            stock_symbol_with_suffix = f"{self.stock_symbol}.US"
            resp = ctx.candlesticks(stock_symbol_with_suffix, Period.Day, 1000, AdjustType.ForwardAdjust)
            if not resp:
                self.error_occurred.emit(f"No data retrieved for {stock_symbol_with_suffix}.")
                return

            # 保存数据到 PostgreSQL
            table_name = self.stock_symbol  # 表名不包含 ".US"
            engine = get_engine()
            save_to_table(resp, table_name, engine)  # 传递 engine 参数
            self.fetch_complete.emit(table_name)  # 发送信号，表示数据获取完成
        except Exception as e:
            self.error_occurred.emit(f"Error fetching data: {e}")
