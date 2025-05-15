# 获取单个股票的数据存入数据库
from PySide6.QtCore import QThread, Signal
from longport.openapi import QuoteContext, Config, Period, AdjustType
from database.db_operations import save_to_table
from database.db_connection import get_engine

class DataFetcher(QThread):
    fetch_complete = Signal(str)
    error_occurred = Signal(str)

    def __init__(self, stock_symbol):
        super().__init__()
        self.stock_symbol = stock_symbol

    def run(self):
        try:
            config = Config.from_env()
            ctx = QuoteContext(config)
            stock_symbol_with_suffix = f"{self.stock_symbol}.US"
            resp = ctx.candlesticks(stock_symbol_with_suffix, Period.Day, 1000, AdjustType.ForwardAdjust)
            if not resp:
                self.error_occurred.emit(f"No data retrieved for {stock_symbol_with_suffix}.")
                return

            engine = get_engine()
            save_to_table(resp, self.stock_symbol, engine)  # ticker 而非 table_name
            self.fetch_complete.emit(self.stock_symbol)
        except Exception as e:
            self.error_occurred.emit(f"Error fetching data: {e}")