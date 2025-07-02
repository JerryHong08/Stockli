import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from src.config import db_config
from tqdm import tqdm

def get_db_engine():
    """获取SQLAlchemy数据库引擎"""
    connection_string = (
        f"postgresql://{db_config.DB_CONFIG['user']}:"
        f"{db_config.DB_CONFIG['password']}@"
        f"{db_config.DB_CONFIG['host']}:"
        f"{db_config.DB_CONFIG['port']}/"
        f"{db_config.DB_CONFIG['dbname']}"
    )
    return create_engine(connection_string)

def load_stock_data(days: int = 400) -> pd.DataFrame:
    """
    从数据库一次性加载所有股票最近 days 天数据
    """
    engine = get_db_engine()
    query = f"""
        SELECT ticker, timestamp, close, volume
        FROM stock_daily
        WHERE timestamp >= (CURRENT_DATE - INTERVAL '{days} days')
        ORDER BY ticker, timestamp;
    """
    print("📥 正在从数据库加载数据...")
    df = pd.read_sql(query, engine)
    print(f"✅ 数据加载完成，行数: {len(df)}")
    return df

def calculate_obv_and_score_fast(df: pd.DataFrame, window: int = 252, mean_window: int = 20) -> pd.DataFrame:
    print("⚙️ 正在计算 OBV 和 rolling min/max...")
    df = df.sort_values(['ticker', 'timestamp']).copy()

    # OBV
    df['price_diff'] = df.groupby('ticker')['close'].diff()
    df['direction'] = np.sign(df['price_diff']).fillna(0)
    df['obv_step'] = df['direction'] * df['volume']
    df['obv'] = df.groupby('ticker')['obv_step'].cumsum()

    # obv rolling min/max
    df['obv_min'] = df.groupby('ticker')['obv'].transform(lambda x: x.rolling(window).min())
    df['obv_max'] = df.groupby('ticker')['obv'].transform(lambda x: x.rolling(window).max())
    df['obv_pct'] = (df['obv'] - df['obv_min']) / (df['obv_max'] - df['obv_min'] + 1e-5)

    # close rolling min/max
    df['close_min'] = df.groupby('ticker')['close'].transform(lambda x: x.rolling(window).min())
    df['close_max'] = df.groupby('ticker')['close'].transform(lambda x: x.rolling(window).max())
    df['close_pct'] = (df['close'] - df['close_min']) / (df['close_max'] - df['close_min'] + 1e-5)

    # 中期均值
    df['obv_pct_mean'] = df.groupby('ticker')['obv_pct'].transform(lambda x: x.rolling(mean_window).mean())
    df['close_pct_mean'] = df.groupby('ticker')['close_pct'].transform(lambda x: x.rolling(mean_window).mean())

    df['obv_pct_mean_z'] = df.groupby('ticker')['obv_pct_mean'].transform(lambda x: (x - x.mean()) / (x.std() + 1e-5))
    df['close_pct_mean_z'] = df.groupby('ticker')['close_pct_mean'].transform(lambda x: (x - x.mean()) / (x.std() + 1e-5))

    df['score'] = df['obv_pct_mean_z'] - df['close_pct_mean_z']

    print("✅ 计算完成")
    return df

def find_top_obv_stocks(top_n: int = 10, days: int = 400, window: int = 252, mean_window: int = 20):
    """
    主函数：加载数据，计算指标，返回 top_n 股票
    """
    df_all = load_stock_data(days=days)
    df_all = calculate_obv_and_score_fast(df_all, window=window, mean_window=mean_window)

    # 每只股票取最后一天的 score
    df_latest = (
        df_all.sort_values(['ticker', 'timestamp'])
             .groupby('ticker')
             .tail(1)
             .dropna(subset=['score'])
    )

    df_top = df_latest[['ticker', 'score', 'obv_pct', 'close_pct']].sort_values('score', ascending=False).head(top_n)
    return df_top

if __name__ == "__main__":
    print("=== 📊 OBV 资金背离选股 ===")
    top_n = 10
    df_result = find_top_obv_stocks(top_n=top_n, days=400, window=252, mean_window=20)
    print(f"\n🏆 Top {top_n} OBV 背离股票：")
    print(df_result.to_string(index=False, float_format="%.4f"))
