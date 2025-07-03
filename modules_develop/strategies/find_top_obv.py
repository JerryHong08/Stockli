from datetime import date
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from src.config import db_config
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import seaborn as sns
import matplotlib.font_manager as fm
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import os
from pathlib import Path

plt.rcParams['font.sans-serif'] = 'Microsoft YaHei'
plt.rcParams['axes.unicode_minus'] = False

# ----------------------------DATA CONFIG&FETCH---------------------------------

# 输出目录配置
OUTPUTS_DIR = 'outputs'
INDICATOR_OUTPUT_DIR = 'indicator_output_src'
# 使用 pathlib 更优雅地处理路径
OUTPUT_DIR = Path('outputs/indicator_output_src')
# 设置输出目录
def setup_output_directory():
    """设置输出目录"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"✅ 输出目录已准备: {OUTPUT_DIR}")
# 生成输出文件名和路径的函数
def get_output_filename(ticker: str, test_date: str, file_type: str = 'html') -> str:
    """
    生成输出文件名和路径
    
    Args:
        ticker: 股票代码
        test_date: 测试日期
        file_type: 文件类型 ('html' 或 'png')
    
    Returns:
        完整的文件路径
    """
    # 确保输出目录存在
    output_dir = os.path.join(OUTPUTS_DIR, INDICATOR_OUTPUT_DIR)
    os.makedirs(output_dir, exist_ok=True)
    
    # 生成文件名
    date_str = test_date.replace("-", "")
    filename = f'{ticker}_obv_analysis_{date_str}.{file_type}'
    
    return os.path.join(output_dir, filename)
# 定义标签字典
def get_labels():    
    return {
        'price': '价格 ($)',
        'date': '日期',
        'obv_original': 'OBV 原始值',
        'obv_percentile': 'OBV 百分位 & 均值',
        'zscore': 'Z-Score',
        'candlestick_title': 'K线图与OBV背离分析',
        'obv_details': 'OBV 指标详情',
        'divergence_analysis': 'OBV与价格背离分析 (Z-Score标准化)',
        'ma20': 'MA20',
        'ma50': 'MA50',
        'obv': 'OBV',
        'obv_pct': 'OBV百分位',
        'obv_mean': 'OBV均值',
        'midline': '中位线',
        'high_line': '高位线',
        'low_line': '低位线',
        'obv_zscore': 'OBV Z-Score',
        'price_zscore': 'Price Z-Score',
        'divergence_score': '背离Score',
        'std_line': '±1标准差',
        'positive_divergence': '正向背离区域',
        'negative_divergence': '负向背离区域'
}
# 获取SQLAlchemy数据库引擎
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
# 加载股票数据
def load_stock_data(test_date: str, days: int = 1000, tickers: list = None) -> pd.DataFrame:
    """
    一次性加载所有股票或指定股票最近 days 天数据
    """
    engine = get_db_engine()
    
    # 构建 ticker 筛选条件
    ticker_condition = ""
    if tickers:
        ticker_list = "', '".join(tickers)
        ticker_condition = f"AND ticker IN ('{ticker_list}')"
    
    query = f"""
        SELECT ticker, timestamp, close, volume
        FROM stock_daily
        WHERE timestamp >= ('{test_date}'::date - INTERVAL '{days} days')
        AND timestamp <= '{test_date}'::date
        {ticker_condition}
        ORDER BY ticker, timestamp;
    """
    print(f"📥 正在加载数据库数据{f'（指定股票: {tickers}）' if tickers else ''}...")
    df = pd.read_sql(query, engine)
    print(f"✅ 加载完成，共 {len(df)} 行")
    return df
# 创建交易日索引，避免停牌期影响
def create_trading_day_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    传统方法：避免groupby.apply警告
    """
    df = df.copy()
    
    # 按ticker分组处理
    result_dfs = []
    for ticker, group in df.groupby('ticker'):
        group = group.sort_values('timestamp').copy()
        group['trading_day_index'] = range(len(group))
        result_dfs.append(group)
    
    return pd.concat(result_dfs, ignore_index=True)
# 获取单个股票的详细数据（包括OHLC如果可用）
def get_ticker_detailed_data(ticker: str, test_date: str, days: int = 400) -> pd.DataFrame:
    """
    获取单个股票的详细数据（包括OHLC如果可用）
    """
    engine = get_db_engine()
    
    # 尝试获取完整的OHLC数据
    query = f"""
        SELECT ticker, timestamp, 
               COALESCE(open, close) as open,
               COALESCE(high, close) as high, 
               COALESCE(low, close) as low,
               close, volume
        FROM stock_daily
        WHERE ticker = '{ticker}'
        AND timestamp >= ('{test_date}'::date - INTERVAL '{days} days')
        AND timestamp <= '{test_date}'::date
        ORDER BY timestamp;
    """
    
    df = pd.read_sql(query, engine)
    return df

# ----------------------------Strategy---------------------------------

# 计算 OBV 和相关指标
def calculate_obv_and_score(df: pd.DataFrame, window: int = 252, mean_window: int = 20) -> pd.DataFrame:
    """
    修复版本：正确处理交易日数据的时间序列计算，避免停牌期影响
    """
    print("⚙️ 正在计算指标...")
    df = df.sort_values(['ticker', 'timestamp']).copy()
    
    # 创建交易日索引，避免停牌期影响rolling计算
    df = create_trading_day_index(df)

    # 计算 OBV
    df['price_diff'] = df.groupby('ticker')['close'].diff()
    df['direction'] = np.sign(df['price_diff']).fillna(0)
    df['obv_step'] = df['direction'] * df['volume']
    df['obv'] = df.groupby('ticker')['obv_step'].cumsum()

    # 设置合理的 min_periods 来处理数据不足的情况
    window_min_periods = min(50, window // 3)  # 至少50天数据
    mean_min_periods = min(5, mean_window // 2)  # 至少5天数据

    # rolling min/max - 基于实际交易日数量而不是日历日期
    df['obv_min'] = df.groupby('ticker')['obv'].transform(
        lambda x: x.rolling(window=window, min_periods=window_min_periods).min()
    )
    df['obv_max'] = df.groupby('ticker')['obv'].transform(
        lambda x: x.rolling(window=window, min_periods=window_min_periods).max()
    )
    
    # 避免除零错误
    obv_range = df['obv_max'] - df['obv_min']
    df['obv_pct'] = np.where(
        obv_range > 1e-10,
        (df['obv'] - df['obv_min']) / obv_range,
        0.5  # 如果范围太小，设为中间值
    )

    df['close_min'] = df.groupby('ticker')['close'].transform(
        lambda x: x.rolling(window=window, min_periods=window_min_periods).min()
    )
    df['close_max'] = df.groupby('ticker')['close'].transform(
        lambda x: x.rolling(window=window, min_periods=window_min_periods).max()
    )
    
    close_range = df['close_max'] - df['close_min']
    df['close_pct'] = np.where(
        close_range > 1e-10,
        (df['close'] - df['close_min']) / close_range,
        0.5
    )

    # 中期均值 - 基于实际交易日的rolling
    df['obv_pct_mean'] = df.groupby('ticker')['obv_pct'].transform(
        lambda x: x.rolling(window=mean_window, min_periods=mean_min_periods).mean()
    )
    df['close_pct_mean'] = df.groupby('ticker')['close_pct'].transform(
        lambda x: x.rolling(window=mean_window, min_periods=mean_min_periods).mean()
    )

    # 对中期均值做 z-score 标准化（每只股票内部）- 增强数值稳定性
    def safe_zscore(x):
        """安全的Z-score计算"""
        valid_data = x.dropna()
        if len(valid_data) < 2:  # 需要至少2个有效值
            return pd.Series(np.zeros(len(x)), index=x.index)
        
        mean_val = valid_data.mean()
        std_val = valid_data.std()
        
        if std_val < 1e-10:  # 标准差太小，认为是常数序列
            return pd.Series(np.zeros(len(x)), index=x.index)
        
        return (x - mean_val) / std_val

    df['obv_z'] = df.groupby('ticker')['obv_pct_mean'].transform(safe_zscore)
    df['close_z'] = df.groupby('ticker')['close_pct_mean'].transform(safe_zscore)

    # 最终 score：obv 越强（z 越高） & close 越弱（z 越低） → score 越大
    df['score'] = df['obv_z'] - df['close_z']

    print("✅ 指标计算完成")
    return df

# def calculate_obv_and_score(df: pd.DataFrame, window: int = 252, mean_window: int = 20, slope_window: int = 20) -> pd.DataFrame:
    """
    新版本：quantile + slope + cross-sectional z-score + 排除流动性差
    """
    print("⚙️ 正在计算指标...")
    df = df.sort_values(['ticker', 'timestamp']).copy()

    # 添加交易日索引（可选）
    df = create_trading_day_index(df)

    # OBV
    df['price_diff'] = df.groupby('ticker')['close'].diff()
    df['direction'] = np.sign(df['price_diff']).fillna(0)
    df['obv_step'] = df['direction'] * df['volume']
    df['obv'] = df.groupby('ticker')['obv_step'].cumsum()

    window_min_periods = min(50, window // 3)
    mean_min_periods = min(5, mean_window // 2)

    # rolling quantile 替代 min/max
    df['obv_q_low'] = df.groupby('ticker')['obv'].transform(
        lambda x: x.rolling(window, min_periods=window_min_periods).quantile(0.05))
    df['obv_q_high'] = df.groupby('ticker')['obv'].transform(
        lambda x: x.rolling(window, min_periods=window_min_periods).quantile(0.95))
    obv_range = df['obv_q_high'] - df['obv_q_low']
    df['obv_pct'] = np.where(obv_range > 1e-10, (df['obv'] - df['obv_q_low']) / obv_range, 0.5)

    df['close_q_low'] = df.groupby('ticker')['close'].transform(
        lambda x: x.rolling(window, min_periods=window_min_periods).quantile(0.05))
    df['close_q_high'] = df.groupby('ticker')['close'].transform(
        lambda x: x.rolling(window, min_periods=window_min_periods).quantile(0.95))
    close_range = df['close_q_high'] - df['close_q_low']
    df['close_pct'] = np.where(close_range > 1e-10, (df['close'] - df['close_q_low']) / close_range, 0.5)

    # 中期均值
    df['obv_pct_mean'] = df.groupby('ticker')['obv_pct'].transform(
        lambda x: x.rolling(mean_window, min_periods=mean_min_periods).mean())
    df['close_pct_mean'] = df.groupby('ticker')['close_pct'].transform(
        lambda x: x.rolling(mean_window, min_periods=mean_min_periods).mean())
    
    # 对中期均值做 z-score 标准化（每只股票内部）- 增强数值稳定性
    def safe_zscore(x):
        """安全的Z-score计算"""
        valid_data = x.dropna()
        if len(valid_data) < 2:  # 需要至少2个有效值
            return pd.Series(np.zeros(len(x)), index=x.index)
        
        mean_val = valid_data.mean()
        std_val = valid_data.std()
        
        if std_val < 1e-10:  # 标准差太小，认为是常数序列
            return pd.Series(np.zeros(len(x)), index=x.index)
        
        return (x - mean_val) / std_val

    df['obv_z'] = df.groupby('ticker')['obv_pct_mean'].transform(safe_zscore)
    df['close_z'] = df.groupby('ticker')['close_pct_mean'].transform(safe_zscore)
    
    # obv slope
    def calc_slope(x):
        if len(x.dropna()) < slope_window:
            return np.nan
        y = x.values
        x_idx = np.arange(len(y))
        slope = np.polyfit(x_idx, y, 1)[0]
        return slope

    df['obv_slope'] = df.groupby('ticker')['obv'].transform(
        lambda x: x.rolling(slope_window, min_periods=5).apply(calc_slope, raw=False))

    # cross-sectional z-score（只做最新日期）
    latest = df.sort_values(['ticker', 'timestamp']).groupby('ticker').tail(1).copy()
    for col in ['obv_pct_mean', 'close_pct_mean', 'obv_slope']:
        mean = latest[col].mean()
        std = latest[col].std()
        latest[f'{col}_z'] = (latest[col] - mean) / (std + 1e-5)

    # score: obv_pct_mean 越高 + obv_slope 越大 + close_pct_mean 越低
    latest['score'] = latest['obv_pct_mean_z'] + latest['obv_slope_z'] - latest['close_pct_mean_z']

    # 排除流动性差：volume 均值最低1%剔除
    # vol_mean = df.groupby('ticker')['volume'].mean()
    # vol_threshold = vol_mean.quantile(0.01)
    # liquid_tickers = vol_mean[vol_mean > vol_threshold].index
    # latest = latest[latest['ticker'].isin(liquid_tickers)]

    print("✅ 指标计算完成")
    return latest

# ----------------------------Process-----------------------------------
# 为这些tickers生成图表
def chart_tickers(tickers: list, test_date: str, days: int = 1000, df_all: pd.DataFrame = None):
    # 为每个股票生成图表
    print(f"\n📈 正在为 {len(tickers)} 只股票生成图表...")
    
    for ticker in tickers:
        ticker_data = df_all[df_all['ticker'] == ticker].copy()
        
        if not ticker_data.empty:
            print(f"📊 正在生成 {ticker} 的图表...")
            
            # 获取更详细的OHLC数据
            detailed_data = get_ticker_detailed_data(ticker, test_date, days)
            
            # 合并计算的指标到详细数据
            if not detailed_data.empty:
                detailed_data = detailed_data.merge(
                    ticker_data[['timestamp', 'obv', 'obv_pct', 'obv_pct_mean', 'obv_z', 'close_z', 'score']], 
                    on='timestamp', 
                    how='left'
                )
                plot_interactive_candlestick_with_obv(ticker, detailed_data, test_date)
            else:
                plot_interactive_candlestick_with_obv(ticker, ticker_data, test_date)
        else:
            print(f"❌ {ticker} 没有足够的数据生成图表")
    
    print("✅ 所有图表生成完成!") 
# 1.1
def calculate_tickers_with_charts(tickers: list, test_date: str, days: int = 1000):
    """
    包含数据质量诊断
    """
    print(f"=== 📊 计算指定股票 OBV 背离指标并生成图表 ===")
    
    # 获取完整的原始数据用于绘图
    df_all = load_stock_data(test_date=test_date, days=days, tickers=tickers)
    
    if df_all.empty:
        print("❌ 无法获取绘图数据")
        return
    
    # 数据质量诊断
    for ticker in tickers:
        ticker_data = df_all[df_all['ticker'] == ticker]
        if not ticker_data.empty:
            diagnose_data_quality(ticker_data, ticker)
    
    # 计算所有指标
    df_all = calculate_obv_and_score(df_all, window=252, mean_window=20)
    
    # 验证rolling计算
    for ticker in tickers:
        verify_rolling_calculation(df_all, ticker, window=20)
    
    # 计算排名结果
    df_result = find_top_obv_stocks(test_date=test_date, top_n=50, days=days, 
                                   window=252, mean_window=20, tickers=tickers)
    
    if not df_result.empty:
        print(f"\n🏆 指定股票 OBV 背离结果：")
        print(df_result.to_string(index=False, float_format="%.4f"))
    
    chart_tickers(tickers,test_date,days,df_all)
# 1.2 图表
def plot_interactive_candlestick_with_obv(ticker: str, df_ticker: pd.DataFrame, test_date: str, save_chart: bool = True):
    """
    绘制交互式K线图并叠加OBV指标
    """
    if df_ticker.empty:
        print(f"❌ {ticker} 没有数据可绘制")
        return
    
    # 获取最新的指标值
    latest_data = df_ticker.iloc[-1]
    labels = get_labels()
    
    # 转换日期格式
    df_ticker['date'] = pd.to_datetime(df_ticker['timestamp'])
    
    # 创建子图 - 关键修复：添加 shared_xaxes=True
    fig = make_subplots(
        rows=3, cols=1,
        row_heights=[0.5, 0.3, 0.2],
        subplot_titles=[
            f'{ticker} - {labels["candlestick_title"]} ({test_date})',
            labels["obv_details"],
            labels["divergence_analysis"]
        ],
        specs=[[{"secondary_y": False}],
               [{"secondary_y": True}], 
               [{"secondary_y": False}]],
        vertical_spacing=0.08,
        shared_xaxes=True,  # 关键修复：绑定X轴
        x_title=labels['date']  # 统一X轴标题
    )
    
    # === 上图：K线图 ===
    # 修复K线图的日期连续性问题
    fig.add_trace(
        go.Candlestick(
            x=df_ticker['date'],
            open=df_ticker['open'] if 'open' in df_ticker.columns else df_ticker['close'],
            high=df_ticker['high'] if 'high' in df_ticker.columns else df_ticker['close'],
            low=df_ticker['low'] if 'low' in df_ticker.columns else df_ticker['close'],
            close=df_ticker['close'],
            name=ticker,
            increasing_line_color='red',
            decreasing_line_color='green',
            xaxis='x'  # 确保使用正确的X轴
        ),
        row=1, col=1
    )
    
    # 添加移动平均线
    if len(df_ticker) >= 20:
        df_ticker['ma20'] = df_ticker['close'].rolling(20).mean()
        fig.add_trace(
            go.Scatter(
                x=df_ticker['date'],
                y=df_ticker['ma20'],
                mode='lines',
                name=labels['ma20'],
                line=dict(color='blue', width=1),
                connectgaps=False  # 不连接空白数据点
            ),
            row=1, col=1
        )
    
    if len(df_ticker) >= 50:
        df_ticker['ma50'] = df_ticker['close'].rolling(50).mean()
        fig.add_trace(
            go.Scatter(
                x=df_ticker['date'],
                y=df_ticker['ma50'],
                mode='lines',
                name=labels['ma50'],
                line=dict(color='orange', width=1),
                connectgaps=False
            ),
            row=1, col=1
        )
    
    # === 中图：OBV指标 ===
    # OBV原始值（左轴）
    fig.add_trace(
        go.Scatter(
            x=df_ticker['date'],
            y=df_ticker['obv'],
            mode='lines',
            name=labels['obv'],
            line=dict(color='purple', width=2),
            connectgaps=False
        ),
        row=2, col=1
    )
    
    # OBV百分位（右轴）
    if 'obv_pct' in df_ticker.columns:
        fig.add_trace(
            go.Scatter(
                x=df_ticker['date'],
                y=df_ticker['obv_pct'],
                mode='lines',
                name=labels['obv_pct'],
                line=dict(color='green', width=1, dash='dash'),
                connectgaps=False
            ),
            row=2, col=1, secondary_y=True
        )
    
    # OBV均值（右轴）
    if 'obv_pct_mean' in df_ticker.columns:
        fig.add_trace(
            go.Scatter(
                x=df_ticker['date'],
                y=df_ticker['obv_pct_mean'],
                mode='lines',
                name=labels['obv_mean'],
                line=dict(color='red', width=2),
                connectgaps=False
            ),
            row=2, col=1, secondary_y=True
        )

    # obv_slope 曲线（右轴）
    # if 'obv_slope' in df_ticker.columns:
    #     fig.add_trace(
    #         go.Scatter(
    #             x=df_ticker['date'],
    #             y=df_ticker['obv_slope'],
    #             mode='lines',
    #             name='OBV Slope',
    #             line=dict(color='teal', width=1, dash='dot'),
    #             connectgaps=False
    #         ),
    #         row=2, col=1, secondary_y=True
    #     )
        
    # 添加关键水平线
    fig.add_hline(y=0.5, line_dash="dot", line_color="gray", row=2, col=1, secondary_y=True)
    fig.add_hline(y=0.8, line_dash="dot", line_color="red", row=2, col=1, secondary_y=True)
    fig.add_hline(y=0.2, line_dash="dot", line_color="blue", row=2, col=1, secondary_y=True)
    
    # === 下图：Z-Score对比 ===
    if 'obv_z' in df_ticker.columns and 'close_z' in df_ticker.columns:
        fig.add_trace(
            go.Scatter(
                x=df_ticker['date'],
                y=df_ticker['obv_z'],
                mode='lines',
                name=labels['obv_zscore'],
                line=dict(color='purple', width=2),
                connectgaps=False
            ),
            row=3, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=df_ticker['date'],
                y=df_ticker['close_z'],
                mode='lines',
                name=labels['price_zscore'],
                line=dict(color='brown', width=2),
                connectgaps=False
            ),
            row=3, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=df_ticker['date'],
                y=df_ticker['score'],
                mode='lines',
                name=labels['divergence_score'],
                line=dict(color='red', width=2),
                connectgaps=False
            ),
            row=3, col=1
        )
        
        # 添加零线和±1标准差线
        fig.add_hline(y=0, line_dash="solid", line_color="black", row=3, col=1)
        fig.add_hline(y=1, line_dash="dot", line_color="red", row=3, col=1)
        fig.add_hline(y=-1, line_dash="dot", line_color="red", row=3, col=1)
    
    # 更新布局
    fig.update_layout(
        title={
            'text': f'{ticker} - Score: {latest_data["score"]:.4f} | OBV_Z: {latest_data["obv_z"]:.4f} | Close_Z: {latest_data["close_z"]:.4f}',
            'x': 0.5,
            'font': {'size': 16}
        },
        height=900,
        showlegend=True,
        xaxis_rangeslider_visible=False,
        hovermode='x unified'
    )
    
    # 更新所有X轴格式 - 关键修复：处理交易日间隔，使用category轴避免空白
    fig.update_xaxes(
        type='category',  # 使用category类型避免日期间隔
        tickformat='%m-%d',
        tickmode='auto',
        rangeslider_visible=False,
        # 只显示部分日期标签避免拥挤
        nticks=20
    )
    
    # 更新y轴标签
    fig.update_yaxes(title_text=labels['price'], row=1, col=1)
    fig.update_yaxes(title_text=labels['obv_original'], row=2, col=1)
    fig.update_yaxes(title_text=labels['obv_percentile'], row=2, col=1, secondary_y=True)
    fig.update_yaxes(title_text=labels['zscore'], row=3, col=1)
    
    # 保存HTML文件
    if save_chart:
        html_filename = get_output_filename(ticker, test_date, 'html')
        fig.write_html(html_filename)
        print(f"📊 交互式图表已保存为: {html_filename}")
    
    fig.show()
# 1.3 数据质量诊断和验证(后续可删去)
def diagnose_data_quality(df: pd.DataFrame, ticker: str = None):
    """
    诊断数据质量和计算结果
    """
    if ticker:
        df_check = df[df['ticker'] == ticker].copy()
        print(f"=== 📊 {ticker} 数据质量诊断 ===")
    else:
        df_check = df.copy()
        print("=== 📊 整体数据质量诊断 ===")
    
    if df_check.empty:
        print("❌ 没有数据")
        return
    
    # 检查时间间隔
    df_check['date_diff'] = df_check['timestamp'].diff().dt.days
    
    print(f"📅 时间序列分析：")
    print(f"  总记录数: {len(df_check)}")
    print(f"  日期范围: {df_check['timestamp'].min()} 到 {df_check['timestamp'].max()}")
    print(f"  理论天数: {(df_check['timestamp'].max() - df_check['timestamp'].min()).days}")
    print(f"  实际记录数: {len(df_check)}")
    
    # 检查间隔分布
    gap_stats = df_check['date_diff'].value_counts().sort_index()
    print(f"  时间间隔分布: {gap_stats.head(10).to_dict()}")
    
    # 检查指标计算结果
    if 'score' in df_check.columns:
        print(f"\n📈 指标计算结果：")
        print(f"  有效Score数量: {df_check['score'].notna().sum()}/{len(df_check)}")
        print(f"  Score范围: {df_check['score'].min():.4f} 到 {df_check['score'].max():.4f}")
        print(f"  最新Score: {df_check['score'].iloc[-1]:.4f}")
        
        # 检查各指标的有效性
        for col in ['obv', 'obv_pct', 'obv_pct_mean', 'obv_z', 'close_z']:
            if col in df_check.columns:
                valid_count = df_check[col].notna().sum()
                print(f"  {col} 有效数量: {valid_count}/{len(df_check)}")
# 1.4 验证rolling计算(后续可删去)
def verify_rolling_calculation(df: pd.DataFrame, ticker: str, window: int = 20):
    """
    验证rolling计算是否正确处理了停牌期
    """
    ticker_data = df[df['ticker'] == ticker].copy().sort_values('timestamp')
    if len(ticker_data) < window:
        print(f"❌ {ticker} 数据不足以验证rolling计算")
        return
    
    print(f"=== 📊 {ticker} Rolling计算验证 ===")
    
    # 检查最近一段时间的数据
    recent_data = ticker_data.tail(window + 5)
    
    print(f"最近 {len(recent_data)} 条记录的时间间隔：")
    recent_data.loc[:, 'time_gap'] = recent_data['timestamp'].diff().dt.days
    for i, row in recent_data.iterrows():
        gap = row['time_gap']
        date_str = row['timestamp'].strftime('%Y-%m-%d')
        if pd.notna(gap) and gap > 3:
            print(f"  ⚠️  {date_str}: {gap}天间隔 (可能停牌)")
        elif pd.notna(gap):
            print(f"  ✅ {date_str}: {gap}天间隔")
        else:
            print(f"  📅 {date_str}: 首条记录")
    
    # 验证rolling均值计算
    if 'obv_pct_mean' in recent_data.columns:
        print(f"\nOBV均值计算验证（最近5条）：")
        for i, row in recent_data.tail(5).iterrows():
            date_str = row['timestamp'].strftime('%Y-%m-%d')
            obv_pct = row['obv_pct']
            obv_mean = row['obv_pct_mean']
            print(f"  {date_str}: OBV_pct={obv_pct:.4f}, Mean={obv_mean:.4f}")
    
    return recent_data
# 2. 查询股票市场排名
def check_market_ranking(tickers: list, test_date: str, days: int = 1000):
    """
    专门用于查询股票在全市场中的排名
    """
    print(f"=== 📊 查询股票市场排名 ===")
    print(f"查询股票: {tickers}")
    print(f"测试日期: {test_date}")
    
    # 获取全市场数据
    print("📥 正在获取全市场数据...")
    all_market = find_top_obv_stocks(
        test_date=test_date, 
        top_n=10000,  # 设置一个大数字获取所有数据
        days=days, 
        return_all=True
    )
    
    if all_market.empty:
        print("❌ 没有获取到市场数据")
        return
    
    print(f"✅ 全市场共有 {len(all_market)} 只股票")
    
    # 为排名添加序号列
    all_market.reset_index(drop=True, inplace=True)
    all_market['rank'] = range(1, len(all_market) + 1)
    
    # 查询指定股票
    results = []
    for ticker in tickers:
        ticker_data = all_market[all_market['ticker'] == ticker]
        if not ticker_data.empty:
            row = ticker_data.iloc[0]
            results.append({
                'ticker': ticker,
                'rank': row['rank'],
                'total': len(all_market),
                'score': row['score'],
                'obv_z': row['obv_z'],
                'close_z': row['close_z'],
                'percentile': (len(all_market) - row['rank'] + 1) / len(all_market) * 100
            })
        else:
            results.append({
                'ticker': ticker,
                'rank': None,
                'total': len(all_market),
                'score': None,
                'obv_z': None,
                'close_z': None,
                'percentile': None
            })
    
    # 显示结果
    print(f"\n📊 排名结果：")
    for result in results:
        ticker = result['ticker']
        if result['rank'] is not None:
            print(f"  🎯 {ticker}:")
            print(f"    排名: #{result['rank']}/{result['total']}")
            print(f"    百分位: 前 {result['percentile']:.1f}%")
            print(f"    Score: {result['score']:.4f}")

            # 判断背离情况
            if result['score'] > 0:
                divergence = "正向背离" if result['obv_z'] > 0 and result['close_z'] < 0 else "同向强势"
            else:
                divergence = "负向背离" if result['obv_z'] < 0 and result['close_z'] > 0 else "同向弱势"
                
            print(f"    OBV_Z: {result['obv_z']:.4f}, Close_Z: {result['close_z']:.4f} | {divergence}")
        else:
            print(f"  ❌ {ticker}: 无有效数据")
    
    return results
# 3. 查找 top N OBV 背离股票
def find_top_obv_stocks(test_date: str, top_n: int = 10, days: int = 1000, window: int = 252, mean_window: int = 20, tickers: list = None, return_all: bool = False, generate_charts: bool = False) -> pd.DataFrame:
    """
    主函数：加载数据 → 计算指标 → 返回 top_n 股票
    """
    df_all = load_stock_data(test_date=test_date, days=days, tickers=tickers)
    
    if df_all.empty:
        print("❌ 没有找到数据")
        return pd.DataFrame()
        
    df_all = calculate_obv_and_score(df_all, window=window, mean_window=mean_window)

    # 每只股票取最新一天的 score
    df_latest = (
        df_all.sort_values(['ticker', 'timestamp'])
              .groupby('ticker')
              .tail(1)
              .dropna(subset=['score'])
    )
    
    if not return_all:
        df_top = df_latest[['ticker', 'score', 'obv_z', 'close_z', 'obv_pct_mean', 'close_pct_mean']] \
                    .sort_values('score', ascending=False).head(top_n)
        
    else:
        df_top = df_latest[['ticker', 'score', 'obv_z', 'close_z', 'obv_pct_mean', 'close_pct_mean']] \
                    .sort_values('score', ascending=False)
    if generate_charts:
        chart_tickers(df_top['ticker'].tolist(), test_date, days, df_all)  # 传递完整的df_all
    return df_top

# 修改主函数
if __name__ == "__main__":
    choice = input("选择运行模式 (1: tickers指标测试+图表, 2: 排名查询, 3: OBV背离选股): ")

    if choice == "1":
        test_tickers = input("输入要计算的股票代码（用逗号分隔）: ").split(',')
        test_tickers = [t.strip().upper() for t in test_tickers]
        test_date = input("请输入要计算的日期，格式如2025-05-28: ")
        test_date = test_date if test_date else '2025-05-28'
        calculate_tickers_with_charts(test_tickers, test_date)
    elif choice == "2":
        test_tickers = input("输入要查询的股票代码（用逗号分隔）: ").split(',')
        test_tickers = [t.strip().upper() for t in test_tickers]
        test_date = input("请输入要查询的日期，格式如2025-05-28: ")
        check_market_ranking(test_tickers, test_date)
    elif choice == "3":
        top_n = input("请输入要查询的前 N 名股票数量 (默认 10): ")
        top_n = int(top_n) if top_n.isdigit() else 10
        if_generate = input("是否生成图表？(y/n, 默认n): ").strip().lower() == 'y'
        test_date = input("请输入要查询的日期，格式如2025-05-28: ")
        test_date = test_date if test_date else '2025-05-28'
        df_result = find_top_obv_stocks(test_date=test_date, top_n=top_n, days=1000, window=252, mean_window=20, generate_charts=if_generate)
        print(f"\n🏆 Top {top_n} OBV 背离股票：")
        print(df_result.to_string(index=False, float_format="%.4f"))
    else:
        print("默认运行ticker测试...")
        test_tickers = input("输入要计算的股票代码（用逗号分隔）: ").split(',')
        test_tickers = [t.strip().upper() for t in test_tickers]
        test_date = input("请输入要计算的日期，格式如2025-05-28: ")
        test_date = test_date if test_date else '2025-05-28'
        calculate_tickers_with_charts(test_tickers, test_date)