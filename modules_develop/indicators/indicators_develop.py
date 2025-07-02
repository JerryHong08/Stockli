from src.config import db_config
import psycopg2
import pandas as pd
import numpy as np
import talib
from typing import List, Tuple, Optional
from datetime import datetime
import sqlalchemy
from sqlalchemy import create_engine
import ta.volatility
import ta.momentum
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.model_selection import train_test_split,TimeSeriesSplit, GridSearchCV
from sklearn.metrics import accuracy_score, classification_report
import matplotlib.pyplot as plt
import seaborn as sns

def get_db_connection():
    """获取数据库连接"""
    return psycopg2.connect(**db_config.DB_CONFIG)

def get_db_engine():
    """获取SQLAlchemy数据库引擎"""
    # 构建连接字符串
    connection_string = f"postgresql://{db_config.DB_CONFIG['user']}:{db_config.DB_CONFIG['password']}@{db_config.DB_CONFIG['host']}:{db_config.DB_CONFIG['port']}/{db_config.DB_CONFIG['dbname']}"
    return create_engine(connection_string)

def get_ticker_daily_data(ticker: str) -> List[Tuple]:
    """获取指定ticker的日线数据"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, timestamp, open, high, low, close, volume
        FROM stock_daily
        WHERE ticker = %s
        ORDER BY timestamp ASC;
    """, (ticker,))
    
    rows = cursor.fetchall()  # 每行是一个 tuple: (id, timestamp, open, high, low, close, volume)
    
    cursor.close()
    conn.close()
    
    return rows  # 返回所有行的数据

def get_ticker_dataframe(ticker: str, limit: Optional[int] = None) -> pd.DataFrame:
    """获取指定ticker的数据并转换为DataFrame"""
    engine = get_db_engine()
    
    query = """
        SELECT timestamp, open, high, low, close, volume
        FROM stock_daily
        WHERE ticker = %(ticker)s
        ORDER BY timestamp DESC
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    df = pd.read_sql_query(query, engine, params={"ticker": ticker})
    engine.dispose()
    
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    
    return df

def get_all_tickers():
    """获取所有ticker的列表"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT DISTINCT ticker
        FROM stock_daily
        ORDER BY ticker ASC;
    """)
    
    tickers = [row[0] for row in cursor.fetchall()]  # 提取ticker列
    
    cursor.close()
    conn.close()
    
    return tickers  # 返回所有ticker的列表

# ===============================
# 技术指标计算模块 (Technical Indicators)
# ===============================

def calculate_sma(df: pd.DataFrame, window: int = 20) -> pd.Series:
    """简单移动平均线 (Simple Moving Average)"""
    return df['Close'].rolling(window=window).mean()

def calculate_ema(df: pd.DataFrame, window: int = 20) -> pd.Series:
    """指数移动平均线 (Exponential Moving Average)"""
    return df['Close'].ewm(span=window, adjust=False).mean()

def calculate_bollinger_bands(df: pd.DataFrame, window: int = 20, num_std: float = 2) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """布林带 (Bollinger Bands)"""
    sma = calculate_sma(df, window)
    std = df['Close'].rolling(window=window).std()
    upper_band = sma + (std * num_std)
    lower_band = sma - (std * num_std)
    return upper_band, sma, lower_band

def calculate_rsi(df: pd.DataFrame, window: int = 14) -> pd.Series:
    """相对强弱指数 (Relative Strength Index)"""
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """MACD指标 (Moving Average Convergence Divergence)"""
    ema_fast = calculate_ema(df, fast)
    ema_slow = calculate_ema(df, slow)
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def calculate_stochastic(df: pd.DataFrame, k_window: int = 14, d_window: int = 3) -> Tuple[pd.Series, pd.Series]:
    """随机震荡指标 (Stochastic Oscillator)"""
    low_min = df['Low'].rolling(window=k_window).min()
    high_max = df['High'].rolling(window=k_window).max()
    k_percent = 100 * ((df['Close'] - low_min) / (high_max - low_min))
    d_percent = k_percent.rolling(window=d_window).mean()
    return k_percent, d_percent

def calculate_williams_r(df: pd.DataFrame, window: int = 14) -> pd.Series:
    """威廉指标 (Williams %R)"""
    high_max = df['High'].rolling(window=window).max()
    low_min = df['Low'].rolling(window=window).min()
    williams_r = -100 * ((high_max - df['Close']) / (high_max - low_min))
    return williams_r

# def calculate_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
#     """平均真实波幅 (Average True Range)"""
#     high_low = df['High'] - df['Low']
#     high_close_prev = np.abs(df['High'] - df['Close'].shift())
#     low_close_prev = np.abs(df['Low'] - df['Close'].shift())
#     true_range = pd.concat([high_low, high_close_prev, low_close_prev], axis=1).max(axis=1)
#     atr = true_range.rolling(window=window).mean()
#     return atr

def calculate_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    atr = ta.volatility.AverageTrueRange(df["High"], df["Low"], df["Close"], window=window).average_true_range()
    atr_level = atr.rolling(window=window).rank(pct=True)
    return atr, atr_level

def calculate_obv(df: pd.DataFrame) -> pd.Series:
    """能量潮指标 (On-Balance Volume)"""
    obv = [0]
    for i in range(1, len(df)):
        if df['Close'].iloc[i] > df['Close'].iloc[i-1]:
            obv.append(obv[-1] + df['Volume'].iloc[i])
        elif df['Close'].iloc[i] < df['Close'].iloc[i-1]:
            obv.append(obv[-1] - df['Volume'].iloc[i])
        else:
            obv.append(obv[-1])
    return pd.Series(obv, index=df.index)

def calculate_percentile(series: pd.Series, window: int = 252) -> pd.Series:
    """
    计算 rolling 百分位（当前位置在过去 window 天的百分位）
    """
    return series.rolling(window).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False)

def calculate_vwap(df: pd.DataFrame) -> pd.Series:
    """成交量加权平均价格 (Volume Weighted Average Price)"""
    typical_price = (df['High'] + df['Low'] + df['Close']) / 3
    cumulative_volume = df['Volume'].cumsum()
    cumulative_volume_price = (typical_price * df['Volume']).cumsum()
    vwap = cumulative_volume_price / cumulative_volume
    return vwap

# ===============================
# Alpha信号生成模块 (Alpha Signals)
# ===============================

def momentum_signal(df: pd.DataFrame, window: int = 20) -> pd.Series:
    """动量信号 - 价格相对于N日前的变化率"""
    return (df['Close'] / df['Close'].shift(window) - 1) * 100

def mean_reversion_signal(df: pd.DataFrame, window: int = 20) -> pd.Series:
    """均值回归信号 - 当前价格相对于均线的偏离度"""
    sma = calculate_sma(df, window)
    return (df['Close'] - sma) / sma * 100

def volume_price_trend(df: pd.DataFrame) -> pd.Series:
    """量价趋势信号"""
    pct_change = df['Close'].pct_change()
    vpt = (pct_change * df['Volume']).cumsum()
    return vpt

def price_volume_divergence(df: pd.DataFrame, window: int = 20) -> pd.Series:
    """价量背离信号"""
    price_momentum = momentum_signal(df, window)
    volume_sma = df['Volume'].rolling(window=window).mean()
    volume_momentum = (df['Volume'] / volume_sma - 1) * 100
    # 简单的背离度量：价格动量与成交量动量的差值
    divergence = price_momentum - volume_momentum
    return divergence

def volatility_signal(df: pd.DataFrame, window: int = 20) -> pd.Series:
    """波动率信号 - 基于价格波动率的交易信号"""
    returns = df['Close'].pct_change()
    volatility = returns.rolling(window=window).std() * np.sqrt(252)  # 年化波动率
    return volatility

def relative_strength_vs_market(df: pd.DataFrame, market_df: pd.DataFrame, window: int = 60) -> pd.Series:
    """相对强度信号 - 个股相对于市场的表现"""
    stock_returns = df['Close'].pct_change(window)
    market_returns = market_df['Close'].pct_change(window) if 'Close' in market_df.columns else 0
    relative_strength = stock_returns - market_returns
    return relative_strength

def breakout_signal(df: pd.DataFrame, window: int = 20) -> pd.Series:
    """突破信号 - 基于价格突破历史区间的信号"""
    high_max = df['High'].rolling(window=window).max()
    low_min = df['Low'].rolling(window=window).min()
    
    # 突破上轨为正信号，突破下轨为负信号
    signal = pd.Series(0, index=df.index)
    signal[df['Close'] > high_max.shift(1)] = 1  # 向上突破
    signal[df['Close'] < low_min.shift(1)] = -1  # 向下突破
    
    return signal


# ===============================
# 综合分析功能
# ===============================

def comprehensive_analysis(ticker: str, days: int = 252) -> dict:
    """综合技术分析 - 返回所有指标的计算结果"""
    df = get_ticker_dataframe(ticker, days)
    
    if df.empty:
        return {"error": f"No data found for ticker {ticker}"}
    
    analysis = {
        "ticker": ticker,
        "data_points": len(df),
        "date_range": {
            "start": df.index[0].strftime('%Y-%m-%d'),
            "end": df.index[-1].strftime('%Y-%m-%d')
        },
        "current_price": df['Close'].iloc[-1],
        "indicators": {}
    }
    
    # 趋势指标
    analysis["indicators"]["sma_20"] = calculate_sma(df, 20).iloc[-1]
    analysis["indicators"]["ema_20"] = calculate_ema(df, 20).iloc[-1]
    
    upper, middle, lower = calculate_bollinger_bands(df)
    analysis["indicators"]["bollinger"] = {
        "upper": upper.iloc[-1],
        "middle": middle.iloc[-1], 
        "lower": lower.iloc[-1],
        "position": "above" if df['Close'].iloc[-1] > upper.iloc[-1] else "below" if df['Close'].iloc[-1] < lower.iloc[-1] else "middle"
    }
    
    # 动量指标
    analysis["indicators"]["rsi"] = calculate_rsi(df).iloc[-1]
    
    macd, signal, histogram = calculate_macd(df)
    analysis["indicators"]["macd"] = {
        "macd": macd.iloc[-1],
        "signal": signal.iloc[-1],
        "histogram": histogram.iloc[-1],
        "trend": "bullish" if histogram.iloc[-1] > 0 else "bearish"
    }
    
    k_percent, d_percent = calculate_stochastic(df)
    analysis["indicators"]["stochastic"] = {
        "k": k_percent.iloc[-1],
        "d": d_percent.iloc[-1],
        "signal": "overbought" if k_percent.iloc[-1] > 80 else "oversold" if k_percent.iloc[-1] < 20 else "neutral"
    }
    
    # 成交量指标
    analysis["indicators"]["obv"] = calculate_obv(df).iloc[-1]
    analysis["indicators"]["vwap"] = calculate_vwap(df).iloc[-1]
    
    # Alpha信号
    analysis["alpha_signals"] = {
        "momentum_20d": momentum_signal(df, 20).iloc[-1],
        "mean_reversion": mean_reversion_signal(df, 20).iloc[-1],
        "volatility": volatility_signal(df, 20).iloc[-1],
        "breakout": breakout_signal(df, 20).iloc[-1]
    }
    
    # 风险指标
    atr, atr_level = calculate_atr(df)
    analysis["risk_metrics"] = {
        "atr": atr.iloc[-1],
        "atr_level": atr_level.iloc[-1],
        "volatility_20d": df['Close'].pct_change().rolling(20).std().iloc[-1] * np.sqrt(252),
        "max_drawdown": calculate_max_drawdown(df),
        "sharpe_ratio": calculate_sharpe_ratio(df)
    }
    
    return analysis

def calculate_max_drawdown(df: pd.DataFrame) -> float:
    """最大回撤计算"""
    rolling_max = df['Close'].expanding().max()
    drawdown = (df['Close'] - rolling_max) / rolling_max
    return drawdown.min()

def calculate_sharpe_ratio(df: pd.DataFrame, risk_free_rate: float = 0.02) -> float:
    """夏普比率计算"""
    returns = df['Close'].pct_change().dropna()
    excess_returns = returns - risk_free_rate/252  # 日化无风险收益率
    if returns.std() == 0:
        return 0
    return excess_returns.mean() / returns.std() * np.sqrt(252)

def generate_trading_signals(ticker: str, days: int = 60) -> pd.DataFrame:
    """生成交易信号组合"""
    df = get_ticker_dataframe(ticker, days)
    
    if df.empty:
        return pd.DataFrame()
    
    signals_df = df.copy()
    
    # 添加各种信号
    signals_df['SMA_20'] = calculate_sma(df, 20)
    signals_df['EMA_12'] = calculate_ema(df, 12)
    signals_df['RSI'] = calculate_rsi(df)
    signals_df['MACD'], signals_df['MACD_Signal'], signals_df['MACD_Hist'] = calculate_macd(df)
    signals_df['Momentum_20'] = momentum_signal(df, 20)
    signals_df['Mean_Reversion'] = mean_reversion_signal(df, 20)
    signals_df['Breakout'] = breakout_signal(df, 20)
    
    signals_df['Signal_Score'] = 0.0 
    
    # RSI信号权重
    rsi_buy_mask = signals_df['RSI'] < 30
    rsi_sell_mask = signals_df['RSI'] > 70
    signals_df.loc[rsi_buy_mask, 'Signal_Score'] = signals_df.loc[rsi_buy_mask, 'Signal_Score'] + 0.3
    signals_df.loc[rsi_sell_mask, 'Signal_Score'] = signals_df.loc[rsi_sell_mask, 'Signal_Score'] - 0.3
    
    # MACD信号权重
    macd_bull_mask = signals_df['MACD_Hist'] > 0
    macd_bear_mask = signals_df['MACD_Hist'] < 0
    signals_df.loc[macd_bull_mask, 'Signal_Score'] = signals_df.loc[macd_bull_mask, 'Signal_Score'] + 0.2
    signals_df.loc[macd_bear_mask, 'Signal_Score'] = signals_df.loc[macd_bear_mask, 'Signal_Score'] - 0.2
    
    # 趋势信号权重
    trend_up_mask = signals_df['Close'] > signals_df['SMA_20']
    trend_down_mask = signals_df['Close'] < signals_df['SMA_20']
    signals_df.loc[trend_up_mask, 'Signal_Score'] = signals_df.loc[trend_up_mask, 'Signal_Score'] + 0.2
    signals_df.loc[trend_down_mask, 'Signal_Score'] = signals_df.loc[trend_down_mask, 'Signal_Score'] - 0.2
    
    # 突破信号权重
    signals_df['Signal_Score'] = signals_df['Signal_Score'] + signals_df['Breakout'] * 0.3
    
    # 生成最终交易信号
    signals_df['Trading_Signal'] = 'HOLD'
    buy_mask = signals_df['Signal_Score'] > 0.4
    sell_mask = signals_df['Signal_Score'] < -0.4
    signals_df.loc[buy_mask, 'Trading_Signal'] = 'BUY'
    signals_df.loc[sell_mask, 'Trading_Signal'] = 'SELL'
    
    return signals_df[['Close', 'Volume', 'RSI', 'MACD_Hist', 'Signal_Score', 'Trading_Signal']]

def backtest_strategy(ticker: str, days: int = 252, initial_capital: float = 100000) -> dict:
    """简单回测策略"""
    signals_df = generate_trading_signals(ticker, days)
    
    if signals_df.empty:
        return {"error": "No data available for backtesting"}
    
    # 初始化回测参数
    capital = initial_capital
    position = 0  # 持仓数量
    trades = []
    portfolio_value = []
    
    for i, row in signals_df.iterrows():
        price = row['Close']
        signal = row['Trading_Signal']
        
        if signal == 'BUY' and position <= 0:
            # 买入信号且当前无持仓或有空头持仓
            shares_to_buy = capital // price
            if shares_to_buy > 0:
                position += shares_to_buy
                capital -= shares_to_buy * price
                trades.append({
                    'date': i,
                    'action': 'BUY',
                    'price': price,
                    'shares': shares_to_buy,
                    'capital': capital
                })
                
        elif signal == 'SELL' and position > 0:
            # 卖出信号且当前有持仓
            capital += position * price
            trades.append({
                'date': i,
                'action': 'SELL',
                'price': price,
                'shares': position,
                'capital': capital
            })
            position = 0
            
        # 计算当前组合价值
        current_value = capital + position * price
        portfolio_value.append(current_value)
    
    # 最终清仓
    if position > 0:
        final_price = signals_df['Close'].iloc[-1]
        capital += position * final_price
        portfolio_value[-1] = capital
    
    # 计算回测结果
    total_return = (capital - initial_capital) / initial_capital * 100
    buy_hold_return = (signals_df['Close'].iloc[-1] - signals_df['Close'].iloc[0]) / signals_df['Close'].iloc[0] * 100
    
    return {
        "ticker": ticker,
        "initial_capital": initial_capital,
        "final_capital": capital,
        "total_return": total_return,
        "buy_hold_return": buy_hold_return,
        "excess_return": total_return - buy_hold_return,
        "total_trades": len(trades),
        "trades": trades[:10],  # 只返回前10笔交易
        "portfolio_values": portfolio_value
    }

def culculate_monthly_avg_close(df: pd.DataFrame) -> pd.DataFrame:
    if 'Close' not in df.columns:
        raise ValueError("DataFrame must contain 'Close' column")
    monthly_avg = df['Close'].resample('M').mean().to_frame(name='Monthly_Avg_Close')
    monthly_avg = monthly_avg['Monthly_Avg_Close'].round(4)
    
    return monthly_avg

# 特征工程和模型训练
def predict_price_direction_optimized(ticker: str, window: int = 14) -> dict:
    """
    优化版：使用 RandomForestClassifier 预测股票价格方向，加入 PCA 降维和相关性热力图
    优化点：更多特征、超参数调优、时间序列交叉验证、PCA 降维、相关性分析
    """
    # 获取数据
    df = get_ticker_dataframe(ticker, limit=1000)
    if df.empty:
        raise ValueError(f"No data for ticker {ticker}")

    # 特征工程：增加更多技术指标
    df['SMA_14'] = df['Close'].rolling(window=window).mean()  # 14 天 SMA
    df['SMA_50'] = df['Close'].rolling(window=50).mean()  # 50 天 SMA
    df['RSI'] = ta.momentum.RSIIndicator(df['Close'], window=window).rsi()
    df['ATR'] = ta.volatility.AverageTrueRange(
        high=df['High'], low=df['Low'], close=df['Close'], window=window
    ).average_true_range()
    df['MACD'] = ta.trend.MACD(df['Close']).macd()  # MACD 线
    df['Close_Change'] = df['Close'].pct_change()
    df['Volume_Change'] = df['Volume'].pct_change()
    df['Volatility'] = df['Close'].rolling(window=window).std()  # 历史波动率

    # 目标变量：下一交易日价格方向
    df['Target'] = (df['Close'].shift(-1) > df['Close']).astype(int)

    # 删除 NaN
    df = df.dropna()

    # 特征列表
    features = ['SMA_14', 'SMA_50', 'RSI', 'ATR', 'MACD', 'Close_Change', 'Volume_Change', 'Volatility']
    X = df[features]
    y = df['Target']

    # 相关性热力图
    correlation_matrix = X.corr()
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1)
    plt.title(f'Correlation Heatmap of Features for {ticker}')
    plt.savefig(f'./outputs/correlation_heatmap_{ticker}.png')
    plt.close()

    # 标准化
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # PCA 降维：保留 90% 方差
    pca = PCA(n_components=0.9)  # 保留 90% 方差
    X_pca = pca.fit_transform(X_scaled)
    
    # PCA 信息
    n_components = X_pca.shape[1]
    explained_variance = np.sum(pca.explained_variance_ratio_)
    # print(f"PCA 主成分数量: {n_components}")
    # print(f"解释方差比例: {explained_variance:.4f}")

    # 时间序列交叉验证
    tscv = TimeSeriesSplit(n_splits=5)  # 5 折时间序列交叉验证
    model = RandomForestClassifier(random_state=42)

    # 超参数调优
    param_grid = {
        'n_estimators': [50, 100, 200],  # 树的数量
        'max_depth': [5, 10, None],     # 树的最大深度
        'min_samples_split': [2, 5]      # 节点分裂的最小样本数
    }
    grid_search = GridSearchCV(model, param_grid, cv=tscv, scoring='accuracy')
    grid_search.fit(X_pca, y)

    # 最佳模型
    best_model = grid_search.best_estimator_
    # print(f"最佳参数: {grid_search.best_params_}")

    # 测试集预测（最后 20% 数据）
    split_idx = int(len(X_pca) * 0.8)
    X_train, X_test = X_pca[:split_idx], X_pca[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    best_model.fit(X_train, y_train)
    y_pred = best_model.predict(X_test)

    # 评估
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred, output_dict=True)
    # 特征重要性基于主成分
    feature_importance = pd.Series(
        best_model.feature_importances_, 
        index=[f'PC{i+1}' for i in range(n_components)]
    ).sort_values(ascending=False)

    return {
        'accuracy': round(accuracy, 4),
        'classification_report': report,
        'feature_importance': feature_importance.round(4).to_dict(),
        'best_params': grid_search.best_params_,
        'n_components': n_components,
        'explained_variance': round(explained_variance, 4),
        'correlation_heatmap': f'./outputs/correlation_heatmap_{ticker}.png'
    }

# 示例使用
def main():
    ticker = "BURU"
    result = predict_price_direction_optimized(ticker)
    print(f"预测准确率: {result['accuracy']}")
    print("\n分类报告:")
    print(pd.DataFrame(result['classification_report']).T)
    print("\n特征重要性（主成分）:")
    print(result['feature_importance'])
    print(f"\nPCA 主成分数量: {result['n_components']}")
    print(f"解释方差比例: {result['explained_variance']}")
    print(f"相关性热力图保存为: {result['correlation_heatmap']}")
    
    # print(f"=== 量化分析报告：{ticker} ===\n")
    
    # # 1. 基础数据获取
    # daily_data = get_ticker_dataframe(ticker)
    # current_date_count = len(daily_data)
    # print(f"📊 数据概览：共获取 {len(daily_data)} 条日线数据")
    
    # mothly_average = culculate_monthly_avg_close(daily_data)
    # print(f"📅 月度平均收盘价：\n{mothly_average}\n")
    
    # # 2. 综合技术分析
    # print(f"\n📈 技术指标分析：")
    # analysis = comprehensive_analysis(ticker, current_date_count)
    
    # if "error" not in analysis:
    #     print(f"当前价格: ${analysis['current_price']:.2f}")
    #     print(f"RSI: {analysis['indicators']['rsi']:.2f}")
    #     print(f"MACD趋势: {analysis['indicators']['macd']['trend']}")
    #     print(f"布林带位置: {analysis['indicators']['bollinger']['position']}")
    #     print(f"平均真实波幅: {analysis['risk_metrics']['atr']:.2f}")
    #     print(f"平均真实波幅: {analysis['risk_metrics']['atr_level']:.2f}")
    #     print(f"波动率: {analysis['risk_metrics']['volatility_20d']:.2f}")
    #     print(f"夏普比率: {analysis['risk_metrics']['sharpe_ratio']:.2f}")
    
    # # 3. Alpha信号分析
    # print(f"\n🎯 Alpha信号：")
    # if "error" not in analysis:
    #     signals = analysis['alpha_signals']
    #     print(f"动量信号(20日): {signals['momentum_20d']:.2f}%")
    #     print(f"均值回归信号: {signals['mean_reversion']:.2f}%")
    #     print(f"突破信号: {signals['breakout']}")
    
    # # 4. 交易信号生成
    # print(f"\n💡 交易信号：")
    # trading_signals = generate_trading_signals(ticker, 30)
    # if not trading_signals.empty:
    #     latest_signal = trading_signals.iloc[-1]
    #     print(f"最新信号: {latest_signal['Trading_Signal']}")
    #     print(f"信号强度: {latest_signal['Signal_Score']:.2f}")
    
    # # 5. 策略回测
    # print(f"\n📊 策略回测结果：")
    # backtest_results = backtest_strategy(ticker, current_date_count, 100000)
    
    # if "error" not in backtest_results:
    #     print(f"总收益率: {backtest_results['total_return']:.2f}%")
    #     print(f"买入持有收益率: {backtest_results['buy_hold_return']:.2f}%")
    #     print(f"超额收益: {backtest_results['excess_return']:.2f}%")
    #     print(f"交易次数: {backtest_results['total_trades']}")
    
    # print(f"\n✅ 分析完成！")

    # prices = [100, 102, 105, 107, 104, 101, 99, 95]
    # returns = [(prices[i] / prices[i - 1]) - 1 for i in range(1, len(prices))]
    # pct_returns = [f"{r * 100:.2f}%" for r in returns]  # 格式化为百分比，保留2位小数
    # print("简单收益率序列:", pct_returns)
    # returns = np.array(returns)
    # mean_return = returns.mean()
    # std_return = returns.std(ddof=1)
    
    # z_score = [(returns[i] - mean_return) / std_return for i in range(len(returns))]
    
    # print("价格序列:", prices)
    
    # print(f"收益率均值 {mean_return: .4f}")
    # print(f"收益率标准版 {std_return: .4f}")
    # print(f"最后一日z-score {z_score[-1]: .4f}")
    

if __name__ == "__main__":
    main()