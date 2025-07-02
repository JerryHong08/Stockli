"""
量化策略框架 - 用于面试展示
这个模块展示了量化交易策略开发的完整流程
"""

import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from modules_develop.indicators.indicators_develop import (
    get_ticker_dataframe, calculate_rsi, calculate_macd, 
    calculate_sma, calculate_ema, calculate_bollinger_bands, calculate_obv, calculate_percentile, get_all_tickers
)

class BaseStrategy(ABC):
    """策略基类 - 定义策略接口"""
    
    def __init__(self, name: str):
        self.name = name
        self.signals = []
        self.positions = []
        self.trades = []
        
    @abstractmethod
    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        """生成交易信号 - 子类必须实现"""
        pass
    
    def backtest(self, ticker: str, days: int = 252, initial_capital: float = 100000) -> Dict:
        """执行回测"""
        df = get_ticker_dataframe(ticker, days)
        if df.empty:
            return {"error": "No data available"}
            
        signals = self.generate_signals(df)
        return self._calculate_performance(df, signals, initial_capital)
    
    def _calculate_performance(self, df: pd.DataFrame, signals: pd.Series, initial_capital: float) -> Dict:
        """计算策略表现"""
        capital = initial_capital
        position = 0
        trades = []
        portfolio_values = []
        
        for i in range(len(df)):
            price = df['Close'].iloc[i]
            signal = signals.iloc[i]
            date = df.index[i]
            
            if signal == 1 and position <= 0:  # 买入信号
                shares_to_buy = capital // price
                if shares_to_buy > 0:
                    position += shares_to_buy
                    capital -= shares_to_buy * price
                    trades.append({
                        'date': date,
                        'action': 'BUY',
                        'price': price,
                        'shares': shares_to_buy
                    })
                    
            elif signal == -1 and position > 0:  # 卖出信号
                capital += position * price
                trades.append({
                    'date': date,
                    'action': 'SELL',
                    'price': price,
                    'shares': position
                })
                position = 0
            
            # 记录组合价值
            current_value = capital + position * price
            portfolio_values.append(current_value)
        
        # 最终清仓
        if position > 0:
            final_price = df['Close'].iloc[-1]
            capital += position * final_price
            portfolio_values[-1] = capital
        
        # 计算绩效指标
        total_return = (capital - initial_capital) / initial_capital * 100
        buy_hold_return = (df['Close'].iloc[-1] - df['Close'].iloc[0]) / df['Close'].iloc[0] * 100
        
        # 计算夏普比率
        returns = pd.Series(portfolio_values).pct_change().dropna()
        sharpe_ratio = returns.mean() / returns.std() * np.sqrt(252) if returns.std() > 0 else 0
        
        # 计算最大回撤
        portfolio_series = pd.Series(portfolio_values)
        rolling_max = portfolio_series.expanding().max()
        drawdown = (portfolio_series - rolling_max) / rolling_max
        max_drawdown = drawdown.min() * 100
        
        return {
            "strategy": self.name,
            "total_return": total_return,
            "buy_hold_return": buy_hold_return,
            "excess_return": total_return - buy_hold_return,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": max_drawdown,
            "total_trades": len(trades),
            "win_rate": self._calculate_win_rate(trades),
            "trades": trades[:10]  # 返回前10笔交易
        }
    
    def _calculate_win_rate(self, trades: List[Dict]) -> float:
        """计算胜率"""
        if len(trades) < 2:
            return 0
        
        buy_trades = [t for t in trades if t['action'] == 'BUY']
        sell_trades = [t for t in trades if t['action'] == 'SELL']
        
        if len(buy_trades) == 0 or len(sell_trades) == 0:
            return 0
        
        wins = 0
        for i in range(min(len(buy_trades), len(sell_trades))):
            if sell_trades[i]['price'] > buy_trades[i]['price']:
                wins += 1
        
        return wins / min(len(buy_trades), len(sell_trades)) * 100

class MeanReversionStrategy(BaseStrategy):
    """均值回归策略"""
    
    def __init__(self, window: int = 20, std_multiplier: float = 2.0):
        super().__init__("Mean Reversion Strategy")
        self.window = window
        self.std_multiplier = std_multiplier
    
    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        """基于布林带的均值回归信号"""
        upper, middle, lower = calculate_bollinger_bands(df, self.window, self.std_multiplier)
        
        signals = pd.Series(0, index=df.index)
        
        # 价格触及下轨时买入（超卖）
        signals.loc[df['Close'] <= lower] = 1
        
        # 价格触及上轨时卖出（超买）
        signals.loc[df['Close'] >= upper] = -1
        
        return signals

class MomentumStrategy(BaseStrategy):
    """动量策略"""
    
    def __init__(self, short_window: int = 12, long_window: int = 26):
        super().__init__("Momentum Strategy")
        self.short_window = short_window
        self.long_window = long_window
    
    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        """基于MACD的动量信号"""
        macd, signal_line, histogram = calculate_macd(df, self.short_window, self.long_window)
        
        signals = pd.Series(0, index=df.index)
        
        # MACD向上突破信号线时买入
        signals.loc[(macd > signal_line) & (macd.shift(1) <= signal_line.shift(1))] = 1
        
        # MACD向下跌破信号线时卖出
        signals.loc[(macd < signal_line) & (macd.shift(1) >= signal_line.shift(1))] = -1
        
        return signals

class RSIStrategy(BaseStrategy):
    """RSI策略"""
    
    def __init__(self, window: int = 14, oversold: float = 30, overbought: float = 70):
        super().__init__("RSI Strategy")
        self.window = window
        self.oversold = oversold
        self.overbought = overbought
    
    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        """基于RSI的超买超卖信号"""
        rsi = calculate_rsi(df, self.window)
        
        signals = pd.Series(0, index=df.index)
        
        # RSI < 30时买入（超卖）
        signals.loc[rsi < self.oversold] = 1
        
        # RSI > 70时卖出（超买）
        signals.loc[rsi > self.overbought] = -1
        
        return signals

class MultiFactorStrategy(BaseStrategy):
    """多因子策略 - 综合多个信号"""
    
    def __init__(self):
        super().__init__("Multi-Factor Strategy")
    
    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        """综合多个技术指标的信号"""
        # 计算各种指标
        rsi = calculate_rsi(df, 14)
        macd, signal_line, histogram = calculate_macd(df)
        sma_20 = calculate_sma(df, 20)
        sma_50 = calculate_sma(df, 50)
        
        # 初始化信号
        signals = pd.Series(0, index=df.index)
        
        # 多因子条件
        # 买入条件：RSI超卖 + MACD向上 + 短期均线上穿长期均线
        buy_condition = (
            (rsi < 30) &  # RSI超卖
            (histogram > 0) &  # MACD柱状图为正
            (sma_20 > sma_50)  # 短期均线在长期均线之上
        )
        
        # 卖出条件：RSI超买 + MACD向下 + 短期均线下穿长期均线
        sell_condition = (
            (rsi > 70) &  # RSI超买
            (histogram < 0) &  # MACD柱状图为负
            (sma_20 < sma_50)  # 短期均线在长期均线之下
        )
        
        signals.loc[buy_condition] = 1
        signals.loc[sell_condition] = -1
        
        return signals

class PairsTradingStrategy(BaseStrategy):
    """配对交易策略示例"""
    
    def __init__(self, ticker_pair: Tuple[str, str] = ("AAPL", "MSFT")):
        super().__init__("Pairs Trading Strategy")
        self.ticker_pair = ticker_pair
    
    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        """基于价格比率的配对交易信号"""
        # 这里简化处理，实际需要两个股票的数据
        # 使用价格的移动平均作为代理
        sma_20 = calculate_sma(df, 20)
        price_ratio = df['Close'] / sma_20
        
        signals = pd.Series(0, index=df.index)
        
        # 价格比率偏离均值时进行交易
        std_ratio = price_ratio.rolling(20).std()
        mean_ratio = price_ratio.rolling(20).mean()
        
        # 比率过高时卖出
        signals.loc[price_ratio > mean_ratio + 2 * std_ratio] = -1
        
        # 比率过低时买入
        signals.loc[price_ratio < mean_ratio - 2 * std_ratio] = 1
        
        return signals

class OBVStrategy(BaseStrategy):
    """基于 OBV 百分位背离的选股策略"""

    def __init__(self, window: int = 252):
        super().__init__("OBV Strategy")
        self.window = window

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        """
        OBV 百分位 / 价格百分位 得到 score，根据 score 给出信号
        """
        if df.empty or len(df) < self.window:
            return pd.Series(0, index=df.index)

        obv = calculate_obv(df)
        obv_pct = calculate_percentile(obv, self.window)
        price_pct = calculate_percentile(df['Close'], self.window)

        # score 越高说明 OBV 高位，价格低位
        score = obv_pct / (price_pct + 1e-5)

        signals = pd.Series(0, index=df.index)

        # 简单策略示例：score > 3 买入，score < 0.5 卖出
        signals[score > 3] = 1
        signals[score < 0.5] = -1

        return signals

    def calculate_latest_score(self, df: pd.DataFrame) -> float:
        """
        返回最新一个交易日的 score，供多股票排序
        """
        if df.empty or len(df) < self.window:
            return np.nan

        obv = calculate_obv(df)
        obv_pct = calculate_percentile(obv, self.window)
        price_pct = calculate_percentile(df['Close'], self.window)

        latest_score = (obv_pct.iloc[-1]) / (price_pct.iloc[-1] + 1e-5)
        return latest_score
    
def compare_strategies(ticker: str, days: int = 252) -> pd.DataFrame:
    """比较多个策略的表现"""
    strategies = [
        MeanReversionStrategy(),
        MomentumStrategy(),
        RSIStrategy(),
        MultiFactorStrategy(),
        PairsTradingStrategy()
    ]
    
    results = []
    
    for strategy in strategies:
        result = strategy.backtest(ticker, days)
        if "error" not in result:
            results.append({
                "策略名称": result["strategy"],
                "总收益率(%)": result["total_return"],
                "超额收益(%)": result["excess_return"],
                "夏普比率": result["sharpe_ratio"],
                "最大回撤(%)": result["max_drawdown"],
                "交易次数": result["total_trades"],
                "胜率(%)": result["win_rate"]
            })
    
    return pd.DataFrame(results)

def portfolio_optimization(tickers: List[str], days: int = 252) -> Dict:
    """简单的投资组合优化"""
    # 获取多个股票的收益率数据
    returns_data = {}
    
    for ticker in tickers:
        df = get_ticker_dataframe(ticker, days)
        if not df.empty:
            returns = df['Close'].pct_change().dropna()
            returns_data[ticker] = returns
    
    if not returns_data:
        return {"error": "No data available for portfolio optimization"}
    
    # 构建收益率矩阵
    returns_df = pd.DataFrame(returns_data)
    returns_df = returns_df.dropna()
    
    # 计算年化收益率和协方差矩阵
    annual_returns = returns_df.mean() * 252
    cov_matrix = returns_df.cov() * 252
    
    # 等权重组合（简化版）
    num_assets = len(tickers)
    weights = np.array([1/num_assets] * num_assets)
    
    # 计算组合指标
    portfolio_return = np.sum(annual_returns * weights)
    portfolio_volatility = np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))
    sharpe_ratio = portfolio_return / portfolio_volatility
    
    return {
        "tickers": tickers,
        "weights": dict(zip(tickers, weights)),
        "expected_return": portfolio_return,
        "volatility": portfolio_volatility,
        "sharpe_ratio": sharpe_ratio,
        "individual_returns": annual_returns.to_dict()
    }

def risk_management_analysis(ticker: str, days: int = 252) -> Dict:
    """风险管理分析"""
    df = get_ticker_dataframe(ticker, days)
    if df.empty:
        return {"error": "No data available"}
    
    returns = df['Close'].pct_change().dropna()
    
    # VaR计算（Value at Risk）
    var_95 = np.percentile(returns, 5)  # 95% VaR
    var_99 = np.percentile(returns, 1)  # 99% VaR
    
    # CVaR计算（Conditional Value at Risk）
    cvar_95 = returns[returns <= var_95].mean()
    cvar_99 = returns[returns <= var_99].mean()
    
    # 最大回撤
    rolling_max = df['Close'].expanding().max()
    drawdown = (df['Close'] - rolling_max) / rolling_max
    max_drawdown = drawdown.min()
    
    # 波动率分析
    volatility_1d = returns.std()
    volatility_annual = volatility_1d * np.sqrt(252)
    
    return {
        "ticker": ticker,
        "var_95": var_95 * 100,  # 转换为百分比
        "var_99": var_99 * 100,
        "cvar_95": cvar_95 * 100,
        "cvar_99": cvar_99 * 100,
        "max_drawdown": max_drawdown * 100,
        "daily_volatility": volatility_1d * 100,
        "annual_volatility": volatility_annual * 100,
        "skewness": returns.skew(),  # 偏度
        "kurtosis": returns.kurtosis()  # 峰度
    }

def find_top_obv_stocks(top_n: int = 10, days: int = 252):
    tickers = get_all_tickers()
    strategy = OBVStrategy(window=252)
    scores = []

    def process_ticker(ticker):
        df = get_ticker_dataframe(ticker, days)
        if not df.empty:
            score = strategy.calculate_latest_score(df)
            if not np.isnan(score):
                return {"ticker": ticker, "obv_score": score}
        return None

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process_ticker, ticker): ticker for ticker in tickers}
        for future in tqdm(as_completed(futures), total=len(tickers), desc="计算 OBV Score"):
            result = future.result()
            if result:
                scores.append(result)

    sorted_scores = sorted(scores, key=lambda x: x["obv_score"], reverse=True)
    return sorted_scores[:top_n]

def main():
    print("=== OBV 多股票选股演示 ===")
    top_n = 30
    top_stocks = find_top_obv_stocks(top_n=top_n, days=252)

    print(f"\nTop {top_n} OBV 异动股票:")
    for item in top_stocks:
        print(f"Ticker: {item['ticker']} | OBV Score: {item['obv_score']:.2f}")

if __name__ == "__main__":
    main()
    
# def main():
#     """主函数 - 展示策略框架的使用"""
#     print("=== 量化策略框架演示 ===\n")
    
#     ticker = "AAPL"
    
#     # 1. 单策略回测
#     print("📊 单策略回测：")
#     momentum_strategy = MomentumStrategy()
#     result = momentum_strategy.backtest(ticker, 60)
    
#     if "error" not in result:
#         print(f"策略: {result['strategy']}")
#         print(f"总收益率: {result['total_return']:.2f}%")
#         print(f"夏普比率: {result['sharpe_ratio']:.2f}")
#         print(f"最大回撤: {result['max_drawdown']:.2f}%")
    
#     # 2. 策略比较
#     print(f"\n📈 策略比较：")
#     comparison = compare_strategies(ticker, 60)
#     if not comparison.empty:
#         print(comparison.to_string(index=False))
    
#     # 3. 风险分析
#     print(f"\n⚠️ 风险分析：")
#     risk_analysis = risk_management_analysis(ticker, 60)
#     if "error" not in risk_analysis:
#         print(f"95% VaR: {risk_analysis['var_95']:.2f}%")
#         print(f"最大回撤: {risk_analysis['max_drawdown']:.2f}%")
#         print(f"年化波动率: {risk_analysis['annual_volatility']:.2f}%")
    
#     # 4. 投资组合优化
#     print(f"\n📊 投资组合优化：")
#     portfolio_tickers = ["AAPL", "MSFT", "GOOGL"]
#     portfolio_result = portfolio_optimization(portfolio_tickers, 60)
#     if "error" not in portfolio_result:
#         print(f"预期收益率: {portfolio_result['expected_return']:.2f}")
#         print(f"组合波动率: {portfolio_result['volatility']:.2f}")
#         print(f"夏普比率: {portfolio_result['sharpe_ratio']:.2f}")

if __name__ == "__main__":
    main()
