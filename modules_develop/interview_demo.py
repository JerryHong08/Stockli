"""
量化面试演示脚本
展示完整的量化分析流程，包括数据处理、指标计算、策略开发和风险管理
"""

import sys
import os

# 添加项目根目录到 Python 路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

try:
    # 导入自定义模块
    from modules_develop.indicators.indicators_develop import (
        get_all_tickers, get_ticker_dataframe, comprehensive_analysis,
        generate_trading_signals, backtest_strategy
    )
    from modules_develop.strategies.strategy_framework import (
        MomentumStrategy, MeanReversionStrategy, RSIStrategy, MultiFactorStrategy,
        compare_strategies, portfolio_optimization, risk_management_analysis
    )
except ImportError as e:
    print(f"模块导入失败: {e}")
    print("正在使用基础功能演示...")
    
    # 基础演示模式
    def get_all_tickers():
        return ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
    
    def get_ticker_dataframe(ticker, days):
        import pandas as pd
        import numpy as np
        dates = pd.date_range(end='2024-12-01', periods=days, freq='D')
        np.random.seed(42)
        data = {
            'Open': 150 + np.random.randn(days).cumsum() * 2,
            'High': 150 + np.random.randn(days).cumsum() * 2 + 2,
            'Low': 150 + np.random.randn(days).cumsum() * 2 - 2,
            'Close': 150 + np.random.randn(days).cumsum() * 2,
            'Volume': np.random.randint(1000000, 10000000, days)
        }
        return pd.DataFrame(data, index=dates)

def demonstrate_data_processing():
    """演示数据处理能力"""
    print("=" * 60)
    print("🔧 数据处理能力演示")
    print("=" * 60)
    
    # 获取可用的ticker列表
    tickers = get_all_tickers()
    print(f"📊 数据库中共有 {len(tickers)} 只股票数据")
    print(f"📊 样例股票: {tickers[:10]}")
    
    # 选择一只股票进行详细分析
    demo_ticker = "AAPL"
    df = get_ticker_dataframe(demo_ticker, 100)
    
    if not df.empty:
        print(f"\n📈 {demo_ticker} 数据概览:")
        print(f"   数据时间范围: {df.index[0].strftime('%Y-%m-%d')} 至 {df.index[-1].strftime('%Y-%m-%d')}")
        print(f"   总交易日数: {len(df)}")
        print(f"   当前价格: ${df['Close'].iloc[-1]:.2f}")
        print(f"   期间最高价: ${df['High'].max():.2f}")
        print(f"   期间最低价: ${df['Low'].min():.2f}")
        print(f"   平均成交量: {df['Volume'].mean():,.0f}")
        
        # 数据质量检查
        missing_data = df.isnull().sum().sum()
        print(f"   数据完整性: {100-missing_data/len(df)*100:.1f}% (缺失 {missing_data} 个数据点)")
    
    return demo_ticker

def demonstrate_technical_analysis(ticker):
    """演示技术分析能力"""
    print("\n" + "=" * 60)
    print("📊 技术分析能力演示")
    print("=" * 60)
    
    # 综合技术分析
    analysis = comprehensive_analysis(ticker, 90)
    
    if "error" not in analysis:
        print(f"\n🎯 {ticker} 技术指标分析:")
        
        # 趋势指标
        print(f"\n📈 趋势指标:")
        print(f"   20日均线: ${analysis['indicators']['sma_20']:.2f}")
        print(f"   20日指数均线: ${analysis['indicators']['ema_20']:.2f}")
        
        bollinger = analysis['indicators']['bollinger']
        print(f"   布林带上轨: ${bollinger['upper']:.2f}")
        print(f"   布林带中轨: ${bollinger['middle']:.2f}")
        print(f"   布林带下轨: ${bollinger['lower']:.2f}")
        print(f"   当前位置: {bollinger['position']}")
        
        # 动量指标
        print(f"\n⚡ 动量指标:")
        print(f"   RSI: {analysis['indicators']['rsi']:.2f}")
        
        macd_info = analysis['indicators']['macd']
        print(f"   MACD: {macd_info['macd']:.4f}")
        print(f"   MACD信号线: {macd_info['signal']:.4f}")
        print(f"   MACD柱状图: {macd_info['histogram']:.4f}")
        print(f"   MACD趋势: {macd_info['trend']}")
        
        stoch_info = analysis['indicators']['stochastic']
        print(f"   随机指标K: {stoch_info['k']:.2f}")
        print(f"   随机指标D: {stoch_info['d']:.2f}")
        print(f"   市场状态: {stoch_info['signal']}")
        
        # Alpha信号
        print(f"\n🎯 Alpha信号:")
        alpha_signals = analysis['alpha_signals']
        print(f"   20日动量: {alpha_signals['momentum_20d']:.2f}%")
        print(f"   均值回归信号: {alpha_signals['mean_reversion']:.2f}%")
        print(f"   波动率: {alpha_signals['volatility']:.2f}")
        print(f"   突破信号: {alpha_signals['breakout']}")
        
        # 风险指标
        print(f"\n⚠️ 风险指标:")
        risk_metrics = analysis['risk_metrics']
        print(f"   ATR: {risk_metrics['atr']:.2f}")
        print(f"   20日波动率: {risk_metrics['volatility_20d']:.2f}")
        print(f"   最大回撤: {risk_metrics['max_drawdown']:.2%}")
        print(f"   夏普比率: {risk_metrics['sharpe_ratio']:.2f}")

def demonstrate_strategy_development(ticker):
    """演示策略开发能力"""
    print("\n" + "=" * 60)
    print("🧠 策略开发能力演示")
    print("=" * 60)
    
    # 单策略详细分析
    print(f"\n📊 多因子策略回测 ({ticker}):")
    multi_factor = MultiFactorStrategy()
    result = multi_factor.backtest(ticker, 90, 100000)
    
    if "error" not in result:
        print(f"   策略名称: {result['strategy']}")
        print(f"   总收益率: {result['total_return']:.2f}%")
        print(f"   基准收益率: {result['buy_hold_return']:.2f}%")
        print(f"   超额收益: {result['excess_return']:.2f}%")
        print(f"   夏普比率: {result['sharpe_ratio']:.2f}")
        print(f"   最大回撤: {result['max_drawdown']:.2f}%")
        print(f"   交易次数: {result['total_trades']}")
        print(f"   胜率: {result['win_rate']:.1f}%")
        
        if result['trades']:
            print(f"\n   近期交易记录:")
            for trade in result['trades'][:3]:
                print(f"   {trade['date'].strftime('%Y-%m-%d')}: {trade['action']} {trade['shares']} 股 @ ${trade['price']:.2f}")
    
    # 策略对比
    print(f"\n📈 多策略对比分析:")
    comparison = compare_strategies(ticker, 90)
    
    if not comparison.empty:
        print("\n" + comparison.to_string(index=False, formatters={
            '总收益率(%)': '{:.2f}'.format,
            '超额收益(%)': '{:.2f}'.format,
            '夏普比率': '{:.2f}'.format,
            '最大回撤(%)': '{:.2f}'.format,
            '胜率(%)': '{:.1f}'.format
        }))
        
        # 找出最佳策略
        best_strategy = comparison.loc[comparison['超额收益(%)'].idxmax()]
        print(f"\n🏆 最佳策略: {best_strategy['策略名称']}")
        print(f"   超额收益: {best_strategy['超额收益(%)']:.2f}%")
        print(f"   夏普比率: {best_strategy['夏普比率']:.2f}")

def demonstrate_portfolio_optimization():
    """演示投资组合优化能力"""
    print("\n" + "=" * 60)
    print("📊 投资组合优化演示")
    print("=" * 60)
    
    # 选择几只科技股进行组合优化
    tech_stocks = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
    available_tickers = get_all_tickers()
    
    # 筛选出数据库中存在的股票
    portfolio_tickers = [ticker for ticker in tech_stocks if ticker in available_tickers]
    
    if len(portfolio_tickers) >= 2:
        print(f"\n🏢 科技股投资组合分析:")
        print(f"   组合成分: {', '.join(portfolio_tickers)}")
        
        portfolio_result = portfolio_optimization(portfolio_tickers, 90)
        
        if "error" not in portfolio_result:
            print(f"\n📊 组合优化结果:")
            print(f"   预期年化收益率: {portfolio_result['expected_return']:.2%}")
            print(f"   组合年化波动率: {portfolio_result['volatility']:.2%}")
            print(f"   夏普比率: {portfolio_result['sharpe_ratio']:.2f}")
            
            print(f"\n📈 个股权重分配:")
            for ticker, weight in portfolio_result['weights'].items():
                expected_return = portfolio_result['individual_returns'][ticker]
                print(f"   {ticker}: {weight:.1%} (预期收益: {expected_return:.2%})")
    else:
        print(f"\n⚠️ 可用股票数量不足，无法进行组合优化演示")

def demonstrate_risk_management(ticker):
    """演示风险管理能力"""
    print("\n" + "=" * 60)
    print("⚠️ 风险管理能力演示")
    print("=" * 60)
    
    risk_analysis = risk_management_analysis(ticker, 120)
    
    if "error" not in risk_analysis:
        print(f"\n🎯 {ticker} 风险分析报告:")
        
        print(f"\n📉 VaR分析 (风险价值):")
        print(f"   95% VaR: {risk_analysis['var_95']:.2f}% (95%置信度下的日最大损失)")
        print(f"   99% VaR: {risk_analysis['var_99']:.2f}% (99%置信度下的日最大损失)")
        
        print(f"\n📉 CVaR分析 (条件风险价值):")
        print(f"   95% CVaR: {risk_analysis['cvar_95']:.2f}% (超过VaR时的平均损失)")
        print(f"   99% CVaR: {risk_analysis['cvar_99']:.2f}% (极端情况下的平均损失)")
        
        print(f"\n📊 波动率分析:")
        print(f"   日波动率: {risk_analysis['daily_volatility']:.2f}%")
        print(f"   年化波动率: {risk_analysis['annual_volatility']:.2f}%")
        print(f"   最大回撤: {risk_analysis['max_drawdown']:.2f}%")
        
        print(f"\n📈 分布特征:")
        print(f"   偏度: {risk_analysis['skewness']:.2f} ({'右偏' if risk_analysis['skewness'] > 0 else '左偏' if risk_analysis['skewness'] < 0 else '对称'})")
        print(f"   峰度: {risk_analysis['kurtosis']:.2f} ({'尖峰' if risk_analysis['kurtosis'] > 0 else '平峰'})")
        
        # 风险等级评估
        volatility = risk_analysis['annual_volatility']
        if volatility < 15:
            risk_level = "低风险"
        elif volatility < 25:
            risk_level = "中等风险"
        elif volatility < 35:
            risk_level = "较高风险"
        else:
            risk_level = "高风险"
        
        print(f"\n🎯 风险等级评估: {risk_level}")

def demonstrate_trading_signals(ticker):
    """演示交易信号生成"""
    print("\n" + "=" * 60)
    print("💡 交易信号生成演示")
    print("=" * 60)
    
    signals = generate_trading_signals(ticker, 30)
    
    if not signals.empty:
        print(f"\n📊 {ticker} 最近交易信号:")
        
        # 显示最近的信号
        recent_signals = signals.tail(10)[['Close', 'RSI', 'Signal_Score', 'Trading_Signal']]
        print("\n最近10个交易日信号:")
        print(recent_signals.to_string(formatters={
            'Close': '${:.2f}'.format,
            'RSI': '{:.1f}'.format,
            'Signal_Score': '{:.2f}'.format
        }))
        
        # 当前信号分析
        latest = signals.iloc[-1]
        print(f"\n🎯 当前交易建议:")
        print(f"   信号: {latest['Trading_Signal']}")
        print(f"   信号强度: {latest['Signal_Score']:.2f}")
        print(f"   当前价格: ${latest['Close']:.2f}")
        print(f"   RSI: {latest['RSI']:.1f}")
        
        if latest['Trading_Signal'] == 'BUY':
            print(f"   建议: 考虑买入，信号强度较强")
        elif latest['Trading_Signal'] == 'SELL':
            print(f"   建议: 考虑卖出，信号强度较强")
        else:
            print(f"   建议: 持有观望，等待更强信号")

def main():
    """主演示函数"""
    print("🚀 量化实习生技能演示")
    print("=" * 80)
    print("本演示展示了量化投资中的核心技能:")
    print("• 数据处理与清洗")
    print("• 技术指标计算")
    print("• 策略开发与回测")
    print("• 投资组合优化")
    print("• 风险管理分析")
    print("• 交易信号生成")
    print("=" * 80)
    
    try:
        # 1. 数据处理演示
        demo_ticker = demonstrate_data_processing()
        
        # 2. 技术分析演示
        demonstrate_technical_analysis(demo_ticker)
        
        # 3. 交易信号演示
        demonstrate_trading_signals(demo_ticker)
        
        # 4. 策略开发演示
        demonstrate_strategy_development(demo_ticker)
        
        # 5. 投资组合优化演示
        demonstrate_portfolio_optimization()
        
        # 6. 风险管理演示
        demonstrate_risk_management(demo_ticker)
        
        # 总结
        print("\n" + "=" * 60)
        print("✅ 演示完成!")
        print("=" * 60)
        print("\n🎯 技能总结:")
        print("✓ Python数据分析 (pandas, numpy)")
        print("✓ 量化指标计算 (RSI, MACD, 布林带等)")
        print("✓ 策略框架设计与实现")
        print("✓ 回测与绩效评估")
        print("✓ 风险管理 (VaR, CVaR, 波动率)")
        print("✓ 投资组合优化")
        print("✓ 数据库操作 (PostgreSQL)")
        print("✓ 面向对象编程")
        print("\n💡 这些技能完全符合量化实习生的岗位要求!")
        
    except Exception as e:
        print(f"\n❌ 演示过程中出现错误: {e}")
        print("请检查数据库连接和数据完整性")

if __name__ == "__main__":
    main()
