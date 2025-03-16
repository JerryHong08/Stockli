import pyqtgraph as pg
import numpy as np
from PyQt5.QtWidgets import QGraphicsRectItem, QToolTip
from PyQt5.QtCore import Qt
from pyqtgraph.Qt import QtCore
from pyqtgraph import SignalProxy

def plot_candlestick(main_plot, volume_plot, df, enable_hover):
    if df.empty:
        return
    
    # 清空图表
    main_plot.clear()
    volume_plot.clear()
    
    # 使用连续索引作为横坐标
    x = np.arange(len(df))

    # 设置主图横坐标为日期格式
    axis = pg.DateAxisItem(orientation='bottom')
    main_plot.setAxisItems({'bottom': axis})

    # 主图绘制蜡烛图
    for i in range(len(df)):
        open_price = df["Open"].iloc[i]
        close_price = df["Close"].iloc[i]
        high = df["High"].iloc[i]
        low = df["Low"].iloc[i]

        color = 'g' if close_price >= open_price else 'r'
        body = QGraphicsRectItem(QtCore.QRectF(x[i] - 0.2, min(open_price, close_price), 0.4, abs(close_price - open_price)))
        body.setBrush(pg.mkBrush(color))
        body.setPen(pg.mkPen(color))
        main_plot.addItem(body)

        shadow = pg.PlotCurveItem([x[i], x[i]], [low, high], pen=pg.mkPen(color))
        main_plot.addItem(shadow)

    # 设置主图轴标签
    main_plot.setLabel('bottom', 'Date')
    main_plot.setLabel('left', 'Price')

    # 成交量图绘制
    volume_bars = pg.BarGraphItem(x=x, height=df["Volume"], width=0.4, brush='b')
    volume_plot.addItem(volume_bars)

    # 设置成交量图标签
    volume_plot.setLabel('left', 'Volume')

    # X轴同步
    volume_plot.setXLink(main_plot)
    
    # 强制初始化视图范围
    main_plot.getPlotItem().vb.sigResized.emit(main_plot.getPlotItem().vb)
    volume_plot.getPlotItem().vb.sigResized.emit(volume_plot.getPlotItem().vb)

    # 设置横坐标刻度为日期
    ticks = [(x[i], df["Date"].iloc[i].strftime('%Y%m%d')) for i in range(0, len(df), 5)]
    axis.setTicks([ticks])
    
    if enable_hover:
        print(enable_hover)
        def on_mouse_hover(event):
            pos = event[0]  # 获取鼠标位置
            if main_plot.sceneBoundingRect().contains(pos):
                mouse_point = main_plot.getViewBox().mapSceneToView(pos)
                index = int(mouse_point.x())
                if 0 <= index < len(df):  # 确保索引在有效范围内
                    date = df["Date"].iloc[index]
                    open_price = df["Open"].iloc[index]
                    close_price = df["Close"].iloc[index]
                    high = df["High"].iloc[index]
                    low = df["High"].iloc[index]
                    volume = df["Volume"].iloc[index]
                    
                    # 在图表标题中显示信息
                    main_plot.setTitle(
                        f"Date: {date}\n"
                        f"Open: {open_price:.2f}, Close: {close_price:.2f}\n"
                        f"High: {high:.2f}, Low: {low:.2f}\n"
                        f"Volume: {volume:,}"
                    )
                    
                    # 在鼠标位置显示提示框
                    tooltip_text = (
                        f"Date: {date}\n"
                        f"Open: {open_price:.2f}\n"
                        f"Close: {close_price:.2f}\n"
                        f"High: {high:.2f}\n"
                        f"Low: {low:.2f}\n"
                        f"Volume: {volume:,}"
                    )
                    QToolTip.showText(main_plot.mapToGlobal(pos.toPoint()), tooltip_text, main_plot)
                else:
                    main_plot.setTitle("")  # 如果超出范围，清空标题
                    QToolTip.hideText()  # 隐藏提示框
            else:
                main_plot.setTitle("")  # 如果不在图表范围内，清空标题
                QToolTip.hideText()  # 隐藏提示框
                
        main_plot.proxy = SignalProxy(main_plot.scene().sigMouseMoved, rateLimit=60, slot=on_mouse_hover)
    else:
        # 取消勾选时，移除或禁用 SignalProxy
        if hasattr(main_plot, 'proxy'):
            main_plot.proxy.disconnect()  # 断开信号连接
            del main_plot.proxy  # 删除 SignalProxy 对象
        main_plot.setTitle("")  # 清空标题
        QToolTip.hideText()  # 隐藏提示框