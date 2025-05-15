import pyqtgraph as pg
import numpy as np
from PySide6.QtWidgets import QGraphicsRectItem, QToolTip
from PySide6.QtCore import Qt, QRectF, QPointF
from pyqtgraph import SignalProxy
import pandas as pd

def plot_candlestick(main_plot, df, enable_hover, auto_range=True):
    if df.empty:
        return
    
    main_plot.clear()
    
    x = np.arange(len(df))

    axis = pg.DateAxisItem(orientation='bottom')
    main_plot.setAxisItems({'bottom': axis})

    for i in range(len(df)):
        open_price = df["Open"].iloc[i]
        close_price = df["Close"].iloc[i]
        high = df["High"].iloc[i]
        low = df["Low"].iloc[i]

        color = 'g' if close_price >= open_price else 'r'
        body = QGraphicsRectItem(QRectF(x[i] - 0.2, min(open_price, close_price), 0.4, abs(close_price - open_price)))
        body.setBrush(pg.mkBrush(color))
        body.setPen(pg.mkPen(color))
        main_plot.addItem(body)

        shadow = pg.PlotCurveItem([x[i], x[i]], [low, high], pen=pg.mkPen(color))
        main_plot.addItem(shadow)

    main_plot.setLabel('bottom', 'Date')
    main_plot.setLabel('left', 'Price')

    if auto_range:
        main_plot.getPlotItem().vb.autoRange()

    ticks = [(x[i], df["Date"].iloc[i].strftime('%Y%m%d')) for i in range(0, len(df), 5)]
    axis.setTicks([ticks]) 
    
    if enable_hover:
        print("Hover enabled")
        def on_mouse_hover(event):
            pos = event[0]
            if main_plot.sceneBoundingRect().contains(pos):
                mouse_point = main_plot.getViewBox().mapSceneToView(pos)
                index = int(mouse_point.x())
                if 0 <= index < len(df):
                    date = df["Date"].iloc[index]
                    open_price = df["Open"].iloc[index]
                    close_price = df["Close"].iloc[index]
                    high = df["High"].iloc[index]
                    low = df["Low"].iloc[index]
                    volume = df["Volume"].iloc[index]
                    
                    main_plot.setTitle(
                        f"Date: {date}\n"
                        f"Open: {open_price:.2f}, Close: {close_price:.2f}\n"
                        f"High: {high:.2f}, Low: {low:.2f}\n"
                        f"Volume: {volume:,}"
                    )
                    
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
                    main_plot.setTitle("")
                    QToolTip.hideText()
            else:
                main_plot.setTitle("")
                QToolTip.hideText()
                
        main_plot.proxy = SignalProxy(main_plot.scene().sigMouseMoved, rateLimit=60, slot=on_mouse_hover)
    else:
        if hasattr(main_plot, 'proxy'):
            main_plot.proxy.disconnect()
            del main_plot.proxy
        main_plot.setTitle("")
        QToolTip.hideText()

def plot_volume(subplot, df):
    subplot.clear()
    x = np.arange(len(df))
    volume_bars = pg.BarGraphItem(x=x, height=df["Volume"], width=0.4, brush='b')
    subplot.addItem(volume_bars)
    subplot.setLabel('left', 'Volume')
    subplot.enableAutoRange('y', True)
    subplot.getAxis('bottom').setStyle(showValues=False)

def calculate_obv(df):
    # Assuming this function calculates OBV correctly based on your DataFrame
    # Replace with your actual OBV calculation logic if different
    obv = [0]  # Starting value
    for i in range(1, len(df)):
        if df["Close"].iloc[i] > df["Close"].iloc[i - 1]:
            obv.append(obv[-1] + df["Volume"].iloc[i])
        elif df["Close"].iloc[i] < df["Close"].iloc[i - 1]:
            obv.append(obv[-1] - df["Volume"].iloc[i])
        else:
            obv.append(obv[-1])
    return pd.Series(obv, index=df.index)

def plot_obv(subplot, df):
    subplot.clear()
    obv = calculate_obv(df)
    x = np.arange(len(df))
    obv_line = pg.PlotCurveItem(x, obv.values, pen='b')  # Convert Series to NumPy array
    subplot.addItem(obv_line)
    subplot.setLabel('left', 'OBV')
    subplot.enableAutoRange('y', True)
    subplot.getAxis('bottom').setStyle(showValues=False)