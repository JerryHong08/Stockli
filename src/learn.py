import yfinance as yf


def main():
    ticker = yf.Ticker("AAPL")
    df = ticker.history(raise_errors=True)
    print(df)


if __name__ == "__main__":
    main()