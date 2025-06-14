from datetime import datetime, date
from longport.openapi import QuoteContext, Config, Period, AdjustType
                
def main():
    config = Config.from_env()
    ctx = QuoteContext(config)
    # Query 
    resp = ctx.candlesticks("TSLA.US", Period.Day, 1000, AdjustType.ForwardAdjust)
    print(resp)
    
if __name__ == "__main__":
    main()