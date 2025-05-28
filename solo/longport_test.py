# Get Basic Information Of Securities
# https://open.longportapp.com/docs/quote/pull/static
# Before running, please visit the "Developers to ensure that the account has the correct quotes authority.
# If you do not have the quotes authority, you can enter "Me - My Quotes - Store" to purchase the authority through the "LongPort" mobile app.
from longport.openapi import QuoteContext, Config, OpenApiException
import logging

logging.basicConfig(level=logging.DEBUG)

config = Config.from_env()

try:
    ctx = QuoteContext(config)  # Increase timeout to 30 seconds
    resp = ctx.static_info(["700.HK", "AAPL.US", "TSLA.US", "NFLX.US"])
    print(resp)
except OpenApiException as e:
    print(f"Failed to connect to LongPort API: {e}")
    print("Please check your network connection, API server status, or token configuration.")