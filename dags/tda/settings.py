import datetime as dt
import os

## en var
accountID = os.environ["TDA_ACCOUNT"]
client_id = os.environ["TDA_API_KEY"]

## paths
DL_DIR = os.environ["DL_DIR"]
MY_WATCHLIST = DL_DIR + "/watchlist/alex"
MY_WATCHLIST_LATEST = MY_WATCHLIST + "/latest.parquet"
OPTIONS = DL_DIR + "/tda/options"
OPTIONS_SINGLE = DL_DIR + "/tda-options-single"
OPTIONS_ANALYTICAL = DL_DIR + "/tda-options-analytical"
OPTIONS_ANALYTICAL_NEW = DL_DIR + "/tda-options/analytical"

## urls
TDA = "https://api.tdameritrade.com/v1"
TOKEN_URL = f"{TDA}/oauth2/token"
WATCHLIST_URL = f"{TDA}/accounts/{accountID}/watchlists"
OPTIONS_URL = TDA + "/marketdata/chains?apikey={API}&symbol={ticker}&contractType=ALL"
ANALYTICAL_OPTIONS_URL = (
    TDA
    + "/marketdata/chains?apikey="
    + client_id
    + "&symbol={ticker}&contractType=ALL&range=ALL&strategy=ANALYTICAL"
)

## types
options_types = {
    "putCall": str,
    "symbol": str,
    "description": str,
    "exchangeName": str,
    "bid": float,
    "ask": float,
    "last": float,
    "mark": float,
    "bidSize": float,
    "askSize": float,
    "bidAskSize": str,
    "lastSize": float,
    "highPrice": float,
    "lowPrice": float,
    "openPrice": float,
    "closePrice": float,
    "totalVolume": float,
    "tradeDate": str,
    "tradeTimeInLong": float,
    "quoteTimeInLong": float,
    "netChange": float,
    "top_level_volatility": float,
    "volatility": float,
    "delta": float,
    "gamma": float,
    "theta": float,
    "vega": float,
    "rho": float,
    "openInterest": float,
    "timeValue": float,
    "theoreticalOptionValue": float,
    "theoreticalVolatility": float,
    "optionDeliverablesList": str,
    "strikePrice": float,
    "expirationDate": str,
    "daysToExpiration": float,
    "expirationType": str,
    "lastTradingDay": float,
    "multiplier": float,
    "settlementType": str,
    "deliverableNote": str,
    "isIndexOption": float,
    "percentChange": float,
    "markChange": float,
    "markPercentChange": float,
    "intrinsicValue": float,
    "pennyPilot": bool,
    "mini": bool,
    "inTheMoney": bool,
    "nonStandard": bool,
    "collected": dt.datetime,
    "interestRate": float,
    "underlyingPrice": float,
}
