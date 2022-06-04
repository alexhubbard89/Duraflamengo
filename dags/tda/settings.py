import datetime as dt
import os

## en var
accountID = os.environ["TDA_ACCOUNT"]
refresh_token = os.environ["TDA_REFRESH"]
client_id = os.environ["TDA_API_KEY"]

## paths
DL_DIR = os.environ["DL_DIR"]

## urls
TDA = "https://api.tdameritrade.com/v1"
TOKEN_URL = f"{TDA}/oauth2/token"
WATCHLIST_URL = f"{TDA}/accounts/{accountID}/watchlists"

## types
_types = {"nothing": "yet"}
