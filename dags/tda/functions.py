import pandas as pd
import requests
import datetime as dt
import os
import tda.settings as s


def check_token_expiration() -> bool:
    """
    Check if Beaer token is expired.
    Datetime now is not greater than the time
    the token was created + 30 minutes.
    """
    token_df = pd.read_parquet(os.environ["TDA_KEYS"])
    return (token_df.loc[0, "requested"] + dt.timedelta(minutes=30)) < dt.datetime.now()


def get_new_tokens():
    """
    Using the refresh token, get a new
    bearer and refresh token.
    """
    token_df = pd.read_parquet(os.environ["TDA_KEYS"])
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    payload = {
        "grant_type": "refresh_token",
        "access_type": "offline",
        "refresh_token": token_df.loc[0, "refresh_token"],
        "client_id": s.client_id,
    }
    authReply = requests.post(s.TOKEN_URL, headers=headers, data=payload)
    new_token = authReply.json()
    new_token["requested"] = dt.datetime.now()
    new_token_df = pd.DataFrame([new_token])
    new_token_df.to_parquet(os.environ["TDA_KEYS"])


def get_bearer() -> str:
    """
    If the token is expired then update.
    Grab the token and return.
    """
    if check_token_expiration():
        get_new_tokens()
    token_df = pd.read_parquet(os.environ["TDA_KEYS"])
    return token_df.loc[0, "access_token"]


def get_watchlist():
    bearer_token = get_bearer()
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    r = requests.get(s.WATCHLIST_URL, headers=headers)
    return r.json()


def collect_watchlist():
    """
    Check if watchlist has been updated
    and keep the most recent version.
    If no update, do nothing.
    """
    ## get current lists
    watchlist = get_watchlist()
    watchlist_df = pd.DataFrame()
    for i in range(len(watchlist)):
        for wli in watchlist[i]["watchlistItems"]:
            index_ = len(watchlist_df)
            watchlist_df.loc[index_, "list_name"] = watchlist[i]["name"]
            watchlist_df.loc[index_, "symbol"] = wli["instrument"]["symbol"]
    ## get old list and compare
    old_list_df = pd.read_parquet(s.MY_WATCHLIST_LATEST)
    try:
        pd.testing.assert_frame_equal(old_list_df, watchlist_df)
    except AssertionError:
        ## DataFrames are different
        now_time = str(dt.datetime.now()).split(".")[0].replace(":", " ").split(" ")
        fn = "{}H{}M{}S{}".format(now_time[0], now_time[1], now_time[2], now_time[3])
        watchlist_df.to_parquet(s.MY_WATCHLIST + f"/{fn}.parquet", index=False)
        watchlist_df.to_parquet(s.MY_WATCHLIST_LATEST, index=False)
