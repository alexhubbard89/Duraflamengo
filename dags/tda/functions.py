import pandas as pd
import requests
import tda.settings as s


def get_tokens() -> dict:
    """
    Using the refresh token, get a new
    bearer and refresh token.
    """
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    payload = {
        "grant_type": "refresh_token",
        "access_type": "offline",
        "refresh_token": s.refresh_token,
        "client_id": s.client_id,
    }
    authReply = requests.post(s.TOKEN_URL, headers=headers, data=payload)
    return authReply.json()


def get_watchlist():
    tokens = get_tokens()
    headers = {"Authorization": "Bearer {}".format(tokens["access_token"])}
    r = requests.get(s.WATCHLIST_URL, headers=headers)
    return r.json()
