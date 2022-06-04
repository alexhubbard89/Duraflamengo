import pandas as pd
import os
import datetime as dt
from common.spoof import Ip_Spoofer

from airflow.models import Variable

sm_data_lake_dir = Variable.get("sm_data_lake_dir")
VIX_DIR = sm_data_lake_dir + "/volatility_index"


def get_historical():
    url = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
    spoof = Ip_Spoofer()
    raw_data = str(Ip_Spoofer.request_page(spoof, url, collection_type="csv")).split(
        "\n"
    )

    ## format
    cols = [x.lower() for x in raw_data[0].split(",")]
    df = pd.DataFrame([x.split(",") for x in raw_data[1:]], columns=cols)
    df = df.loc[df["date"] != ""]
    df["date"] = pd.to_datetime(df["date"]).apply(lambda r: r.date())
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    ## save
    df.to_parquet(VIX_DIR + "/historical/vix.parquet", index=False)


def get_current():
    url = "https://finance.yahoo.com/quote/%5EVIX/"
    spoof = Ip_Spoofer()
    page = Ip_Spoofer.request_page(spoof, url)
    v = float(
        [x for x in page.findAll("fin-streamer") if "^VIX" in x.get("data-symbol")][
            0
        ].get("value")
    )

    now = dt.datetime.now()
    today = now.date()
    dir_ = VIX_DIR + "/current/" + str(today)
    if not os.path.isdir(dir_):
        os.mkdir(dir_)
        todays_df = pd.DataFrame([[now, v]], columns=["timestamp", "close"])
    else:
        todays_df = pd.read_csv(dir_ + "/data.csv").append(
            pd.DataFrame([[now, v]], columns=["timestamp", "close"]), ignore_index=True
        )
    todays_df.to_csv(dir_ + "/data.csv", index=False)
