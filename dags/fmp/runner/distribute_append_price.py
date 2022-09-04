import pandas as pd
from fmp import stocks
import common.utils as utils
import os

if __name__ == "__main__":
    ds = pd.to_datetime(os.environ["ds"]).tz_convert("US/Eastern").date()

    yesterday = os.environ["yesterday"]
    stocks.distribute_append_price(ds, yesterday)
