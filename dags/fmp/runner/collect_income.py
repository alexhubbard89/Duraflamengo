import pandas as pd
from fmp import stocks
import datetime as dt
import fmp.settings as s
import common.utils as utils
import os

if __name__ == "__main__":
    ds = pd.to_datetime(os.environ["collect_income_ds"]).tz_convert("US/Eastern").date()
    stocks.collect_income(ds)
