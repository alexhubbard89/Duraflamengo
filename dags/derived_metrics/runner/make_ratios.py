import pandas as pd
import datetime as dt
import derived_metrics.fundamentals as fundamentals
import os

if __name__ == "__main__":
    ds = pd.to_datetime(os.environ["der_ratios_ds"]).tz_convert("US/Eastern").date()
    ## always work for yesterdays data, no price data for today
    ds = ds - dt.timedelta(1)
    fundamentals.make_ratios(ds)
