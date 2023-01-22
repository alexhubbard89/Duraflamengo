import pandas as pd
import datetime as dt
import derived_metrics.fundamentals as fundamentals
import os

if __name__ == "__main__":
    ds = pd.to_datetime(os.environ["ds"]).date()
    fundamentals.distribute_append_price(ds, yesterday=True)
