## python scripts
from derived_metrics import technical_analysis as ta
import pandas as pd
import datetime as dt
import os

if __name__ == "__main__":
    ds = pd.to_datetime(os.environ["ds"]).tz_convert("US/Eastern").date()
    ta.enrich_watchlist(ds)
