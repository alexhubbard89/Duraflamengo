import pandas as pd
import datetime as dt
import derived_metrics.ml_signals as ml_signals
import os

if __name__ == "__main__":
    ds = pd.to_datetime(os.environ["ds"]).date()
    ml_signals.join_ticker_signals(ds, yesterday=True)
