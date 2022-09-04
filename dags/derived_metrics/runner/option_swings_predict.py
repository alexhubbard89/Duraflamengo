import pandas as pd
import derived_metrics.option_swings as option_swings
import os

if __name__ == "__main__":
    ds = pd.to_datetime(os.environ["ds"]).date()
    yesterday = os.environ["yesterday"]
    option_swings.predict_swing(ds=ds, yesterday=yesterday)
