import pandas as pd
import derived_metrics.technicals as technicals
import os

if __name__ == "__main__":
    ds = pd.to_datetime(os.environ["ds"]).date()
    yesterday = os.environ["yesterday"]
    technicals.distribute_sr_creation(ds=ds, yesterday=yesterday)
