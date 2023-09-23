import pandas as pd
import os
import tda.collect as collect

if __name__ == "__main__":
    ds = pd.to_datetime(os.environ["ds"]).tz_convert("US/Eastern").date()
    collect.distribute_options(ds)
