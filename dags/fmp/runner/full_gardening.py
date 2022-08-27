import pandas as pd
import datetime as dt
import fmp.gardening as gardening
from fmp import stocks
import derived_metrics.stock_prices as pv
import fmp.settings as fmp_s
import common.utils as utils
import os
from pyspark.sql import SparkSession
import pandas_market_calendars as mcal

if __name__ == "__main__":
    ## find missing dates
    # find market dates
    nyse = mcal.get_calendar("NYSE")
    today = pd.to_datetime(os.environ["gardening_ds"]).tz_convert("US/Eastern").date()
    ds_start = today - dt.timedelta(days=180)
    operating_dates = [
        x.date() for x in nyse.valid_days(start_date=ds_start, end_date=today)
    ]

    # read collected data
    params = {}
    distribution_list = [
        f"{fmp_s.historical_daily_price_full}/{ds}/" for ds in operating_dates
    ]
    spark = SparkSession.builder.appName("read-files").getOrCreate()
    sc = spark.sparkContext
    dfs = (
        sc.parallelize(distribution_list)
        .map(lambda r: utils.read_protect_parquet(r, params))
        .collect()
    )
    sc.stop()
    spark.stop()
    price_df = pd.concat(dfs, ignore_index=True)
    daily_counts_df = (
        pd.DataFrame(price_df["date"].value_counts())
        .rename(columns={"date": "count"})
        .reset_index()
        .rename(columns={"index": "date"})
    )
    # find dates with little or no data
    missing_dates = [
        x
        for x in sorted(
            daily_counts_df.loc[daily_counts_df["count"] < 100, "date"].tolist()
        )
    ]
    ## Collect missing day
    # append prices to daily files
    for ds in missing_dates:
        stocks.collect_end_of_day_prices(ds, yesterday=False)
        stocks.distribute_append_price(ds, yesterday=False)
    ## Make price volume average for missing dates
    for ds in missing_dates:
        pv.make_pv(ds)
