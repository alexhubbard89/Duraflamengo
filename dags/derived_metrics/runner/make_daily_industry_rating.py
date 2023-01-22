import pandas as pd
import datetime as dt
import fmp.settings as s
import derived_metrics.settings as dm_s
import common.utils as utils
import os
from pyspark.sql import SparkSession


if __name__ == "__main__":
    ds = pd.to_datetime(os.environ["ds"]).tz_convert("US/Eastern").date()
    ## always work for yesterdays data, no price data for today
    ds = ds - dt.timedelta(1)
    if os.path.isdir(f"{s.historical_rating}/{ds}/"):
        ## get industry and sector
        company_profile_df = utils.read_many_parquet(s.company_profile_ticker + "/")
        company_profile_df.loc[~company_profile_df["sector"].isin(["", "N/A"])]
        ## load ratings for the date
        spark = SparkSession.builder.appName("read-api").getOrCreate()
        df = (
            spark.read.format("parquet")
            .option("path", f"{s.historical_rating}/{ds}/")
            .load()
            .toPandas()
        )
        df["rating_int"] = df["rating"].map(dm_s.rating_map)
        ## join industry and sector
        rating_industry_df = df[["symbol", "date", "rating_int"]].merge(
            company_profile_df[["symbol", "industry"]],
            how="inner",
            on="symbol",
        )
        if len(rating_industry_df) > 0:
            ## aggregate
            industry_agg = (
                rating_industry_df.groupby(["industry", "date"])["rating_int"]
                .describe()
                .reset_index()
            )
            ## type datasets
            industry_agg = utils.format_data(industry_agg, dm_s.industry_rating_types)
            ## write
            industry_agg.to_parquet(f"{dm_s.industry_rating}/{ds}.parquet", index=False)
