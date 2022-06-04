import pandas as pd
import os
from pyspark.sql import SparkSession
from common.spoof import Ip_Spoofer
import common.utils as utils

RATINGS_COALESCE_DIR = os.environ["DL_DIR"] + "/benzinga-rating-changes-coalesce"


if __name__ == "__main__":
    spark = SparkSession.builder.appName("migrate-rating").getOrCreate()
    ratings_df = (
        spark.read.format("orc")
        .option("path", RATINGS_COALESCE_DIR + "/*")
        .load()
        .toPandas()
    )
    spark.stop()
    ratings_df["date"] = pd.to_datetime(ratings_df["date"]).apply(lambda r: r.date())
    ratings_df.to_parquet(f"{RATINGS_COALESCE_DIR}/data.parquet", index=False)
