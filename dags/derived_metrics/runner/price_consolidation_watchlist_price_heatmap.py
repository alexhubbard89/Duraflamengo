import common.utils as utils
from derived_metrics.price_consolidation import price_heatmap
from pyspark.sql import SparkSession

if __name__ == "__main__":
    watch_list = utils.get_watchlist()

    spark = SparkSession.builder.appName(
        f"price-consolidation-watchlist-price-heatmap"
    ).getOrCreate()
    sc = spark.sparkContext
    _ = sc.parallelize(watch_list).map(price_heatmap).collect()
    spark.stop()
