import common.utils as utils
from derived_metrics.price_consolidation import make_sparse
from pyspark.sql import SparkSession

if __name__ == "__main__":
    watch_list = utils.get_watchlist()

    spark = SparkSession.builder.appName(
        f"price-consolidation-watchlist-make-sparse"
    ).getOrCreate()
    sc = spark.sparkContext
    _ = sc.parallelize(watch_list).map(make_sparse).collect()
    spark.stop()
