import common.generic as generic
import pandas as pd
import datetime as dt
import os
import fmp.settings as fmp_s
import common.utils as utils
from io import StringIO
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_date, date_add, to_date
import psycopg2
from common.utils import write_to_postgres


FMP_KEY = os.environ["FMP_KEY"]


def collect_mxm_price(ds: dt.date):
    """
    Collect minute by minute price for watchlist, using parallelization.
    """
    config_ = {
        "add_ticker": True,
        "url": fmp_s.ONE_MINUTE_INTRADAY_PRICE,
        "dl_ticker_dir": fmp_s.ticker_minute_price,
        "dtypes": fmp_s.intraday_price_types,
        "end_date": ds,
    }
    generic.collect_generic_distributed(
        distribute_through=generic.collect_generic_ticker,
        spark_app="mxm-collection",
        **config_,
    )


# Fetch maximum datetime for each symbol from database
def get_max_datetime(ds):
    with utils.get_db_connection() as conn:
        query = f"""
        SELECT symbol, MAX(date) as max_datetime
        FROM symbol_minute_price
        WHERE DATE(date) = '{ds}'
        GROUP BY symbol;
        """
        data = pd.read_sql(query, conn)
    return data if not data.empty else pd.DataFrame(columns=["symbol", "max_datetime"])


# Prepare dataset for processing
def prepare_base_dataset(ds):
    collected_symbols = utils.find_symbols_collected(fmp_s.ticker_minute_price)
    watchlist_symbols = utils.get_distinct_watchlist_symbols('Options Watchlist')
    migrate_symbols = list(set(collected_symbols) & set(watchlist_symbols))

    max_dates_df = get_max_datetime(ds)
    tickers_df = pd.DataFrame(migrate_symbols, columns=["symbol"]).drop_duplicates()
    return tickers_df.merge(max_dates_df, how="left", on=["symbol"]).fillna(
        dt.datetime(ds.year, ds.month, ds.day, 9, 30)
    )


# Load and filter data
def load_and_filter_data(symbol, max_date):
    file_path = f"{fmp_s.ticker_minute_price}/{symbol}.parquet"
    try:
        df = pd.read_parquet(file_path)
        df = df.loc[df["date"].apply(lambda r: r.date()) == pd.to_datetime(max_date).date()] # Slice for only date in question to avoid loading too much data
    except Exception as e:
        print(f"Failed to load data for {symbol}: {str(e)}")
        return pd.DataFrame()  # Return an empty DataFrame on failure to load

    if max_date and "date" in df.columns:
        df = df.loc[df["date"] > max_date]

    for col in ["open", "low", "high", "close"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda r: round(r, 2))

    if df.empty:
        return df

    return df.drop_duplicates(["symbol", "date"])


# Collect new data for all symbols
def get_data_to_ship(ds):
    # Create a Spark session
    spark = (
        SparkSession.builder.appName("Prep mXm migration")
        .config("spark.ui.port", "4050")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )

    df = prepare_base_dataset(ds)
    collection_plan = [list(x) for x in df.values]

    sc = spark.sparkContext
    results = (
        sc.parallelize(collection_plan)
        .map(lambda r: load_and_filter_data(r[0], r[1]))
        .collect()
    )
    return pd.concat(results, ignore_index=True)


# Main execution function
def migrate_data(ds):
    spark = (
        SparkSession.builder.appName("mXm migration")
        .config("spark.ui.port", "4050")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )
    pd_df = get_data_to_ship(ds)
    if not pd_df.empty:
        data_to_ship = spark.createDataFrame(pd_df)
        utils.write_to_postgres(
            df=data_to_ship, table_name="symbol_minute_price", mode="append"
        )
