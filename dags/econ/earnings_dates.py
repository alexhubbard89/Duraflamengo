import yahoo_fin.stock_info as si
import pandas as pd

## spark
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf

from airflow.models import Variable
sm_data_lake_dir = Variable.get("sm_data_lake_dir")
WRITE_PATH = sm_data_lake_dir + '/earnings-report-dates/data.csv'

def get_earnings(ticker: str) -> pd.DataFrame:
    """
    Get earnings dates with numbers.
    """
    try:
        data = si.get_earnings_history(ticker)
    except:
        return pd.DataFrame()
    return pd.DataFrame(data)

def earnings_collection() -> bool:
    """
    Get earnings dates and reported 
    numbers for all tickers. Bring the
    data together and save as a large file.
    """
    ## get ticker list
    ticker_file = sm_data_lake_dir + '/seed-data/nasdaq_screener_1628807233734.csv'
    ticker_df = pd.read_csv(ticker_file)
    all_ticker_list = ticker_df['Symbol'].tolist()

    ## start spark session
    spark = (
        SparkSession
        .builder 
        .appName('earnings-dates') 
        .getOrCreate()
    )
    sc = spark.sparkContext
    
    ## make requests
    earnings_df_list = (
        sc
        .parallelize(all_ticker_list)
        .map(get_earnings)
        .collect()
    )
    full_earnings_df = pd.concat([x for x in earnings_df_list]).reset_index(drop=True)
    _ = full_earnings_df.to_csv(WRITE_PATH, index=False)
    return True

if __name__ == "__main__":
    _ = earnings_collection()