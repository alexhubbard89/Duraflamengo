## python scripts
import econ.stockanalysis_statistics as sa
import pandas as pd
import glob
## spark
from pyspark.sql import SparkSession
from pyspark import SparkContext

if __name__ == "__main__":
    sm_data_lake_dir = '/Users/alexanderhubbard/stock-market/data'
    MB_BUFFER = sm_data_lake_dir+'/buffer/marketbeat-earnings'
    SA_BUFFER = sm_data_lake_dir+'/buffer/stock-analysis'
    ##### Get tickers to collect
    ue_df = pd.read_csv(MB_BUFFER+'/to-collect/data.csv')
    ticker_list = ue_df['ticker'].tolist()
    ## start spark session
    spark = (
        SparkSession
        .builder 
        .appName('stockanalysis-income-collection') 
        .getOrCreate()
    )
    sc = spark.sparkContext
    ##### Get tickers to collect
    buffer_tmp = SA_BUFFER+'/quarterly/income/*'
    collected_list = [x.split('/')[-1].split('.csv')[0] for x in glob.glob(buffer_tmp)]
    tickers_left = (
        list( 
            set(ticker_list) 
            - 
            set(collected_list)
        )
    )
    ## distribute requests
    ## make requests
    collected_results_list = (
        sc
        .parallelize(tickers_left)
        .map(lambda t: sa.get_SA_financials(t, 'income_statement'))
        .collect()
    )
    sc.stop()
    spark.stop()