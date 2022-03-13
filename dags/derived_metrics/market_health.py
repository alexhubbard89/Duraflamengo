import pandas as pd
import numpy as np
import datetime as dt

## Local code
import common.scripts.utils as utils

## spark
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf

from airflow.models import Variable
## Set location variables
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
sm_data_lake_dir = Variable.get("sm_data_lake_dir")
AVG_PRICE_DIR = sm_data_lake_dir+'/derived-measurements/avg-price-vol'
DIRECTION_DIR = sm_data_lake_dir+'/derived-measurements/market-direction'
DIRECTION_BUFFER = sm_data_lake_dir+'/buffer/der-market-direction'

def prepare_market_direction():
    spark = (
        SparkSession
        .builder 
        .appName('market-direction') 
        .getOrCreate()
    )
    schema = (
        T.StructType(
            [
                T.StructField('ticker', T.StringType(), True),
                T.StructField('date', T.DateType(), True),
                T.StructField('close', T.FloatType(), True),
                T.StructField('avg_price_5', T.FloatType(), True),
                T.StructField('avg_price_10', T.FloatType(), True),
                T.StructField('avg_price_50', T.FloatType(), True),
                T.StructField('avg_price_200', T.FloatType(), True),
                T.StructField('avg_volume_5', T.FloatType(), True),
                T.StructField('avg_volume_10', T.FloatType(), True)
            ]
        )
    )
    price_df = (
        spark
        .read
        .format("orc")
        .option("path", DIRECTION_BUFFER+'/raw/*')
        .schema(schema)
        .load()
        .repartition(1) ## do not partition
        .write
        .format("csv")
        .mode("overwrite")
        .save(DIRECTION_DIR, header=True)
    )
    _ = spark.stop()
    
def make_direction():
    df = pd.read_csv(DIRECTION_DIR+'/data.csv')
    df['date'] = (
        pd.to_datetime(df['date'])
        .apply(lambda r: r.date())
    )
    ## make compare 200
    df['compare_200'] = (df['close'] - df['avg_price_200']) / df['avg_price_200']
    df_agg_200 = (
        df
        .groupby('date')['compare_200']
        .agg(['mean', 'std', 'count', utils.skewed_simga])
        .rename(columns={
            'mean': 'mean_200',
            'std': 'std_200',
            'count': 'count_200',
            'skewed_simga': 'skewed_sigma_200'
        })
    )
    df_agg_200['ul_sigma_200'] = df_agg_200['skewed_sigma_200'].apply(lambda r: r[0])
    df_agg_200['ll_sigma_200'] = df_agg_200['skewed_sigma_200'].apply(lambda r: r[1])
    df_agg_200['ul_mean_200'] = df_agg_200['skewed_sigma_200'].apply(lambda r: r[2])
    df_agg_200['ll_mean_200'] = df_agg_200['skewed_sigma_200'].apply(lambda r: r[3])
    df_agg_200['ul_ul_200'] = df_agg_200['ul_mean_200'] + df_agg_200['ul_sigma_200']
    df_agg_200['ll_ll_200'] = df_agg_200['ll_mean_200'] - df_agg_200['ll_sigma_200']
    
    ## make compare 50
    ## the name on the udf is compare_200, fix later
    df['compare_200'] = (df['close'] - df['avg_price_50']) / df['avg_price_50']
    df_agg_50 = (
        df
        .groupby('date')['compare_200']
        .agg(['mean', 'std', 'count', utils.skewed_simga])
        .rename(columns={
            'mean': 'mean_50',
            'std': 'std_50',
            'count': 'count_50',
            'skewed_simga': 'skewed_sigma_50'
        })
    )
    df_agg_50['ul_sigma_50'] = df_agg_50['skewed_sigma_50'].apply(lambda r: r[0])
    df_agg_50['ll_sigma_50'] = df_agg_50['skewed_sigma_50'].apply(lambda r: r[1])
    df_agg_50['ul_mean_50'] = df_agg_50['skewed_sigma_50'].apply(lambda r: r[2])
    df_agg_50['ll_mean_50'] = df_agg_50['skewed_sigma_50'].apply(lambda r: r[3])
    df_agg_50['ul_ul_50'] = df_agg_50['ul_mean_50'] + df_agg_50['ul_sigma_50']
    df_agg_50['ll_ll_50'] = df_agg_50['ll_mean_50'] - df_agg_50['ll_sigma_50']
    
    df_agg = df_agg_200.join(df_agg_50).reset_index()
    df_agg.to_parquet(DIRECTION_DIR+'/data.parquet', index=False)