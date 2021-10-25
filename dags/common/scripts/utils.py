from pyspark.sql import DataFrame
import shutil
import os
from airflow.models import Variable
sm_data_lake_dir = Variable.get("sm_data_lake_dir")
BUFFER_DIR = sm_data_lake_dir+'/buffer/{}/'
DL_WRITE_DIR = sm_data_lake_dir+'/{subdir}/{date}/'

## functions
def clear_buffer(subdir):
    print('clear buffer')
    _dir = BUFFER_DIR.format(subdir)
    shutil.rmtree(_dir)
    os.mkdir(_dir)
    return True

def write_spark(spark, df, subdir, date):
    file_path = DL_WRITE_DIR.format(subdir=subdir, date=str(date))
    if isinstance(df, DataFrame) == False:
        df_sp = spark.createDataFrame(df)
    else:
        df_sp = df
    _ = (
        df_sp
        .write
        .format("orc")
        .mode("overwrite")
        .option("compression", "snappy")
        .save(file_path)
    )
    return True

def is_between(time, time_range):
    if time_range[1] < time_range[0]:
        return time >= time_range[0] or time <= time_range[1]
    return time_range[0] <= time <= time_range[1]