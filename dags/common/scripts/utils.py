import shutil
import os
from airflow.models import Variable
sm_data_lake_dir = Variable.get("sm_data_lake_dir")
BUFFER_DIR = sm_data_lake_dir+'/buffer/{}/'
DL_WRITE_DIR = sm_data_lake_dir+'/{subdir}/{date}/'

def clear_buffer(subdir):
    print('clear buffer')
    _dir = BUFFER_DIR.format(subdir)
    shutil.rmtree(_dir)
    os.mkdir(_dir)
    return True

## functions
def write_spark(spark, df, subdir, date):
    file_path = DL_WRITE_DIR.format(subdir=subdir, date=str(date))
    try:
        df_sp = spark.createDataFrame(df)
    except:
        df_sp = df ## already in spark
    _ = (
        df_sp
        .write
        .format("orc")
        .mode("overwrite")
        .option("compression", "snappy")
        .save(file_path)
    )
    return True