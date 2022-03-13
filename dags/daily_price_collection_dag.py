## python
import os
from datetime import datetime, timedelta
import pendulum

## airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

## python scripts
import econ.tda_collection as tda
import common.scripts.utils as utils
import derived_metrics.market_health as health

## get global vars
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
sm_data_lake_dir = Variable.get("sm_data_lake_dir")
AVG_PRICE_DIR = sm_data_lake_dir+'/derived-measurements/avg-price-vol'
DIRECTION_DIR = sm_data_lake_dir+'/derived-measurements/market-direction'
DIRECTION_BUFFER = sm_data_lake_dir+'/buffer/der-market-direction'

local_tz = pendulum.timezone("US/Eastern")
default_args = {
    'owner': 'alex',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 21, tzinfo=local_tz),
    'email': ['alexhubbard89@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=5)
}

## make DAG tree
dag = DAG(dag_id='tda-daily-price-collection',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 2 * * 2-6") ## weekdays at 2am

tda_daily_price_collection= SparkSubmitOperator(
    task_id='tda_daily_price_collection',
    application=f'{pyspark_app_home}/dags/econ/tda_collection.py',
    executor_memory='15g',
    driver_memory='15g',
    name='tda_daily_price_collection',
    execution_timeout=timedelta(minutes=150),
    conf={'master':'spark://localhost:7077'},
    dag=dag
)

############################
##### Market Direction #####
############################
move_raw_files_pv = PythonOperator(
    task_id='move_raw_files_pv',
    python_callable=utils.move_files,
    dag=dag,
    op_kwargs={
        'data_loc': AVG_PRICE_DIR,
        'date': '{{execution_date}}',
        'days': 365,
        'buffer_loc': DIRECTION_BUFFER+'/raw'
    }
)
prepare_market_direction= SparkSubmitOperator(
    task_id='prepare_market_direction',
    application=f'{pyspark_app_home}/dags/derived_metrics/runner/prepare_market_direction.py',
    executor_memory='15g',
    driver_memory='15g',
    name='prepare_market_direction',
    execution_timeout=timedelta(minutes=15),
    conf={'master':'spark://localhost:7077'},
    dag=dag
)
market_direction_rename = PythonOperator(
    task_id='market_direction_rename' ,
    python_callable=utils.rename_file,
    dag=dag,
    op_kwargs={
        'path': DIRECTION_DIR+'/',
        'fn': 'data'
    }
)
make_market_direction = PythonOperator(
    task_id='make_market_direction' ,
    python_callable=health.make_direction,
    dag=dag,
)


[
    tda_daily_price_collection >> 
    move_raw_files_pv >> 
    prepare_market_direction >>
    market_direction_rename >> 
    make_market_direction
]
