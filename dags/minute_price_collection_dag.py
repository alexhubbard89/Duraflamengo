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

## get global vars
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
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
dag = DAG(dag_id='tda-minute-by-minute-price-collection',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 6 * * 7") ## sundays at 6am

tda_minute_make_buffer = PythonOperator(
    task_id='tda_minute_make_buffer' ,
    python_callable=tda.make_minute_buffer,
    dag=dag,
)

collect_tda_minute_price = SparkSubmitOperator(
    task_id='collect_tda_minute_price',
    application=f'{pyspark_app_home}/dags/econ/runner/collect_tda_minute.py',
    executor_memory='15g',
    driver_memory='15g',
    name='collect_tda_minute_price',
    execution_timeout=timedelta(minutes=150),
    conf={'master':'spark://localhost:7077'},
    dag=dag,
)

migrate_tda_minute_price = SparkSubmitOperator(
    task_id='migrate_tda_minute_price',
    application=f'{pyspark_app_home}/dags/econ/runner/migrate_tda_minute.py',
    executor_memory='15g',
    driver_memory='15g',
    name='migrate_tda_minute_price',
    execution_timeout=timedelta(minutes=150),
    conf={'master':'spark://localhost:7077'},
    dag=dag,
)

clear_buffer_tda_minute = PythonOperator(
    task_id='clear_buffer_tda_minute' ,
    python_callable=utils.clear_buffer,
    op_kwargs={'subdir': 'tda-minute-price'},
    dag=dag,
)

## DAG pipeline
[ 
    tda_minute_make_buffer >> 
    collect_tda_minute_price >> 
    migrate_tda_minute_price >>
    clear_buffer_tda_minute
]