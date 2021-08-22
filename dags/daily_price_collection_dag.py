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
dag = DAG(dag_id='tda-daily-price-collection',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 6 * * 2-6") ## weekdays at 6am

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

[tda_daily_price_collection]
