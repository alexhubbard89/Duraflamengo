"""
TO DOs:
- Add arguments for sm data lake directory
- Fire off every 2 hours
- Deduplicate data
- Find tickers that were unsuccessfully collected and retry
"""

## python
import os
from datetime import datetime, timedelta
import pendulum

## airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

## get global vars
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
sm_data_lake_dir = Variable.get("sm_data_lake_dir")
local_tz = pendulum.timezone("US/Eastern")
default_args = {
    'owner': 'alex',
    'depends_on_past': False,
    'start_date': datetime(2021, 6, 5, tzinfo=local_tz),
    'email': ['alexhubbard89@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


## make DAG tree
dag = DAG(dag_id='analyst-estimates',
          default_args=default_args,
          catchup=False,
          schedule_interval="@daily")

market_watch_collection= SparkSubmitOperator(
    task_id='market_watch_collection',
    application=f'{pyspark_app_home}/dags/econ/analyst_opinion.py',
    total_executor_cores=4,
    executor_cores=2,
    executor_memory='5g',
    driver_memory='5g',
    name='market_watch_collection',
    execution_timeout=timedelta(minutes=10),
    conf={'master':'spark://localhost:7077'},
    dag=dag
)

market_watch_collection
