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

## python scripts
import common.scripts.utils as utils

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
          schedule_interval="@daily"
          # Turn this on after you properly configure 
          # spark resources
          # concurrency=2,
          # max_active_runs=2,
          )

market_watch_collection = SparkSubmitOperator(
    task_id='market_watch_collection',
    application=f'{pyspark_app_home}/dags/econ/analyst_opinion.py',
    executor_memory='15g',
    driver_memory='15g',
    name='market_watch_collection',
    execution_timeout=timedelta(minutes=120),
    conf={'master':'spark://localhost:7077'},
    dag=dag,
)

benzinga_collection = SparkSubmitOperator(
    task_id='benzinga_collection',
    application=f'{pyspark_app_home}/dags/econ/runner/collect_benzinga.py',
    executor_memory='15g',
    driver_memory='15g',
    name='benzinga_collection',
    execution_timeout=timedelta(minutes=120),
    conf={'master':'spark://localhost:7077'},
    dag=dag,
)

benzinga_migration = SparkSubmitOperator(
    task_id='benzinga_migration',
    application=f'{pyspark_app_home}/dags/econ/runner/migrate_benzinga.py',
    executor_memory='15g',
    driver_memory='15g',
    name='benzinga_migration',
    execution_timeout=timedelta(minutes=30),
    conf={'master':'spark://localhost:7077'},
    dag=dag,
)

benzinga_clear_buffer = PythonOperator(
    task_id='benzinga_clear_buffer' ,
    python_callable=utils.clear_buffer,
    op_kwargs={'subdir': 'target-price-benzinga'},
    dag=dag,
)

[
    market_watch_collection, 
    [benzinga_collection >> benzinga_migration >> benzinga_clear_buffer]
]
