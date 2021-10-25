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
    'start_date': datetime(2021, 10, 26, tzinfo=local_tz),
    'email': ['alexhubbard89@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


## make DAG tree
dag = DAG(dag_id='earnings-reports',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 12 * * 1-5"
          ## “At 12:00PM on every day-of-week from Monday through Friday.”
          )

## Get earnings reports dates
earnings_reports_collection = SparkSubmitOperator(
    task_id='earnings_reports_collection',
    application=f'{pyspark_app_home}/dags/econ/earnings_dates.py',
    executor_memory='15g',
    driver_memory='15g',
    name='earnings_reports_collection',
    execution_timeout=timedelta(minutes=30),
    conf={'master':'spark://localhost:7077'},
    dag=dag,
)

[earnings_reports_collection]