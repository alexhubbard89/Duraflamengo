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
    'start_date': datetime(2021, 10, 25, tzinfo=local_tz),
    'email': ['alexhubbard89@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


## make DAG tree
dag = DAG(dag_id='marketwatch-financials',
          default_args=default_args,
          catchup=False,
          schedule_interval="30 19 * * 1-5"
          ## “At 19:30 on every day-of-week from Monday through Friday.”
          )

## balance sheets
collect_balance_sheet = SparkSubmitOperator(
    task_id='collect_balance_sheet',
    application=f'{pyspark_app_home}/dags/econ/runner/collect_balance_sheet.py',
    executor_memory='15g',
    driver_memory='15g',
    name='collect_balance_sheet',
    execution_timeout=timedelta(minutes=90),
    conf={'master':'spark://localhost:7077'},
    dag=dag,
)
migrage_balance_sheet = SparkSubmitOperator(
    task_id='migrage_balance_sheet',
    application=f'{pyspark_app_home}/dags/econ/runner/migrage_balance_sheet.py',
    executor_memory='15g',
    driver_memory='15g',
    name='migrage_balance_sheet',
    execution_timeout=timedelta(minutes=10),
    conf={'master':'spark://localhost:7077'},
    dag=dag,
)

## cash flow
collect_cash_flow = SparkSubmitOperator(
    task_id='collect_cash_flow',
    application=f'{pyspark_app_home}/dags/econ/runner/collect_cash_flow.py',
    executor_memory='15g',
    driver_memory='15g',
    name='collect_cash_flow',
    execution_timeout=timedelta(minutes=90),
    conf={'master':'spark://localhost:7077'},
    dag=dag,
)
migrage_cash_flow = SparkSubmitOperator(
    task_id='migrage_cash_flow',
    application=f'{pyspark_app_home}/dags/econ/runner/migrage_cash_flow.py',
    executor_memory='15g',
    driver_memory='15g',
    name='migrage_cash_flow',
    execution_timeout=timedelta(minutes=10),
    conf={'master':'spark://localhost:7077'},
    dag=dag,
)

## income
collect_income = SparkSubmitOperator(
    task_id='collect_income',
    application=f'{pyspark_app_home}/dags/econ/runner/collect_income.py',
    executor_memory='15g',
    driver_memory='15g',
    name='collect_income',
    execution_timeout=timedelta(minutes=90),
    conf={'master':'spark://localhost:7077'},
    dag=dag,
)
migrage_income = SparkSubmitOperator(
    task_id='migrage_income',
    application=f'{pyspark_app_home}/dags/econ/runner/migrage_income.py',
    executor_memory='15g',
    driver_memory='15g',
    name='migrage_income',
    execution_timeout=timedelta(minutes=10),
    conf={'master':'spark://localhost:7077'},
    dag=dag,
)

## No overlap on them.
## Colection followed by buffer to 
## give downtown on requests.
[   
    collect_balance_sheet >>
    migrage_balance_sheet >>
    collect_cash_flow >> 
    migrage_cash_flow >> 
    collect_income >> 
    migrage_income
]