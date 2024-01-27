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
local_tz = pendulum.timezone("US/Eastern")
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "start_date": datetime(2023, 9, 23, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

## make DAG tree
dag = DAG(
    dag_id="collect-earnigns",
    default_args=default_args,
    catchup=False,
    schedule_interval="15 6,18 * * *"
    ## At minute 15 past hour 6 and 18
)


collect_earnings_call_transcript = SparkSubmitOperator(
    task_id="collect_earnings_call_transcript",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_earnings_call_transcript.py",
    executor_memory="5g",
    driver_memory="5g",
    name="collect_earnings_call_transcript",
    execution_timeout=timedelta(minutes=5),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

collect_balance_sheets = SparkSubmitOperator(
    task_id="collect_balance_sheets",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_balance_sheets.py",
    executor_memory="5g",
    driver_memory="5g",
    name="collect_balance_sheets",
    execution_timeout=timedelta(minutes=5),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

collect_cash_flow = SparkSubmitOperator(
    task_id="collect_cash_flow",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_cash_flow.py",
    executor_memory="5g",
    driver_memory="5g",
    name="collect_cash_flow",
    execution_timeout=timedelta(minutes=5),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

collect_income = SparkSubmitOperator(
    task_id="collect_income",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_income.py",
    executor_memory="5g",
    driver_memory="5g",
    name="collect_income",
    execution_timeout=timedelta(minutes=5),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

distribute_ratio = SparkSubmitOperator(
    task_id="distribute_ratio",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/distribute_ratio.py",
    executor_memory="5g",
    driver_memory="5g",
    name="distribute_ratio",
    execution_timeout=timedelta(minutes=5),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

[
    collect_earnings_call_transcript,
    collect_balance_sheets >> distribute_ratio,
    collect_cash_flow >> distribute_ratio,
    collect_income >> distribute_ratio,
]
