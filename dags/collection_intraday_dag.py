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
    "start_date": datetime(2021, 8, 21, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 10,
    "retry_delay": timedelta(minutes=5),
}

## make DAG tree
dag = DAG(
    dag_id="intraday-collection",
    default_args=default_args,
    catchup=False,
    schedule_interval="*/5 9-16 * * MON-FRI",
)  ## every 5 minutes

historical_chart_1_min = SparkSubmitOperator(
    task_id="historical_chart_1_min",
    application=f"{pyspark_app_home}/dags/fmp/intraday_runner/historical_chart_1_min.py",
    executor_memory="15g",
    driver_memory="15g",
    name="historical_chart_1_min",
    execution_timeout=timedelta(minutes=2),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

historical_chart_15_min = SparkSubmitOperator(
    task_id="historical_chart_15_min",
    application=f"{pyspark_app_home}/dags/fmp/intraday_runner/historical_chart_15_min.py",
    executor_memory="15g",
    driver_memory="15g",
    name="historical_chart_15_min",
    execution_timeout=timedelta(minutes=2),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

technical_indicator_1_min_rsi = SparkSubmitOperator(
    task_id="technical_indicator_1_min_rsi",
    application=f"{pyspark_app_home}/dags/fmp/intraday_runner/technical_indicator_1_min_rsi.py",
    executor_memory="15g",
    driver_memory="15g",
    name="technical_indicator_1_min_rsi",
    execution_timeout=timedelta(minutes=2),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

technical_indicator_1_min_sma = SparkSubmitOperator(
    task_id="technical_indicator_1_min_sma",
    application=f"{pyspark_app_home}/dags/fmp/intraday_runner/technical_indicator_1_min_sma.py",
    executor_memory="15g",
    driver_memory="15g",
    name="technical_indicator_1_min_sma",
    execution_timeout=timedelta(minutes=2),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

technical_indicator_15_min_rsi = SparkSubmitOperator(
    task_id="technical_indicator_15_min_rsi",
    application=f"{pyspark_app_home}/dags/fmp/intraday_runner/technical_indicator_15_min_rsi.py",
    executor_memory="15g",
    driver_memory="15g",
    name="technical_indicator_15_min_rsi",
    execution_timeout=timedelta(minutes=2),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

technical_indicator_15_min_sma = SparkSubmitOperator(
    task_id="technical_indicator_15_min_sma",
    application=f"{pyspark_app_home}/dags/fmp/intraday_runner/technical_indicator_15_min_sma.py",
    executor_memory="15g",
    driver_memory="15g",
    name="technical_indicator_15_min_sma",
    execution_timeout=timedelta(minutes=2),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

[
    historical_chart_1_min,
    historical_chart_15_min,
    technical_indicator_1_min_rsi,
    technical_indicator_1_min_sma,
    technical_indicator_15_min_rsi,
    technical_indicator_15_min_sma,
]
