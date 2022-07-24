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
import yahoo.trending as trending

## get global vars
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
local_tz = pendulum.timezone("US/Eastern")
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "start_date": datetime(2021, 8, 23, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

## make DAG tree
dag = DAG(
    dag_id="yahoo-trending-collection",
    default_args=default_args,
    catchup=False,
    schedule_interval="*/10 9-17 * * 1-5"
    ## every 10 minutes M-F between 9-5
)

collect_yahoo_trending = PythonOperator(
    task_id="collect_yahoo_trending",
    python_callable=trending.get_yahoo_trending,
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

migrate_yahoo_trending = SparkSubmitOperator(
    task_id="migrate_yahoo_trending",
    application=f"{pyspark_app_home}/dags/econ/runner/migrate_yahoo_trending.py",
    executor_memory="15g",
    driver_memory="15g",
    name="migrate_yahoo_trending",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

clear_buffer_yahoo_trending = PythonOperator(
    task_id="clear_buffer_yahoo_trending",
    python_callable=trending.clear_yahoo_trending_buffer,
    execution_timeout=timedelta(minutes=1),
    dag=dag,
)

[collect_yahoo_trending >> migrate_yahoo_trending >> clear_buffer_yahoo_trending]
