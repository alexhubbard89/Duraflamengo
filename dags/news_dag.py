## python
import os
from datetime import datetime, timedelta
import pendulum

## airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

# python code
from derived_metrics import bull_call_strategy_analysis
from econ import tda_options_collection
from tda import collect

## get global vars
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
local_tz = pendulum.timezone("US/Eastern")
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 20, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

## make DAG tree
dag = DAG(
    dag_id="collect-news",
    default_args=default_args,
    catchup=False,
    schedule_interval="0,30 6-20 * * *",
    ## At minute 0 and 30 past every hour from 6 through
    # through 10 on every day-of-week
)


collect_stock_news = SparkSubmitOperator(
    task_id="collect_stock_news",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_stock_news.py",
    name="single_collection",
    conn_id="spark_default",  # This now properly enforces local[*] master
    executor_memory="15g",
    driver_memory="15g",
    conf={
        "spark.executor.memory": "15g",
        "spark.driver.memory": "15g",
    },
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

migrate_news = SparkSubmitOperator(
    task_id="migrate_news",
    application=f"{pyspark_app_home}/dags/fmp/runner/migrate_news.py",
    name="single_collection",
    conn_id="spark_default",
    executor_memory="15g",
    driver_memory="15g",
    conf={
        "spark.executor.memory": "15g",
        "spark.driver.memory": "15g",
    },
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

[collect_stock_news >> migrate_news]
