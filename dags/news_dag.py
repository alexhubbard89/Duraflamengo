## python
import os
from datetime import datetime, timedelta
import pendulum

## airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
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
    schedule_interval="0,30 6-20 * * *"
    ## At minute 0 and 30 past every hour from 6 through
    # through 10 on every day-of-week
)


collect_stock_news = SparkSubmitOperator(
    task_id="collect_stock_news",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_stock_news.py",
    executor_memory="15g",
    driver_memory="15g",
    name="single_collection",
    execution_timeout=timedelta(minutes=15),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
## press release needs fixing

[collect_stock_news]
