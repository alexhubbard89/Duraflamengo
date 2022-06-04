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
    "start_date": datetime(2022, 4, 1, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

## make DAG tree
dag = DAG(
    dag_id="yahoo-api-collection",
    default_args=default_args,
    catchup=False,
    schedule_interval="0 7 * * MON"
    ## “At 07:00 on Monday.”
)

collect_insights = SparkSubmitOperator(
    task_id="collect_insights",
    application=f"{pyspark_app_home}/dags/yahoo/runner/collect_insights.py",
    executor_memory="15g",
    driver_memory="15g",
    name="collect_insights",
    execution_timeout=timedelta(minutes=120),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

collect_insights
