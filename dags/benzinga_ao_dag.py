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
import common.utils as utils
import econ.benzinga_collection as bc

## get global vars
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
sm_data_lake_dir = Variable.get("sm_data_lake_dir")
local_tz = pendulum.timezone("US/Eastern")
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 6, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


## make DAG tree
dag = DAG(
    dag_id="benzinga-analyst-ratings",
    default_args=default_args,
    catchup=False,
    schedule_interval="0,15,30,45 6-23 * * 1-5"
    ## “At minute 0, 15, 30, and 45 past every hour
    ## from 6 through 23 on every day-of-week
    ## from Monday through Friday.”
)

## unusual options
collect_data = PythonOperator(
    task_id="collect_data",
    python_callable=bc.get_rating_changes,
    op_kwargs={"date": "{{execution_date}}"},
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)
migrate_data = SparkSubmitOperator(
    task_id="migrate_data",
    application=f"{pyspark_app_home}/dags/econ/runner/migrate_benzinga_ratings.py",
    executor_memory="3g",
    driver_memory="3g",
    name="migrate_data",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
clear_buffer = PythonOperator(
    task_id="clear_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "benzinga-rating-changes"},
    dag=dag,
)

coalesce_benzinga_ratings = SparkSubmitOperator(
    task_id="coalesce_benzinga_ratings",
    application=f"{pyspark_app_home}/dags/econ/runner/coalesce_benzinga_ratings.py",
    executor_memory="5g",
    driver_memory="5g",
    name="migrate_data",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

[collect_data >> migrate_data >> clear_buffer >> coalesce_benzinga_ratings]
