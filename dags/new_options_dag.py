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
    "start_date": datetime(2023, 6, 11, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

## make DAG tree
dag = DAG(
    dag_id="new-tda-options-collection",
    default_args=default_args,
    catchup=False,
    schedule_interval="0,30 9-16 * * MON-FRI",
    ## At minute 0 and 30 past every hour from 9 through
    # through 16 on every day-of-week from Monday
    # through Friday.
)


options_watchlist_collection = SparkSubmitOperator(
    task_id="options_watchlist_collection",
    application=f"{pyspark_app_home}/dags/tda/runner/distributed_options_collection.py",
    executor_memory="15g",
    driver_memory="15g",
    name="single_collection",
    execution_timeout=timedelta(minutes=15),
    conf={"master": "spark://localhost:7077"},
    env_vars={"ds": " {{ ts_nodash_with_tz }} "},
    dag=dag,
)

bull_call_strategy_analysis_watch_list = PythonOperator(
    task_id="bull_call_strategy_analysis_watch_list",
    python_callable=bull_call_strategy_analysis.watch_list_pipeline,
    op_kwargs={"ds": " {{ ts_nodash_with_tz }} "},
    dag=dag,
)

[options_watchlist_collection >> bull_call_strategy_analysis_watch_list]
