## python
from analysis.options import options_discovery
import derived_metrics.asset_metrics as asset_metrics
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
    "start_date": datetime(2022, 6, 6, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

## make DAG tree
dag = DAG(
    dag_id="intraday-analysis",
    default_args=default_args,
    catchup=False,
    schedule_interval="31 09-16 * * 1-5"
    ## “At minute 31 past every hour from 9 through 16
    # on every day-of-week from Monday through Friday.”
)

collect_thirty_minute_price = SparkSubmitOperator(
    task_id="collect_thirty_minute_price",
    application=f"{pyspark_app_home}/dags/fmp/runner/thirty_minute_price.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_thirty_minute_price_ds": " {{ ts_nodash_with_tz }} "},
)

attach_metrics = PythonOperator(
    task_id="attach_metrics",
    python_callable=asset_metrics.attach_metrics,
    op_kwargs={"ds": " {{ execution_date }} ", "window": 20, "yesterday": "False"},
    dag=dag,
    execution_timeout=timedelta(minutes=10),
)

options_discovery = PythonOperator(
    task_id="options_discovery",
    python_callable=options_discovery,
    op_kwargs={"ds": "{{ ds }}"},
    dag=dag,
    execution_timeout=timedelta(minutes=10),
)

distributed_options_collection = SparkSubmitOperator(
    task_id="distributed_options_collection",
    application=f"{pyspark_app_home}/dags/tda/runner/distributed_options_collection.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=15),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"distributed_options_collection_ds": " {{ execution_date }} "},
)

[
    collect_thirty_minute_price
    >> attach_metrics
    >> options_discovery
    >> distributed_options_collection
]
