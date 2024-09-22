import os
from datetime import datetime, timedelta
import pendulum
from fmp import stocks, macro_econ

## airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
local_tz = pendulum.timezone("US/Eastern")
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "start_date": datetime(2023, 5, 22, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

## make DAG tree
dag = DAG(
    dag_id="price_collection_and_technical_analysis",
    default_args=default_args,
    catchup=False,
    schedule_interval="0 6 * * TUE-SAT",
    ## At 06:00 on every day-of-week from Monday through Friday.
)

collect_watchlist_daily_price = PythonOperator(
    task_id="collect_watchlist_daily_price",
    python_callable=stocks.collect_watchlist_daily_price,
    op_kwargs={"ds": " {{ ts_nodash_with_tz }} "},
    dag=dag,
    execution_timeout=timedelta(minutes=10),
)

migrate_daily_price = SparkSubmitOperator(
    task_id="migrate_daily_price",
    application=f"{pyspark_app_home}/dags/fmp/runner/migrate_daily_price.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=5),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

collect_sector_price_earning_ratio = PythonOperator(
    task_id="collect_sector_price_earning_ratio",
    python_callable=macro_econ.collect_sector_price_earning_ratio,
    op_kwargs={"ds": "{{ ds }}"},
    dag=dag,
    execution_timeout=timedelta(minutes=5),
)

[
    collect_watchlist_daily_price
    >> [
        migrate_daily_price,
    ],
    collect_sector_price_earning_ratio,
]
