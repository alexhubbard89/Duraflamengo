## python
from fmp import coffee
import os
from datetime import datetime, timedelta
import pendulum

## airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
local_tz = pendulum.timezone("US/Eastern")
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "start_date": datetime(2023, 2, 24, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

## make DAG tree
dag = DAG(
    dag_id="morning-coffee-run",
    default_args=default_args,
    catchup=True,
    schedule_interval="30 5 * * *",  ## 5:30am Daily
)

collect_delisted = PythonOperator(
    task_id="collect_delisted",
    python_callable=coffee.collect_delisted,
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

collect_market_constituents = PythonOperator(
    task_id="collect_market_constituents",
    python_callable=coffee.collect_market_constituents,
    op_kwargs={
        "ds": "{{ ds }}",
    },
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

collected_calendar_data = PythonOperator(
    task_id="collected_calendar_data",
    python_callable=coffee.collect_calendar_data,
    dag=dag,
    op_kwargs={
        "ds": "{{ ds }}",
        "ds_delta": 14,
    },
    execution_timeout=timedelta(minutes=10),
)

make_collection_list = PythonOperator(
    task_id="make_collection_list",
    python_callable=coffee.make_collection_list,
    dag=dag,
    op_kwargs={
        "ds": "{{ ds }}",
    },
    execution_timeout=timedelta(minutes=10),
)

earning_calendar = SparkSubmitOperator(
    task_id="earning_calendar",
    application=f"{pyspark_app_home}/dags/fmp/runner/earning_calendar.py",
    executor_memory="15g",
    driver_memory="15g",
    name="earning_calendar",
    execution_timeout=timedelta(minutes=2),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

shares_float = SparkSubmitOperator(
    task_id="shares_float",
    application=f"{pyspark_app_home}/dags/fmp/runner/shares_float.py",
    executor_memory="15g",
    driver_memory="15g",
    name="shares_float",
    execution_timeout=timedelta(minutes=2),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

[
    [
        collect_delisted
        >> collect_market_constituents
        >> collected_calendar_data
        >> make_collection_list
    ],
    earning_calendar,
    shares_float,
]
