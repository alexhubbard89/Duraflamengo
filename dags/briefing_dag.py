## python
import os
from datetime import datetime, timedelta
import pendulum
from fmp import macro_econ
from fmp import coffee
from marty.NewsSummarizer import NewsSummarizer

## airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

## get global vars
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
local_tz = pendulum.timezone("US/Eastern")
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 21, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

## make DAG tree
dag = DAG(
    dag_id="briefing-tasks",
    default_args=default_args,
    catchup=False,
    schedule_interval="30 6 * * *",
    ## At 06:30 on every day-of-week.
)


# collect_sector_price_earning_ratio = PythonOperator(
#     task_id="collect_sector_price_earning_ratio",
#     python_callable=macro_econ.collect_sector_price_earning_ratio,
#     op_kwargs={"ds": "{{ ds }}"},
#     dag=dag,
#     execution_timeout=timedelta(minutes=5),
# )

# daily_summarization_pipeline = PythonOperator(
#     task_id="daily_summarization_pipeline",
#     python_callable=NewsSummarizer.daily_summarization_pipeline,
#     op_kwargs={"ds": "{{ ds }}"},
#     dag=dag,
#     execution_timeout=timedelta(minutes=10),
# )

collected_calendar_data = SparkSubmitOperator(
    task_id="earning_calendar",
    application=f"{pyspark_app_home}/dags/fmp/runner/earning_calendar.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=5),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

migrate_earning_calendar = SparkSubmitOperator(
    task_id="migrate_earning_calendar",
    application=f"{pyspark_app_home}/dags/fmp/runner/migrate_earning_calendar.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=5),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

[
    [collected_calendar_data >> migrate_earning_calendar],
]
