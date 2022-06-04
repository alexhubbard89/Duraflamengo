## python
from fmp import macro_econ
import os
from datetime import datetime, timedelta
import pendulum

## airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

local_tz = pendulum.timezone("US/Eastern")
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 28, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

## make DAG tree
dag = DAG(
    dag_id="macro-collection",
    default_args=default_args,
    catchup=True,
    schedule_interval="00 6 * * *",  ## 6:00am Daily
)

collect_sector_performance = PythonOperator(
    task_id="collect_sector_performance",
    python_callable=macro_econ.collect_sector_performance,
    dag=dag,
)

collect_sector_pe = PythonOperator(
    task_id="collect_sector_pe",
    python_callable=macro_econ.collect_sector_pe,
    dag=dag,
    op_kwargs={"ds": "{{ ds }}"},
)

collect_industry_pe = PythonOperator(
    task_id="collect_industry_pe",
    python_callable=macro_econ.collect_industry_pe,
    dag=dag,
    op_kwargs={"ds": "{{ ds }}"},
)

collect_treasury_rate = PythonOperator(
    task_id="collect_treasury_rate",
    python_callable=macro_econ.collect_treasury_rate,
    dag=dag,
    op_kwargs={"ds": "{{ ds }}"},
)

collect_all_economic_indicators = PythonOperator(
    task_id="collect_all_economic_indicators",
    python_callable=macro_econ.collect_all_economic_indicators,
    dag=dag,
    op_kwargs={"ds": "{{ ds }}"},
)

[
    collect_sector_performance,
    collect_sector_pe,
    collect_industry_pe,
    collect_treasury_rate,
    collect_all_economic_indicators,
]
