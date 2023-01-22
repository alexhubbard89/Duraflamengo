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
import econ.tda_options_collection as options

## get global vars
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
local_tz = pendulum.timezone("US/Eastern")
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "start_date": datetime(2021, 10, 20, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

## make DAG tree
dag = DAG(
    dag_id="tda-options-collection",
    default_args=default_args,
    catchup=False,
    schedule_interval="*/5 9-16 * * MON-FRI"
    ## At every 5th minute past every hour from 9
    # through 16 on every day-of-week from Monday
    # through Friday.
)

## clear buffers
clear_single_buffer = PythonOperator(
    task_id="clear_single_buffer",
    python_callable=options.clear_buffer,
    op_kwargs={"strategy": "single"},
    dag=dag,
)

# clear_analytical_buffer = PythonOperator(
#     task_id='clear_analytical_buffer' ,
#     python_callable=options.clear_buffer,
#     op_kwargs={'strategy': 'analytical'},
#     dag=dag,
# )

## collect and migrate single options
single_collection = SparkSubmitOperator(
    task_id="single_collection",
    application=f"{pyspark_app_home}/dags/econ/runner/collect_tda_options_single.py",
    executor_memory="15g",
    driver_memory="15g",
    name="single_collection",
    execution_timeout=timedelta(minutes=150),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

single_migration = SparkSubmitOperator(
    task_id="single_migration",
    application=f"{pyspark_app_home}/dags/econ/runner/migrate_tda_options_single.py",
    executor_memory="15g",
    driver_memory="15g",
    name="single_migration",
    execution_timeout=timedelta(minutes=150),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

# ## collect and migrate analytical options
# analytical_collection = SparkSubmitOperator(
#     task_id='analytical_collection',
#     application=f'{pyspark_app_home}/dags/econ/runner/collect_tda_options_analytical.py',
#     executor_memory='15g',
#     driver_memory='15g',
#     name='analytical_collection',
#     execution_timeout=timedelta(minutes=150),
#     conf={'master':'spark://localhost:7077'},
#     dag=dag
# )

# analytical_migration = SparkSubmitOperator(
#     task_id='analytical_migration',
#     application=f'{pyspark_app_home}/dags/econ/runner/migrate_tda_options_analytical.py',
#     executor_memory='15g',
#     driver_memory='15g',
#     name='analytical_migration',
#     execution_timeout=timedelta(minutes=150),
#     conf={'master':'spark://localhost:7077'},
#     dag=dag
# )

[clear_single_buffer >> single_collection >> single_migration]
