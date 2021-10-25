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
import common.scripts.utils as utils
import econ.benzinga_collection as bc

## get global vars
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
sm_data_lake_dir = Variable.get("sm_data_lake_dir")
local_tz = pendulum.timezone("US/Eastern")
default_args = {
    'owner': 'alex',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 19, tzinfo=local_tz),
    'email': ['alexhubbard89@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


## make DAG tree
dag = DAG(dag_id='benzinga-unusual-options',
          default_args=default_args,
          catchup=False,
          schedule_interval=timedelta(hours=1.5)
          )
## unusual options
collect_benzinga_unusual_options = PythonOperator(
    task_id='collect_benzinga_unusual_options' ,
    python_callable=bc.get_unusual_options,
    op_kwargs={'date': '{{execution_date}}'},
    dag=dag,
)
migrate_benzinga_unusual_options = SparkSubmitOperator(
    task_id='migrate_benzinga_unusual_options',
    application=f'{pyspark_app_home}/dags/econ/runner/migrate_benzinga_options.py',
    executor_memory='15g',
    driver_memory='15g',
    name='migrate_benzinga_unusual_options',
    execution_timeout=timedelta(minutes=30),
    conf={'master':'spark://localhost:7077'},
    dag=dag,
)
benzinga_clear_unusual_options_buffer = PythonOperator(
    task_id='benzinga_clear_unusual_options_buffer' ,
    python_callable=utils.clear_buffer,
    op_kwargs={'subdir': 'unusual-options-benzinga'},
    dag=dag,
)

[
    collect_benzinga_unusual_options >> 
    migrate_benzinga_unusual_options >> 
    benzinga_clear_unusual_options_buffer
]