## python
import os
from datetime import datetime, timedelta
import pendulum

## airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

## python scripts
import econ.volatility_index as vix

## Set location variables
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")

local_tz = pendulum.timezone("US/Eastern")
default_args = {
    'owner': 'alex',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 2, tzinfo=local_tz),
    'email': ['alexhubbard89@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


## make DAG tree
dag = DAG(dag_id='volatility-index',
          default_args=default_args,
          catchup=False,
          schedule_interval=timedelta(minutes=20)
          )

collect_current = PythonOperator(
    task_id='collect_current' ,
    python_callable=vix.get_current,
    dag=dag
)

collect_historical = PythonOperator(
    task_id='collect_historical' ,
    python_callable=vix.get_historical,
    dag=dag
)

[
    collect_current,
    collect_historical
]