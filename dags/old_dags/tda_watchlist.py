## python
import tda.functions as tda
from datetime import datetime, timedelta
import pendulum

## airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

## get global vars
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
local_tz = pendulum.timezone("US/Eastern")
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "start_date": datetime(2022, 6, 4, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

## make DAG tree
dag = DAG(
    dag_id="tda-watchlist",
    default_args=default_args,
    catchup=False,
    schedule_interval="*/20 * * * *"
    ## “At every 20th minute.”
)

update_watchlist = PythonOperator(
    task_id="update_watchlist",
    python_callable=tda.collect_watchlist,
    dag=dag,
)

update_token = PythonOperator(
    task_id="update_token",
    python_callable=tda.get_new_tokens,
    dag=dag,
)

update_watchlist, update_token
