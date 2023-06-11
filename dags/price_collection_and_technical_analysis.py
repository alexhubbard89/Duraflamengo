import os
from datetime import datetime, timedelta
import pendulum
from fmp import stocks
from derived_metrics import crossovers
from derived_metrics import bull_classifier, bear_classifier

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

technial_analysis_enrich_watchlist = SparkSubmitOperator(
    task_id="technial_analysis_enrich_watchlist",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/technial_analysis_enrich_watchlist.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=15),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"ds": " {{ ts_nodash_with_tz }} "},
)

predict_all_crossovers = PythonOperator(
    task_id="predict_all_crossovers",
    python_callable=crossovers.predict_all_crossovers,
    op_kwargs={"ds": " {{ ts_nodash_with_tz }} "},
    dag=dag,
    execution_timeout=timedelta(minutes=10),
)

bull_classifier_daily_predict = PythonOperator(
    task_id="bull_classifier_daily_predict",
    python_callable=bull_classifier.daily_predict,
    op_kwargs={"ds": " {{ ts_nodash_with_tz }} "},
    dag=dag,
    execution_timeout=timedelta(minutes=10),
)

bear_classifier_daily_predict = PythonOperator(
    task_id="bear_classifier_daily_predict",
    python_callable=bear_classifier.daily_predict,
    op_kwargs={"ds": " {{ ts_nodash_with_tz }} "},
    dag=dag,
    execution_timeout=timedelta(minutes=10),
)


[
    collect_watchlist_daily_price
    >> technial_analysis_enrich_watchlist
    >> [
        predict_all_crossovers,
        bull_classifier_daily_predict,
        bear_classifier_daily_predict,
    ]
]
