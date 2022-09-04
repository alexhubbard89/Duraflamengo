## python
from datetime import datetime, timedelta
import pendulum

## airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

## python scripts
import derived_metrics.stock_prices as pv
import derived_metrics.business_health as bh
from fmp import macro_econ
import derived_metrics.settings as s

## Set location variables
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")

local_tz = pendulum.timezone("US/Eastern")
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 29, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


## make DAG tree
dag = DAG(
    dag_id="derived-metrics",
    default_args=default_args,
    catchup=False,
    schedule_interval="30 */2 * * *"
    ## At minute 30 past every 2nd hour.
)

make_pv = PythonOperator(
    task_id="make_pv",
    python_callable=pv.make_pv,
    op_kwargs={
        "ds": "{{ ds }}",
    },
    dag=dag,
)

make_business_health = PythonOperator(
    task_id="make_business_health",
    python_callable=bh.pipeline,
    dag=dag,
    execution_timeout=timedelta(minutes=10),
)

make_ratios = SparkSubmitOperator(
    task_id="make_ratios",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/make_ratios.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"der_ratios_ds": " {{ ts_nodash_with_tz }} "},
)

make_daily_industry_rating = SparkSubmitOperator(
    task_id="make_daily_industry_rating",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/make_daily_industry_rating.py",
    executor_memory="5g",
    driver_memory="5g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"ds": " {{ ts_nodash_with_tz }} "},
)

make_daily_sector_rating = SparkSubmitOperator(
    task_id="make_daily_sector_rating",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/make_daily_sector_rating.py",
    executor_memory="5g",
    driver_memory="5g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"ds": " {{ ts_nodash_with_tz }} "},
)

make_support_resistance = SparkSubmitOperator(
    task_id="make_support_resistance",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/make_support_resistance.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=20),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"ds": " {{ ds_nodash }} ", "yesterday": "True"},
)

option_swings_discovery = SparkSubmitOperator(
    task_id="option_swings_discovery",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/option_swings_discovery.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"ds": " {{ ds_nodash }} ", "yesterday": "True"},
)

option_swings_predict = SparkSubmitOperator(
    task_id="option_swings_predict",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/option_swings_predict.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"ds": " {{ ds_nodash }} ", "yesterday": "True"},
)

merge_macro_signals = PythonOperator(
    task_id="merge_macro_signals",
    python_callable=macro_econ.merge_signals,
    dag=dag,
    execution_timeout=timedelta(minutes=10),
)

## DAG Order
[
    make_business_health,
    make_pv,
    make_ratios,
    make_daily_industry_rating,
    make_daily_sector_rating,
    make_support_resistance >> option_swings_discovery >> option_swings_predict,
    merge_macro_signals,
]
