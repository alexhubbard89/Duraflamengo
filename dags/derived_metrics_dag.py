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
import common.utils as utils
import derived_metrics.stock_prices as pv
import derived_metrics.business_health as bh

## Set location variables
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
sm_data_lake_dir = Variable.get("sm_data_lake_dir")
PRICE_DIR = sm_data_lake_dir + "/tda-daily-price/date"
STOCK_AVG_BUFFER = sm_data_lake_dir + "/buffer/der-m-avg-price"

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

##############################
##### Make Price, Volume #####
##############################
## Prepare
move_raw_files_pv = PythonOperator(
    task_id="move_raw_files_pv",
    python_callable=utils.move_files,
    dag=dag,
    op_kwargs={
        "data_loc": PRICE_DIR,
        "date": "{{execution_date}}",
        "days": 400,
        "buffer_loc": STOCK_AVG_BUFFER + "/raw",
    },
    execution_timeout=timedelta(minutes=5),
)
prepare_pv = SparkSubmitOperator(
    task_id="prepare_pv",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/prepare_pv.py",
    executor_memory="30g",
    driver_memory="30g",
    name="prepare_pv",
    execution_timeout=timedelta(minutes=60),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
pv_rename = PythonOperator(
    task_id="pv_rename",
    python_callable=utils.rename_file,
    dag=dag,
    op_kwargs={"path": STOCK_AVG_BUFFER + "/prepared/", "fn": "{{execution_date}}"},
)
## Make price, volume averages
make_pv = PythonOperator(task_id="make_pv", python_callable=pv.make_pv, dag=dag)
migrate_pv = SparkSubmitOperator(
    task_id="migrate_pv",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/migrate_pv.py",
    executor_memory="15g",
    driver_memory="15g",
    name="migrate_pv",
    execution_timeout=timedelta(minutes=60),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
## Clear
clear_pv_prepared_buffer = PythonOperator(
    task_id="clear_pv_prepared_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "der-m-avg-price/prepared"},
    dag=dag,
    execution_timeout=timedelta(minutes=5),
)
clear_pv_raw_buffer = PythonOperator(
    task_id="clear_pv_raw_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "der-m-avg-price/raw"},
    dag=dag,
    execution_timeout=timedelta(minutes=1),
)
clear_pv_finished_buffer = PythonOperator(
    task_id="clear_pv_finished_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "der-m-avg-price/finished"},
    dag=dag,
    execution_timeout=timedelta(minutes=1),
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

## DAG Order
[
    make_business_health,
    move_raw_files_pv >> prepare_pv >> pv_rename >> make_pv >> migrate_pv,
    migrate_pv >> clear_pv_prepared_buffer,
    migrate_pv >> clear_pv_raw_buffer,
    migrate_pv >> clear_pv_finished_buffer,
    make_ratios,
    make_daily_industry_rating,
    make_daily_sector_rating,
]
