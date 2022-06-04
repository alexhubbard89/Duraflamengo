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
import derived_metrics.analyst_opinions as aos
import derived_metrics.stock_prices as pv
import growth_classifier.training_data as training
import growth_classifier.train_predict as clf

## Set location variables
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
sm_data_lake_dir = Variable.get("sm_data_lake_dir")
AO_RATINGS = sm_data_lake_dir + "/benzinga-rating-changes"
AO_BUFFER = sm_data_lake_dir + "/buffer/der-m-ao"
AO_SCORES_BUFFER = sm_data_lake_dir + "/buffer/der-m-ao-score"
PRICE_DIR = sm_data_lake_dir + "/tda-daily-price/date"
STOCK_AVG_BUFFER = sm_data_lake_dir + "/buffer/der-m-avg-price"
TRAINING_BUFFER = sm_data_lake_dir + "/buffer/training-data"

local_tz = pendulum.timezone("US/Eastern")
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 19, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


## make DAG tree
dag = DAG(
    dag_id="growth-classifier",
    default_args=default_args,
    catchup=False,
    schedule_interval="0 5 * * 2-6"  ## T-S at 5am
    # Turn this on after you properly configure
    # spark resources
    # concurrency=2,
    # max_active_runs=2,
)

#########################
##### Make AO Stuff #####
#########################
## Prepare
move_raw_files_ao = PythonOperator(
    task_id="move_raw_files_ao",
    python_callable=utils.move_files,
    dag=dag,
    op_kwargs={
        "data_loc": AO_RATINGS,
        "date": "{{execution_date}}",
        "days": int(90 * 3),
        "buffer_loc": AO_BUFFER + "/raw",
    },
)
prepare_ao = SparkSubmitOperator(
    task_id="prepare_ao",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/prepare_ao.py",
    executor_memory="15g",
    driver_memory="15g",
    name="prepare_ao",
    execution_timeout=timedelta(minutes=120),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
ao_rename = PythonOperator(
    task_id="ao_rename",
    python_callable=utils.rename_file,
    dag=dag,
    op_kwargs={"path": AO_BUFFER + "/prepared/", "fn": "{{execution_date}}"},
)
## Scores
make_ao_scores = PythonOperator(
    task_id="make_ao_scores",
    python_callable=aos.make_scores,
    dag=dag,
)
migrate_ao_scores = SparkSubmitOperator(
    task_id="migrate_ao_scores",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/migrate_ao_scores.py",
    executor_memory="15g",
    driver_memory="15g",
    name="migrate_ao_scores",
    execution_timeout=timedelta(minutes=120),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
clear_ao_scored_buffer = PythonOperator(
    task_id="clear_ao_scored_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "der-m-ao-score/scored"},
    dag=dag,
)
## Price targets
make_ao_pt = PythonOperator(
    task_id="make_ao_pt",
    python_callable=aos.make_pt,
    dag=dag,
)
migrate_ao_pt = SparkSubmitOperator(
    task_id="migrate_ao_pt",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/migrate_ao_pt.py",
    executor_memory="15g",
    driver_memory="15g",
    name="migrate_ao_pt",
    execution_timeout=timedelta(minutes=120),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
clear_ao_pt_buffer = PythonOperator(
    task_id="clear_ao_pt_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "der-m-ao-pt/targeted"},
    dag=dag,
)
## Clear
clear_ao_prepared_buffer = PythonOperator(
    task_id="clear_ao_prepared_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "der-m-ao/prepared"},
    dag=dag,
)
clear_ao_raw_buffer = PythonOperator(
    task_id="clear_ao_raw_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "der-m-ao/raw"},
    dag=dag,
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
        "days": 20,
        "buffer_loc": STOCK_AVG_BUFFER + "/raw",
    },
)
prepare_pv = SparkSubmitOperator(
    task_id="prepare_pv",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/prepare_pv.py",
    executor_memory="15g",
    driver_memory="15g",
    name="prepare_pv",
    execution_timeout=timedelta(minutes=120),
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
make_pv = PythonOperator(
    task_id="make_pv", python_callable=pv.make_pv, dag=dag, op_kwargs={"window": 10}
)
migrate_pv = SparkSubmitOperator(
    task_id="migrate_pv",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/migrate_pv.py",
    executor_memory="15g",
    driver_memory="15g",
    name="migrate_pv",
    execution_timeout=timedelta(minutes=120),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
## Clear
clear_pv_prepared_buffer = PythonOperator(
    task_id="clear_pv_prepared_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "der-m-avg-price/prepared"},
    dag=dag,
)
clear_pv_raw_buffer = PythonOperator(
    task_id="clear_pv_raw_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "der-m-avg-price/raw"},
    dag=dag,
)
clear_pv_finished_buffer = PythonOperator(
    task_id="clear_pv_finished_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "der-m-avg-price/finished"},
    dag=dag,
)

###########################
##### Make Financials #####
###########################
## income
prepare_income = SparkSubmitOperator(
    task_id="prepare_income",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/prepare_income.py",
    executor_memory="15g",
    driver_memory="15g",
    name="prepare_income",
    execution_timeout=timedelta(minutes=120),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
migrate_income = SparkSubmitOperator(
    task_id="migrate_income",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/migrate_income.py",
    executor_memory="15g",
    driver_memory="15g",
    name="migrate_income",
    execution_timeout=timedelta(minutes=120),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
## balance sheet
prepare_balance_sheet = SparkSubmitOperator(
    task_id="prepare_balance_sheet",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/prepare_balance_sheet.py",
    executor_memory="15g",
    driver_memory="15g",
    name="prepare_balance_sheet",
    execution_timeout=timedelta(minutes=120),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
migrate_balance_sheet = SparkSubmitOperator(
    task_id="migrate_balance_sheet",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/migrate_balance_sheet.py",
    executor_memory="15g",
    driver_memory="15g",
    name="migrate_balance_sheet",
    execution_timeout=timedelta(minutes=120),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
## cash flow
prepare_cash_flow = SparkSubmitOperator(
    task_id="prepare_cash_flow",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/prepare_cash_flow.py",
    executor_memory="15g",
    driver_memory="15g",
    name="prepare_cash_flow",
    execution_timeout=timedelta(minutes=120),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
migrate_cash_flow = SparkSubmitOperator(
    task_id="migrate_cash_flow",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/migrate_cash_flow.py",
    executor_memory="15g",
    driver_memory="15g",
    name="migrate_cash_flow",
    execution_timeout=timedelta(minutes=120),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

##############################
##### Make Training Data #####
##############################
## set date
clear_training_buffer = PythonOperator(
    task_id="clear_training_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "training-data"},
    dag=dag,
)
set_training_date = PythonOperator(
    task_id="set_training_date",
    python_callable=training.set_date,
    dag=dag,
    op_kwargs={"date": "{{execution_date}}"},
)
## make training
make_training_data = SparkSubmitOperator(
    task_id="make_training_data",
    application=f"{pyspark_app_home}/dags/growth_classifier/runner/make_training_data.py",
    executor_memory="15g",
    driver_memory="15g",
    name="migrate_cash_flow",
    execution_timeout=timedelta(minutes=120),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

##########################
##### Predict Growth #####
##########################
predict_growth = PythonOperator(
    task_id="predict_growth",
    python_callable=clf.train_and_predict,
    dag=dag,
    op_kwargs={"date": "{{execution_date}}", "threshold": 0.7},
)

## DAG Order
[
    [move_raw_files_ao >> prepare_ao >> ao_rename],
    ao_rename >> make_ao_scores,
    ao_rename >> make_ao_pt,
    [make_ao_scores >> migrate_ao_scores >> clear_ao_scored_buffer],
    [make_ao_pt >> migrate_ao_pt >> clear_ao_pt_buffer],
    clear_ao_scored_buffer >> clear_ao_prepared_buffer,
    clear_ao_pt_buffer >> clear_ao_prepared_buffer,
    clear_ao_scored_buffer >> clear_ao_raw_buffer,
    clear_ao_pt_buffer >> clear_ao_raw_buffer,
    [move_raw_files_pv >> prepare_pv >> pv_rename >> make_pv >> migrate_pv],
    migrate_pv >> clear_pv_prepared_buffer,
    migrate_pv >> clear_pv_raw_buffer,
    migrate_pv >> clear_pv_finished_buffer,
    [prepare_income >> migrate_income],
    [prepare_balance_sheet >> migrate_balance_sheet],
    [prepare_cash_flow >> migrate_cash_flow],
    ## after all data is cleared, make training
    migrate_income >> clear_training_buffer,
    migrate_balance_sheet >> clear_training_buffer,
    migrate_cash_flow >> clear_training_buffer,
    clear_ao_prepared_buffer >> clear_training_buffer,
    clear_ao_raw_buffer >> clear_training_buffer,
    clear_pv_prepared_buffer >> clear_training_buffer,
    clear_pv_raw_buffer >> clear_training_buffer,
    clear_pv_finished_buffer >> clear_training_buffer,
    [
        clear_training_buffer
        >> set_training_date
        >> make_training_data
        >> predict_growth
    ],
]
