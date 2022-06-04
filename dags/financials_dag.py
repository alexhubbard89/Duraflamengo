## python
import os
from datetime import datetime, timedelta
import pendulum

## my code
import common.utils as utils
import econ.marketbeat_earnings as mb

## airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

## get global vars
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
sm_data_lake_dir = Variable.get("sm_data_lake_dir")
local_tz = pendulum.timezone("US/Eastern")
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "start_date": datetime(2021, 10, 25, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


## make DAG tree
dag = DAG(
    dag_id="financials-collection",
    default_args=default_args,
    catchup=False,
    schedule_interval="35 7-20 * * 1-5"
    ## “At minute 35 past every hour
    ## from 7 through 20 on every day-of-week
    ## from Monday through Friday.”
)

#######################
##### MARKET BEAT #####
#######################
clear_to_collect_buffer = PythonOperator(
    task_id="clear_to_collect_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "marketbeat-earnings/to-collect"},
    dag=dag,
)
find_upcoming_earnings = PythonOperator(
    task_id="find_upcoming_earnings",
    python_callable=mb.find_upcoming,
    op_kwargs={"date": "{{execution_date}}"},
    dag=dag,
)
make_upcoming_earning_table = PythonOperator(
    task_id="make_upcoming_earning_table",
    python_callable=mb.make_upcoming_table,
    dag=dag,
)
mb_collect_earnings = SparkSubmitOperator(
    task_id="mb_collect_earnings",
    application=f"{pyspark_app_home}/dags/econ/runner/mb_collect_earnings.py",
    executor_memory="15g",
    driver_memory="15g",
    name="mb_collect_earnings",
    execution_timeout=timedelta(minutes=90),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
mb_migrate_earnings = SparkSubmitOperator(
    task_id="mb_migrate_earnings",
    application=f"{pyspark_app_home}/dags/econ/runner/mb_migrate_earnings.py",
    executor_memory="15g",
    driver_memory="15g",
    name="mb_migrate_earnings",
    execution_timeout=timedelta(minutes=90),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
clear_mb_collected_buffer = PythonOperator(
    task_id="clear_mb_collected_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "marketbeat-earnings/collected"},
    dag=dag,
)

#############################
##### StockAnalysis.com #####
#############################
clear_sa_buffer = PythonOperator(
    task_id="clear_sa_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "stock-analysis/profile"},
    dag=dag,
)
clear_sa_bs_q_buffer = PythonOperator(
    task_id="clear_balance_sheet_quarter_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "quarterly/balance-sheet"},
    dag=dag,
)
clear_sa_bs_a_buffer = PythonOperator(
    task_id="clear_balance_sheet_annual_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "annual/balance-sheet"},
    dag=dag,
)
clear_sa_cf_q_buffer = PythonOperator(
    task_id="clear_cash_flow_quarter_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "quarterly/cash-flow"},
    dag=dag,
)
clear_sa_cf_a_buffer = PythonOperator(
    task_id="clear_cash_flow_annual_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "annual/cash-flow"},
    dag=dag,
)
clear_sa_income_q_buffer = PythonOperator(
    task_id="clear_income_quarter_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "quarterly/income"},
    dag=dag,
)
clear_sa_income_a_buffer = PythonOperator(
    task_id="clear_income_annual_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "annual/income"},
    dag=dag,
)
clear_sa_ratios_q_buffer = PythonOperator(
    task_id="clear_ratios_quarter_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "quarterly/ratios"},
    dag=dag,
)
clear_sa_ratios_a_buffer = PythonOperator(
    task_id="clear_ratios_annual_buffer",
    python_callable=utils.clear_buffer,
    op_kwargs={"subdir": "annual/ratios"},
    dag=dag,
)
sa_collect_profile = SparkSubmitOperator(
    task_id="sa_collect_profile",
    application=f"{pyspark_app_home}/dags/econ/runner/sa_collect_profile.py",
    executor_memory="15g",
    driver_memory="15g",
    name="sa_collect_profile",
    execution_timeout=timedelta(minutes=90),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
sa_migrate_profile = SparkSubmitOperator(
    task_id="sa_migrate_profile",
    application=f"{pyspark_app_home}/dags/econ/runner/sa_migrate_profile.py",
    executor_memory="15g",
    driver_memory="15g",
    name="sa_migrate_profile",
    execution_timeout=timedelta(minutes=90),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
sa_collect_balance_sheet = SparkSubmitOperator(
    task_id="sa_collect_balance_sheet",
    application=f"{pyspark_app_home}/dags/econ/runner/sa_collect_balance_sheet.py",
    executor_memory="15g",
    driver_memory="15g",
    name="sa_collect_balance_sheet",
    execution_timeout=timedelta(minutes=90),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
sa_collect_cash_flow = SparkSubmitOperator(
    task_id="sa_collect_cash_flow",
    application=f"{pyspark_app_home}/dags/econ/runner/sa_collect_cash_flow.py",
    executor_memory="15g",
    driver_memory="15g",
    name="sa_collect_cash_flow",
    execution_timeout=timedelta(minutes=90),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
sa_collect_income = SparkSubmitOperator(
    task_id="sa_collect_income",
    application=f"{pyspark_app_home}/dags/econ/runner/sa_collect_income.py",
    executor_memory="15g",
    driver_memory="15g",
    name="sa_collect_income",
    execution_timeout=timedelta(minutes=90),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
sa_collect_ratios = SparkSubmitOperator(
    task_id="sa_collect_ratios",
    application=f"{pyspark_app_home}/dags/econ/runner/sa_collect_ratios.py",
    executor_memory="15g",
    driver_memory="15g",
    name="sa_collect_ratios",
    execution_timeout=timedelta(minutes=90),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)
sa_migrate_financials = SparkSubmitOperator(
    task_id="sa_migrate_financials",
    application=f"{pyspark_app_home}/dags/econ/runner/sa_migrate_financials.py",
    executor_memory="15g",
    driver_memory="15g",
    name="sa_migrate_financials",
    execution_timeout=timedelta(minutes=90),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

## No overlap on them.
## Colection followed by buffer to
## give downtown on requests.
[
    [clear_to_collect_buffer >> find_upcoming_earnings],
    find_upcoming_earnings >> make_upcoming_earning_table,
    find_upcoming_earnings
    >> clear_mb_collected_buffer
    >> mb_collect_earnings
    >> mb_migrate_earnings,
    find_upcoming_earnings
    >> clear_sa_buffer
    >> sa_collect_profile
    >> sa_migrate_profile,
    find_upcoming_earnings
    >> clear_sa_bs_q_buffer
    >> clear_sa_bs_a_buffer
    >> sa_collect_balance_sheet,
    find_upcoming_earnings
    >> clear_sa_cf_q_buffer
    >> clear_sa_cf_a_buffer
    >> sa_collect_cash_flow,
    find_upcoming_earnings
    >> clear_sa_income_q_buffer
    >> clear_sa_income_a_buffer
    >> sa_collect_income,
    find_upcoming_earnings
    >> clear_sa_ratios_q_buffer
    >> clear_sa_ratios_a_buffer
    >> sa_collect_ratios,
    sa_collect_balance_sheet >> sa_migrate_financials,
    sa_collect_cash_flow >> sa_migrate_financials,
    sa_collect_income >> sa_migrate_financials,
    sa_collect_ratios >> sa_migrate_financials,
]
