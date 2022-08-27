from fmp import stocks
import os
from datetime import datetime, timedelta
import pendulum
import derived_metrics.asset_metrics as asset_metrics

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
    "start_date": datetime(2022, 8, 22, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

## make DAG tree
dag = DAG(
    dag_id="fmp-ticker-metrics-collection",
    default_args=default_args,
    catchup=False,
    schedule_interval="00 2 * * *",  ## 2:00am Daily
)

## fmp does not like this collection method
# collect_all_full_price = SparkSubmitOperator(
#     task_id="collect_all_full_price",
#     application=f"{pyspark_app_home}/dags/fmp/runner/price.py",
#     executor_memory="15g",
#     driver_memory="15g",
#     name="{{ task_instance.task_id }}",
#     execution_timeout=timedelta(minutes=10),
#     conf={"master": "spark://localhost:7077"},
#     dag=dag,
#     env_vars={"collect_full_price_ds": " {{ ts_nodash_with_tz }} "},
# )

distribute_append_price = SparkSubmitOperator(
    task_id="distribute_append_price",
    application=f"{pyspark_app_home}/dags/fmp/runner/distribute_append_price.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"ds": " {{ ds_nodash }} "},
)

collect_peers = PythonOperator(
    task_id="collect_peers",
    python_callable=stocks.collect_peers,
    op_kwargs={
        "ds": " {{ ts_nodash_with_tz }} ",
    },
    dag=dag,
    execution_timeout=timedelta(minutes=10),
)

collect_dcf = SparkSubmitOperator(
    task_id="collect_dcf",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_dcf.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_dcf_ds": " {{ ts_nodash_with_tz }} "},
)

collect_rating = SparkSubmitOperator(
    task_id="collect_rating",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_rating.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_rating_ds": " {{ ts_nodash_with_tz }} "},
)


collect_enterprise_values_annual = SparkSubmitOperator(
    task_id="collect_enterprise_values_annual",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_enterprise_values_annual.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_enterprise_values_annual_ds": " {{ ts_nodash_with_tz }} "},
)

collect_enterprise_values_quarter = SparkSubmitOperator(
    task_id="collect_enterprise_values_quarter",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_enterprise_values_quarter.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_enterprise_values_quarter_ds": " {{ ts_nodash_with_tz }} "},
)

collect_grade = SparkSubmitOperator(
    task_id="collect_grade",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_grade.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_grade_ds": " {{ ts_nodash_with_tz }} "},
)

collect_sentiment = SparkSubmitOperator(
    task_id="collect_sentiment",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_sentiment.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_sentiment_ds": " {{ ts_nodash_with_tz }} "},
)

collect_analyst_estimates = SparkSubmitOperator(
    task_id="collect_analyst_estimates",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_analyst_estimates.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_analyst_estimates_ds": " {{ ts_nodash_with_tz }} "},
)

collect_analyst_estimates_quarter = SparkSubmitOperator(
    task_id="collect_analyst_estimates_quarter",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_analyst_estimates_quarter.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_analyst_estimates_quarter_ds": " {{ ts_nodash_with_tz }} "},
)

collect_balance_sheets = SparkSubmitOperator(
    task_id="collect_balance_sheets",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_balance_sheets.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_balance_sheets_ds": " {{ ts_nodash_with_tz }} "},
)

collect_balance_sheets_quarter = SparkSubmitOperator(
    task_id="collect_balance_sheets_quarter",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_balance_sheets_quarter.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_balance_sheets_quarter_ds": " {{ ts_nodash_with_tz }} "},
)

collect_cash_flow = SparkSubmitOperator(
    task_id="collect_cash_flow",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_cash_flow.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_cash_flow_ds": " {{ ts_nodash_with_tz }} "},
)

collect_cash_flow_quarter = SparkSubmitOperator(
    task_id="collect_cash_flow_quarter",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_cash_flow_quarter.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_cash_flow_quarter_ds": " {{ ts_nodash_with_tz }} "},
)

collect_income = SparkSubmitOperator(
    task_id="collect_income",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_income.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_income_ds": " {{ ts_nodash_with_tz }} "},
)

collect_income_quarter = SparkSubmitOperator(
    task_id="collect_income_quarter",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_income_quarter.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_income_quarter_ds": " {{ ts_nodash_with_tz }} "},
)

collect_key_metrics = SparkSubmitOperator(
    task_id="collect_key_metrics",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_key_metrics.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_key_metrics_ds": " {{ ts_nodash_with_tz }} "},
)

collect_key_metrics_quarter = SparkSubmitOperator(
    task_id="collect_key_metrics_quarter",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_key_metrics_quarter.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_key_metrics_quarter_ds": " {{ ts_nodash_with_tz }} "},
)

collect_ratios = SparkSubmitOperator(
    task_id="collect_ratios",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_ratios.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_ratios_ds": " {{ ts_nodash_with_tz }} "},
)

collect_ratios_quarter = SparkSubmitOperator(
    task_id="collect_ratios_quarter",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_ratios_quarter.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_ratios_quarter_ds": " {{ ts_nodash_with_tz }} "},
)

collect_earnings_surprises = SparkSubmitOperator(
    task_id="collect_earnings_surprises",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_earnings_surprises.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_earnings_surprises_ds": " {{ ts_nodash_with_tz }} "},
)

collect_insider_trading = SparkSubmitOperator(
    task_id="collect_insider_trading",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_insider_trading.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_insider_trading_ds": " {{ ts_nodash_with_tz }} "},
)

collect_stock_news = SparkSubmitOperator(
    task_id="collect_stock_news",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_stock_news.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_stock_news_ds": " {{ ts_nodash_with_tz }} "},
)

collect_press_releases = SparkSubmitOperator(
    task_id="collect_press_releases",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_press_releases.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_press_releases_ds": " {{ ts_nodash_with_tz }} "},
)

collect_company_profile = SparkSubmitOperator(
    task_id="collect_company_profile",
    application=f"{pyspark_app_home}/dags/fmp/runner/collect_company_profile.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"collect_company_profile_ds": " {{ ts_nodash_with_tz }} "},
)

full_gardening = SparkSubmitOperator(
    task_id="full_gardening",
    application=f"{pyspark_app_home}/dags/fmp/runner/full_gardening.py",
    executor_memory="15g",
    driver_memory="15g",
    name="{{ task_instance.task_id }}",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
    env_vars={"gardening_ds": " {{ ts_nodash_with_tz }} "},
)

attach_metrics = PythonOperator(
    task_id="attach_metrics",
    python_callable=asset_metrics.attach_metrics,
    op_kwargs={"ds": " {{ execution_date }} ", "window": 20, "yesterday": "True"},
    dag=dag,
    execution_timeout=timedelta(minutes=10),
)

[
    distribute_append_price >> full_gardening >> attach_metrics,
    collect_peers,
    collect_dcf,
    collect_rating,
    collect_enterprise_values_annual,
    collect_enterprise_values_quarter,
    collect_grade,
    collect_sentiment,
    collect_analyst_estimates,
    collect_analyst_estimates_quarter,
    collect_balance_sheets,
    collect_balance_sheets_quarter,
    collect_cash_flow,
    collect_cash_flow_quarter,
    collect_income_quarter,
    collect_key_metrics,
    collect_key_metrics_quarter,
    collect_ratios,
    collect_ratios_quarter,
    collect_earnings_surprises,
    collect_insider_trading,
    collect_stock_news,
    collect_press_releases,
    collect_company_profile,
]
