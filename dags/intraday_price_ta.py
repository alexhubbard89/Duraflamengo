from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pendulum

# Assuming the necessary imports are done for the functions you've mentioned
import derived_metrics.settings as der_s
from fmp import stocks

# Define local timezone
local_tz = pendulum.timezone("US/Eastern")

# Retrieve PySpark application home path from Airflow Variables
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")

# DAG definition with timezone
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 22, tzinfo=local_tz),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "intraday_collection_technical_analysis_dag",
    default_args=default_args,
    description="DAG for Intraday Collection and Technical Analysis",
    schedule_interval="*/5 9-16 * * 1-5",
    catchup=False,
    tags=["finance", "market_research"],
)

# Task to update watchlist prices
update_watchlist_prices_task = PythonOperator(
    task_id="update_watchlist_prices",
    python_callable=stocks.update_watchlist_prices,
    dag=dag,
)

# Task to update database


# Setting up the task dependencies
(update_watchlist_prices_task)
