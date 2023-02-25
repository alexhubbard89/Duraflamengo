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
from fmp import stocks
import derived_metrics.technicals as technicals
import graphs.candlestick as candlestick
import graphs.price_consolidation as price_consolidation

## get global vars
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
local_tz = pendulum.timezone("US/Eastern")
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "start_date": datetime(2023, 2, 21, tzinfo=local_tz),
    "email": ["alexhubbard89@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

## make DAG tree
dag = DAG(
    dag_id="watchlist-enrichment",
    default_args=default_args,
    catchup=False,
    schedule_interval="00 5 * * *",  ## 5:00am Daily
)

collect_watchlist_daily_price = PythonOperator(
    task_id="collect_watchlist_daily_price",
    python_callable=stocks.collect_watchlist_daily_price,
    op_kwargs={"ds": " {{ ts_nodash_with_tz }} "},
    dag=dag,
)

candlestick_graph_prep = PythonOperator(
    task_id="candlestick_graph_prep",
    python_callable=technicals.candlestick_graph_prep,
    dag=dag,
)

graph_watchlist_candlesticks = PythonOperator(
    task_id="graph_watchlist_candlesticks",
    python_callable=candlestick.graph_watchlist_candlesticks,
    dag=dag,
)

price_consolidation_watchlist_make_sparse = SparkSubmitOperator(
    task_id="price_consolidation_watchlist_make_sparse",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/price_consolidation_watchlist_make_sparse.py",
    executor_memory="15g",
    driver_memory="15g",
    name="price_consolidation_watchlist_make_sparse",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

price_consolidation_watchlist_price_heatmap = SparkSubmitOperator(
    task_id="price_consolidation_watchlist_price_heatmap",
    application=f"{pyspark_app_home}/dags/derived_metrics/runner/price_consolidation_watchlist_price_heatmap.py",
    executor_memory="15g",
    driver_memory="15g",
    name="price_consolidation_watchlist_price_heatmap",
    execution_timeout=timedelta(minutes=10),
    conf={"master": "spark://localhost:7077"},
    dag=dag,
)

graph_watchlist_consolidation_max = PythonOperator(
    task_id="graph_watchlist_consolidation_max",
    python_callable=price_consolidation.graph_watchlist_consolidation,
    op_kwargs={"method": "max"},
    dag=dag,
)

graph_watchlist_consolidation_avg = PythonOperator(
    task_id="graph_watchlist_consolidation_avg",
    python_callable=price_consolidation.graph_watchlist_consolidation,
    op_kwargs={"method": "avg"},
    dag=dag,
)

[
    [
        collect_watchlist_daily_price
        >> candlestick_graph_prep
        >> graph_watchlist_candlesticks
    ],
    [
        collect_watchlist_daily_price
        >> price_consolidation_watchlist_make_sparse
        >> price_consolidation_watchlist_price_heatmap
        >> [graph_watchlist_consolidation_max, graph_watchlist_consolidation_avg]
    ],
]
