## Test: 
# 1) file imporats
# 2) web scrapping
# 3) running spark notebook


import datetime as dt
import logging
import json
import os

import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

# pylint: disable=logging-format-interpolation

with DAG(
    dag_id="test_dag",
    description="Tests airflow setup.",
    start_date=dt.datetime(2020, 5, 22),
    schedule_interval="@daily",
) as dag:

    def _test(**_):
        return True

    test = PythonOperator(
        task_id="_test",
        python_callable=_test
    )

    def _test_two(**_):
        return True

    test_two = PythonOperator(
        task_id="_test_two",
        python_callable=_test_two
    )

    test >> test_two