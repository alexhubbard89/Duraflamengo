import datetime as dt
import logging
import json
import os

import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator


from econ.test import test_function

# pylint: disable=logging-format-interpolation

with DAG(
    dag_id="test_dag",
    description="Tests airflow setup.",
    start_date=dt.datetime(2020, 5, 22),
    schedule_interval="@daily",
) as dag:

    test = PythonOperator(
        task_id="Test-One",
        python_callable=test_function
    )

    def _test_two(**_):
        return True

    test_two = PythonOperator(
        task_id="Test-Two",
        python_callable=_test_two
    )

    run_this = PapermillOperator(
        task_id="Run-Example-Notebook",
        input_nb="econ/hello_world.ipynb"
    )

    test >> test_two >> run_this

print('''




Bring in a notebook and run it with papermill
Testing spark loads
Load spoofer into notebook
Make requests in parallel from hard coded ticker list

THIS WORKED CORRECTLY







END
''')

