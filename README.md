# Duraflamengo

Steps to complete in set up tests before making production pipiline.

```
DONE: 
- file imports
- web scrapping
- running spark notebook

TODO:
- Remove test code
- Connect to postgres
    - Ensure that postgress query executes
    - Print length of list of returned tickers
- Point to datalake in airflow and environment vars
- Parallelize requests
- Unpack data
- Write to data lake
- Add airflow variable for date
    - This will be good to track on the airflow side
    - Might as well pass it as a notebook variable to get used to it and keep consistency among the date.
``` 

After chaning dag files make sure to copy it to the dag bag: `cp {NEW_DAG_FILE}.py ~/airflow/dags/`