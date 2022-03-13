import json

import pendulum
from operators.s3_operator import S3toStringOperator

from airflow.decorators import dag, task


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["piotrek"],
)
def my_taskflow_api_etl():
    extract = S3toStringOperator(
        filename="olist_customers_dataset.csv",
        task_id="extract_olist_customers_dataset",
    )

    @task()
    def log_df(data):
        print(data)

    log_df(extract.output["data"])



my_dag = my_taskflow_api_etl()
