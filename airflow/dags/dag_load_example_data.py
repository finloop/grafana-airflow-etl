import pendulum
from airflow.decorators import dag, task

from operators.s3_operator import S3toJSONOperator
from operators.postgres import JSONtoPostgresOverrideOperator


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["piotrek"],
)
def seller_order_items_dag():
    sellers = S3toJSONOperator(
        filename="olist_sellers_dataset.csv",
        task_id="extract_olist_sellers_dataset",
    )

    orders = S3toJSONOperator(
        filename="olist_order_items_dataset.csv",
        task_id="extract_olist_order_items_dataset",
    )

    @task()
    def transform(f_sellers, f_orders):
        import pandas as pd

        df_sellers = pd.read_json(f_sellers["data"])
        df_orders = pd.read_json(f_orders["data"])
        df = df_orders.merge(
            df_sellers, left_on="seller_id", right_on="seller_id", how="inner"
        )
        return df.to_json()
    
    transformed = transform(f_sellers=sellers.output, f_orders=orders.output)

    load = JSONtoPostgresOverrideOperator(
        task_id="upload_to_postgres",
        table_name="sellers_orders",
        data=transformed,
    )

    sellers >> transformed
    orders >> transformed
    transformed >> load


my_dag = seller_order_items_dag()
