import pendulum
from airflow.decorators import dag, task

from operators.s3_operator import S3toDataFrame
from operators.postgres import DataFrametoPostgresOverrideOperator


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["piotrek"],
)
def top_sellers_dag():
    sellers = S3toDataFrame(
        filename="olist_sellers_dataset.csv",
        task_id="extract_olist_sellers_dataset",
    )

    @task()
    def transform(df_sellers):
        import pandas as pd
        sellers = df_sellers
        top_sellers = sellers.seller_state.value_counts()[:5]
        sellers.loc[~sellers.seller_state.isin(top_sellers.index), "seller_state"] = "other"
        
        top_sellers = pd.DataFrame(sellers.seller_state.value_counts())
        top_sellers.columns = ["sellers_count"]

        return top_sellers
    
    transformed = transform(df_sellers=sellers.output)

    load = DataFrametoPostgresOverrideOperator(
        task_id="upload_to_postgres",
        table_name="top_sellers",
        data=transformed,
    )

    sellers >> transformed
    transformed >> load


my_dag = top_sellers_dag()
