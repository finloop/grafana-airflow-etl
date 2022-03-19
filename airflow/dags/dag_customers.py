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
def customers_dag():
    customers = S3toDataFrame(
        filename="olist_customers_dataset.csv",
        task_id="extract_customers_dataset",
    )

    orders = S3toDataFrame(
        filename="olist_orders_dataset.csv",
        task_id="extract_olist_orders_dataset_dataset",
    )

    @task
    def merge_orders_customers(customers, orders):
        import pandas as pd
        df_orders_customers = orders.merge(customers, on="customer_id")
        return df_orders_customers

    @task()
    def orders_num_per_customer(df_orders_customers):
        import pandas as pd
        df_orders_num_per_customer = df_orders_customers.groupby("customer_unique_id").count().iloc[:, 0]
        df_orders_num_per_customer = pd.DataFrame(df_orders_num_per_customer)
        df_orders_num_per_customer.columns = ["orders_num_per_customer"]

        return df_orders_num_per_customer

    @task
    def group_by_order_count(df_orders_num_per_customer):
        import pandas as pd

        customers_with_order_count = pd.DataFrame(df_orders_num_per_customer.value_counts())
        customers_with_order_count.columns = ["number_of_customers"]
        return customers_with_order_count

    @task
    def add_returing_column(customers, orders_num_per_customer):
        import pandas as pd
        customers["returing"] = customers.customer_unique_id.map(orders_num_per_customer.iloc[:, 0]> 1)

        return customers

    merge_orders_customers_result = merge_orders_customers(customers=customers.output, orders=orders.output)
    orders_num_per_customer_result = orders_num_per_customer(df_orders_customers=merge_orders_customers_result)
    group_by_order_count_result = group_by_order_count(df_orders_num_per_customer=orders_num_per_customer_result)
    add_returing_column_result = add_returing_column(customers=merge_orders_customers_result, orders_num_per_customer=orders_num_per_customer_result)

    DataFrametoPostgresOverrideOperator(
        task_id="upload_to_postgres_group_by_order_count_result",
        table_name="group_by_order_count_result",
        data=group_by_order_count_result,
    )

    DataFrametoPostgresOverrideOperator(
        task_id="upload_to_postgres_customers_result",
        table_name="customers",
        data=add_returing_column_result,
    )


my_dag = customers_dag()
