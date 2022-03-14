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
def customers_dag():
    customers = S3toJSONOperator(
        filename="olist_customers_dataset.csv",
        task_id="extract_customers_dataset",
    )

    orders = S3toJSONOperator(
        filename="olist_orders_dataset.csv",
        task_id="extract_olist_orders_dataset_dataset",
    )

    @task()
    def orders_num_per_customer(f_customers, f_orders):
        import pandas as pd

        customers = pd.read_json(f_customers)
        orders = pd.read_json(f_orders)

        df_orders_num_per_customer = orders.merge(customers, on="customer_id").groupby("customer_unique_id").count().iloc[:, 0]
        df_orders_num_per_customer = pd.DataFrame(df_orders_num_per_customer)
        df_orders_num_per_customer.columns = ["orders_num_per_customer"]

        return df_orders_num_per_customer.to_json()

    @task
    def group_by_order_count(f_orders_num_per_customer):
        import pandas as pd

        df_orders_num_per_customer = pd.read_json(f_orders_num_per_customer)
        customers_with_order_count = pd.DataFrame(df_orders_num_per_customer.value_counts())
        customers_with_order_count.columns = ["number_of_customers"]
        return customers_with_order_count.to_json()

    
    orders_num_per_customer_result = orders_num_per_customer(f_customers=customers.output, f_orders=orders.output)
    group_by_order_count_result = group_by_order_count(f_orders_num_per_customer=orders_num_per_customer_result)

    JSONtoPostgresOverrideOperator(
        task_id="upload_to_postgres_group_by_order_count_result",
        table_name="group_by_order_count_result",
        data=group_by_order_count_result,
    )


my_dag = customers_dag()
