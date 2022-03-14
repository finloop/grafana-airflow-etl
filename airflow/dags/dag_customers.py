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

    @task
    def merge_orders_customers(f_customers, f_orders):
        import pandas as pd
        customers = pd.read_json(f_customers)
        orders = pd.read_json(f_orders)

        df_orders_customers = orders.merge(customers, on="customer_id")
        return df_orders_customers.to_json()

    @task()
    def orders_num_per_customer(f_orders_customers):
        import pandas as pd

        df_orders_customers = pd.read_json(f_orders_customers)

        df_orders_num_per_customer = df_orders_customers.groupby("customer_unique_id").count().iloc[:, 0]
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

    @task
    def add_returing_column(f_customers, f_orders_num_per_customer):
        import pandas as pd

        customers = pd.read_json(f_customers)
        orders_num_per_customer = pd.read_json(f_orders_num_per_customer)

        customers["returing"] = customers.customer_unique_id.map(orders_num_per_customer.iloc[:, 0]> 1)

        return customers.to_json()

    merge_orders_customers_result = merge_orders_customers(f_customers=customers.output, f_orders=orders.output)
    orders_num_per_customer_result = orders_num_per_customer(f_orders_customers=merge_orders_customers_result)
    group_by_order_count_result = group_by_order_count(f_orders_num_per_customer=orders_num_per_customer_result)
    add_returing_column_result = add_returing_column(f_customers=merge_orders_customers_result, f_orders_num_per_customer=orders_num_per_customer_result)

    JSONtoPostgresOverrideOperator(
        task_id="upload_to_postgres_group_by_order_count_result",
        table_name="group_by_order_count_result",
        data=group_by_order_count_result,
    )

    JSONtoPostgresOverrideOperator(
        task_id="upload_to_postgres_customers_result",
        table_name="customers",
        data=add_returing_column_result,
    )


my_dag = customers_dag()
