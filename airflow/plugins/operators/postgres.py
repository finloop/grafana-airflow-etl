from airflow.models.baseoperator import BaseOperator


class JSONtoPostgresOverrideOperator(BaseOperator):
    template_fields = ('data',)

    def __init__(
        self,
        table_name,
        data,
        connection_uri: str = "postgresql://postgres:postgres@warehouse-postgres:5432/postgres",
        *args,
        **kwargs
    ):
        super(JSONtoPostgresOverrideOperator, self).__init__(*args, **kwargs)
        self.connection_uri = connection_uri
        self.table_name = table_name
        self.data = data

    def execute(self, context):
        import pandas as pd
        from sqlalchemy import create_engine

        con = create_engine(self.connection_uri)
        df = pd.read_json(self.data)

        df.to_sql(name=self.table_name, con=con, if_exists="replace")
