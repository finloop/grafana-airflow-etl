import io
import boto3
import pandas as pd
from airflow.models.baseoperator import BaseOperator


class S3toStringOperator(BaseOperator):
    def __init__(
        self,
        filename: str,
        endpoint_url: str = "http://minio:9000",
        aws_access_key_id: str = "myaccesskey",
        aws_secret_access_key: str = "mysecretkey",
        bucket: str = "datasets",
        *args,
        **kwargs
    ):
        super(S3toStringOperator, self).__init__(*args, **kwargs)
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.bucket = bucket
        self.filename = filename

    def execute(self, context):
        s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )
        obj = s3.get_object(Bucket=self.bucket, Key=self.filename)
        data = str(io.BytesIO(obj["Body"].read()))
        return {"data": data}
