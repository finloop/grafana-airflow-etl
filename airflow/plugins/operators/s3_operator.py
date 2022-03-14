from airflow.models.baseoperator import BaseOperator
class S3toJSONOperator(BaseOperator):
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
        super(S3toJSONOperator, self).__init__(*args, **kwargs)
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.bucket = bucket
        self.filename = filename

    def execute(self, context):
        import io
        import boto3
        import pandas as pd
        s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )
        obj = s3.get_object(Bucket=self.bucket, Key=self.filename)
        data = pd.read_csv(io.BytesIO(obj["Body"].read()))
        return {"data": data.to_json()}
