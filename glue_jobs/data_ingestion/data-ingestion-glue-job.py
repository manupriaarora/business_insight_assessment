import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import boto3
import json

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

secret = get_secret('rds_credentials_secret')

connection_properties = {
    "user": secret['username'],
    "password": secret['password'],
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

print(f"connection_properties={connection_properties}")

jdbc_url = "jdbc:sqlserver://my-sqlserver-db.cmn64k4yi5vh.us-east-1.rds.amazonaws.com:1433;databaseName=GlobalPartners"
try:
    date_dim_df = spark.read.jdbc(url=jdbc_url, table="dbo.date_dim", properties=connection_properties)
    order_items_df = spark.read.jdbc(url=jdbc_url, table="dbo.order_items", properties=connection_properties)
    order_item_options_df = spark.read.jdbc(url=jdbc_url, table="dbo.order_item_options", properties=connection_properties)
    
except Exception as e:
    print(f"Connection failed: {str(e)}")
    raise

# Write to S3 as Parquet
# e.g., s3://my-output-bucket/date_dim/
print(f"Write to S3 as Parquet")
try:
    for df, path in [(date_dim_df, 'date_dim'), (order_items_df, 'order_items'), (order_item_options_df, 'order_item_options')]:
        df.write.mode("overwrite").parquet(f"s3://global-partners-de-project2/landing-zone/{path}/")
        print(f"Successfully wrote data to s3://global-partners-de-project2/landing-zone/{path}/")
except Exception as e:
    print(f"Failed to write to S3: {str(e)}")
    raise
job.commit()
