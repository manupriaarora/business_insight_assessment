import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import row_number, col, to_date, to_timestamp, year, month, weekofyear, lit, date_format
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame
import boto3
import json

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

# --------------------------
# Configuration
# --------------------------
# S3 path to store LPT (JSON with last processed timestamp)
s3_checkpoint_path = "s3://global-partners-de-project2/checkpoints/fact_orders_lpt.json"


# --------------------------
# Load Last Processed Timestamp
# --------------------------
s3 = boto3.client('s3')
bucket, key = s3_checkpoint_path.replace("s3://", "").split("/", 1)

try:
    response = s3.get_object(Bucket=bucket, Key=key)
    last_lpt = json.loads(response['Body'].read())['last_processed_timestamp']
    logger.info(f"Last processed timestamp: {last_lpt}")
except s3.exceptions.NoSuchKey:
    last_lpt = None
except Exception as e:
    print(f"Error reading checkpoint from S3: {str(e)}")
    raise


# ==============================
# Load DataFrames from Glue Catalog
# ==============================
date_dim_df = glueContext.create_dynamic_frame.from_catalog(
    database="landing_zone_db", 
    table_name="date_dim"
).toDF()

order_item_options_df = glueContext.create_dynamic_frame.from_catalog(
    database="landing_zone_db", 
    table_name="order_item_options"
).toDF()

order_item_df = glueContext.create_dynamic_frame.from_catalog(
    database="landing_zone_db", 
    table_name="order_items"
).toDF()

# Convert creation_time_utc from ISO 8601 format to Spark timestamp
order_item_df = order_item_df.withColumn(
    "creation_time_utc",
    to_timestamp(col("creation_time_utc"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
)

logger.info("Order Items Schema:")
order_item_df.printSchema()
logger.info("Order Item Options Schema:")
order_item_options_df.printSchema()
logger.info("Date Dim Schema:")
date_dim_df.printSchema()


# --------------------------
# Incremental filter: only new orders
# --------------------------
# Create an empty DataFrame with the same schema as order_item_df
new_order_item_df = spark.createDataFrame([], schema=order_item_df.schema)

if last_lpt:
    new_order_item_df = order_item_df.filter(col("creation_time_utc") > lit(last_lpt).cast("timestamp"))
else:
    new_order_item_df = order_item_df

if not new_order_item_df.head(1):
    logger.info("No new orders to process. Exiting job.")
    job.commit()

new_order_item_df = new_order_item_df.cache()

#  Filter related order items and options
new_order_item_options_df = order_item_options_df.join(
    new_order_item_df.select("order_id", "lineitem_id"), 
    ["order_id", "lineitem_id"], 
    "inner"
).cache()

# ==============================
# Dimension Tables
# ==============================

# ---Table 1: Date Dimension ---
# Transform the date dim table with proper datatypes
date_dim_transformed_df = (
    date_dim_df
    .withColumn("date_key", to_date(col("date_key"), "dd-MM-yyyy"))
    .withColumn("year", col("year").cast("int"))
)
# Convert back to a DynamicFrame for writing
dynamic_date_dim_df = DynamicFrame.fromDF(date_dim_transformed_df, glueContext, "dynamic_date_dim_df")


# Table 2 - dim_app
dim_app_spark_df = new_order_item_df.select( "app_name").dropDuplicates()
# Use row_number() for strictly sequential IDs
windowSpec_app = Window.orderBy("app_name") 
new_dim_app_spark_df = dim_app_spark_df.withColumn("app_id", row_number().over(windowSpec_app))
# Reorder so app_id is the first column
new_dim_app_spark_df = new_dim_app_spark_df.select("app_id", "app_name")
# Convert back to a DynamicFrame for writing
dynamic_dim_app_df = DynamicFrame.fromDF(new_dim_app_spark_df, glueContext, "dynamic_dim_app_df")


# ==============================
# Fact Tables
# ==============================

# --- Table 3 - Fact Orders ---
fact_orders_spark_joined_df = (
                        new_order_item_df
                        .join(new_dim_app_spark_df, "app_name", "inner")
                        )

fact_orders_spark_results_df = (fact_orders_spark_joined_df.select("order_id", "app_id", "restaurant_id",
                                                                  "user_id", "printed_card_number", "is_loyalty",
                                                                  "creation_time_utc", "currency")
                                                                  .fillna({"user_id": "UNKNOWN"})
                                                                  .dropDuplicates())

transformed_fact_orders_results_df = (fact_orders_spark_results_df
                                      .withColumnRenamed("currency", "currency_used"))
# Convert back to a DynamicFrame for writing
dynamic_fact_orders_df = DynamicFrame.fromDF(transformed_fact_orders_results_df, glueContext, "dynamic_fact_orders_df")


 # ---- Table 4 - Fact Items ----
fact_items_spark_results_df = (new_order_item_df.select("lineitem_id","order_id", "item_category", 
                                                        "item_name", "item_quantity", "item_price")
                                                        .withColumn("item_quantity", col("item_quantity").cast("int"))
                                                        .withColumn("item_price", col("item_price").cast("float"))
                                                        .withColumn("item_total", col("item_quantity") * col("item_price")))
# Convert back to a DynamicFrame for writing
dynamic_fact_items_df = DynamicFrame.fromDF(fact_items_spark_results_df, glueContext, "dynamic_fact_items_df")


# ---- Table 5 - Fact Item Options ----
fact_item_options_spark_results_df = (new_order_item_options_df
                                      .select("lineitem_id", "order_id", "option_group_name", "option_name", "option_quantity", "option_price")
                                      .withColumn("option_quantity", col("option_quantity").cast("float"))
                                      .withColumn("option_price", col("option_price").cast("float"))
                                      .withColumn("option_total", col("option_quantity") * col("option_price"))
                                      .dropDuplicates())

fact_item_options_dynamic_df = DynamicFrame.fromDF(fact_item_options_spark_results_df, glueContext, "fact_item_options_dynamic_df")
                   

# ==============================
# Write Outputs to S3
# ==============================

output_path = "s3://global-partners-de-project2/curated/"

transformed_df_s3_path_list = [(dynamic_date_dim_df, "date_dim"), (dynamic_dim_app_df, "dim_app"), 
                               (dynamic_fact_orders_df, "fact_orders"), (dynamic_fact_items_df, "fact_items"), 
                               (fact_item_options_dynamic_df, "fact_items_options")]

try:
    for df, s3_path in transformed_df_s3_path_list:
        # Write the transformed data to the processed S3 bucket
        glueContext.write_dynamic_frame.from_options(
            frame=df,
            connection_type="s3",
            connection_options={"path": f"{output_path}{s3_path}/"},
            format="parquet"  # It's a best practice to use a columnar format like Parquet
        )
    
    # --------------------------
    # Update Last Processed Timestamp
    # --------------------------
    if new_order_item_df.head(1):
        max_timestamp = new_order_item_df.agg({"creation_time_utc": "max"}).collect()[0][0]
        s3.put_object(
            Bucket=bucket,
            Key=key,    
            Body=json.dumps({"last_processed_timestamp": str(max_timestamp)})
        )
    else:
        max_timestamp = None
        print("No new orders found. Max timestamp is None.")
    
    if max_timestamp:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps({"last_processed_timestamp": str(max_timestamp)})
        )
        print(f"Successfully updated checkpoint: {max_timestamp}")

    job.commit()
except Exception as e:
    print(f"Job failed: {str(e)}")
    raise
