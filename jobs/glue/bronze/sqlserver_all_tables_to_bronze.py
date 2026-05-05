import sys
import json
import boto3
from datetime import datetime

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, lit, col, year, month, dayofmonth

required_args = [
    "JOB_NAME",
    "jdbc_url",
    "secret_name",
    "aws_region",
    "target_bucket",
    "dynamodb_table",
    "pipeline_name"
]

optional_args = [
    "default_bookmark_value",
    "fetchsize"
]

present_optional_args = [arg for arg in optional_args if f"--{arg}" in sys.argv]
args = getResolvedOptions(sys.argv, required_args + present_optional_args)

job_name = args["JOB_NAME"]
jdbc_url = args["jdbc_url"]
secret_name = args["secret_name"]
aws_region = args["aws_region"]
target_bucket = args["target_bucket"]
dynamodb_table = args["dynamodb_table"]
pipeline_name = args["pipeline_name"]
default_bookmark_value = args.get("default_bookmark_value", "1900-01-01 00:00:00")
fetchsize = args.get("fetchsize", "10000")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(job_name, args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

secrets_client = boto3.client("secretsmanager", region_name=aws_region)
dynamodb = boto3.resource("dynamodb", region_name=aws_region)
watermark_table = dynamodb.Table(dynamodb_table)

def log(message):
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def get_secret(secret_name: str) -> dict:
    response = secrets_client.get_secret_value(SecretId=secret_name)
    if "SecretString" not in response:
        raise Exception(f"Secret {secret_name} does not contain SecretString")
    return json.loads(response["SecretString"])

def get_bookmark_value(table, pipeline_name: str, default_value: str) -> str:
    response = table.get_item(Key={"pipeline_name": pipeline_name})
    item = response.get("Item")
    if item and "bookmark_value" in item:
        return item["bookmark_value"]
    return default_value

def update_bookmark_value(table, pipeline_name: str, new_bookmark_value: str):
    table.put_item(
        Item={
            "pipeline_name": pipeline_name,
            "bookmark_value": str(new_bookmark_value),
            "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        }
    )

secret = get_secret(secret_name)
jdbc_user = secret["username"]
jdbc_password = secret["password"]

bookmark_value = get_bookmark_value(
    table=watermark_table,
    pipeline_name=pipeline_name,
    default_value=default_bookmark_value
)

log(f"Using bookmark_value: {bookmark_value}")

table_configs = [
    {
        "table_name": "order_items",
        "query": f"""
            (
                SELECT
                    app_name,
                    restaurant_id,
                    creation_time_utc,
                    order_id,
                    user_id,
                    printed_card_number,
                    is_loyalty,
                    currency,
                    lineitem_id,
                    item_category,
                    item_name,
                    item_price,
                    item_quantity
                FROM dbo.order_items
                WHERE creation_time_utc > '{bookmark_value}'
            ) src
        """,
        "partition_date_column": "creation_time_utc",
        "track_watermark": True,
        "write_mode": "append"
    },
    {
        "table_name": "order_item_options",
        "query": """
            (
                SELECT
                    order_id,
                    lineitem_id,
                    option_group_name,
                    option_name,
                    option_price,
                    option_quantity
                FROM dbo.order_item_options
            ) src
        """,
        "partition_date_column": None,
        "track_watermark": False,
        "write_mode": "append"
    },
    {
        "table_name": "date_dim",
        "query": """
            (
                SELECT
                    date_key,
                    day_of_week,
                    week,
                    month,
                    year,
                    is_weekend,
                    is_holiday,
                    holiday_name
                FROM dbo.date_dim
            ) src
        """,
        "partition_date_column": None,
        "track_watermark": False,
        "write_mode": "overwrite"
    }
]

def read_from_sql(query: str):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("fetchsize", fetchsize)
        .load()
    )

def prepare_bronze_df(df, table_name: str, partition_date_column: str = None):
    df_bronze = (
        df.withColumn("bronze_ingestion_ts", current_timestamp())
          .withColumn("bronze_source_table", lit(table_name))
    )

    if partition_date_column and partition_date_column in df_bronze.columns:
        df_bronze = (
            df_bronze
            .withColumn("partition_date", F.to_date(col(partition_date_column)))
            .withColumn("p_year", year(col("partition_date")))
            .withColumn("p_month", month(col("partition_date")))
            .withColumn("p_day", dayofmonth(col("partition_date")))
        )
    else:
        df_bronze = (
            df_bronze
            .withColumn("p_year", year(col("bronze_ingestion_ts")))
            .withColumn("p_month", month(col("bronze_ingestion_ts")))
            .withColumn("p_day", dayofmonth(col("bronze_ingestion_ts")))
        )

    return df_bronze

def write_bronze(df_bronze, table_name: str, mode: str):
    target_path = f"s3://{target_bucket}/bronze/{table_name}/"
    (
        df_bronze.write
        .mode(mode)
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("p_year", "p_month", "p_day")
        .save(target_path)
    )
    log(f"Successfully wrote {table_name} to {target_path}")

max_order_items_watermark = None
tables_processed = 0

try:
    for cfg in table_configs:
        table_name = cfg["table_name"]
        query = cfg["query"]
        partition_date_column = cfg["partition_date_column"]
        track_watermark = cfg["track_watermark"]
        write_mode = cfg["write_mode"]

        log(f"Starting extraction for table: {table_name}")
        df = read_from_sql(query)

        row_count = df.count()
        log(f"{table_name} row_count = {row_count}")

        if row_count == 0:
            log(f"No records found for {table_name}")
            continue

        if track_watermark and "creation_time_utc" in df.columns:
            max_order_items_watermark = (
                df.agg(F.max(col("creation_time_utc")).alias("max_watermark"))
                  .collect()[0]["max_watermark"]
            )
            log(f"Current batch max watermark: {max_order_items_watermark}")

        df_bronze = prepare_bronze_df(
            df=df,
            table_name=table_name,
            partition_date_column=partition_date_column
        )

        write_bronze(df_bronze, table_name, write_mode)
        tables_processed += 1

    if max_order_items_watermark is not None:
        update_bookmark_value(
            table=watermark_table,
            pipeline_name=pipeline_name,
            new_bookmark_value=str(max_order_items_watermark)
        )
        log(f"Updated DynamoDB watermark to: {max_order_items_watermark}")
    else:
        log("No new order_items records found, watermark not updated.")

    log(f"Tables processed successfully: {tables_processed}")
    job.commit()

except Exception as e:
    log(f"Bronze job failed: {str(e)}")
    raise
