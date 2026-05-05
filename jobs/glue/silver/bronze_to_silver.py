import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket_name"])
bucket_name = args["bucket_name"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bronze_base = f"s3://{bucket_name}/bronze"
silver_base = f"s3://{bucket_name}/silver"

def log(msg): print(msg)

def deduplicate_latest(df, business_keys, order_col="bronze_ingestion_ts"):
    w = Window.partitionBy(*business_keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

def trim_str(c): return F.trim(F.col(c)).cast(StringType())

def write_parquet(df, path, partition_cols=None):
    writer = df.write.mode("overwrite").format("parquet").option("compression", "snappy")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
    log(f"Wrote {path}")

bronze_order_items = spark.read.parquet(f"{bronze_base}/order_items/")
bronze_order_item_options = spark.read.parquet(f"{bronze_base}/order_item_options/")
bronze_date_dim = spark.read.parquet(f"{bronze_base}/date_dim/")

if "option_quantity" not in bronze_order_item_options.columns and "option quantity" in bronze_order_item_options.columns:
    bronze_order_item_options = bronze_order_item_options.withColumnRenamed("option quantity", "option_quantity")

order_items_raw = bronze_order_items.select(
    "app_name", "restaurant_id", "creation_time_utc", "order_id", "user_id",
    "printed_card_number", "is_loyalty", "currency", "lineitem_id",
    "item_category", "item_name", "item_price", "item_quantity", "bronze_ingestion_ts"
)

silver_order_items = deduplicate_latest(order_items_raw, ["order_id", "lineitem_id"])
silver_order_items = (
    silver_order_items
    .withColumn("app_name", trim_str("app_name"))
    .withColumn("restaurant_id", trim_str("restaurant_id"))
    .withColumn("creation_time_utc", F.to_timestamp("creation_time_utc"))
    .withColumn("order_id", trim_str("order_id"))
    .withColumn("user_id", trim_str("user_id"))
    .withColumn("printed_card_number", trim_str("printed_card_number"))
    .withColumn("is_loyalty", F.col("is_loyalty").cast(BooleanType()))
    .withColumn("currency", trim_str("currency"))
    .withColumn("lineitem_id", trim_str("lineitem_id"))
    .withColumn("item_category", trim_str("item_category"))
    .withColumn("item_name", trim_str("item_name"))
    .withColumn("item_price", F.coalesce(F.col("item_price").cast(DoubleType()), F.lit(0.0)))
    .withColumn("item_quantity", F.coalesce(F.col("item_quantity").cast(IntegerType()), F.lit(1)))
    .withColumn("base_item_total", F.col("item_price") * F.col("item_quantity"))
    .withColumn("order_date", F.to_date("creation_time_utc"))
    .withColumn("order_year", F.year("creation_time_utc"))
    .withColumn("order_month", F.month("creation_time_utc"))
    .withColumn("order_day", F.dayofmonth("creation_time_utc"))
    .withColumn("silver_processed_ts", F.current_timestamp())
)

options_raw = bronze_order_item_options.select(
    "order_id", "lineitem_id", "option_group_name", "option_name",
    "option_price", "option_quantity", "bronze_ingestion_ts"
)

silver_order_item_options = deduplicate_latest(
    options_raw, ["order_id", "lineitem_id", "option_group_name", "option_name"]
)
silver_order_item_options = (
    silver_order_item_options
    .withColumn("order_id", trim_str("order_id"))
    .withColumn("lineitem_id", trim_str("lineitem_id"))
    .withColumn("option_group_name", trim_str("option_group_name"))
    .withColumn("option_name", trim_str("option_name"))
    .withColumn("option_price", F.coalesce(F.col("option_price").cast(DoubleType()), F.lit(0.0)))
    .withColumn("option_quantity", F.coalesce(F.col("option_quantity").cast(IntegerType()), F.lit(1)))
    .withColumn("option_total", F.col("option_price") * F.col("option_quantity"))
    .withColumn("silver_processed_ts", F.current_timestamp())
)

date_dim_raw = bronze_date_dim.select(
    "date_key", "day_of_week", "week", "month", "year",
    "is_weekend", "is_holiday", "holiday_name", "bronze_ingestion_ts"
)

silver_date_dim = deduplicate_latest(date_dim_raw, ["date_key"])
silver_date_dim = (
    silver_date_dim
    .withColumn("date_key", F.to_date("date_key"))
    .withColumn("day_of_week", trim_str("day_of_week"))
    .withColumn("week", F.col("week").cast(IntegerType()))
    .withColumn("month", F.col("month").cast(IntegerType()))
    .withColumn("year", F.col("year").cast(IntegerType()))
    .withColumn("is_weekend", F.col("is_weekend").cast(BooleanType()))
    .withColumn("is_holiday", F.col("is_holiday").cast(BooleanType()))
    .withColumn("holiday_name", trim_str("holiday_name"))
    .withColumn("silver_processed_ts", F.current_timestamp())
)

option_agg = (
    silver_order_item_options
    .groupBy("order_id", "lineitem_id")
    .agg(
        F.sum("option_total").alias("options_total"),
        F.count("*").alias("option_row_count"),
        F.countDistinct("option_name").alias("distinct_option_count")
    )
)

silver_order_line_enriched = (
    silver_order_items.alias("oi")
    .join(option_agg.alias("oa"), ["order_id", "lineitem_id"], "left")
    .join(silver_date_dim.alias("dd"), F.col("oi.order_date") == F.col("dd.date_key"), "left")
    .select(
        F.col("oi.app_name"), F.col("oi.restaurant_id"), F.col("oi.creation_time_utc"),
        F.col("oi.order_date"), F.col("oi.order_id"), F.col("oi.user_id"),
        F.col("oi.printed_card_number"), F.col("oi.is_loyalty"), F.col("oi.currency"),
        F.col("oi.lineitem_id"), F.col("oi.item_category"), F.col("oi.item_name"),
        F.col("oi.item_price"), F.col("oi.item_quantity"), F.col("oi.base_item_total"),
        F.coalesce(F.col("oa.options_total"), F.lit(0.0)).alias("options_total"),
        F.coalesce(F.col("oa.option_row_count"), F.lit(0)).alias("option_row_count"),
        F.coalesce(F.col("oa.distinct_option_count"), F.lit(0)).alias("distinct_option_count"),
        (F.col("oi.base_item_total") + F.coalesce(F.col("oa.options_total"), F.lit(0.0))).alias("final_line_total"),
        F.col("dd.day_of_week"), F.col("dd.week").alias("calendar_week"),
        F.col("dd.month").alias("calendar_month"), F.col("dd.year").alias("calendar_year"),
        F.col("dd.is_weekend"), F.col("dd.is_holiday"), F.col("dd.holiday_name"),
        F.current_timestamp().alias("silver_processed_ts")
    )
    .withColumn("p_year", F.year("order_date"))
    .withColumn("p_month", F.month("order_date"))
    .withColumn("p_day", F.dayofmonth("order_date"))
)

write_parquet(silver_order_items, f"{silver_base}/silver_order_items/", ["order_year", "order_month", "order_day"])
write_parquet(silver_order_item_options, f"{silver_base}/silver_order_item_options/")
write_parquet(silver_date_dim, f"{silver_base}/silver_date_dim/")
write_parquet(silver_order_line_enriched, f"{silver_base}/silver_order_line_enriched/", ["p_year", "p_month", "p_day"])

job.commit()
