import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket_name"])
bucket_name = args["bucket_name"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

silver_base = f"s3://{bucket_name}/silver"
gold_base = f"s3://{bucket_name}/gold"

def log(msg): print(msg)

def write_parquet(df, path, partition_cols=None):
    writer = df.write.mode("overwrite").format("parquet").option("compression", "snappy")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
    log(f"Wrote {path}")

base = spark.read.parquet(f"{silver_base}/silver_order_line_enriched/")
date_dim = spark.read.parquet(f"{silver_base}/silver_date_dim/")

base = (
    base
    .withColumn("app_name", F.trim(F.col("app_name")).cast(StringType()))
    .withColumn("restaurant_id", F.trim(F.col("restaurant_id")).cast(StringType()))
    .withColumn("order_id", F.trim(F.col("order_id")).cast(StringType()))
    .withColumn("user_id", F.trim(F.col("user_id")).cast(StringType()))
    .withColumn("lineitem_id", F.trim(F.col("lineitem_id")).cast(StringType()))
    .withColumn("item_category", F.trim(F.col("item_category")).cast(StringType()))
    .withColumn("item_name", F.trim(F.col("item_name")).cast(StringType()))
    .withColumn("currency", F.trim(F.col("currency")).cast(StringType()))
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("item_quantity", F.coalesce(F.col("item_quantity"), F.lit(0)))
    .withColumn("base_item_total", F.coalesce(F.col("base_item_total"), F.lit(0.0)))
    .withColumn("options_total", F.coalesce(F.col("options_total"), F.lit(0.0)))
    .withColumn("final_line_total", F.coalesce(F.col("final_line_total"), F.lit(0.0)))
    .withColumn("is_loyalty", F.coalesce(F.col("is_loyalty"), F.lit(False)))
)

gold_order_summary = (
    base.groupBy("order_date", "restaurant_id", "order_id", "user_id", "currency", "is_loyalty")
    .agg(
        F.min("creation_time_utc").alias("order_created_ts"),
        F.first("app_name", ignorenulls=True).alias("app_name"),
        F.countDistinct("lineitem_id").alias("distinct_line_item_count"),
        F.sum("item_quantity").alias("total_item_quantity"),
        F.sum("base_item_total").alias("order_base_item_total"),
        F.sum("options_total").alias("order_options_total"),
        F.sum("final_line_total").alias("order_total_amount"),
        F.countDistinct("item_name").alias("distinct_item_count")
    )
    .withColumn("avg_revenue_per_line_item", F.when(F.col("distinct_line_item_count") > 0, F.col("order_total_amount") / F.col("distinct_line_item_count")).otherwise(F.lit(0.0)))
    .withColumn("p_year", F.year("order_date"))
    .withColumn("p_month", F.month("order_date"))
    .withColumn("p_day", F.dayofmonth("order_date"))
    .withColumn("gold_processed_ts", F.current_timestamp())
)

gold_daily_restaurant_sales = (
    gold_order_summary.groupBy("order_date", "restaurant_id", "currency")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("user_id").alias("distinct_customers"),
        F.sum("distinct_line_item_count").alias("total_line_items"),
        F.sum("total_item_quantity").alias("total_quantity_sold"),
        F.sum("order_base_item_total").alias("base_item_revenue"),
        F.sum("order_options_total").alias("option_revenue"),
        F.sum("order_total_amount").alias("gross_revenue"),
        F.sum(F.when(F.col("is_loyalty") == True, F.col("order_total_amount")).otherwise(F.lit(0.0))).alias("loyalty_revenue"),
        F.sum(F.when(F.col("is_loyalty") == False, F.col("order_total_amount")).otherwise(F.lit(0.0))).alias("non_loyalty_revenue")
    )
    .withColumn("average_order_value", F.when(F.col("total_orders") > 0, F.col("gross_revenue") / F.col("total_orders")).otherwise(F.lit(0.0)))
    .withColumn("revenue_per_customer", F.when(F.col("distinct_customers") > 0, F.col("gross_revenue") / F.col("distinct_customers")).otherwise(F.lit(0.0)))
    .withColumn("p_year", F.year("order_date"))
    .withColumn("p_month", F.month("order_date"))
    .withColumn("p_day", F.dayofmonth("order_date"))
    .withColumn("gold_processed_ts", F.current_timestamp())
)

gold_item_performance = (
    base.groupBy("order_date", "restaurant_id", "currency", "item_category", "item_name")
    .agg(
        F.countDistinct("order_id").alias("distinct_orders"),
        F.countDistinct("user_id").alias("distinct_customers"),
        F.sum("item_quantity").alias("total_quantity_sold"),
        F.sum("base_item_total").alias("item_base_revenue"),
        F.sum("options_total").alias("item_option_revenue"),
        F.sum("final_line_total").alias("item_total_revenue"),
        F.avg("item_price").alias("avg_item_price"),
        F.sum(F.when(F.col("is_loyalty") == True, F.col("final_line_total")).otherwise(F.lit(0.0))).alias("loyalty_item_revenue"),
        F.sum(F.when(F.col("is_loyalty") == False, F.col("final_line_total")).otherwise(F.lit(0.0))).alias("non_loyalty_item_revenue")
    )
    .withColumn("avg_quantity_per_order", F.when(F.col("distinct_orders") > 0, F.col("total_quantity_sold") / F.col("distinct_orders")).otherwise(F.lit(0.0)))
    .withColumn("p_year", F.year("order_date"))
    .withColumn("p_month", F.month("order_date"))
    .withColumn("p_day", F.dayofmonth("order_date"))
    .withColumn("gold_processed_ts", F.current_timestamp())
)

gold_loyalty_sales_summary = (
    gold_order_summary
    .withColumn("loyalty_segment", F.when(F.col("is_loyalty") == True, F.lit("LOYALTY")).otherwise(F.lit("NON_LOYALTY")))
    .groupBy("order_date", "restaurant_id", "currency", "loyalty_segment")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("user_id").alias("distinct_customers"),
        F.sum("total_item_quantity").alias("total_item_quantity"),
        F.sum("order_base_item_total").alias("base_item_revenue"),
        F.sum("order_options_total").alias("option_revenue"),
        F.sum("order_total_amount").alias("gross_revenue")
    )
    .withColumn("average_order_value", F.when(F.col("total_orders") > 0, F.col("gross_revenue") / F.col("total_orders")).otherwise(F.lit(0.0)))
    .withColumn("revenue_per_customer", F.when(F.col("distinct_customers") > 0, F.col("gross_revenue") / F.col("distinct_customers")).otherwise(F.lit(0.0)))
    .withColumn("p_year", F.year("order_date"))
    .withColumn("p_month", F.month("order_date"))
    .withColumn("p_day", F.dayofmonth("order_date"))
    .withColumn("gold_processed_ts", F.current_timestamp())
)

calendar_lookup = date_dim.select(
    F.col("date_key").alias("calendar_date"),
    "day_of_week", "week", "month", "year", "is_weekend", "is_holiday", "holiday_name"
)

def add_calendar(df):
    return df.alias("g").join(calendar_lookup.alias("c"), F.col("g.order_date") == F.col("c.calendar_date"), "left").drop("calendar_date")

write_parquet(add_calendar(gold_order_summary), f"{gold_base}/gold_order_summary/", ["p_year", "p_month", "p_day"])
write_parquet(add_calendar(gold_daily_restaurant_sales), f"{gold_base}/gold_daily_restaurant_sales/", ["p_year", "p_month", "p_day"])
write_parquet(add_calendar(gold_item_performance), f"{gold_base}/gold_item_performance/", ["p_year", "p_month", "p_day"])
write_parquet(add_calendar(gold_loyalty_sales_summary), f"{gold_base}/gold_loyalty_sales_summary/", ["p_year", "p_month", "p_day"])

job.commit()
