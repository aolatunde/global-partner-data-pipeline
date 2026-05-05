CREATE EXTERNAL TABLE IF NOT EXISTS global_partner_gold.gold_daily_restaurant_sales (
  order_date date,
  restaurant_id string,
  currency string,
  total_orders bigint,
  distinct_customers bigint,
  total_line_items bigint,
  total_quantity_sold bigint,
  base_item_revenue double,
  option_revenue double,
  gross_revenue double,
  loyalty_revenue double,
  non_loyalty_revenue double,
  average_order_value double,
  revenue_per_customer double,
  day_of_week string,
  week int,
  month int,
  year int,
  is_weekend boolean,
  is_holiday boolean,
  holiday_name string,
  gold_processed_ts timestamp
)
PARTITIONED BY (p_year int, p_month int, p_day int)
STORED AS PARQUET
LOCATION 's3://ola-global-partner-project-bucket/gold/gold_daily_restaurant_sales/';

CREATE EXTERNAL TABLE IF NOT EXISTS global_partner_gold.gold_order_summary (
  order_date date,
  restaurant_id string,
  order_id string,
  user_id string,
  currency string,
  is_loyalty boolean,
  order_created_ts timestamp,
  app_name string,
  distinct_line_item_count bigint,
  total_item_quantity bigint,
  order_base_item_total double,
  order_options_total double,
  order_total_amount double,
  distinct_item_count bigint,
  avg_revenue_per_line_item double,
  day_of_week string,
  week int,
  month int,
  year int,
  is_weekend boolean,
  is_holiday boolean,
  holiday_name string,
  gold_processed_ts timestamp
)
PARTITIONED BY (p_year int, p_month int, p_day int)
STORED AS PARQUET
LOCATION 's3://ola-global-partner-project-bucket/gold/gold_order_summary/';

CREATE EXTERNAL TABLE IF NOT EXISTS global_partner_gold.gold_item_performance (
  order_date date,
  restaurant_id string,
  currency string,
  item_category string,
  item_name string,
  distinct_orders bigint,
  distinct_customers bigint,
  total_quantity_sold bigint,
  item_base_revenue double,
  item_option_revenue double,
  item_total_revenue double,
  avg_item_price double,
  loyalty_item_revenue double,
  non_loyalty_item_revenue double,
  avg_quantity_per_order double,
  day_of_week string,
  week int,
  month int,
  year int,
  is_weekend boolean,
  is_holiday boolean,
  holiday_name string,
  gold_processed_ts timestamp
)
PARTITIONED BY (p_year int, p_month int, p_day int)
STORED AS PARQUET
LOCATION 's3://ola-global-partner-project-bucket/gold/gold_item_performance/';

CREATE EXTERNAL TABLE IF NOT EXISTS global_partner_gold.gold_loyalty_sales_summary (
  order_date date,
  restaurant_id string,
  currency string,
  loyalty_segment string,
  total_orders bigint,
  distinct_customers bigint,
  total_item_quantity bigint,
  base_item_revenue double,
  option_revenue double,
  gross_revenue double,
  average_order_value double,
  revenue_per_customer double,
  day_of_week string,
  week int,
  month int,
  year int,
  is_weekend boolean,
  is_holiday boolean,
  holiday_name string,
  gold_processed_ts timestamp
)
PARTITIONED BY (p_year int, p_month int, p_day int)
STORED AS PARQUET
LOCATION 's3://ola-global-partner-project-bucket/gold/gold_loyalty_sales_summary/';
