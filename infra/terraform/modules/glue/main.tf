variable "project_name" { type = string }
variable "glue_role_arn" { type = string }
variable "data_bucket_name" { type = string }
variable "aws_region" { type = string }
variable "sql_secret_name" { type = string }
variable "dynamodb_table" { type = string }

resource "aws_glue_job" "bronze" {
  name     = "sqlserver_all_tables_to_bronze"
  role_arn = var.glue_role_arn

  command {
    script_location = "s3://${var.data_bucket_name}/scripts/glue/bronze/sqlserver_all_tables_to_bronze.py"
    python_version  = "3"
  }

  glue_version      = "5.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 120

  default_arguments = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                  = "true"
    "--job-language"                    = "python"
    "--secret_name"                     = var.sql_secret_name
    "--aws_region"                      = var.aws_region
    "--target_bucket"                   = var.data_bucket_name
    "--dynamodb_table"                  = var.dynamodb_table
    "--pipeline_name"                   = "sqlserver_bronze_ingestion"
    "--default_bookmark_value"          = "1900-01-01 00:00:00"
    "--fetchsize"                       = "10000"
  }
}

resource "aws_glue_job" "silver" {
  name     = "global_partner_bronze_to_silver"
  role_arn = var.glue_role_arn

  command {
    script_location = "s3://${var.data_bucket_name}/scripts/glue/silver/bronze_to_silver.py"
    python_version  = "3"
  }

  glue_version      = "5.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 120

  default_arguments = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                  = "true"
    "--job-language"                    = "python"
    "--bucket_name"                     = var.data_bucket_name
  }
}

resource "aws_glue_job" "gold" {
  name     = "global_partner_silver_to_gold"
  role_arn = var.glue_role_arn

  command {
    script_location = "s3://${var.data_bucket_name}/scripts/glue/gold/silver_to_gold.py"
    python_version  = "3"
  }

  glue_version      = "5.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 120

  default_arguments = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                  = "true"
    "--job-language"                    = "python"
    "--bucket_name"                     = var.data_bucket_name
  }
}
