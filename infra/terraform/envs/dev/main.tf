module "s3" {
  source      = "../../modules/s3"
  bucket_name = var.data_bucket_name
}

module "dynamodb" {
  source     = "../../modules/dynamodb"
  table_name = "global_partner_pipeline_watermarks"
}

module "iam" {
  source             = "../../modules/iam"
  project_name       = var.project_name
  data_bucket_name   = var.data_bucket_name
  dynamodb_table_arn = module.dynamodb.table_arn
  sql_secret_name    = var.sql_secret_name
  kms_key_arn        = var.kms_key_arn
}

module "glue" {
  source           = "../../modules/glue"
  project_name     = var.project_name
  glue_role_arn    = module.iam.glue_role_arn
  data_bucket_name = var.data_bucket_name
  aws_region       = var.aws_region
  sql_secret_name  = var.sql_secret_name
  dynamodb_table   = module.dynamodb.table_name
}

module "stepfunctions" {
  source       = "../../modules/stepfunctions"
  project_name = var.project_name
  sfn_role_arn = module.iam.sfn_role_arn
}
