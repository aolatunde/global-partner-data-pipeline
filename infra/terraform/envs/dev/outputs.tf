output "data_bucket_name" {
  value = module.s3.bucket_name
}

output "watermark_table_name" {
  value = module.dynamodb.table_name
}

output "glue_role_arn" {
  value = module.iam.glue_role_arn
}
