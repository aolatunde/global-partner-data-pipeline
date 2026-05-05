variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project_name" {
  type    = string
  default = "global-partner"
}

variable "data_bucket_name" {
  type    = string
  default = "ola-global-partner-project-bucket"
}

variable "kms_key_arn" {
  type        = string
  description = "Optional KMS key ARN used for encrypted S3 writes."
  default     = ""
}

variable "sql_secret_name" {
  type    = string
  default = "golbal-partner-db-secrete"
}
