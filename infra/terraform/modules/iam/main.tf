variable "project_name" { type = string }
variable "data_bucket_name" { type = string }
variable "dynamodb_table_arn" { type = string }
variable "sql_secret_name" { type = string }
variable "kms_key_arn" { type = string }

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "glue.amazonaws.com" },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_policy" "glue_policy" {
  name = "${var.project_name}-glue-policy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid = "S3Access",
        Effect = "Allow",
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
        Resource = [
          "arn:aws:s3:::${var.data_bucket_name}",
          "arn:aws:s3:::${var.data_bucket_name}/*",
          "arn:aws:s3:::aws-glue-assets-${data.aws_caller_identity.current.account_id}-*",
          "arn:aws:s3:::aws-glue-assets-${data.aws_caller_identity.current.account_id}-*/*"
        ]
      },
      {
        Sid = "SecretsManagerAccess",
        Effect = "Allow",
        Action = ["secretsmanager:GetSecretValue"],
        Resource = "arn:aws:secretsmanager:*:${data.aws_caller_identity.current.account_id}:secret:${var.sql_secret_name}*"
      },
      {
        Sid = "DynamoDBWatermarkAccess",
        Effect = "Allow",
        Action = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DescribeTable"],
        Resource = var.dynamodb_table_arn
      },
      {
        Sid = "KmsAccess",
        Effect = "Allow",
        Action = ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey", "kms:DescribeKey"],
        Resource = var.kms_key_arn != "" ? var.kms_key_arn : "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_custom" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_policy.arn
}

resource "aws_iam_role" "sfn_role" {
  name = "${var.project_name}-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "states.amazonaws.com" },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_policy" "sfn_policy" {
  name = "${var.project_name}-sfn-policy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"],
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "sfn_custom" {
  role       = aws_iam_role.sfn_role.name
  policy_arn = aws_iam_policy.sfn_policy.arn
}

output "glue_role_arn" {
  value = aws_iam_role.glue_role.arn
}

output "sfn_role_arn" {
  value = aws_iam_role.sfn_role.arn
}
