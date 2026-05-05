variable "table_name" {
  type = string
}

resource "aws_dynamodb_table" "watermarks" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pipeline_name"

  attribute {
    name = "pipeline_name"
    type = "S"
  }
}

output "table_name" {
  value = aws_dynamodb_table.watermarks.name
}

output "table_arn" {
  value = aws_dynamodb_table.watermarks.arn
}
