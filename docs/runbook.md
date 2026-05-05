# Operations Runbook

## Normal Pipeline Run

1. Run Bronze Glue job.
2. Confirm S3 bronze paths are populated.
3. Run Silver Glue job.
4. Confirm silver datasets are written.
5. Run Gold Glue job.
6. Run Athena partition repair or Glue crawler.
7. Refresh QuickSight SPICE datasets.

## Common Issues

### Glue cannot connect to SQL Server
Check:
- JDBC URL starts with `jdbc:sqlserver://`
- database name is correct
- Glue VPC/subnet/security group can reach RDS on port 1433

### DynamoDB key schema error
The watermark table must have:
- partition key: `pipeline_name`
- type: String

### KMS GenerateDataKey error
Grant Glue role:
- `kms:Encrypt`
- `kms:Decrypt`
- `kms:GenerateDataKey`
- `kms:DescribeKey`

Also update the KMS key policy.

### Athena shows no data
Check:
- S3 data exists
- Athena table location is correct
- partitions are registered

Run:

```sql
MSCK REPAIR TABLE global_partner_gold.gold_daily_restaurant_sales;
SHOW PARTITIONS global_partner_gold.gold_daily_restaurant_sales;
```

### QuickSight import permission failure
Check:
- QuickSight has Athena enabled
- QuickSight has access to the data S3 bucket
- QuickSight has access to Athena query result bucket
- KMS policy allows QuickSight role to decrypt data
