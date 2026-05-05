# Global Partner Data Pipeline
This project ingests SQL Server transactional data into an S3 data lake, transforms it through Bronze, Silver, and Gold layers using AWS Glue PySpark, exposes curated datasets through Athena, and supports dashboarding in Amazon QuickSight/Streamlit.

## Architecture
SQL Server / RDS
   ↓
AWS Glue Bronze Job
   ↓
S3 Bronze
   ↓
AWS Glue Silver Job
   ↓
S3 Silver
   ↓
AWS Glue Gold Job
   ↓
S3 Gold
   ↓
Glue Data Catalog / Athena
   ↓
QuickSight/Streamlit


![alt text](image.png)

## Core AWS Services
| Service | Purpose |
|---|---|
| AWS Glue | PySpark ETL for Bronze, Silver, and Gold layers |
| Amazon S3 | Data lake storage |
| DynamoDB | Watermark tracking for incremental ingestion |
| Secrets Manager | SQL Server credential storage |
| KMS | Encryption for S3 and secrets |
| Athena | Serverless SQL query layer |
| QuickSight | BI dashboarding |
| Step Functions | Pipeline orchestration |
| IAM | Least-privilege access control |

## Data Layers

### Bronze
Raw-ish SQL Server extracts written to S3 as Parquet.

Tables:
- `order_items`
- `order_item_options`
- `date_dim`

### Silver
Cleaned, deduplicated, typed, enriched data.

Datasets:
- `silver_order_items`
- `silver_order_item_options`
- `silver_date_dim`
- `silver_order_line_enriched`

### Gold
Business-ready aggregates.

Datasets:
- `gold_order_summary`
- `gold_daily_restaurant_sales`
- `gold_item_performance`
- `gold_loyalty_sales_summary`

## Repository Layout
.
├── athena/ddl/                  # Athena external table DDLs
├── docs/                        # Architecture and operations docs
├── infra/terraform/             # Infrastructure as Code
├── jobs/glue/                   # Glue PySpark ETL jobs
├── stepfunctions/               # Step Functions ASL definitions
└── .github/workflows/           # CI/CD workflow skeleton

## Deployment Order
1. Create/update Secrets Manager secret for SQL Server credentials.
2. Deploy Terraform infrastructure.
3. Upload Glue scripts to the Glue script S3 location.
4. Run Bronze job.
5. Run Silver job.
6. Run Gold job.
7. Create Athena tables and repair partitions.
8. Connect Athena datasets to QuickSight/Stremalit

## Notes
- Bronze uses DynamoDB watermarking for `order_items`.
- `order_item_options` is loaded as a full source extract because it does not have a reliable timestamp column.
- Silver deduplicates full-loaded datasets using business keys.
- Athena partitions must be repaired or crawled after new partitioned writes.