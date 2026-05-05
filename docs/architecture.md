# Architecture Design

## Overview

This project implements a serverless AWS data pipeline using a Medallion Architecture.

```text
SQL Server → Glue Bronze → S3 Bronze → Glue Silver → S3 Silver → Glue Gold → S3 Gold → Athena → QuickSight
```

## Design Decisions

### AWS Glue
Used for distributed PySpark ETL. It avoids server management and integrates well with S3, JDBC, Secrets Manager, and Step Functions.

Alternative: EMR, Lambda, AWS Batch.  
Justification: Glue is simpler for managed Spark ETL and is serverless.

### Amazon S3
Used as the central data lake storage layer.

Alternative: Redshift-only warehouse storage, EFS.  
Justification: S3 is durable, scalable, low-cost, and works well with Athena and Glue.

### DynamoDB
Used to track incremental watermarks.

Alternative: Glue bookmarks, S3 metadata file, RDS control table.  
Justification: DynamoDB gives explicit, flexible control over multi-table watermarks.

### Secrets Manager
Used to store SQL Server username/password.

Alternative: Glue connection credentials, environment variables.  
Justification: Secrets Manager provides centralized secure credential rotation and access control.

### Athena
Used for querying Gold datasets from S3.

Alternative: Redshift Spectrum, Redshift Serverless, Snowflake.  
Justification: Athena is serverless and cost-effective for S3-based analytics.

### QuickSight
Used for BI dashboards.

Alternative: Tableau, Power BI, Looker.  
Justification: QuickSight integrates natively with Athena and supports SPICE for performance.

### Step Functions
Used for orchestration of Bronze → Silver → Gold.

Alternative: Airflow, EventBridge-only schedules, Glue Workflows.  
Justification: Step Functions provides native Glue integration, retries, and clear workflow visibility.
