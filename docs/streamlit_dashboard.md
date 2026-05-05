# Streamlit Dashboard Design

## Purpose

The Streamlit dashboard provides a custom analytics application on top of the same Gold datasets used by QuickSight.

## Recommended Mode

Use the Athena-backed dashboard:

```bash
cd dashboard
streamlit run streamlit_app.py
```

This version uses Athena to query the Gold tables and is faster than reading the entire S3 Gold layer directly into Streamlit.

## Current Behavior

The dashboard:

- reads from Athena Gold tables
- removes the currency filter
- uses restaurant name labels for filters and plots
- defaults restaurant name to `restaurant_id AS restaurant_name`

If a real `restaurant_name` field is later added to Gold tables, set:

```bash
export RESTAURANT_NAME_SQL=restaurant_name
```

## Architecture

```text
S3 Gold → Glue Data Catalog → Athena → Streamlit
```

This does not replace QuickSight. It adds an optional custom analytics frontend.

## Why Athena Instead of Direct S3

Athena is better for larger data because filtering happens in SQL before results are returned to Streamlit. Direct S3 reads are simpler, but they can be slow because the app loads Parquet datasets into memory.
