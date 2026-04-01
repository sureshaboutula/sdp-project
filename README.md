# 🚀 SDP ETL Pipeline — Databricks Medallion Architecture

A production-grade ETL pipeline built on **Databricks Serverless Declarative Pipelines (SDP)** implementing the Medallion Architecture (Bronze → Silver → Gold) with full CI/CD automation using **GitHub Actions** and **Databricks Asset Bundles**.

---

## 📐 Architecture

```
Data Sources
├── NYC Yellow Taxi CSV (2016)     → Auto Loader (cloudFiles)
└── TPCH Orders (Delta table)      → Spark ReadStream

         │
         ▼
┌─────────────────────────────────────────────────┐
│  BRONZE LAYER  (Streaming Tables)               │
│  • yellow_taxi_raw                              │
│  • orders_raw                                   │
│  • @dlt.expect — log violations, keep all rows  │
│  • OPTIMIZE + ZORDER + VACUUM                   │
└───────────────────┬─────────────────────────────┘
                    │                    │ bad rows
                    ▼                    ▼
┌───────────────────────────┐   ┌────────────────────┐
│  SILVER LAYER             │   │  QUARANTINE        │
│  (Materialized Views)     │   │  yellow_taxi_      │
│  • yellow_taxi_clean      │   │  bad_data          │
│  • orders_clean           │   │  Bad rows captured │
│  • cast types             │   │  for investigation │
│  • dropDuplicates         │   └────────────────────┘
│  • @dlt.expect_or_drop    │
└───────────────────┬───────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│  GOLD LAYER  (Materialized Views)               │
│  • vendors_monthly_trips_revenue                │
│  • customers_order_summary                      │
│  • Business KPIs ready for BI / ML              │
└─────────────────────────────────────────────────┘
```

---

## 🔄 CI/CD Pipeline

```
Developer pushes feature branch
         │
         ▼
  Raise Pull Request
         │
         ▼
  ✅ databricks bundle validate     ← GitHub Actions (validate.yml)
         │
         ▼
  Merge to main
         │
         ▼
  ✅ databricks bundle deploy -t dev ← GitHub Actions (deploy.yml)
         │
         ▼
  ✅ pytest databricks/tests/        ← Unit tests with fake DataFrames
         │
         ▼
  ✅ databricks bundle deploy -t prod ← Auto deploy on test success
         │
         ▼
  ✅ databricks bundle run sdp_ecommerce_job -t prod ← Pipeline triggered
```

---

## 🛠️ Tech Stack

| Technology | Purpose |
|---|---|
| Databricks SDP | Declarative pipeline framework |
| Delta Lake | Storage layer with ACID transactions |
| Unity Catalog | Three-level namespace data governance |
| Databricks Asset Bundles | Infrastructure as code, multi-env deployment |
| Auto Loader | Incremental file ingestion from cloud storage |
| PySpark | Data transformation logic |
| GitHub Actions | CI/CD automation |
| pytest | Unit testing with fake DataFrames |
| Databricks CLI | Local development and deployment |

---

## 📁 Project Structure

```
sdp-project/
├── databricks.yml                    ← Asset Bundle root config (dev + prod targets)
├── databricks/
│   ├── resources/
│   │   └── pipeline.yml              ← SDP pipeline + orchestration job definition
│   ├── notebooks/
│   │   ├── 00_setup.py               ← Catalog, schema, volume bootstrap
│   │   ├── 01_bronze_ingestion.py    ← Streaming Tables with Auto Loader
│   │   ├── 02_silver_cleansing.py    ← Materialized Views with DQ rules
│   │   ├── 03_gold_aggregations.py   ← Business KPI aggregations
│   │   ├── 04_dq_quarantine.py       ← Bad data capture for investigation
│   │   └── 05_optimize.py            ← OPTIMIZE, ZORDER, VACUUM post-pipeline
│   ├── utils/
│   │   ├── __init__.py
│   │   └── transformations.py        ← Reusable helper functions (testable)
│   └── tests/
│       ├── conftest.py               ← SparkSession fixture for pytest
│       ├── test_silver.py            ← Silver transformation unit tests
│       └── test_gold.py              ← Gold aggregation unit tests
└── .github/
    └── workflows/
        ├── validate.yml              ← Validate on every PR
        └── deploy.yml                ← Full 4-stage deployment pipeline
```

---

## 🥉 Bronze Layer — Raw Ingestion

Streaming Tables ingest raw data incrementally using **Auto Loader** for CSV files and **Delta ReadStream** for existing Delta tables.

```python
@dlt.table(
    name=f"sdp_catalog_{env}.{schema_bronze}.yellow_taxi_raw",
    table_properties={"quality": "bronze"}
)
@dlt.expect("fare_amount_positive", "fare_amount > 0")   # log, keep row
@dlt.expect("vendor_id_not_null", "VendorID is not null")
def ingest_taxi_data_bronze():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", f"/Volumes/sdp_catalog_{env}/bronze/schema_store/yellow_taxi")
            .load(source_path)
    )
```

**Key features:**
- Auto Loader checkpoint tracks processed files — no duplicate ingestion
- Schema stored in Unity Catalog Volume for persistence across restarts
- `@dlt.expect` logs violations without dropping rows — Bronze is the safety net

---

## 🥈 Silver Layer — Cleansing & Enrichment

Materialized Views clean and enrich Bronze data. Transformation logic is extracted into testable helper functions.

```python
def transform_taxi_data(df):
    return (
        df.dropDuplicates(["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"])
          .withColumn("fare_amount", F.col("fare_amount").cast("double"))
          .withColumn("pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
          .withColumn("trip_year", F.year(F.col("tpep_pickup_datetime")))
          .withColumn("trip_month", F.month(F.col("tpep_pickup_datetime")))
          .withColumn("trip_hour", F.hour(F.col("tpep_pickup_datetime")))
    )

@dlt.table(name=f"sdp_catalog_{env}.{schema_silver}.yellow_taxi_clean")
@dlt.expect_or_drop("valid_fare_amount", "fare_amount > 0")   # drop bad rows
@dlt.expect_or_drop("vendor_id_not_null", "VendorID is not null")
def yellow_taxi_data_clean():
    df = dlt.read(f"sdp_catalog_{env}.{schema_bronze}.yellow_taxi_raw")
    return transform_taxi_data(df)
```

---

## 🥇 Gold Layer — Business KPIs

Materialized Views aggregate Silver data into business-ready analytics tables.

```python
def aggregate_vendor_monthly_trips(df):
    return (
        df.groupBy("VendorID", "trip_year", "trip_month")
          .agg(
              F.count("*").alias("total_rides"),
              F.sum("total_amount").alias("total_revenue")
          )
    )
```

**Results (2016 NYC Taxi data):**
- 🚕 131 million raw trips ingested (Bronze)
- ✅ 130.9 million clean trips (Silver — 200k bad rows quarantined)
- 📊 27 monthly aggregation rows per vendor (Gold)

---

## 🗑️ Data Quality Quarantine

Bad rows dropped by Silver are captured in a quarantine table instead of being silently lost:

```python
@dlt.table(name=f"sdp_catalog_{env}.{schema_quarantine}.yellow_taxi_bad_data")
def yellow_taxi_bad_data_clean():
    return (
        dlt.read(f"sdp_catalog_{env}.{schema_bronze}.yellow_taxi_raw")
           .filter(F.col("fare_amount") < 0)   # keep only bad rows
    )
```

---

## ⚡ Optimizations

Post-pipeline optimization runs as a separate job task:

```python
# OPTIMIZE + ZORDER — compact files and co-locate data for faster queries
def optimize_table(table_name, *zorder_columns):
    spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({','.join(zorder_columns)})")

# VACUUM — remove old Delta files beyond retention window
def vacuum_table(table_name, retention_hours=168):
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")

optimize_table(f"sdp_catalog_{env}.{schema_bronze}.yellow_taxi_raw",
               "VendorID", "tpep_pickup_datetime")
vacuum_table(f"sdp_catalog_{env}.{schema_bronze}.yellow_taxi_raw")
```

---

## 🧪 Unit Testing

Transformation logic is extracted into plain helper functions and tested with fake DataFrames — no live cluster needed:

```python
# conftest.py — SparkSession available to all tests
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("unit-tests").getOrCreate()

# test_silver.py
def test_derived_columns_exist(spark):
    df = spark.createDataFrame(data)
    result_df = transform_taxi_data(df)
    assert "trip_year" in result_df.columns
    assert "trip_month" in result_df.columns
    assert "trip_hour" in result_df.columns

def test_column_data_types(spark):
    schema_dict = dict(transform_taxi_data(spark.createDataFrame(data)).dtypes)
    assert schema_dict["fare_amount"] == "double"
    assert schema_dict["pickup_datetime"] == "timestamp"
```

---

## 📦 Databricks Asset Bundles

Multi-environment deployment using variable-driven configuration:

```yaml
# databricks.yml
variables:
  env:
    description: Deployment environment (dev or prod)
  schema_bronze:
    default: bronze
  schema_silver:
    default: silver
  schema_gold:
    default: gold

targets:
  dev:
    mode: development   # adds [dev username] prefix — isolates resources
    default: true
    workspace:
      host: https://dbc-xxxxx.cloud.databricks.com/
    variables:
      env: dev

  prod:
    mode: production
    workspace:
      host: https://dbc-yyyyy.cloud.databricks.com/
      root_path: /Workspace/Users/user@email.com/.bundle/${bundle.name}/${bundle.target}
    variables:
      env: prod
```

**Deploy commands:**
```bash
databricks bundle validate --profile sdp-dev   # validate locally
databricks bundle deploy -t dev                 # deploy to dev
databricks bundle deploy -t prod               # deploy to prod
databricks bundle run sdp_ecommerce_job -t dev # trigger job
```

---

## 🚀 Getting Started

### Prerequisites
- Databricks free edition workspace (dev + prod)
- Databricks CLI v0.200+
- Python 3.10+
- GitHub account

### 1. Clone the repository
```bash
git clone https://github.com/sureshaboutula/sdp-project.git
cd sdp-project
```

### 2. Configure Databricks CLI
```bash
databricks configure --profile sdp-dev --host https://your-workspace-url/
# Enter your Personal Access Token when prompted
```

### 3. Bootstrap infrastructure (first time only)
```sql
-- Run in your Databricks workspace notebook
CREATE CATALOG IF NOT EXISTS sdp_catalog_dev;
```

### 4. Validate and deploy
```bash
databricks bundle validate --profile sdp-dev
databricks bundle deploy -t dev
```

### 5. Run the pipeline
```bash
databricks bundle run sdp_ecommerce_job -t dev
```

---

## 🔐 GitHub Actions Setup

Add these secrets in GitHub → Settings → Secrets and variables → Actions:

| Secret | Description |
|---|---|
| `DATABRICKS_HOST` | Dev workspace URL |
| `DATABRICKS_TOKEN` | Dev Personal Access Token |
| `DATABRICKS_PROD_HOST` | Prod workspace URL |
| `DATABRICKS_PROD_TOKEN` | Prod Personal Access Token |

---

## 📊 Pipeline Results

| Layer | Table | Row Count |
|---|---|---|
| Bronze | yellow_taxi_raw | 131,165,043 |
| Silver | yellow_taxi_clean | 130,960,828 |
| Quarantine | yellow_taxi_bad_data | ~204,215 |
| Gold | vendors_monthly_trips_revenue | 27 |
| Gold | customers_order_summary | 499989 |

---

## 🤝 Connect

Built by **Suresh Aboutula**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue)](www.linkedin.com/in/suresh-aboutula-686136a6)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-black)](https://github.com/sureshaboutula)

---

*Built with ❤️ using Databricks, PySpark, Delta Lake, GitHub Actions and Python*
