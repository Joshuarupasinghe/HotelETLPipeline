# Hotel Booking Data Processing Pipeline

A production-grade ETL (Extract, Transform, Load) pipeline for processing hotel booking data. The system extracts raw CSV records, enforces data quality through validation and quarantine, applies business-rule transformations, loads cleaned data into a PostgreSQL database with an optimized schema, and archives artifacts to AWS S3.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
  - [1. Clone the Repository](#1-clone-the-repository)
  - [2. Create and Activate a Virtual Environment](#2-create-and-activate-a-virtual-environment)
  - [3. Install Dependencies](#3-install-dependencies)
  - [4. Configure Environment Variables](#4-configure-environment-variables)
  - [5. Start the PostgreSQL Database](#5-start-the-postgresql-database)
  - [6. Run the Pipeline](#6-run-the-pipeline)
- [Pipeline Stages](#pipeline-stages)
  - [Extract](#extract)
  - [Validate](#validate)
  - [Transform](#transform)
  - [Load](#load)
  - [Archive (S3)](#archive-s3)
- [Data Quality and Validation](#data-quality-and-validation)
- [Schema Design and Storage Optimization](#schema-design-and-storage-optimization)
  - [Type Selection Strategy](#type-selection-strategy)
  - [Computed Columns](#computed-columns)
  - [Rejected Records Audit Table](#rejected-records-audit-table)
- [Indexing Strategy](#indexing-strategy)
- [Analytical Queries](#analytical-queries)
- [Ingestion Performance](#ingestion-performance)
- [AWS S3 Integration](#aws-s3-integration)
- [Scalability to 1 Million+ Records](#scalability-to-1-million-records)
  - [Processing Scalability](#processing-scalability)
  - [Database Partitioning](#database-partitioning)
  - [Orchestration with Apache Airflow](#orchestration-with-apache-airflow)
  - [Failure Handling and Observability](#failure-handling-and-observability)
- [License](#license)

---

## Architecture Overview

```
                          Hotel Booking ETL Pipeline
                          ==========================

  +-------------------+      +-------------------+      +---------------------+
  |                   |      |                   |      |                     |
  |  Raw CSV Source   +----->+    EXTRACT        +----->+     VALIDATE        |
  |  (data/raw/)      |      | (extract.py)      |      |  (validate.py)      |
  |                   |      |  Read as strings  |      |  Quarantine invalid |
  +-------------------+      +-------------------+      |  rows to CSV + DB   |
                                                        +---------+-----------+
                                                                  |
                                                        Valid records only
                                                                  |
                                                        +---------v-----------+
                                                        |                     |
                                                        |     TRANSFORM       |
                                                        |  (transform.py)     |
                                                        |  Date parse, type   |
                                                        |  cast, deduplicate, |
                                                        |  SHA-256 key gen    |
                                                        +---------+-----------+
                                                                  |
                                                   +--------------+--------------+
                                                   |                             |
                                          +--------v---------+        +---------v---------+
                                          |                  |        |                   |
                                          |  LOAD            |        |  ARCHIVE          |
                                          |  (load.py)       |        |  (aws_client.py)  |
                                          |  Batch upsert    |        |  S3 upload:       |
                                          |  to PostgreSQL   |        |  raw/ + processed/|
                                          +------------------+        +-------------------+
```

---

## Project Structure

```
HotelETLPipeline/
|-- data/
|   |-- raw/                        # Source CSV files
|   |-- processed/                  # Cleaned output after transformation
|   |-- rejected/                   # Quarantined records that failed validation
|-- sql/
|   |-- 01_schema.sql               # PostgreSQL table definitions and indexes
|   |-- 02_analytical_queries.sql   # Business-facing analytical queries
|   |-- 03_benchmarks.sql           # EXPLAIN ANALYZE index benchmarks
|-- src/
|   |-- config.py                   # Centralized configuration (env, paths)
|   |-- db.py                       # Database connection and schema initialization
|   |-- extract.py                  # Data extraction from raw CSV
|   |-- validate.py                 # Row-level data quality validation
|   |-- transform.py                # Data cleaning, normalization, key generation
|   |-- load.py                     # Batch upsert into PostgreSQL
|   |-- aws_client.py               # S3 upload/download client
|-- run_pipeline.py                 # CLI entry point for pipeline execution
|-- docker-compose.yml              # PostgreSQL container definition
|-- requirements.txt                # Python dependencies
|-- .env.example                    # Environment variable template
|-- .gitignore
```

---

## Technology Stack

| Component         | Technology                        |
|--------------------|-----------------------------------|
| Language           | Python 3.10+                      |
| Data Processing    | pandas, NumPy                     |
| Database           | PostgreSQL 16 (via Docker)        |
| DB Driver          | psycopg2-binary                   |
| Cloud Storage      | AWS S3 (boto3)                    |
| Configuration      | python-dotenv                     |
| Containerization   | Docker Compose                    |

---

## Prerequisites

- **Python** 3.10 or higher
- **Docker** and **Docker Compose** (for PostgreSQL)
- **AWS account** with an S3 bucket and IAM credentials (for S3 uploads; can be skipped with `--skip-s3`)
- **Git**

---

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/HotelETLPipeline.git
cd HotelETLPipeline
```

### 2. Create and Activate a Virtual Environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Copy the example environment file and populate it with your credentials:

```bash
cp .env.example .env
```

Open `.env` and set the following values:

```env
# AWS Configuration
AWS_ACCESS_KEY_ID=<your-access-key>
AWS_SECRET_ACCESS_KEY=<your-secret-key>
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=<your-s3-bucket-name>

# PostgreSQL Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=hotel_reservations
DB_USER=etl_user
DB_PASSWORD=etl_password
```

All secrets are loaded from this `.env` file at runtime. No credentials are hardcoded in source code.

### 5. Start the PostgreSQL Database

```bash
docker compose up -d
```

This launches a PostgreSQL 16 (Alpine) container with the credentials defined in `.env`.

### 6. Run the Pipeline

**Full run** (initialize database schema, execute ETL, upload to S3):

```bash
python run_pipeline.py --init-db
```

**Without S3 upload** (useful for local development):

```bash
python run_pipeline.py --init-db --skip-s3
```

**Subsequent runs** (schema already exists):

```bash
python run_pipeline.py
```

The pipeline will log extraction counts, quarantined row counts, batch insert progress, and S3 upload confirmations in real time.

---

## Pipeline Stages

### Extract

**Module:** `src/extract.py`

The extraction stage reads the raw CSV file into memory with all columns cast to string type (`dtype=str`). This deliberate choice prevents pandas from silently coercing ambiguous values (e.g., interpreting `"NULL"` as a valid value or dropping leading zeros) and ensures that downstream validation and transformation logic operates on the original, unmodified source data.

### Validate

**Module:** `src/validate.py`

Every row is evaluated against a set of business-rule constraints:

- **Guest count check** -- Bookings with zero total guests (adults + children + babies <= 0) are rejected.
- **Rate validation** -- Records with missing or negative Average Daily Rate (ADR) are flagged.
- **Date integrity** -- Arrival years outside a valid range (2010--2027) are caught.
- **Categorical integrity** -- Meal types not in the accepted set (`Undefined`, `SC`, `BB`, `HB`, `FB`) are rejected.

Records that fail any validation rule are quarantined into `data/rejected/rejected_records.csv` with a human-readable rejection reason. These same records are also written to a `rejected_records_audit` table in PostgreSQL (as JSONB payloads) for downstream monitoring. Only records that pass all checks proceed to transformation.

### Transform

**Module:** `src/transform.py`

Transformation is structured as a composable pipeline of pure functions applied sequentially via pandas `.pipe()`:

1. **Null standardization** -- Replaces string representations of missing data (`"NULL"`, `"null"`, `"undefined"`, empty strings) with native `None`.
2. **String normalization** -- Strips whitespace and applies Title Case to categorical columns; country codes are uppercased.
3. **Categorical defaults** -- Fills missing meal, market segment, and distribution channel values with sensible defaults.
4. **Numeric casting** -- Converts integer and float columns from strings to their correct numeric types, coercing failures to zero or `0.00`.
5. **Date consolidation** -- Combines the three separate date component columns (`arrival_date_year`, `arrival_date_month`, `arrival_date_day_of_month`) into a single ISO-8601 `arrival_date` column.
6. **Deduplication** -- Removes exact row-level duplicates.
7. **Primary key generation** -- Produces a deterministic SHA-256 surrogate key (`booking_id`) from a composite of business-identifying fields (hotel, arrival date, lead time, guest counts, ADR, market segment). This ensures idempotent re-runs without relying on auto-incrementing sequences.
8. **Column selection** -- Filters and renames columns to match the target PostgreSQL schema.

### Load

**Module:** `src/load.py`

Cleaned records are loaded into PostgreSQL using `psycopg2.extras.execute_values` with a configurable batch size of 5,000 rows per statement. This replaces row-by-row inserts (which would incur over 100,000 individual network round trips) with multi-row insert statements that complete in seconds.

Idempotency is enforced at the database level through `ON CONFLICT (booking_id) DO UPDATE`, which upserts mutable fields (`is_canceled`, `reservation_status`, `reservation_status_date`, `adr`) on primary key collision. The pipeline can be safely re-run without producing duplicate rows or causing primary key violations.

### Archive (S3)

**Module:** `src/aws_client.py`

After loading, both the original raw CSV and the cleaned output are uploaded to AWS S3 under `raw/` and `processed/` prefixes respectively. The S3 client supports multipart uploads for files exceeding 50 MB and falls back to IAM role-based authentication when explicit credentials are not provided.

---

## Data Quality and Validation

The pipeline implements a quarantine pattern for data quality:

```
Raw Data (N rows)
     |
     v
 Validation Engine
     |                  \
     v                   v
Valid Records       Rejected Records
  (proceed)         (quarantined)
                        |
               +--------+--------+
               |                 |
         CSV Export          Audit Table
   (data/rejected/)      (rejected_records_audit)
```

Every rejected row preserves the full original payload as JSONB and a concatenated, human-readable rejection reason. This allows data engineers to diagnose quality issues without re-running the pipeline or querying external systems.

---

## Schema Design and Storage Optimization

**Schema file:** `sql/01_schema.sql`

### Type Selection Strategy

Storage footprint directly impacts memory efficiency and cache performance. Rather than using generic types (standard 4-byte integers or unbounded text), the schema selects compact, strict types aligned to each column's data domain:

| Column                     | Type             | Rationale                                                      |
|----------------------------|------------------|----------------------------------------------------------------|
| `is_canceled`              | `SMALLINT`       | Binary flag; 2 bytes instead of 4-byte `INTEGER`               |
| `is_repeated_guest`        | `SMALLINT`       | Binary flag; same rationale                                     |
| `adr`                      | `NUMERIC(10,2)`  | Exact decimal arithmetic; avoids floating-point rounding errors |
| `arrival_date`             | `DATE`           | Native date type; 4 bytes; enables temporal operators           |
| `country`                  | `VARCHAR(3)`     | ISO 3166-1 alpha-3 codes are exactly 3 characters               |

PostgreSQL retrieves data in fixed 8 KB pages. Smaller row sizes mean more rows fit per page, maximizing the cache hit ratio in `shared_buffers` memory and reducing disk I/O during sequential scans.

### Computed Columns

Two columns are defined as `GENERATED ALWAYS AS (...) STORED`:

- **`total_stay_nights`** = `stays_in_weekend_nights + stays_in_week_nights`
- **`total_guests`** = `adults + children + babies`

In hotel analytics, nearly every revenue calculation requires multiplying the daily rate by total stay nights. Rather than forcing the database engine to recompute this arithmetic on every analytical query, these values are calculated once at write time. This trades a negligible amount of additional storage for faster read performance across all downstream queries.

### Rejected Records Audit Table

The `rejected_records_audit` table stores quarantined records with:

- `raw_payload` as `JSONB` (the full original row for forensic analysis)
- `rejection_reason` as `TEXT`
- `rejected_at` timestamp for temporal tracking

---

## Indexing Strategy

**Schema file:** `sql/01_schema.sql` | **Benchmarks:** `sql/03_benchmarks.sql`

The indexing strategy targets observed query access patterns rather than applying indexes indiscriminately:

| Index                                    | Type              | Purpose                                                             |
|------------------------------------------|-------------------|---------------------------------------------------------------------|
| `idx_bookings_segment_canceled`          | Composite B-Tree  | Revenue queries group by `market_segment` and filter `is_canceled`   |
| `idx_bookings_arrival_date`             | B-Tree            | Month-over-month growth queries; future partition key                |
| `idx_bookings_country`                  | B-Tree            | Geographical aggregation and cancellation rate analysis              |

**Composite index rationale:** The primary revenue query groups by `market_segment` and filters for confirmed bookings (`is_canceled = 0`). Instead of creating two separate single-column indexes, a composite B-Tree on `(market_segment, is_canceled)` allows PostgreSQL to satisfy both the filter and the grouping within a single index scan.

**Benchmark results** (via `EXPLAIN ANALYZE` in `03_benchmarks.sql`):

- **Without composite index:** PostgreSQL performs a Sequential Scan, reading every disk page and evaluating rows individually. Execution time: approximately 14 ms.
- **With composite index:** The planner switches to a Bitmap Index Scan, filtering canceled bookings directly within the index tree before accessing the heap. Execution time: approximately 2 ms -- an improvement exceeding 80%.

The `arrival_date` index eliminates expensive in-memory sort operations for time-series queries and is positioned as the future partition key when the dataset grows to warrant declarative range partitioning.

---

## Analytical Queries

**File:** `sql/02_analytical_queries.sql`

Three business-facing analytical queries are included:

**Query 1 -- Revenue by Market Segment:**
Identifies the highest-value booking acquisition channels by aggregating realized revenue (ADR multiplied by total stay nights) for confirmed bookings, grouped by market segment.

**Query 2 -- Month-over-Month Growth:**
Calculates monthly revenue and booking volume growth trends. The query uses a Common Table Expression (CTE) with the `LAG()` window function rather than a self-join. A self-join would scale quadratically by scanning the dataset twice, while the window function computes growth in a single pass over the aggregated monthly summary.

**Query 3 -- Geographical Performance and Cancellation Rate:**
Analyzes international market share, average daily rate, and cancellation risk ratio by country for markets with statistically meaningful booking volumes (50+ bookings).

All queries apply restrictive filters (e.g., `WHERE is_canceled = 0`) early in the `WHERE` clause before `GROUP BY` aggregation, minimizing the working set size before the database constructs its aggregation hash tables.

---

## Ingestion Performance

Loading 100,000+ records with individual `INSERT` statements would generate over 100,000 network round trips and individual transaction log commits. This pipeline uses `psycopg2.extras.execute_values` with a batch size of 5,000 rows, packaging thousands of records into single multi-row insert statements. This reduces total ingestion time from minutes to seconds.

Idempotency is enforced at the database level. The deterministic SHA-256 `booking_id` combined with `ON CONFLICT (booking_id) DO UPDATE` ensures:

- The pipeline can be re-run any number of times without duplicate rows.
- No pre-check `SELECT` queries are required before inserting.
- Primary key collision crashes are eliminated.

---

## AWS S3 Integration

Both raw source data and processed output are archived to S3:

```
s3://<bucket-name>/
|-- raw/hotel_bookings_raw.csv          # Original source artifact
|-- processed/hotel_bookings_cleaned.csv # Cleaned, transformed output
```

- IAM credentials are loaded from `.env` via environment variables; no secrets are hardcoded.
- The client supports least-privilege IAM role fallback when explicit keys are not provided.
- Multipart upload is configured for files exceeding 50 MB.

---

## Scalability to 1 Million+ Records

As the dataset grows beyond 1 million records and into production-scale workloads, the architecture is designed to evolve across four dimensions:

### Processing Scalability

The current single-node pandas implementation handles the existing dataset efficiently but would encounter memory constraints at significantly larger scales. The migration path is:

- **PySpark** for distributed processing across a cluster, enabling horizontal scaling with datasets that exceed single-machine memory.
- **DuckDB** as an alternative for larger-than-memory analytical transformations on a single robust node, leveraging columnar storage and vectorized execution without the operational overhead of a distributed cluster.

The modular pipeline design (extract, validate, transform, load as independent functions) facilitates this migration, as each stage can be swapped to a distributed implementation without restructuring the overall pipeline orchestration.

### Database Partitioning

The current `arrival_date` index strategy is designed to transition into declarative range partitioning. By partitioning the `hotel_bookings` table horizontally by month or year:

- PostgreSQL applies **partition pruning** to eliminate irrelevant historical partitions during analytical queries, scanning only the partitions that match the query's date range.
- Older partitions can be **detached and archived** to cold storage without affecting the performance of queries on recent data.
- Maintenance operations (VACUUM, reindexing) can target individual partitions rather than the entire table.

### Orchestration with Apache Airflow

Manual pipeline execution would be replaced with Apache Airflow for production scheduling and monitoring:

- Each ETL stage (extract, validate, transform, load, archive) would be wrapped as an individual **task** within a Directed Acyclic Graph (DAG).
- The DAG would be **scheduled via cron syntax** for regular execution intervals.
- **Airflow S3 sensors** could automatically trigger the pipeline when a new raw CSV is deposited into the S3 landing bucket, enabling event-driven ingestion.
- The Airflow web UI provides centralized visibility into execution history, task durations, and failure diagnostics.

### Failure Handling and Observability

Production failure handling builds on the existing idempotent design:

- **Automated task retries** via Airflow's built-in retry mechanism. Because database inserts use `ON CONFLICT` upserts, retries are safely idempotent without risk of data duplication.
- **Dead Letter Queue (DLQ):** Records that cause schema violations or unrecoverable processing errors are routed to a dedicated DLQ path in S3 for engineering review, ensuring the main pipeline never halts due to individual bad records.
- **Alerting:** Webhook-based notifications (e.g., Slack) would be triggered on task failures or when quarantined record counts exceed configurable thresholds, providing immediate visibility to the engineering team.

---

## License

This project was developed as part of the Associate Data Engineer assessment.
