# Customer Data Ingestion

Production data lakehouse for customer data with medallion architecture, dimensional modeling, and security patterns.

## Architecture

**Medallion Architecture** (Bronze → Silver → Gold):
* **Bronze**: Raw ingestion with metadata
* **Silver**: Cleansed, SCD Type 2, quality checks
* **Gold**: Dimensional models (star, snowflake, wide tables)

**Domains**:
* **Transactions**: Customer purchase data with SCD Type 2
* **Feedback**: Customer feedback with sentiment analysis

## Catalog Structure

```
customer (catalog)
├── transaction (schema)
│   ├── bronze_transactions
│   ├── silver_transactions_scd
│   ├── gold_dim_customer
│   ├── gold_dim_product
│   ├── gold_fact_sales
│   └── ...
├── feedback (schema)
│   ├── bronze_feedback
│   ├── silver_feedback_clean
│   ├── gold_feedback_analytics
│   └── ...
└── control (schema)
    ├── ingestion_metadata
    ├── audit_log
    └── data_quality_checks
```

## Repository Structure

```
MSF_test/
├── config/
│   ├── catalog_setup.py              # Create customer catalog + schemas
│   └── control_tables.py             # Metadata/audit/quality tables
├── customer/
│   ├── transactions/
│   │   ├── bronze/
│   │   │   └── ingest_transactions.py
│   │   ├── silver/
│   │   │   └── transform_transactions_scd2.py
│   │   └── gold/
│   │       └── dimensional_models.py
│   └── feedback/
│       ├── bronze/
│       │   └── ingest_feedback.py
│       ├── silver/
│       │   └── transform_feedback.py
│       └── gold/
│           └── feedback_analytics.py
└── security/
    ├── data_masking.py               # PII masking
    └── audit_logging.py              # Access auditing
```

## Execution Order

### 1. Initialize

```bash
config/catalog_setup.py               # Create catalog + schemas
config/control_tables.py              # Create control tables
```

**Creates**: `customer` catalog with `transaction`, `feedback`, `control` schemas

### 2. Ingest Transactions (Bronze)

```bash
customer/transactions/bronze/ingest_transactions.py
```

**Creates**: `customer.transaction.bronze_transactions`

### 3. Transform Transactions (Silver → Gold)

```bash
customer/transactions/silver/transform_transactions_scd2.py     # SCD Type 2
customer/transactions/gold/dimensional_models.py                # Star/Snowflake/Wide
```

**Creates**:
* Silver: `silver_transactions_scd`
* Gold: `gold_dim_customer`, `gold_dim_product`, `gold_dim_date`, `gold_fact_sales`

### 4. Ingest Feedback (Bronze)

```bash
customer/feedback/bronze/ingest_feedback.py
```

**Creates**: `customer.feedback.bronze_feedback`

### 5. Transform Feedback (Silver → Gold)

```bash
customer/feedback/silver/transform_feedback.py          # Cleanse + sentiment
customer/feedback/gold/feedback_analytics.py            # Analytics models
```

**Creates**:
* Silver: `silver_feedback_clean`
* Gold: `gold_feedback_summary`, `gold_feedback_trends`

### 6. Apply Security

```bash
security/data_masking.py              # Mask PII
security/audit_logging.py             # Enable audit trails
```

**Creates**: Masked views, audit logs, compliance reports

## Key Patterns

**Slowly Changing Dimensions (SCD Type 2)**
* Tracks historical customer changes
* Surrogate keys, effective dates, current flags
* Delta Lake MERGE operations

**Dimensional Modeling**
* **Star Schema**: Denormalized, fast queries
* **Snowflake Schema**: Normalized hierarchies
* **Wide Tables**: Zero-join BI models

**Data Governance**
* PII masking (email, phone, address)
* Role-based dynamic masking
* Audit logging (all access + modifications)
* Quality monitoring with thresholds

**Control Tables**
* Ingestion metadata tracking
* Data quality checks
* Audit trails for compliance

## Sample Queries

**Check Pipeline Status**:
```sql
SELECT pipeline_id, status, error_message 
FROM customer.control.ingestion_metadata 
WHERE status = 'FAILED' 
ORDER BY start_time DESC;
```

**Audit Data Access**:
```sql
SELECT user_name, operation, table_name, timestamp 
FROM customer.control.audit_log 
WHERE table_name LIKE '%transactions%' 
ORDER BY timestamp DESC;
```

**Query Dimensional Model**:
```sql
SELECT 
    d.year, d.month_name, c.customer_segment,
    SUM(f.net_amount) as revenue
FROM customer.transaction.gold_fact_sales f
JOIN customer.transaction.gold_dim_date d ON f.date_key = d.date_key
JOIN customer.transaction.gold_dim_customer c ON f.customer_key = c.customer_key
GROUP BY d.year, d.month_name, c.customer_segment;
```

**SCD Type 2 Query** (point-in-time):
```sql
SELECT customer_id, full_name, customer_segment, effective_start_date, effective_end_date
FROM customer.transaction.silver_transactions_scd
WHERE customer_id = 123
  AND '2024-01-15' BETWEEN effective_start_date AND effective_end_date;
```

## Technical Highlights

* **Delta Lake**: ACID transactions, time travel, schema evolution
* **Unity Catalog**: Centralized governance
* **SCD Type 2**: Historical tracking for customer dimension
* **Dimensional Modeling**: Star, snowflake, wide table approaches
* **Security**: PII masking, audit logging, compliance
* **Control Tables**: Production-grade metadata tracking

## Prerequisites

* Databricks workspace (AWS/Azure/GCP)
* DBR 13.3+ (Spark 3.4+)
* Unity Catalog enabled
* Python 3.9+

---

**Purpose**: Technical demonstration for customer data engineering
**Focus**: Medallion architecture, SCD Type 2, dimensional modeling, security, auditing
