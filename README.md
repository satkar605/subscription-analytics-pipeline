# Trail Trekker Subscription Analytics Pipeline

## Problem
The Trail Trekker growth team needs reliable, timely insight into how customers change their subscriptions over time. They lack a warehouse and scheduled transformations to answer questions such as which plans customers upgrade to, when churn events occur, and the revenue impact of plan changes.

## About the Data
Source files live in `data/` and are ingested into a persistent DuckDB file `trail_trekker.db`.
- Tables: `features`, `plans`, `customers`, `plan_features`, `subscriptions`
- Row counts after ingestion: features 8, plans 6, customers 25, plan_features 24, subscriptions 41
- Notable data quality issue: some date fields contained the literal string "NULL" rather than SQL NULL

Schemas are inferred by DuckDB at ingest and validated in staging models.

## Approach
This project was built incrementally in three layers.

1) Ingestion (Python + DuckDB)
- Persistent database connection using `duckdb.connect('trail_trekker.db')`
- Idempotent table creation using `CREATE OR REPLACE TABLE`
- Minimal, clear CLI script for repeatable runs

2) Transformation (SQLMesh)
- Staging views standardize types, names, and audits
- Dimensional layer with SCD Type 2 dimensions for customers and plans
- Transaction fact table for subscription change events

3) Orchestration (SQLMesh run + cron)
- Hourly execution configured on the fact model
- Local cron job invokes `sqlmesh run prod` and writes logs

## Repository Structure
```text
trail-trekker-demo/
  data/                       # CSV sources
  models/                     # SQLMesh models
    staging_*.sql             # staging layer views
    dim_customers.sql         # SCD Type 2
    dim_plans.sql             # SCD Type 2
    dim_time.sql              # static date dimension
    fct_subscription_changes.sql  # transaction fact
  ingest_data.py              # DuckDB ingestion via CLI
  config.yml                  # SQLMesh configuration (DuckDB gateway)
  README.md                   # this document
```

## Why These Tools
- DuckDB: fast local analytics engine with excellent CSV parsing and persistence
- SQLMesh: dependency-aware, audit-enabled transformations with environment isolation
- Cron: simple, reliable local scheduling

## Key Models
- `staging.features`, `staging.plans`, `staging.customers`, `staging.subscriptions`
- `dim.customers` (SCD Type 2 with `effective_date`, `expiration_date`, `is_current`, `updated_at`)
- `dim.plans` (SCD Type 2 with tier mapping and pricing)
- `dim.dates` (calendar spine for analysis)
- `fct.subscription_changes` (one row per customer per subscription per plan change event)

## Decisions and Roadblocks

1) NULL string in dates
- Problem: DuckDB casting failed on values like "NULL"
- Root cause: string literal instead of SQL NULL in source CSV
- Fix in `staging_subscriptions.sql` using CASE logic:
```sql
CASE
  WHEN subscription_end_date IS NULL OR subscription_end_date = 'NULL' THEN NULL
  ELSE subscription_end_date::timestamp
END AS subscription_ended_at,
CASE
  WHEN next_billing_date IS NULL OR next_billing_date = 'NULL' THEN NULL
  ELSE next_billing_date::timestamp
END AS next_billing_at
```

2) SQLMesh configuration did not see models
- Cause: file named `config.yaml` instead of `config.yml` and defaulting to a separate `db.db`
- Resolution: rename to `config.yml` and explicitly set `database: trail_trekker.db`

3) SCD Type 2 requirements in SQLMesh
- SQLMesh required an `updated_at` column for change detection
- Resolution: add `updated_at` to SCD2 selects and configure `valid_from_name` and `valid_to_name`

4) Date dimension scope
- Strategy: generate a date spine covering data plus buffer
- Final choice: 2022-08-01 through 2025-10-31 to cover subscriptions and near-term planning

## Solution

### Dimensional Model
- `dim.customers` and `dim.plans` use SCD Type 2 to preserve history of attributes that may change
- `dim.dates` provides a complete day-level calendar for joins and trend analysis

### Fact: Subscription Changes (transaction fact)
- Grain: one row per customer per subscription per change
- Change detection with window functions (simplified for learning):
```sql
LAG(plan_id) OVER (PARTITION BY customer_id ORDER BY subscription_started_at) AS previous_plan_id
```
- Primary metrics: `plan_change_amount`, `days_on_previous_plan`, boolean flags for upgrade, downgrade, cancellation

### Results
- 13 plan changes detected
- Most common transitions:
  - Basic Monthly to Pro Annual: 5
  - Basic Monthly to Premium Monthly: 3
  - Premium Monthly to Basic Monthly: 3

These outputs directly support the growth team in targeting upgrade opportunities and addressing downgrade risks.

## Orchestration
Add a schedule to the fact model:
```sql
MODEL (
  name fct.subscription_changes,
  cron '@hourly'
);
```
Local cron job to run hourly:
```bash
0 * * * * cd /Users/satkarkarki/Desktop/portfolio/trail-trekker-demo && \
  source .venv/bin/activate && \
  sqlmesh run prod >> ~/trail_trekker_cron.log 2>&1
```

## How to Run Locally
1) Create and activate a virtualenv, then install SQLMesh
```bash
python -m venv .venv && source .venv/bin/activate
pip install sqlmesh
```
2) Ingest CSVs into DuckDB
```bash
python3 ingest_data.py
```
3) Plan and apply in a dev environment
```bash
sqlmesh plan dev
```
4) Query models
```bash
sqlmesh fetchdf "SELECT * FROM fct__dev.subscription_changes LIMIT 5"
```

## Eureka Moments and Learning
- Guard against string representations of NULL when casting timestamps
- Prefer idempotent ingestion with `CREATE OR REPLACE` to support repeatable runs
- Use SCD Type 2 for entities where attributes change over time
- Define a calendar spine explicitly and scope it to the observed data window plus buffer
- Favor simple, testable steps with tight feedback loops

## Next Steps
- Add weekly periodic snapshot and lifecycle accumulating snapshot facts
- Introduce incremental materialization for large datasets
- Add additional audits and referential integrity checks
- Connect to a BI tool for dashboards

## Who Am I
I am an analytics engineer focused on clear documentation, pragmatic modeling, and reliable pipelines. This project demonstrates hands-on skills in DuckDB, SQLMesh, dimensional modeling, data quality, and orchestration. If you are reviewing this as a hiring manager or collaborator, the repository is organized for reproducibility and clarity, and the README documents tradeoffs and reasoning so the work can be maintained by any future owner.