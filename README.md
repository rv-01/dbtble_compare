# DB_Sentinel_util_super

A robust, production-ready Python utility for comparing and synchronizing tables between two Oracle databases. Designed for data engineers and DBAs, it supports row-level hashing, multi-threaded batch comparison, per-table configuration, audit logging, and restart/resume logic.

---

## 🚀 Features
- **Compare Oracle tables** across two databases using row-level hashing (SHA256).
- **Multi-threaded** batch comparison for high performance.
- **Highly configurable** via `config.yaml`:
  - Any table, schema, primary keys, columns, and per-table exclude columns.
  - Per-table `exclude_columns` for fine-grained control.
  - Optional WHERE clauses and custom batch sizes.
  - **Configurable number of threads** for parallel batch processing.
- **No cx_Oracle**: Uses `oracledb` for modern Oracle connectivity.
- **Progress bar** and detailed logging.
- **Audit and metadata tables** for job tracking and restart/resume.
- **Generates SQL** for syncing source and target tables.
- **Post-comparison reverification** to avoid constraint violations.

---

## ⚙️ Setup

1. **Clone the repository**
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Configure Oracle client libraries** (if needed for `oracledb`)
4. **Edit `config.yaml`** with your database and table details (see below)

---

## 📝 Configuration (`config.yaml`)

Example:
```yaml
source_db:
  user: source_user
  password: source_pass
  dsn: source_host:1521/sourcedb
target_db:
  user: target_user
  password: target_pass
  dsn: target_host:1521/targetdb

max_threads: 4  # Number of threads to use for batch processing

table_config:
  - table_name: "EMPLOYEES"
    schema: "HR"
    primary_key: ["EMPLOYEE_ID"]
    chunk_size: 10000
    exclude_columns: ["LAST_UPDATED"]
  - table_name: "ORDER_ITEMS"
    schema: "SALES"
    primary_key: ["ORDER_ID", "ITEM_ID"]
    chunk_size: 5000
    columns: ["ORDER_ID", "ITEM_ID", "QUANTITY", "PRICE", "STATUS"]
    exclude_columns: ["LAST_UPDATED", "CREATED_BY"]
  # ... more tables ...

paths:
  audit_log: ./logs/audit.log
  source_sql_output: ./output/source_sync_statements.sql
  target_sql_output: ./output/target_sync_statements.sql

flags:
  enable_audit_table: true
  enable_reverification: true
  enable_restart: true
```

**Key options:**
- `max_threads`: Number of threads to use for parallel batch processing (see below).
- `exclude_columns`: List of columns to ignore during comparison (per table).
- `columns`: List of columns to include (optional; all if omitted).
- `where_clause`: Optional SQL WHERE clause for filtering rows.
- `chunk_size`: Batch size for fetching and comparing rows.
- `primary_key`: List of columns that make up the primary key.

---

## 🧵 About `max_threads`

The `max_threads` parameter controls how many batches are processed in parallel using Python threads. This can significantly affect performance and resource usage:

- **Higher `max_threads`** (e.g., 8, 16, or more):
  - More batches are processed at the same time, which can speed up comparison if your system and database can handle the load.
  - Increases CPU and memory usage on your machine.
  - May increase load on your Oracle databases (more concurrent queries).
- **Lower `max_threads`** (e.g., 2 or 4):
  - Less resource usage, safer for smaller systems or when running other workloads.
  - May be slower, especially for large tables or many tables.

**How to choose:**
- For most modern laptops/servers, 4–8 threads is a good starting point.
- If you have many CPU cores and plenty of RAM, you can try higher values.
- If you see your system or database struggling, reduce `max_threads`.
- You can tune this value in `config.yaml` without changing any code.

---

## 📏 About `chunk_size`

The `chunk_size` parameter controls how many rows are fetched and compared in each batch for a table. This has a direct impact on both **performance** and **resource usage**:

- **Larger `chunk_size`** (e.g., 10,000 or 50,000):
  - Fewer database round-trips, faster for small/medium tables.
  - Higher memory usage per thread (may cause issues on very large tables or low-memory systems).
  - May increase network load and risk of timeouts for very large tables.
- **Smaller `chunk_size`** (e.g., 500 or 2,000):
  - Lower memory usage, safer for very large tables.
  - More database round-trips, slightly slower overall.
  - Reduces risk of timeouts or memory errors.

**How to choose:**
- For small tables (<100,000 rows): use a larger chunk (e.g., 10,000 or more).
- For very large tables (millions of rows): use a smaller chunk (e.g., 1,000–5,000).
- If you see memory or timeout errors, reduce `chunk_size`.
- You can set a different `chunk_size` for each table in `config.yaml`.

---

## ▶️ Usage

1. **Run the script in batch mode:**
   ```bash
   python db_sentinel.py
   ```
2. **Monitor progress** in the console and in `logs/audit.log`.
3. **Review output files:**
   - `output/source_sync_statements.sql` (SQL to sync target from source)
   - `output/target_sync_statements.sql` (SQL to sync source from target)
   - `logs/audit.log` (detailed audit log)
4. **Check audit/metadata tables** in your Oracle DB for job and batch status (if enabled).

---

## 📂 Output
- **SQL files** for syncing tables
- **Audit log** for all events and mismatches
- **Audit/metadata tables** in the database (if enabled)

---

## 🛠️ Troubleshooting
- **Connection errors:**
  - Check your `dsn` values and network connectivity.
  - Ensure Oracle client libraries are installed if required.
- **Permission errors:**
  - Ensure the DB user has SELECT privileges on all tables.
- **Restart/resume:**
  - If enabled, the script will resume from the last processed batch after a crash.
- **Audit/metadata tables:**
  - Create these tables in your Oracle DB if you want job/batch tracking (see DDL in documentation).

---

## 🙋‍♂️ Questions?
Open an issue or contact the maintainer for help or enhancements. 