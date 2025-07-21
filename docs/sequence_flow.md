# DB Sentinel Utility – Detailed Sequence Flow

Below is a detailed sequence diagram of the DB Sentinel utility, showing the interactions between the main components during a table comparison and synchronization run. (Note: Mermaid does not support coloring for sequence diagrams, so the legend is for reference only.)

```mermaid
sequenceDiagram
    participant User as User
    participant Main as db_sentinel.py
    participant Config as ConfigLoader
    participant Logger as Logging
    participant SourceDB as Source Oracle DB
    participant TargetDB as Target Oracle DB
    participant Batch as BatchFetcher
    participant Hasher as RowHasher
    participant Comparator as Comparator
    participant SQLGen as SQLGenerator
    participant Audit as AuditLogger
    participant Reverify as ReVerifier

    User->>Main: Start utility
    Main->>Config: load_config('config.yaml')
    Config-->>Main: config dict
    Main->>Logger: setup_logging()
    Main->>SourceDB: Connect
    Main->>TargetDB: Connect
    Main->>Main: For each table in config
    Main->>SourceDB: Get row count
    Main->>SourceDB: Get columns (if not specified)
    Main->>Main: Prepare for restart/resume (if enabled)
    Main->>Main: Calculate batches
    Main->>Main: ThreadPoolExecutor (max_threads)
    loop For each batch (in parallel)
        Main->>Batch: fetch_data_batchwise (source)
        Main->>Batch: fetch_data_batchwise (target)
        Batch-->>Main: rows, col_names
        Main->>Hasher: hash_rows (source)
        Main->>Hasher: hash_rows (target)
        Hasher-->>Main: src_hashes, tgt_hashes
        Main->>Comparator: compare_hashes
        Comparator-->>Main: mismatches, missing_in_source, missing_in_target
        Main->>Audit: log_batch_event
    end
    Main->>Reverify: verify_primary_keys (optional)
    Reverify-->>Main: safe_to_insert, valid_update_pks
    Main->>SQLGen: generate_sql_file
    SQLGen-->>Main: SQL files
    Main->>Audit: log_event (summary)
    Main->>Main: Write comparison report (CSV)
    Main->>Logger: log_event (completed)
    Main-->>User: Output files, logs, report
```

---

## Legend (for reference only)

| Color         | Component/Role         |
|---------------|-----------------------|
| 🟨 Yellow     | User/Config           |
| 🟩 Green      | Main Script           |
| 🟦 Blue       | DB Connections        |
| 🟧 Orange     | Logging/Batch         |
| 🟥 Red        | Hashing               |
| 🟩 Light Green| Comparison            |
| 🟦 Light Blue | Audit Logging         |
| 🟪 Pink       | Reverification        |
| 🟫 Gray Blue  | SQL Generation        |

> Mermaid does not support coloring for sequence diagrams. Use the legend as a reference for roles/components only. 