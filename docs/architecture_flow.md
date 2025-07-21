# DB Sentinel Utility – Architecture Flow

Below is a high-level flowchart of the DB Sentinel utility for Oracle table comparison and synchronization. This diagram illustrates the main steps, threading, and key modules involved in the process. Each node is color-coded for clarity.

```mermaid
flowchart TD
    A([Start])
    B[Load config.yaml]
    C[Setup Logging]
    D[Connect to Source DB]
    E[Connect to Target DB]
    F[For each Table in config]
    G[Get Row Count]
    H[Determine Columns]
    I[Prepare for Restart/Resume]
    J[Batch Processing (ThreadPool)]
    K[Fetch Source Batch]
    L[Fetch Target Batch]
    M[Hash Rows (SHA256)]
    N[Compare Hashes]
    O[Log Audit/Batch Events]
    P[Reverification (optional)]
    Q[Generate SQL for Sync]
    R[Write Comparison Report]
    S([End])

    A --> B --> C --> D --> E --> F
    F --> G --> H --> I --> J
    J --> K
    J --> L
    K --> M
    L --> M
    M --> N
    N --> O
    N --> P
    P --> Q
    Q --> R
    R --> S

    class A,S start;
    class B config;
    class C logging;
    class D,E db;
    class F loop;
    class G,H,I batch;
    class J thread;
    class K,L batch;
    class M hash;
    class N compare;
    class O audit;
    class P verify;
    class Q sql;
    class R output;

    classDef start fill:#b3e6b3,stroke:#333,stroke-width:2;
    classDef config fill:#ffe699,stroke:#333,stroke-width:1.5;
    classDef logging fill:#ffd699,stroke:#333,stroke-width:1.5;
    classDef db fill:#b3d1ff,stroke:#333,stroke-width:1.5;
    classDef loop fill:#e6ccff,stroke:#333,stroke-width:1.5;
    classDef batch fill:#fff2cc,stroke:#333,stroke-width:1.5;
    classDef thread fill:#f9cb9c,stroke:#333,stroke-width:1.5;
    classDef hash fill:#f4cccc,stroke:#333,stroke-width:1.5;
    classDef compare fill:#d9ead3,stroke:#333,stroke-width:1.5;
    classDef audit fill:#cfe2f3,stroke:#333,stroke-width:1.5;
    classDef verify fill:#ead1dc,stroke:#333,stroke-width:1.5;
    classDef sql fill:#d0e0e3,stroke:#333,stroke-width:1.5;
    classDef output fill:#b6d7a8,stroke:#333,stroke-width:1.5;
```

---

## Legend

| Color         | Step/Component         |
|---------------|-----------------------|
| 🟨 Yellow     | Configuration/Setup   |
| 🟦 Blue       | Database Connections  |
| 🟪 Purple     | Table Loop            |
| 🟧 Orange     | Threading/Batch       |
| 🟥 Red        | Hashing               |
| 🟩 Green      | Comparison/Output     |
| 🟦 Light Blue | Audit Logging         |
| 🟪 Pink       | Reverification        |
| 🟫 Gray Blue  | SQL Generation        |
| 🟩 Green      | Start/End             |

> Copy the Mermaid code block above into a Mermaid-compatible viewer/editor that supports classDef for flowcharts to visualize the colored architecture flow. 