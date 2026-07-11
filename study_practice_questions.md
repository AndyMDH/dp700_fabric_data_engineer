# DP-700 Practice Questions

20 questions across all three domains. Answers and explanations at the end of each question.

---

**1.** You need to promote a set of Fabric items from a Test workspace to a Production workspace, automatically swapping a connection string that differs between environments. What should you use?

A. Git integration with a `prod` branch
B. A deployment pipeline with a deployment rule
C. A database project
D. A shortcut

> **Answer: B.** Deployment pipelines promote items across stage workspaces, and deployment rules let you automatically swap environment-specific values (like a connection string) on deploy.

---

**2.** Which security layer would you use to hide an entire staging table from analysts, without affecting other tables in the same lakehouse?

A. Dynamic data masking
B. Row-level security (RLS)
C. Object-level security (OLS)
D. Sensitivity labels

> **Answer: C.** OLS denies access to whole tables/objects, hiding them from schema browsing entirely.

---

**3.** A team needs regional managers to see only rows for their own region in a shared sales table, with everyone querying the same view. What should you configure?

A. Column-level security
B. Row-level security via a T-SQL security policy
C. A OneLake data access role
D. Dynamic data masking

> **Answer: B.** RLS with `CREATE SECURITY POLICY` filters rows based on the querying user.

---

**4.** You need access control that is enforced consistently whether data is read via Spark, the SQL analytics endpoint, Power BI, or an external app reading OneLake files directly. Which feature provides this?

A. Item-level sharing
B. RLS defined on the Warehouse
C. OneLake data access roles
D. Workspace roles

> **Answer: C.** OneLake data access roles are engine-agnostic, enforced at the folder/file level regardless of which engine reads the data — unlike SQL-level RLS, which only protects SQL query access.

---

**5.** Which orchestration tool is best suited for a business analyst who needs to build a connector-rich, low-code transformation over a moderate volume of data?

A. Notebook
B. Dataflow Gen2
C. Pipeline
D. Eventstream

> **Answer: B.** Dataflow Gen2 is the low-code, Power Query–based, business-user-friendly transformation tool.

---

**6.** A pipeline needs to run every time a new file lands in a OneLake folder, rather than on a fixed schedule. What should you configure?

A. A scheduled trigger set to run every minute
B. An event-based trigger on storage events
C. A ForEach activity
D. A tumbling window trigger with a 1-second interval

> **Answer: B.** Event-based triggers fire on events like file arrival, avoiding wasteful polling on a fixed schedule.

---

**7.** You want one pipeline to run the same copy-and-transform logic against 50 source tables listed in a control table. What's the standard pattern?

A. Duplicate the pipeline 50 times
B. A ForEach activity iterating the control table, calling a parameterized child pipeline/notebook
C. 50 separate Dataflows Gen2
D. A single notebook with 50 hardcoded cells

> **Answer: B.** This is the standard metadata-driven orchestration pattern — parameters + dynamic expressions + ForEach.

---

**8.** Which data store should you choose for a workload requiring full read/write T-SQL with multi-table transactions and stored procedures?

A. Lakehouse
B. Warehouse
C. Eventhouse
D. SQL analytics endpoint

> **Answer: B.** The Warehouse supports full read/write T-SQL and transactions; the Lakehouse's SQL analytics endpoint is read-only.

---

**9.** What is the key difference between a OneLake shortcut and mirroring?

A. Shortcuts are read-only; mirroring supports writes
B. A shortcut references data in place without copying; mirroring continuously copies/replicates data into OneLake
C. Shortcuts only work within one workspace; mirroring works across workspaces
D. There is no meaningful difference

> **Answer: B.** Shortcuts are zero-copy virtual references. Mirroring continuously replicates an external operational source into OneLake as Delta tables.

---

**10.** You're loading a fact table and a matching dimension record hasn't arrived yet. What pattern handles this without failing the load?

A. Drop the fact row until the dimension arrives
B. Late-arriving dimension pattern: insert a placeholder dimension row, backfill later
C. Use VACUUM to remove the incomplete fact row
D. Switch the dimension to SCD Type 1

> **Answer: B.** This is the standard late-arriving dimension pattern in dimensional modeling.

---

**11.** Which streaming destination pattern gives near-native-table KQL query performance over data that stays stored in OneLake Delta format without duplication?

A. Native table
B. Standard OneLake shortcut
C. Query-accelerated OneLake shortcut
D. Mirroring

> **Answer: C.** Query acceleration builds a maintained accelerated cache/index over a shortcut's data — near-native performance, no duplication (unlike a native table, which does duplicate).

---

**12.** In Spark Structured Streaming, what does `df.withWatermark("timestamp", "10 minutes")` control?

A. How often the stream triggers a micro-batch
B. How late data may arrive relative to the max event time seen, before a window is finalized
C. The output mode of the write stream
D. The checkpoint location

> **Answer: B.** Watermarking bounds state growth and tells the engine when a windowed aggregation can be finalized.

---

**13.** `window(df.timestamp, "10 minutes", "5 minutes")` produces which type of window?

A. Tumbling
B. Hopping/sliding
C. Session
D. Fixed, non-overlapping

> **Answer: B.** A slide interval (5 min) smaller than the window length (10 min) creates overlapping, hopping windows. Omitting the slide interval would produce a tumbling window.

---

**14.** A Direct Lake semantic model has been getting slower with no visible refresh failures. What should you check first?

A. VACUUM retention settings
B. Fallback-to-DirectQuery events
C. Pipeline run history
D. OneLake shortcut permissions

> **Answer: B.** Direct Lake can silently fall back to slower DirectQuery when limits are exceeded — this shows as a performance regression, not an explicit failure.

---

**15.** A Spark notebook job is failing with executor-lost/OOM errors. What should you investigate first?

A. Increase VACUUM retention
B. Check the Spark UI stage summary for data skew
C. Switch from a Warehouse to a Lakehouse
D. Disable V-Order

> **Answer: B.** Executor-lost/OOM is most commonly caused by data skew (one oversized partition); check task duration distribution in the Spark UI before blindly increasing memory.

---

**16.** Which command both compacts small files and co-locates data by a filter column to enable data skipping?

A. `VACUUM table`
B. `OPTIMIZE table ZORDER BY (col)`
C. `ANALYZE TABLE table COMPUTE STATISTICS`
D. `REPARTITION table`

> **Answer: B.** `OPTIMIZE ... ZORDER BY` compacts files and clusters data by the specified column(s) for better data skipping on filters.

---

**17.** What is V-Order in Microsoft Fabric?

A. A row-level security ordering rule
B. A Fabric-specific write-time Parquet optimization that speeds up downstream reads across engines
C. A pipeline activity ordering setting
D. A KQL window function

> **Answer: B.** V-Order applies special sorting/encoding at write time to accelerate reads in Spark, the SQL analytics endpoint, and Direct Lake.

---

**18.** Which Fabric feature lets multiple notebooks share a single Spark session to reduce cold-start latency?

A. Autoscale
B. High concurrency mode
C. Native Execution Engine
D. Dynamic executor allocation

> **Answer: B.** High concurrency mode shares one Spark session/application across notebooks, cutting per-notebook session spin-up time.

---

**19.** In an Eventhouse, what should you use to avoid recomputing an expensive aggregate query on every run?

A. A standard OneLake shortcut
B. A materialized view
C. VACUUM
D. Dynamic data masking

> **Answer: B.** Materialized views incrementally maintain aggregate results instead of recomputing them from the raw table each query.

---

**20.** Which Fabric workspace item would you choose to migrate an existing set of Airflow DAGs with custom Python operators, rather than rewriting the logic as a Fabric pipeline?

A. Dataflow Gen2
B. Apache Airflow Job
C. Eventstream
D. Data Activator

> **Answer: B.** The Apache Airflow Job item runs a managed Airflow environment, letting you keep existing DAGs and provider-package operators rather than rebuilding the logic as pipeline activities.
