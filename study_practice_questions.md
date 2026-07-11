# DP-700 Practice Questions

34 questions across all three domains. Answers and explanations at the end of each question.

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

---

**21.** A user has the Viewer workspace role on a lakehouse. What data can they see through Spark or the SQL analytics endpoint by default, before any OneLake security role is applied?

A. All lakehouse data
B. None — Viewer-role users have no OneLake data access by default
C. Only bronze-layer tables
D. Whatever their Microsoft Entra group grants

> **Answer: B.** Viewer role (and item-level Read-only permission) grants no OneLake data access by default; OneLake data access roles are what grant it, most commonly starting from the built-in `DefaultReader` role.

---

**22.** You restrict a user to a custom OneLake security role scoped to gold-layer tables only, but they can still query bronze and silver data. What did you most likely forget?

A. To grant them the Contributor workspace role
B. To remove them from the `DefaultReader` role
C. To apply row-level security
D. To enable OneLake workspace settings

> **Answer: B.** Every lakehouse ships with a `DefaultReader` role granting broad read access; a narrower custom role doesn't override it unless the user is also removed from `DefaultReader`.

---

**23.** A dimension attribute (customer address) needs to preserve full history because sales must be attributed to the address at the time of each transaction. Which SCD type fits?

A. Type 0
B. Type 1
C. Type 2
D. Type 3

> **Answer: C.** Type 2 inserts a new row per change and marks the old row expired, preserving full history — the standard choice when a fact needs to reference the dimension state at a point in time.

---

**24.** Which T-SQL masking function would you use to show only the last 4 digits of a credit card number?

A. `default()`
B. `email()`
C. `partial(prefix, padding, suffix)`
D. `random(low, high)`

> **Answer: C.** `partial()` lets you expose a configurable number of characters at the start/end with custom padding in between — e.g. `partial(0,"XXXX-XXXX-XXXX-",4)`.

---

**25.** A table has column-level security applied and is used by a Power BI semantic model in Direct Lake mode. What happens to queries against that table?

A. They fail with a permission error
B. They automatically fall back to DirectQuery
C. CLS is silently ignored in Direct Lake mode
D. The table is excluded from the semantic model automatically

> **Answer: B.** Direct Lake automatically falls back to DirectQuery when CLS is applied to a table — security is still enforced, just at DirectQuery performance instead of the Direct Lake baseline.

---

**26.** What is the recommended way to combine Git integration with deployment pipelines to avoid sync conflicts across environments?

A. Connect every stage workspace (Dev, Test, Prod) to Git independently
B. Connect only the Development workspace to Git; use the deployment pipeline to promote to Test/Production
C. Connect only the Production workspace to Git
D. Git integration and deployment pipelines can't be used together

> **Answer: B.** Connecting only Development to Git avoids Git sync conflicts across multiple stages — deployment pipelines then handle Fabric-side promotion to Test and Production.

---

**27.** In Fabric's 5-level hierarchy, which level sits directly between Tenant and Domain?

A. Workspace
B. Capacity
C. Item
D. Region

> **Answer: B.** The hierarchy is Tenant → Capacity → Domain → Workspace → Item.

---

**28.** An Activator rule needs to catch a sustained temperature problem while ignoring brief sensor spikes. Which combination of settings achieves this?

A. Threshold condition with "Every time" occurrence
B. Summarization (e.g. Average over a window) plus "When it has been true for" occurrence
C. A Property filter with no Condition
D. Missing-data detection

> **Answer: B.** Summarizing the raw signal (e.g. average temperature over a 10-minute window) smooths noise, and "When it has been true for" only fires once the condition is sustained, not on every brief fluctuation.

---

**29.** What differentiates a KQL materialized view from a plain stored function?

A. Materialized views precompute and auto-maintain aggregation results; stored functions just encapsulate reusable query logic with no precomputation
B. Stored functions are faster than materialized views
C. Materialized views can't accept parameters
D. There is no meaningful difference

> **Answer: A.** A materialized view maintains precomputed aggregation results (updated incrementally via a background process) for fast queries over huge tables; a stored function is just a reusable, optionally parameterized query — no precomputation involved.

---

**30.** Which Fabric warehouse monitoring surface would you use to see which queries have been consistently expensive over the past week, as opposed to what's running right now?

A. Dynamic management views (DMVs)
B. Query insights
C. The Capacity Metrics app
D. `.show ingestion failures`

> **Answer: B.** Query insights views track historical querying trends (duration, frequency over time); DMVs show currently-running activity.

---

**31.** In a newly created Fabric workspace, what is V-Order's default state, and why?

A. Enabled by default, to maximize read performance everywhere
B. Disabled by default, to favor write-heavy data engineering workloads
C. Enabled by default, but only for Warehouses
D. There is no default — it must be set explicitly on every write

> **Answer: B.** As of Microsoft's current documentation, V-Order is disabled by default in new workspaces specifically to optimize write-heavy ingestion/transformation performance — enable it deliberately for read-heavy or Direct Lake scenarios. (Older material describing it as "on by default" reflects a since-changed product default.)

---

**32.** You disable V-Order on a Fabric Warehouse using `ALTER DATABASE CURRENT SET VORDER = OFF;`. What should you know before doing this?

A. It only affects newly created tables, existing tables are unaffected
B. It can be re-enabled at any time with no consequence
C. It applies to the whole database, not per table, and cannot be re-enabled once turned off
D. It requires a support ticket to Microsoft to apply

> **Answer: C.** In a Warehouse, V-Order is a whole-database setting (unlike in a Lakehouse, where it can be controlled per session/table/write), and once disabled it cannot be turned back on.

---

**33.** During a deployment pipeline promotion from Test to Production, how does Fabric know which item in Production corresponds to which item in Test, so it overwrites rather than duplicates?

A. Matching item names only
B. A persistent connection between the parent item and its clone across stages ("item pairing"), created on first deploy and reused on every subsequent deploy
C. Fabric always creates new items and never overwrites
D. The deploying user manually maps each item every time

> **Answer: B.** This parent-clone connection is what deployment pipelines use to identify existing target-stage content to overwrite — it's what "item pairing" refers to.

---

**34.** After deploying a semantic model from Test to Production via a deployment pipeline, the data in Production still looks stale. What's the most likely cause?

A. The deployment failed silently
B. Deployment only copies definitions/metadata, not data — the semantic model needs a manual refresh in the target stage
C. Deployment rules weren't configured
D. The gateway is misconfigured

> **Answer: B.** Deployment pipelines copy item definitions and metadata between stages, not the underlying data — a semantic model must be refreshed after deployment before its data is current.
