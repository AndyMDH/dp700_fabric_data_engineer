# DP-700 Flashcards

Format: **Q →** answer. Cover with your hand, work through each section.

## Implement and Manage

- **Q:** What's the difference between the Starter pool and a custom Spark pool? **→** Starter pool is Microsoft-managed for fast session start; custom pool lets you control node family, size, and autoscale range.
- **Q:** What does a Fabric "environment" item bundle? **→** Spark properties, libraries (PyPI/custom wheel/jar), and resources — attach as workspace default so notebooks inherit config automatically.
- **Q:** When would you use an Apache Airflow Job instead of a pipeline? **→** When you need Airflow-specific operators/sensors, complex Python-defined DAG logic, or are migrating existing Airflow DAGs rather than rewriting.
- **Q:** What is OneLake, in one sentence? **→** A single, tenant-wide logical data lake — one Delta Parquet copy of data, addressable via ADLS Gen2-compatible APIs, shared across every Fabric engine with no data movement.
- **Q:** Difference between Git integration and a deployment pipeline? **→** Git integration = version control/collaboration within a workspace branch; deployment pipeline = promotion of items across Dev/Test/Prod stage workspaces, with optional deployment rules.
- **Q:** What is a Fabric database project? **→** A Warehouse/SQL database schema stored as source-controlled `.sql` files (schema-as-code), similar to SSDT.
- **Q:** Order the workspace roles from most to least privileged. **→** Admin > Member > Contributor > Viewer.
- **Q:** RLS vs CLS vs OLS — what does each restrict? **→** RLS: rows. CLS: specific columns. OLS: whole tables/objects (hides them entirely).
- **Q:** What problem does OneLake data access roles (folder/file-level security) solve that SQL-level RLS doesn't? **→** It's engine-agnostic — enforced consistently for Spark, SQL endpoint, Power BI, and direct external OneLake reads, closing the gap where SQL RLS didn't protect direct file access.
- **Q:** Does dynamic data masking change stored data? **→** No — it masks values at query time for non-privileged users; underlying data is untouched.
- **Q:** What's the difference between "Promoted" and "Certified" endorsement? **→** Promoted = a workspace member signals quality/readiness. Certified = a designated certifying authority marks it as an org-approved authoritative source.
- **Q:** Where do Fabric item activities (create/delete/share/export) get logged for compliance? **→** Microsoft Purview / unified Microsoft 365 audit log.
- **Q:** Pipeline, Dataflow Gen2, or Notebook — which is the "orchestrator"? **→** Pipeline — it sequences activities and can call notebooks and dataflows as steps.
- **Q:** Scheduled trigger vs event-based trigger? **→** Scheduled = fixed cadence. Event-based = fires on an event (e.g., file arrival, message on an event stream).
- **Q:** What lets one parameterized pipeline serve many source tables instead of one pipeline per table? **→** A ForEach activity over a control/metadata table, invoking a parameterized child pipeline/notebook per row.

## Ingest and Transform

- **Q:** What tracks progress for an incremental load? **→** A watermark (high-water-mark column value stored in a control table) or Delta Lake's change data feed (CDF).
- **Q:** SCD Type 1 vs Type 2? **→** Type 1 overwrites the existing dimension row (no history). Type 2 inserts a new effective-dated row, preserving history.
- **Q:** Lakehouse vs Warehouse vs Eventhouse — pick the store. **→** Lakehouse: Spark/ML, schema-on-read. Warehouse: transactional multi-table T-SQL. Eventhouse: high-volume streaming/time-series (KQL).
- **Q:** What is a OneLake shortcut? **→** A virtual, zero-copy reference from one location to data stored elsewhere (another Fabric item, ADLS, S3, Dataverse) — reads go straight to the source.
- **Q:** What is mirroring, and how does it differ from a shortcut? **→** Continuous, automatic, low-latency replication of an external operational database into OneLake as Delta tables (copies data); a shortcut references data in place without copying.
- **Q:** Which activity is the primary batch-ingestion mechanism in a pipeline? **→** Copy Data activity.
- **Q:** What Delta operation is the standard upsert primitive for handling late-arriving updates? **→** `MERGE INTO` — `UPDATE SET` on match, `INSERT` when not matched.
- **Q:** What's the "late-arriving dimension" pattern? **→** Insert a placeholder/inferred dimension row so the fact table can load, then backfill real attributes when the dimension record arrives.
- **Q:** Eventstream vs Spark Structured Streaming vs KQL — when do you pick each for streaming transform? **→** Eventstream: no/low-code, simple in-flight transforms. Structured Streaming: complex custom code, unify with batch PySpark. KQL: destination already an Eventhouse, want transform-on-ingest via update policy.
- **Q:** Native table vs standard shortcut vs query-accelerated shortcut in an Eventhouse? **→** Native: best performance, data duplicated. Standard shortcut: zero-copy, slower (queries raw Parquet). Query-accelerated shortcut: zero-copy with a maintained accelerated cache/index for near-native speed.
- **Q:** What does `withWatermark` do in Spark Structured Streaming? **→** Marks how late data may arrive relative to max event time seen, so windowed aggregations can be finalized and old state dropped.
- **Q:** Tumbling vs hopping/sliding vs session windows? **→** Tumbling: fixed, non-overlapping. Hopping/sliding: fixed size, overlapping, advances by a hop smaller than the window. Session: dynamic length, closes after an inactivity gap.
- **Q:** In PySpark's `window()` function, what does omitting the second argument produce? **→** A tumbling window (non-overlapping); providing it produces a sliding/hopping window.

## Monitor and Optimize

- **Q:** What's the first place to check "did my pipeline/notebook/dataflow run, and how long did it take"? **→** The Monitoring Hub.
- **Q:** What does the Fabric Capacity Metrics app show? **→** Capacity-level CU consumption across all workloads, including smoothing (spreading bursts) and throttling (what happens when sustained usage exceeds the CU budget).
- **Q:** For a Direct Lake semantic model, what's the key thing to monitor besides refresh duration? **→** Fallback-to-DirectQuery events — a silent performance regression when Direct Lake limits are exceeded.
- **Q:** What no-code Fabric item handles condition/event-based alerting? **→** Data Activator (Reflex).
- **Q:** Spark job fails with OOM/executor lost — first thing to check? **→** The Spark UI's stage summary for data skew (one partition much larger than others) before tuning memory.
- **Q:** What does `OPTIMIZE table ZORDER BY (col)` do? **→** Compacts small files into larger ones AND co-locates data by the given column so filters on it can skip more files (data skipping).
- **Q:** What does `VACUUM` do, and what's the default retention? **→** Deletes Delta files no longer referenced by the transaction log past the retention window; default is 168 hours (7 days).
- **Q:** What is V-Order? **→** A Fabric-specific write-time Parquet optimization (special sorting/encoding) that speeds up downstream reads across Spark, SQL endpoint, and Direct Lake; on by default.
- **Q:** What Spark feature lets multiple notebooks share one Spark session to cut cold-start time? **→** High concurrency mode.
- **Q:** When should you use a broadcast join? **→** When one side of a join is small enough to fit in executor memory — avoids an expensive shuffle join.
- **Q:** What is the Native Execution Engine in Fabric Spark? **→** A vectorized, non-JVM execution engine for eligible query stages that speeds up supported operations, enabled at the environment/pool level, transparent to notebook code.
- **Q:** In an Eventhouse, what two policies control how much recent data stays fast-queryable vs archived? **→** Caching policy (hot vs cold data) and retention policy (how long data is kept at all).
- **Q:** For KQL query performance on frequently-run aggregates, what should you use instead of scanning the raw table every time? **→** Materialized views.
