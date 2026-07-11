# Ingest and Transform Data

<https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700>

## Design and Implement Loading Patterns

### Full vs incremental loads

- **Full load**: truncate/overwrite the target every run — simple, correct, but expensive at scale and not suitable for large or frequently-updated sources.
- **Incremental load**: load only new/changed rows since the last run, tracked via a **watermark** (a high-water-mark column like `ModifiedDate` or an incrementing key) or, in Fabric specifically, via **Delta Lake's change data feed (CDF)** for lakehouse-to-lakehouse incremental reads.
- Pipeline pattern: store the last-loaded watermark value in a control table, parameterize the source query with it (`WHERE ModifiedDate > @lastWatermark`), and update the control table after a successful run.

### Preparing data for a dimensional model

- Target the classic **star schema**: fact tables (numeric measures, foreign keys to dimensions, grain clearly defined) + dimension tables (descriptive attributes).
- Surrogate keys (not source system keys) on dimensions to support **slowly changing dimensions (SCD)**: Type 1 (overwrite), Type 2 (new row + effective-dated history) are the two you need to recognize.
- Conform dimensions shared across multiple fact tables (e.g., one `DimDate` used by both Sales and Inventory facts) so cross-subject-area analysis works.

### Loading pattern for streaming data

- Streaming loads land data continuously rather than in batch windows; typical Fabric pattern is **Eventstream → Lakehouse table (or Eventhouse/KQL DB)** with a defined **trigger/window** for how often micro-batches materialize.
- Choose an output **destination** based on query pattern: land in a Lakehouse Delta table for batch-style BI/Spark queries; land in an Eventhouse (KQL DB) for high-velocity, ad-hoc time-series/log-style queries.
- See also "Ingest and transform streaming data" below for the engine-level detail.

## Ingest and Transform Batch Data

### Choosing an appropriate data store

| Store | Query engine | Best for |
|---|---|---|
| **Lakehouse** | Spark + SQL analytics endpoint (read-only T-SQL over Delta) | Open Delta Parquet files, data science/Spark-heavy workloads, schema-on-read flexibility |
| **Warehouse** | Full read/write T-SQL, transactional | SQL-first teams, stored procedures, multi-table transactions, familiar Synapse/SQL Server semantics |
| **Eventhouse / KQL Database** | KQL (+ T-SQL subset) | High-volume streaming/time-series/log data, near-real-time analytics |
| **SQL analytics endpoint** (auto-generated on a Lakehouse) | T-SQL, read-only | Let SQL-first consumers query lakehouse Delta tables without touching Spark |

- Rule of thumb: if you need Spark or ML → Lakehouse; if you need transactional multi-table T-SQL writes → Warehouse; if it's streaming/telemetry-shaped → Eventhouse.

### Dataflows Gen2 vs notebooks vs KQL vs T-SQL for transformation

- **Dataflow Gen2**: low-code Power Query, good for smaller/medium volumes and connector-rich sources, business-user friendly.
- **Notebook (PySpark/SQL/Scala)**: code-first, scales to large data, best for complex transformation logic, reusable functions, unit-testable code.
- **KQL**: transform data already in (or streaming into) an Eventhouse — update policies and materialized views do KQL-native transformation close to ingestion.
- **T-SQL**: transform inside a Warehouse using stored procedures/views — best when the pipeline is already SQL-centric and needs set-based transactional logic.

### OneLake shortcuts

- A **shortcut** is a virtual reference from one location (a Lakehouse/KQL DB) to data stored elsewhere — another Fabric item, ADLS Gen2, Amazon S3, Dataverse, or another OneLake path — **without copying the data**.
- Reads through a shortcut go straight to the source; this is how you achieve a "zero-copy" architecture — e.g., a gold Lakehouse shortcuts into a silver Lakehouse's tables instead of duplicating them.
- Manage lifecycle like any item: create, rename, delete a shortcut without touching the underlying data; deleting a shortcut never deletes the source.

### Mirroring

- **Mirroring** continuously, automatically replicates data from an external operational source (Azure SQL DB, Cosmos DB, Snowflake, and on-prem SQL Server via mirroring gateway, etc.) into OneLake as Delta tables — near real-time, low/no-code, and free of Fabric compute charges for the mirroring replication itself.
- Use mirroring instead of a pipeline copy when you want continuous, low-latency replication of an entire operational database into Fabric for analytics, without hand-building CDC pipelines.
- Contrast with a shortcut: mirroring **copies and continuously syncs** data into OneLake Delta format; a shortcut **references** data in place without copying.

### Ingesting data with pipelines

- **Copy Data activity** is the primary batch-ingestion mechanism: source connector → optional staging → sink (Lakehouse/Warehouse table or files), with schema mapping and optional column transformations at copy time.
- Supports a large connector library (databases, SaaS, files, REST) and can run at scale with partitioned/parallel copy for large sources.

### Transforming with PySpark, SQL, and KQL

- **PySpark**: DataFrame API (`select`, `withColumn`, `join`, `groupBy`) or Spark SQL (`spark.sql("...")`) inside a notebook; write results with `df.write.format("delta").mode(...)save(...)` or `saveAsTable`.
- **T-SQL**: standard `SELECT`/`INSERT`/`MERGE`/views/stored procedures against a Warehouse or a Lakehouse's SQL analytics endpoint (read-only there).
- **KQL**: `.set-or-append`, update policies, and query-time transforms (`extend`, `summarize`, `mv-expand`) inside an Eventhouse.

### Denormalize, group, aggregate

- **Denormalize**: joins across normalized source tables to produce wide, query-friendly tables (common in the silver → gold step of a medallion architecture) — trades storage/duplication for simpler, faster downstream queries.
- **Group and aggregate**: `groupBy().agg()` in PySpark or `GROUP BY` in T-SQL/KQL (`summarize` in KQL) to produce rollups feeding fact tables or reporting layers.

### Handling duplicate, missing, and late-arriving data

- **Duplicates**: `dropDuplicates()` in PySpark, or `MERGE`/`ROW_NUMBER() OVER (PARTITION BY ...)` deduplication in T-SQL, keyed on a natural or business key plus a "latest wins" ordering column.
- **Missing data**: decide per-column whether to drop rows (`na.drop()`), impute a default/derived value (`na.fill()`), or flag/quarantine the row for review rather than silently dropping it.
- **Late-arriving data**: for streaming, use a **watermark** (see Streaming section) to bound how long the engine waits before finalizing a window; for batch/dimensional loads, a **late-arriving dimension** pattern inserts a placeholder/inferred dimension row so the fact table can still load, then backfills attributes when the real dimension record arrives.
- `MERGE INTO` (Delta) is the standard upsert primitive tying several of these together: match on business key, `UPDATE SET` on match (handles late-arriving updates), `INSERT` when not matched.

## Ingest and Transform Streaming Data

### Choosing an appropriate streaming engine

- **Eventstream**: no-code/low-code stream ingestion and light in-flight transformation (filter, aggregate, group by, expand) with a visual designer; routes to a Lakehouse, Eventhouse, or another downstream item. Best default choice for connector-driven ingestion with simple transforms.
- **Spark Structured Streaming** (in a notebook): code-first, use when transformation logic is complex, needs custom code/ML, or must integrate tightly with existing PySpark batch logic (unify batch+stream code paths).
- **KQL** (in an Eventhouse, via update policies on streaming ingestion): best when the destination is already an Eventhouse and you want KQL-native, low-latency transform-on-ingest.

### Native tables vs OneLake shortcuts in Real-Time Intelligence

- **Native tables** in an Eventhouse: data is ingested and stored directly in KQL DB's own optimized storage format — best performance, full KQL feature support.
- **OneLake shortcuts** into an Eventhouse: reference Delta tables living elsewhere (e.g., a Lakehouse) without copying — trades some query performance/latency for avoiding duplication, useful when the data already lives in OneLake Delta format and you don't want a second copy.

### Query acceleration for OneLake shortcuts vs standard OneLake shortcuts (Real-Time Intelligence)

- **Standard shortcut**: queries read the Delta/Parquet files directly at query time — simplest, no extra storage, but slower for KQL-style high-performance analytics since Parquet isn't optimized for KQL's engine.
- **Query acceleration**: Fabric transparently builds and maintains an accelerated (KQL-native) cache/index over the shortcut's data, giving near-native-table query performance while the source of truth stays in OneLake Delta — choose this when shortcut query latency on Parquet is a bottleneck but you still don't want to duplicate/own the data as a native table.

### Processing with Eventstreams

- Sources: Azure Event Hubs, IoT Hub, Kafka, Fabric Workspace events, CDC connectors, sample data.
- In-stream operations: filter, aggregate (windowed), manage fields (select/rename), group by, union, expand (flatten arrays/JSON).
- Destinations: Lakehouse (as Delta table), Eventhouse (KQL DB), Reflex/Activator (for alerting), or another Eventstream.

### Processing with Spark structured streaming

- Read a stream: `spark.readStream.format("...").load(...)`; each micro-batch is processed like a bounded DataFrame.
- **`withWatermark(eventTimeCol, delayThreshold)`** marks how late data is allowed to arrive relative to the max event time seen, so the engine knows when a windowed aggregation can be finalized and old state can be dropped — e.g. `df.withWatermark("timestamp", "10 minutes")`.
- Write a stream: `df.writeStream.outputMode(...).format("delta").option("checkpointLocation", ...).start()`; the **checkpoint location** is what makes the stream fault-tolerant/exactly-once on restart.
- Output modes: `append` (only new rows, most common for unbounded sinks), `complete` (whole result table every trigger, needed for some aggregations), `update` (only changed rows).

### Processing with KQL

- Streaming ingestion into an Eventhouse table, optionally transformed on the way in via an **update policy** (a KQL function that runs automatically on newly ingested data and writes derived rows to a target table).
- Query-time operators for time-series work: `summarize ... by bin(Timestamp, 1m)` for time-bucketed aggregation, `make-series` for evenly-spaced series construction (useful for anomaly detection/forecasting).

### Creating windowing functions

- **PySpark**: `from pyspark.sql.functions import window` then `groupBy(window(col("timestamp"), "10 minutes", "5 minutes"), ...)` — first duration is window length, second (optional) is the slide interval; omit the second argument for non-overlapping (tumbling) windows, include it for overlapping (sliding) windows.
  ```python
  from pyspark.sql.functions import window
  (df.withWatermark("timestamp", "10 minutes")
     .groupBy(window(df.timestamp, "10 minutes", "5 minutes"), df.value)
     .count())
  ```
- **KQL**: `summarize count() by bin(Timestamp, 5m)` for tumbling windows; `make-series` with a step size for regularly-spaced windows across a range.
- **Eventstream** designer: a built-in "Aggregate" node lets you pick tumbling, hopping (sliding), or sliding window types visually without writing code.
- Know the three window types conceptually: **tumbling** (fixed, non-overlapping), **hopping/sliding** (fixed size, overlapping, advances by a hop size smaller than the window), **session** (dynamic length, closes after a gap of inactivity).
