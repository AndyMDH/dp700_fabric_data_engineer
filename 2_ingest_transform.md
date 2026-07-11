# Ingest and Transform Data

<https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700>

Sourced from the official DP-700 skills outline plus the actual Microsoft Learn training modules behind it: [Ingest Data with Microsoft Fabric](https://learn.microsoft.com/en-us/training/paths/ingest-data-with-microsoft-fabric/), [Implement a Lakehouse with Microsoft Fabric](https://learn.microsoft.com/en-us/training/paths/implement-lakehouse-microsoft-fabric/), [Implement a data warehouse with Microsoft Fabric](https://learn.microsoft.com/en-us/training/paths/work-with-data-warehouses-using-microsoft-fabric/), and [Implement Real-Time Intelligence with Microsoft Fabric](https://learn.microsoft.com/en-us/training/paths/explore-real-time-analytics-microsoft-fabric/).

## Design and Implement Loading Patterns

*Source: [Load data into a Microsoft Fabric data warehouse](https://learn.microsoft.com/en-us/training/modules/load-data-into-microsoft-fabric-data-warehouse/) — Explore data load strategies.*

Loading data into a warehouse (or lakehouse) follows a predictable process: decide how much data to move, stage it for preparation, then load dimension tables before fact tables since facts reference dimensions.

### Full vs incremental loads

| Load type | What it does | When to use it |
|---|---|---|
| **Full load** | Truncates and reloads all tables; old data replaced entirely | New warehouse setup, or a complete refresh — simpler since you don't track history |
| **Incremental load** | Updates only what changed since the last load; history preserved | Regular updates (daily/hourly) — faster, but needs change-tracking in the source |

Most real warehouses use both: a full load during initial migration, then incremental loads for ongoing updates. Incremental loads are tracked via a **watermark** (a high-water-mark column like `ModifiedDate`) or, in a Lakehouse-to-Lakehouse scenario, **Delta Lake's change data feed (CDF)**. Pipeline pattern: store the last-loaded watermark in a control table, parameterize the source query with it (`WHERE ModifiedDate > @lastWatermark`), update the control table after a successful run.

### Staging, business keys, and surrogate keys

- **Staging** is a workspace to prepare data before it reaches final tables — staging tables/procedures/functions clean, transform, and validate incoming data without touching production tables, and buffer large loads so the warehouse stays responsive.
- A **business key** (natural key) comes from the source system and carries meaning (a SKU, a customer ID) — it lets you trace warehouse records back to source.
- A **surrogate key** is a system-generated integer with no business meaning, assigned by the warehouse. It protects the warehouse from source-system key changes/reuse — if a source system recycles a product code, the surrogate key keeps warehouse relationships intact.

### Preparing data for a dimensional model — slowly changing dimensions

Target the classic **star schema**: fact tables (measures + FKs to dimensions, clear grain) plus dimension tables (descriptive attributes). Dimension attributes change over time (a customer moves, a product gets reclassified) — **slowly changing dimension (SCD)** types define how you handle that:

| Type | Behavior | Use when |
|---|---|---|
| **0** | Attribute never changes | Immutable facts (e.g. date of birth) |
| **1** | Overwrite the existing value, no history | Fixing a data-entry error |
| **2** | Insert a new row per change, mark the old row expired | Need full history (e.g. customer address at time of sale) — **the one you'll see most on the exam alongside Type 1** |
| **3** | Store the previous value in a separate column | Limited history (just "previous value") |
| **4** | Move changing attributes to a separate dimension table | — |
| **5** | Type 4 + Type 1 combined | Large dimensions where Type 2 isn't practical |
| **6** | Type 2 + Type 3 combined | Need both current and historical tracking |

**Type 2 SCD in T-SQL** — expire the current version, insert a new one:

```sql
IF EXISTS (SELECT 1 FROM Dim_Products WHERE SourceKey = @ProductID AND IsActive = 'True')
BEGIN
    UPDATE Dim_Products
    SET ValidTo = GETDATE(), IsActive = 'False'
    WHERE SourceKey = @ProductID AND IsActive = 'True';
END
ELSE
BEGIN
    INSERT INTO Dim_Products (SourceKey, ProductName, StartDate, EndDate, IsActive)
    VALUES (@ProductID, @ProductName, GETDATE(), '9999-12-31', 'True');
END
```

**Loading fact tables**: load after dimensions, since each fact row looks up dimension surrogate keys via business keys. When a dimension uses Type 2 SCD, match the *correct version* of the dimension record (usually the most recent, via `MAX()` on an incrementing surrogate key) — this ensures a sale links to the customer's address *at the time of the transaction*, not their current address.

```sql
INSERT INTO dbo.FactSales
SELECT  (SELECT MAX(DateKey) FROM dbo.DimDate WHERE FullDateAlternateKey = stg.OrderDate) AS OrderDateKey,
        (SELECT MAX(CustomerKey) FROM dbo.DimCustomer WHERE CustomerAlternateKey = stg.CustNo) AS CustomerKey,
        (SELECT MAX(ProductKey) FROM dbo.DimProduct WHERE ProductAlternateKey = stg.ProductID) AS ProductKey,
        OrderNumber, OrderLineItem, OrderQuantity, UnitPrice, Discount, Tax, SalesAmount
FROM dbo.StageSales AS stg
```

Conform dimensions shared across multiple fact tables (one `DimDate` used by both Sales and Inventory facts) so cross-subject-area analysis works.

### Loading pattern for streaming data

- Streaming loads land data continuously rather than in batch windows; typical Fabric pattern is **Eventstream → Lakehouse table (or Eventhouse/KQL DB)** with a defined **trigger/window** for how often micro-batches materialize.
- Choose an output **destination** based on query pattern: land in a Lakehouse Delta table for batch-style BI/Spark queries; land in an Eventhouse (KQL DB) for high-velocity, ad-hoc time-series/log-style queries.

## Ingest and Transform Batch Data

### Choosing an appropriate data store

| Store | Query engine | Best for |
|---|---|---|
| **Lakehouse** | Spark + SQL analytics endpoint (read-only T-SQL over Delta) | Open Delta Parquet files, data science/Spark-heavy workloads, schema-on-read flexibility |
| **Warehouse** | Full read/write T-SQL, transactional | SQL-first teams, stored procedures, multi-table transactions, familiar Synapse/SQL Server semantics |
| **Eventhouse / KQL Database** | KQL (+ T-SQL subset) | High-volume streaming/time-series/log data, near-real-time analytics |
| **SQL analytics endpoint** (auto-generated on a Lakehouse) | T-SQL, read-only | Let SQL-first consumers query lakehouse Delta tables without touching Spark |

Rule of thumb: if you need Spark or ML → Lakehouse; if you need transactional multi-table T-SQL writes → Warehouse; if it's streaming/telemetry-shaped → Eventhouse.

### Loading a warehouse: the three tool options

*Source: [Load data into a Microsoft Fabric data warehouse](https://learn.microsoft.com/en-us/training/modules/load-data-into-microsoft-fabric-data-warehouse/).*

- **Data pipelines** (Copy jobs): connector-driven ingestion into a warehouse table, same Copy Data activity mechanics as elsewhere in Fabric.
- **T-SQL**: the `COPY` statement (bulk-loads from external storage into a table), `CTAS` (`CREATE TABLE ... AS SELECT`, creates and populates a new table in one statement), and `INSERT ... SELECT`.
- **Dataflow Gen2**: imports and transforms data via Power Query before loading — best when transformation logic is easier to express visually than in SQL.

### What is Delta Lake? (foundational for the Lakehouse)

*Source: [Work with Delta Lake tables in Microsoft Fabric](https://learn.microsoft.com/en-us/training/modules/work-delta-lake-tables-fabric/) — Understand Delta Lake.*

Every Lakehouse table in Fabric is a **Delta table** — Delta Lake is an open-source storage layer adding relational-database semantics on top of Spark-based data lake files. Structurally, a Delta table is a folder of **Parquet** data files plus a `_delta_log` folder logging every transaction in JSON.

Delta Lake's benefits, worth knowing precisely for the exam:
- **CRUD + relational semantics**: `SELECT`/`INSERT`/`UPDATE`/`DELETE` like a relational table, on top of Spark.
- **ACID transactions**: atomicity, consistency, isolation, durability — enforced via the transaction log and serializable isolation for concurrent operations.
- **Versioning and time travel**: every transaction is logged, so you can query a previous version of a table.
- **Batch + streaming**: Delta tables work as both a **sink** and a **source** for Spark Structured Streaming, unlike most relational databases which are batch/static-only.
- **Interoperability**: underlying Parquet format, queryable via the SQL analytics endpoint in plain T-SQL.

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

- **PySpark**: DataFrame API (`select`, `withColumn`, `join`, `groupBy`) or Spark SQL (`spark.sql("...")`) inside a notebook; write results with `df.write.format("delta").mode(...).save(...)` or `saveAsTable`.
- **T-SQL**: standard `SELECT`/`INSERT`/`MERGE`/views/stored procedures against a Warehouse or a Lakehouse's SQL analytics endpoint (read-only there).
- **KQL**: `.set-or-append`, update policies, and query-time transforms (`extend`, `summarize`, `mv-expand`) inside an Eventhouse.

### Organizing transformation with medallion architecture

*Source: [Organize a Fabric lakehouse using medallion architecture design](https://learn.microsoft.com/en-us/training/modules/describe-medallion-architecture/) — Describe medallion architecture.*

Source systems organize data for operational efficiency, not analytics. The **medallion architecture** reshapes that data progressively across three layers, each serving a different audience — preserving the original as a source of truth while preparing data layer by layer for analysis:

- **Bronze**: the landing zone for all data (structured, semi-structured, unstructured), stored in its original format, unmodified. Intentionally raw — if a downstream transformation breaks, reprocess from bronze rather than going back to the source system.
- **Silver**: validated and refined data — combining/merging sources, enforcing data quality rules (removing nulls, deduplicating). Produces a consistent, integrated dataset analysts and data scientists can use directly.
- **Gold**: business-ready, typically a **star schema** with facts and dimensions, ready for reports, models, and dashboards.

The three-layer pattern is a starting point, not a rule — add a pre-bronze landing zone or a domain-specific layer beyond gold if your use case needs it. What matters is that each layer has a clear purpose and audience.

### Denormalize, group, aggregate

- **Denormalize**: joins across normalized source tables to produce wide, query-friendly tables (the silver → gold step above) — trades storage/duplication for simpler, faster downstream queries.
- **Group and aggregate**: `groupBy().agg()` in PySpark or `GROUP BY` in T-SQL/KQL (`summarize` in KQL) to produce rollups feeding fact tables or reporting layers.

### Handling duplicate, missing, and late-arriving data

- **Duplicates**: `dropDuplicates()` in PySpark, or `MERGE`/`ROW_NUMBER() OVER (PARTITION BY ...)` deduplication in T-SQL, keyed on a natural or business key plus a "latest wins" ordering column.
- **Missing data**: decide per-column whether to drop rows (`na.drop()`), impute a default/derived value (`na.fill()`), or flag/quarantine the row for review rather than silently dropping it.
- **Late-arriving data**: for streaming, use a **watermark** (see Streaming section) to bound how long the engine waits before finalizing a window; for batch/dimensional loads, a **late-arriving dimension** pattern inserts a placeholder/inferred dimension row so the fact table can still load, then backfills attributes when the real dimension record arrives.
- `MERGE INTO` (Delta) is the standard upsert primitive tying several of these together: match on business key, `UPDATE SET` on match (handles late-arriving updates), `INSERT` when not matched.

## Ingest and Transform Streaming Data

### Choosing an appropriate streaming engine

- **Eventstream**: no-code/low-code stream ingestion and light in-flight transformation with a visual designer; routes to a Lakehouse, Eventhouse, or another downstream item. Best default choice for connector-driven ingestion with simple transforms.
- **Spark Structured Streaming** (in a notebook): code-first, use when transformation logic is complex, needs custom code/ML, or must integrate tightly with existing PySpark batch logic (unify batch+stream code paths).
- **KQL** (in an Eventhouse, via update policies on streaming ingestion): best when the destination is already an Eventhouse and you want KQL-native, low-latency transform-on-ingest.

### Native tables vs OneLake shortcuts in Real-Time Intelligence

- **Native tables** in an Eventhouse: data is ingested and stored directly in KQL DB's own optimized storage format — best performance, full KQL feature support.
- **OneLake shortcuts** into an Eventhouse: reference Delta tables living elsewhere (e.g., a Lakehouse) without copying — trades some query performance/latency for avoiding duplication, useful when the data already lives in OneLake Delta format and you don't want a second copy.

### Query acceleration for OneLake shortcuts vs standard OneLake shortcuts (Real-Time Intelligence)

- **Standard shortcut**: queries read the Delta/Parquet files directly at query time — simplest, no extra storage, but slower for KQL-style high-performance analytics since Parquet isn't optimized for KQL's engine.
- **Query acceleration**: Fabric transparently builds and maintains an accelerated (KQL-native) cache/index over the shortcut's data, giving near-native-table query performance while the source of truth stays in OneLake Delta — choose this when shortcut query latency on Parquet is a bottleneck but you still don't want to duplicate/own the data as a native table.

### Processing with Eventstreams

*Source: [Use Eventstream in Microsoft Fabric](https://learn.microsoft.com/en-us/training/modules/explore-event-streams-microsoft-fabric/) — Eventstream sources and destinations; Eventstream transformations.*

**Sources**: Microsoft sources (Azure Event Hubs, IoT Hub, Service Bus, database CDC feeds), Azure events (Blob Storage events), Fabric events (workspace item changes, OneLake data changes, Fabric job events), and external sources (Apache Kafka, Google Cloud Pub/Sub, MQTT).

**In-stream transformations** (no-code, dragged onto the Eventstream canvas):

| Transform | What it does |
|---|---|
| **Filter** | Keep only events matching a condition (`temperature > 80°`, `status = "error"`) |
| **Manage fields** | Add/remove/rename fields, change data types |
| **Aggregate** | Sum/Min/Max/Average recalculated on each new event over a period; supports multiple aggregations and filtering by other dimensions |
| **Group by** | Aggregations across time windows (tumbling or sliding) — hourly totals, daily averages |
| **Union** | Combine two+ streams with shared fields into one; non-matching fields are dropped |
| **Join** | Combine two streams on a matching condition |
| **Expand** | Create a new row per value in an array field |

**Destinations**:
- **Eventhouse**: KQL-queryable real-time storage.
- **Lakehouse**: converts events to Delta format for lakehouse tables.
- **Derived stream**: a transformed copy of the original stream enabling **content-based routing** — e.g. route high-temperature alerts to Activator while hourly averages go to a KQL database, from the same source stream.
- **Fabric Activator**: direct connection to the event-detection/alerting engine.
- **Custom endpoint**: route to an external system.

An Eventstream can fan out to **multiple destinations simultaneously** without them interfering with each other — e.g. a `GroupByStreet` transformation output routed to both a derived stream (further routed to Activator and an Eventhouse) at once.

### Processing with Spark structured streaming

- Read a stream: `spark.readStream.format("...").load(...)`; each micro-batch is processed like a bounded DataFrame.
- **`withWatermark(eventTimeCol, delayThreshold)`** marks how late data is allowed to arrive relative to the max event time seen, so the engine knows when a windowed aggregation can be finalized and old state can be dropped — e.g. `df.withWatermark("timestamp", "10 minutes")`.
- Write a stream: `df.writeStream.outputMode(...).format("delta").option("checkpointLocation", ...).start()`; the **checkpoint location** is what makes the stream fault-tolerant/exactly-once on restart.
- Output modes: `append` (only new rows, most common for unbounded sinks), `complete` (whole result table every trigger, needed for some aggregations), `update` (only changed rows).

### Processing with KQL

*Source: [Work with real-time data in an Eventhouse in Microsoft Fabric](https://learn.microsoft.com/en-us/training/modules/query-data-kql-database-microsoft-fabric/) — Use KQL effectively; Materialized views and stored functions.*

- Streaming ingestion into an Eventhouse table, optionally transformed on the way in via an **update policy** (a KQL function that runs automatically on newly ingested data and writes derived rows to a target table).
- **Query optimization principle**: the less data a query processes, the faster it runs. Filter early (especially on time, since Eventhouse data is typically time-series and time-indexed), order filters by how much data they eliminate (biggest filter first), and `project` (select) only needed columns before further operators.
  ```kql
  TaxiTrips
  | where pickup_datetime > ago(1d)    // time filter first - eliminates most data
  | where vendor_id == "VTS"           // more specific filter next
  | project trip_id, vendor_id, fare_amount
  | summarize avg_fare = avg(fare_amount) by vendor_id
  ```
- **Joins**: put the smaller table first — KQL processes the first table to match against the second, so starting small reduces rows processed.
  ```kql
  VendorInfo
  | join kind=inner TaxiTrips on vendor_id
  ```
- **Materialized views**: precompute and auto-maintain a `summarize` aggregation instead of recomputing it from raw data every query — essential once an Eventhouse table has millions/billions of rows. A materialized view has two parts combined transparently at query time: the **materialized part** (already-processed results) and the **delta** (new data since the last background update) — so it's always both fast *and* current.
  ```kql
  .create materialized-view TripsByVendor on table TaxiTrips
  {
      TaxiTrips
      | summarize trips = count(), avg_fare = avg(fare_amount), total_revenue = sum(fare_amount)
      by vendor_id, pickup_date = format_datetime(pickup_datetime, "yyyy-MM-dd")
  }
  ```
- **Stored functions**: encapsulate a reusable, optionally parameterized query, ensuring teams apply the same logic consistently instead of copy-pasting KQL.
  ```kql
  .create-or-alter function trips_by_min_passenger_count(num_passengers:long)
  {
      TaxiTrips
      | where passenger_count >= num_passengers
      | project trip_id, pickup_datetime
  }

  trips_by_min_passenger_count(3)
  | take 10
  ```

### Creating windowing functions

- **PySpark**: `from pyspark.sql.functions import window` then `groupBy(window(col("timestamp"), "10 minutes", "5 minutes"), ...)` — first duration is window length, second (optional) is the slide interval; omit the second argument for non-overlapping (tumbling) windows, include it for overlapping (sliding) windows.
  ```python
  from pyspark.sql.functions import window
  (df.withWatermark("timestamp", "10 minutes")
     .groupBy(window(df.timestamp, "10 minutes", "5 minutes"), df.value)
     .count())
  ```
- **KQL**: `summarize count() by bin(Timestamp, 5m)` for tumbling windows; `make-series` with a step size for regularly-spaced windows across a range.
- **Eventstream** designer: the "Aggregate" and "Group by" nodes support tumbling (fixed intervals) and sliding (overlapping intervals) windows visually without writing code.
- Know the three window types conceptually: **tumbling** (fixed, non-overlapping), **hopping/sliding** (fixed size, overlapping, advances by a hop size smaller than the window), **session** (dynamic length, closes after a gap of inactivity).
