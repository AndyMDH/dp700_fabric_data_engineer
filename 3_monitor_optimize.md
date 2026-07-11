# Monitor and Optimize an Analytics Solution

<https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700>

## Monitor Fabric Items

### Central monitoring surfaces

- **Monitoring Hub**: tenant/workspace-scoped list of all item runs (pipelines, notebooks, Spark jobs, dataflows, semantic model refreshes) — the first place to check "did it run, when, how long, did it fail."
- **Fabric Capacity Metrics app**: capacity-level view of **CU (capacity unit) consumption** across all workloads, including *smoothing* (Fabric spreads bursty usage over a rolling window) and *throttling* (what happens when sustained usage exceeds the capacity's CU budget — interactive requests get delayed/rejected before background jobs do).
- Item-specific run history: each pipeline/notebook/dataflow has its own run history pane with per-activity/per-cell status and duration.

### Monitor data ingestion

- Pipeline **Copy Data activity** run details show rows read/written, throughput, and duration per run — compare against historical runs to catch silent slowdowns.
- Eventstream has a live data preview and per-node monitoring to confirm events are flowing and being transformed as expected.
- Mirroring has its own replication status view showing initial-snapshot vs continuous-replication state and any replication lag.

### Monitor data transformation

- Notebook runs show per-cell execution time and Spark UI links (stages, tasks, shuffle read/write) for drilling into a slow transformation.
- Dataflow Gen2 refresh history shows step-level duration inside the Power Query mashup.
- Warehouse/T-SQL: query insights / `sys.dm_exec_requests`-style DMVs for currently running and historical query performance.

### Monitor semantic model refresh

- Refresh history on the semantic model shows duration, rows processed, and failure details per table/partition.
- **Direct Lake** mode (semantic model reads Delta tables directly, no import refresh) shifts monitoring focus from "refresh duration" to **fallback-to-DirectQuery** events — watch for when Direct Lake silently falls back to slower DirectQuery (e.g., framing limits exceeded, unsupported data types) since that's a performance regression without an explicit failure.

### Configure alerts

- **Data Activator (Reflex)**: no-code alerting on conditions/events (e.g., a metric crosses a threshold, an Eventstream value meets a condition) — can trigger email/Teams notification or kick off a Power Automate flow.
- Capacity Metrics app supports alerting on CU utilization approaching throttling thresholds so you can act before users are impacted.
- Pipelines support built-in failure notifications (email on failure) per pipeline/activity.

## Identify and Resolve Errors

| Item | Common error sources | Where to look |
|---|---|---|
| **Pipeline** | Activity timeout, connector auth failure, schema drift at sink, upstream dependency not ready | Run history → activity-level error message and input/output JSON |
| **Dataflow Gen2** | Power Query step failure (type mismatch, source schema change), refresh timeout on large data | Refresh history, per-step error in the Power Query editor |
| **Notebook** | Spark job failure (OOM, executor lost, skew), library/version conflicts, bad input data | Spark application UI (stages/tasks), cell error output/stack trace |
| **Eventhouse** | Ingestion mapping mismatch, update policy failure, capacity/throttling | `.show ingestion failures` command, KQL diagnostic queries |
| **Eventstream** | Source connection drop, malformed/unexpected event schema, transform node misconfiguration | Eventstream monitoring view, per-node data preview |
| **T-SQL** | Constraint violation, permission denied, query timeout, unsupported syntax on SQL analytics endpoint (read-only) | Query error message; Warehouse query insights |
| **OneLake shortcut** | Broken/renamed source path, expired credentials on external source (S3/ADLS), permission denied on target | Shortcut fails to resolve/list; verify source connection and permissions |

- General debugging discipline: reproduce at the smallest scope (one activity, one cell, one partition) before assuming a systemic issue; check whether an error is data-dependent (bad row) vs environment-dependent (auth, capacity, config).
- Spark **OOM/executor-lost** errors usually trace back to **data skew** (one partition much larger than others) or too-small executor memory for the operation (large shuffle, broadcast join gone wrong) — check the Spark UI's stage summary for skewed task durations before tuning memory blindly.

## Optimize Performance

### Optimize a Lakehouse table

Delta table maintenance commands, run from a notebook (SQL or PySpark) or automatically via **table maintenance** settings on the Lakehouse item:

```sql
-- Compact small files into larger ones
OPTIMIZE events;

-- Compact + cluster data by column for faster filtering on that column
OPTIMIZE events ZORDER BY (eventType);

-- Remove files no longer referenced by the Delta log, past the retention window
VACUUM events RETAIN 168 HOURS;   -- default retention is 7 days (168h)
```

```python
from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, "events")
dt.optimize().executeCompaction()
dt.optimize().executeZOrderBy("eventType")
dt.vacuum(168)
```

- **OPTIMIZE**: solves the "many small files" problem from frequent small writes/streaming — compacts them into fewer, larger files, which speeds up reads (fewer file-open operations).
- **ZORDER BY**: co-locates related data within compacted files by the given column(s), so predicates that filter on that column can skip more files (data skipping) — pick high-cardinality columns commonly used in `WHERE` filters.
- **V-Order**: a Fabric-specific write-time optimization (special Parquet sorting/encoding) applied by default on Lakehouse writes to accelerate reads across Spark, SQL endpoint, and Power BI/Direct Lake — usually left on; can be disabled per-write if write throughput matters more than downstream read speed.
- **VACUUM**: deletes stale data files past the retention threshold; don't shorten retention below the point where a concurrent long-running reader or time-travel query might still need those files.
- Table maintenance can be scheduled (via a pipeline notebook activity or the Lakehouse's built-in scheduled maintenance) rather than run ad hoc.

### Optimize a pipeline

- Prefer parallel/concurrent activity branches over unnecessary sequential chaining when activities don't depend on each other.
- Use staged copy and partitioned/parallel copy settings on Copy Data activities for large sources to raise throughput.
- Avoid excessive `ForEach` iteration counts calling expensive child activities serially — batch where possible, or set `ForEach` to run in parallel (respecting sink concurrency limits).
- Reduce activity count/complexity where a single well-parameterized activity can replace many near-duplicate ones (easier to optimize and monitor).

### Optimize a data warehouse

- Keep statistics up to date (auto-created/updated by default, but verify) so the query optimizer picks good plans.
- Design tables with appropriate distribution/partitioning for large fact tables to avoid data movement on joins.
- Use `MERGE` instead of separate `DELETE`+`INSERT` for upserts to reduce transaction log overhead and improve plan efficiency.
- Review query insights for expensive/long-running queries and check for missing filters, unnecessary `SELECT *`, or joins on non-indexed/high-cardinality unfiltered columns.

### Optimize Eventstreams and Eventhouses

- Right-size Eventstream partitioning/throughput settings to match source event volume; under-provisioned throughput causes backpressure/lag.
- In Eventhouse, apply **update policies** to pre-aggregate/transform at ingest time rather than repeating expensive transforms on every query.
- Use **materialized views** for frequently-run aggregate queries so results are incrementally maintained instead of recomputed each time.
- Set appropriate **retention policy** and **caching policy** per table: caching policy controls how much recent data stays in fast (hot) cache vs cold storage — align it with actual query recency patterns instead of leaving defaults for very large tables.

### Optimize Spark performance

- **Autoscale** and **dynamic executor allocation**: let the pool/session scale to the workload instead of fixing node/executor counts too small (slow) or too large (wasteful).
- **High concurrency mode**: share a Spark session across notebooks to cut cold-start (session spin-up) overhead for short, frequent jobs.
- **Partitioning**: ensure DataFrame partition count matches cluster parallelism (`repartition`/`coalesce`); too few partitions underutilizes executors, too many adds scheduling overhead.
- **Broadcast joins**: broadcast the smaller side of a join (`broadcast(df)`) when one side fits in executor memory, avoiding an expensive shuffle join.
- **Native Execution Engine**: a Fabric Spark runtime feature that runs eligible query stages using a vectorized native (non-JVM) engine for significant speedups on supported operations — enabled at the environment/pool level, transparent to notebook code.
- **Cache** intermediate DataFrames (`.cache()`/`.persist()`) only when reused multiple times in the same job; unnecessary caching wastes executor memory.
- Watch for **data skew**: salting keys or using adaptive query execution (AQE, on by default in modern Spark) to handle skewed joins/aggregations automatically.

### Optimize query performance

- **Direct Lake** semantic models: keep the model within Direct Lake's supported limits (row/column count, data types) to avoid silent fallback to DirectQuery, which is slower.
- Push filtering/aggregation as early as possible (predicate/projection pushdown) — filter before joining, select only needed columns, rather than transforming the full table then filtering.
- For the SQL analytics endpoint (read-only T-SQL over a Lakehouse), remember it's a query surface over Delta/Parquet, not a fully independent tunable engine — the real levers are the underlying Delta table's file layout (OPTIMIZE/ZORDER/V-Order above), not SQL-side indexing.
- For KQL, prefer `summarize`/`bin()` time-bucketing and materialized views over scanning full raw tables on every query.
