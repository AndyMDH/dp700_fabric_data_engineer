# Monitor and Optimize an Analytics Solution

<https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700>

Sourced from the official DP-700 skills outline plus the actual Microsoft Learn training modules behind it: [Monitor activities in Microsoft Fabric](https://learn.microsoft.com/en-us/training/modules/monitor-fabric-items/), [Use Activator in Microsoft Fabric](https://learn.microsoft.com/en-us/training/modules/use-fabric-activator/), [Work with Delta Lake tables in Microsoft Fabric](https://learn.microsoft.com/en-us/training/modules/work-delta-lake-tables-fabric/), and [Monitor a Microsoft Fabric data warehouse](https://learn.microsoft.com/en-us/training/modules/monitor-fabric-data-warehouse/).

## Monitor Fabric Items

### The monitoring hub

*Source: [Monitor activities in Microsoft Fabric](https://learn.microsoft.com/en-us/training/modules/monitor-fabric-items/) — Use the Microsoft Fabric monitoring hub.*

The monitoring hub (**Monitor** in the left nav) is a centralized table of recent activity across your workspace — instead of opening each pipeline/dataflow/notebook individually, you see everything in one place. It's the first stop for "did last night's load succeed?" and "how long did the notebook take?"

Each row is one activity execution. **Status** values:

- **Succeeded** — completed without errors.
- **Failed** — select the row for error details/root cause.
- **In progress** — still running; if it stays here far longer than usual, it may be stuck or processing an unusually large dataset.
- **Cancelled** — check whether it was intentional or caused by a timeout/capacity limit.

Selecting an activity's **View details** (the "i" icon) shows start/end time, duration (a sudden increase even on a *succeeded* run can signal a data volume spike or resource contention), status details (error messages/codes on failure), and activity-specific metadata (rows processed, Spark application ID, refresh attempt count).

**Historical runs** (up to 30 days, via "..." → Historical runs) answer questions a single run can't: *when did this start failing* (a fresh failure vs. a days-old pattern points at a specific change), *is it getting slower* (compare durations across runs), and *are there retry patterns* (a semantic model refresh that regularly retries before succeeding suggests a flaky source connection).

**Filters and columns**: filter by Status (isolate failures) or Item type (isolate one part of the pipeline). Useful optional columns: Submitted by, Location, Duration, End time, Refresh type.

**Failure notifications**: the hub's **Schedule failures** page configures email alerts for scheduled item failures tenant/workspace-wide from one place, rather than per-item — requires at least Contributor role to configure.

**Workspace monitoring** (deeper than the hub): enabling it (Workspace settings → Monitoring) spins up an Eventhouse in the workspace that continuously collects diagnostic logs/metrics from Fabric items, retained 30 days, queryable via KQL or SQL — unlike the hub's dashboard view, this is raw log data for long-term trend analysis, cross-item failure correlation, or custom Real-Time dashboards.

### Fabric Capacity Metrics app

- Capacity-level view of **CU (capacity unit) consumption** across all workloads, including *smoothing* (Fabric spreads bursty usage over a rolling window) and *throttling* (what happens when sustained usage exceeds the capacity's CU budget — interactive requests get delayed/rejected before background jobs do).

### Monitor data ingestion

- Pipeline **Copy Data activity** run details show rows read/written, throughput, and duration per run — compare against historical runs to catch silent slowdowns.
- Eventstream has a live data preview and per-node monitoring to confirm events are flowing and being transformed as expected.
- Mirroring has its own replication status view showing initial-snapshot vs continuous-replication state and any replication lag.

### Monitor data transformation

- Notebook runs show per-cell execution time and Spark UI links (stages, tasks, shuffle read/write) for drilling into a slow transformation.
- Dataflow Gen2 refresh history shows step-level duration inside the Power Query mashup.
- Warehouse/T-SQL: query insights / DMVs for currently running and historical query performance (see "Optimize a data warehouse" below for the specific views).

### Monitor semantic model refresh

- Refresh history on the semantic model shows duration, rows processed, and failure details per table/partition.
- **Direct Lake** mode (semantic model reads Delta tables directly, no import refresh) shifts monitoring focus from "refresh duration" to **fallback-to-DirectQuery** events — watch for when Direct Lake silently falls back to slower DirectQuery (e.g., framing limits exceeded, unsupported data types, or column-level security applied to the table) since that's a performance regression without an explicit failure.

### Configure alerts with Activator

*Source: [Monitor activities in Microsoft Fabric](https://learn.microsoft.com/en-us/training/modules/monitor-fabric-items/) — Respond to Fabric events with Activator; [Use Activator in Microsoft Fabric](https://learn.microsoft.com/en-us/training/modules/use-fabric-activator/) — Create rules in Activator.*

The monitoring hub shows what happened; Schedule failures emails you when it happens; neither *acts* for you. **Activator** closes that gap.

**Job events**: every Fabric job run (pipeline, dataflow, notebook, semantic model refresh) emits events — Job created, Job failed, Job succeeded, Job status changed. Activator connects to these through **Real-Time hub**: Real-Time hub → Fabric events → Job events → Set alert, choose the event type (e.g. `Microsoft.Fabric.JobEvents.ItemJobFailed`), the workspace/item to monitor, a condition (every match, or only when a field meets a value), and an action.

**Actions available**: Email; Teams (often better for on-call teams needing immediate visibility); or **run a Fabric activity** (trigger a pipeline, notebook, dataflow, Spark job, or function) — this last one is what differentiates Activator from a plain failure notification: instead of a human reading an alert and reacting, Activator can automatically run a cleanup notebook after a partial load, or kick off a downstream refresh once an upstream job succeeds. (It's not a universal fix, though — re-running a pipeline that failed on a code error or schema change just fails again; Activator is for automated coordination, not root-cause repair.)

**Activator on data, more generally** (beyond job events): Activator can also monitor any data attribute from a source like Eventstream. Configure what to monitor (an **Attribute**, e.g. Temperature, plus a **Summarization** — Average/Min/Max/Count/Total — over a **window size** and **step size**, to smooth noisy raw readings), a **Condition** (threshold, change detection, range, or missing-data detection, plus an Occurrence rule — every time, or only once true for a sustained period), and an optional **Property filter** (up to three conditions narrowing which events the rule evaluates, e.g. `ColdChainType equals "medicine"`).

| Scenario | Use |
|---|---|
| Email alert on a scheduled run failing | Schedule failures page |
| Teams notification to a channel on job failure | Activator |
| Notify analysts when a semantic model refresh *succeeds* | Activator |
| Trigger a follow-up pipeline when an upstream job completes | Activator |
| Run a cleanup job after a partial load fails | Activator |
| Centrally manage notification recipients across all scheduled items | Schedule failures page |

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

*Source: [Work with Delta Lake tables in Microsoft Fabric](https://learn.microsoft.com/en-us/training/modules/work-delta-lake-tables-fabric/) — Optimize delta tables (verified against `delta-io/delta` docs via Context7).*

Spark is a parallel-processing framework writing immutable Parquet files — every update/delete writes a *new* file, which over time produces many small files (the **small file problem**): slow or even failing queries over large amounts of data.

**OptimizeWrite** reduces this at write time — instead of many small files, it writes fewer, larger ones. **Enabled by default** in Fabric; toggle per Spark session:

```python
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", False)  # disable
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", True)   # enable (default)
```

**OPTIMIZE** is table *maintenance* (run after the fact, e.g. post-load) that consolidates small Parquet files into fewer, larger ones — fewer files, better compression, more efficient distribution across nodes. Run it from **Lakehouse Explorer → "..." on a table → Maintenance → Run OPTIMIZE command**, or from code:

```sql
OPTIMIZE events;                          -- compact small files
OPTIMIZE events ZORDER BY (eventType);    -- compact + cluster by column for data skipping
VACUUM events RETAIN 168 HOURS;           -- remove stale files (default retention: 7 days / 168h, cannot go shorter)
```

```python
from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, "events")
dt.optimize().executeCompaction()
dt.optimize().executeZOrderBy("eventType")
dt.vacuum(168)
```

**ZORDER BY** co-locates related data within compacted files by the given column(s), so filters on that column can skip more files (**data skipping**) — pick high-cardinality columns commonly used in `WHERE` filters.

**V-Order** — a Fabric-specific Parquet write-time optimization (special sorting, row-group distribution, dictionary encoding, and compression) applied optionally alongside OPTIMIZE. Exact numbers worth knowing:
- Enabled **by default** in Fabric.
- Incurs roughly **~15% write overhead**.
- In exchange, Power BI and SQL (via Microsoft's VertiScan technology) get dramatically faster reads; Spark and other engines (no VertiScan) still see **~10%, sometimes up to 50%, faster reads**.
- **100% compliant with open-source Parquet** — any Parquet engine can still read a V-Ordered file, it just doesn't get the speedup.
- Not worth it for write-intensive, read-rarely scenarios (e.g. a staging table read once or twice) — disabling it there can reduce ingestion processing time.

**VACUUM** deletes Parquet files no longer referenced by the transaction log, past the retention window — but never deletes the transaction log itself, so `DESCRIBE HISTORY` still shows past versions even after a vacuum (you just can't time-travel to a vacuumed version anymore). Choose retention based on data-retention requirements, storage cost, change frequency, and regulatory needs; **default and minimum is 168 hours (7 days)** — Fabric won't let you set it shorter.

**Partitioning**: organize a Delta table into subfolders by a column (e.g. `year=2024/`) to enable data skipping — queries filtered to one year can skip every other year's partition entirely.

```python
df.write.format("delta").partitionBy("Category").saveAsTable("partitioned_products", path="abfs_path/partitioned_products")
```
```sql
CREATE TABLE partitioned_products (
    ProductID INTEGER, ProductName STRING, Category STRING, ListPrice DOUBLE
)
PARTITIONED BY (Category);
```

Use partitioning when you have very large data that splits into a few large partitions. **Don't** partition small data, a high-cardinality column (too many tiny partitions — worsens the small-file problem), or a column that would create multiple nested partition levels. Partitions are a fixed layout that doesn't adapt to different query patterns — think about actual query granularity before choosing a partition column.

### Optimize a pipeline

- Prefer parallel/concurrent activity branches over unnecessary sequential chaining when activities don't depend on each other.
- Use staged copy and partitioned/parallel copy settings on Copy Data activities for large sources to raise throughput.
- Avoid excessive `ForEach` iteration counts calling expensive child activities serially — batch where possible, or set `ForEach` to run in parallel (respecting sink concurrency limits).
- Reduce activity count/complexity where a single well-parameterized activity can replace many near-duplicate ones (easier to optimize and monitor).

### Optimize a data warehouse

*Source: [Monitor a Microsoft Fabric data warehouse](https://learn.microsoft.com/en-us/training/modules/monitor-fabric-data-warehouse/).*

- Keep statistics up to date (auto-created/updated by default, but verify) so the query optimizer picks good plans.
- Design tables with appropriate distribution/partitioning for large fact tables to avoid data movement on joins.
- Use `MERGE` instead of separate `DELETE`+`INSERT` for upserts to reduce transaction log overhead and improve plan efficiency.
- **Dynamic management views (DMVs)** monitor currently running activity — connection, session, and request-level views for what's executing right now, useful for spotting a long-running or blocked query in real time.
- **Query insights** views monitor *historical* querying trends — aggregated query text, duration, and frequency over time, for spotting which queries are consistently expensive rather than just what's running this second.
- Review query insights for expensive/long-running queries and check for missing filters, unnecessary `SELECT *`, or joins on non-indexed/high-cardinality unfiltered columns.

### Optimize Eventstreams and Eventhouses

- Right-size Eventstream partitioning/throughput settings to match source event volume; under-provisioned throughput causes backpressure/lag.
- In Eventhouse, apply **update policies** to pre-aggregate/transform at ingest time rather than repeating expensive transforms on every query.
- Use **materialized views** for frequently-run aggregate queries so results are incrementally maintained instead of recomputed each time — see the code example in [2_ingest_transform.md](2_ingest_transform.md).
- Set appropriate **retention policy** and **caching policy** per table: caching policy controls how much recent data stays in fast (hot) cache vs cold storage — align it with actual query recency patterns instead of leaving defaults for very large tables.
- Follow KQL query-optimization discipline: filter early (time-based filters first, since Eventhouse data is time-indexed), project only needed columns before aggregating, and put the smaller table first in a join.

### Optimize Spark performance

- **Autoscale** and **dynamic executor allocation**: let the pool/session scale to the workload instead of fixing node/executor counts too small (slow) or too large (wasteful).
- **High concurrency mode**: share a Spark session across notebooks to cut cold-start (session spin-up) overhead for short, frequent jobs.
- **Partitioning**: ensure DataFrame partition count matches cluster parallelism (`repartition`/`coalesce`); too few partitions underutilizes executors, too many adds scheduling overhead.
- **Broadcast joins**: broadcast the smaller side of a join (`broadcast(df)`) when one side fits in executor memory, avoiding an expensive shuffle join.
- **Native Execution Engine**: a Fabric Spark runtime feature that runs eligible query stages using a vectorized native (non-JVM) engine for significant speedups on supported operations — enabled at the environment/pool level, transparent to notebook code.
- **Cache** intermediate DataFrames (`.cache()`/`.persist()`) only when reused multiple times in the same job; unnecessary caching wastes executor memory.
- Watch for **data skew**: salting keys or using adaptive query execution (AQE, on by default in modern Spark) to handle skewed joins/aggregations automatically.
- **OptimizeWrite** (see Lakehouse table section above) is itself a performance feature — leave it on to avoid the small-file problem from Spark's write pattern in the first place.

### Optimize query performance

- **Direct Lake** semantic models: keep the model within Direct Lake's supported limits (row/column count, data types) to avoid silent fallback to DirectQuery, which is slower. Column-level security on a Direct Lake table also forces DirectQuery fallback — a specific, exam-relevant trigger, not just "too much data."
- Push filtering/aggregation as early as possible (predicate/projection pushdown) — filter before joining, select only needed columns, rather than transforming the full table then filtering.
- For the SQL analytics endpoint (read-only T-SQL over a Lakehouse), remember it's a query surface over Delta/Parquet, not a fully independent tunable engine — the real levers are the underlying Delta table's file layout (OPTIMIZE/ZORDER/V-Order above), not SQL-side indexing.
- For KQL, prefer `summarize`/`bin()` time-bucketing and materialized views over scanning full raw tables on every query; put the smaller table first in a join.
