# DP-700 Fabric Data Engineer — Exam Cheat Sheet

Skills measured as of **July 21, 2026**.

---

## Exam Weights

| Section | % | Focus |
|---|---|---|
| Implement and manage an analytics solution | **30–35%** | Workspace config, Git/deployment pipelines, security, orchestration |
| Ingest and transform data | **30–35%** | Loading patterns, batch + streaming ingestion/transformation |
| Monitor and optimize an analytics solution | **30–35%** | Monitoring hub, error triage per item type, performance tuning |

All three are roughly equal weight — there's no section you can skimp on.

---

## Decision Trees

### Dataflow Gen2 vs Pipeline vs Notebook

```
Need to sequence/orchestrate multiple steps, on a schedule/trigger?  →  Pipeline
Low-code, business-user-friendly, connector-rich, small/medium data? →  Dataflow Gen2
Code-first, complex logic, large scale, reusable functions?          →  Notebook (PySpark)
Typical real pattern: Pipeline orchestrates → calls Notebook and/or Dataflow Gen2 activities
```

### Choosing a data store

```
Need Spark / ML / schema-on-read flexibility?        →  Lakehouse
Need full read/write T-SQL, transactions, procs?      →  Warehouse
High-volume streaming / time-series / log data?       →  Eventhouse (KQL DB)
SQL-first read access to a Lakehouse's Delta tables?  →  SQL analytics endpoint (read-only, auto-generated)
```

### Shortcut vs Mirroring

```
Want to reference data in place, zero-copy, engine-agnostic?         →  Shortcut
Want continuous, low-latency, managed replication of an external
operational database (Azure SQL, Cosmos DB, Snowflake, on-prem)?     →  Mirroring
```

### Streaming engine choice

```
No-code/low-code ingestion with light in-flight transforms?  →  Eventstream
Complex custom logic, unify with existing batch PySpark code? →  Spark Structured Streaming
Destination is already an Eventhouse, want KQL-native transform-on-ingest? →  KQL (update policy)
```

### Native table vs shortcut vs query-acceleration in Real-Time Intelligence

```
Best possible KQL performance, OK to duplicate data?                     →  Native table
Data already lives in OneLake Delta, avoid duplication, latency is fine? →  Standard shortcut
Data already in OneLake Delta, avoid duplication, need near-native speed? →  Query-accelerated shortcut
```

### Full vs incremental load

```
Small source, simplicity matters more than cost/time?  →  Full load (truncate + overwrite)
Large or frequently-changing source?                    →  Incremental (watermark or Delta CDF)
```

---

## Security Layers (know which one solves which problem)

| Layer | Scope | Configured via |
|---|---|---|
| Workspace-level | Entire workspace | Roles: Admin > Member > Contributor > Viewer |
| Item-level | One item | Item sharing/permissions |
| Row-level security (RLS) | Rows | T-SQL `CREATE SECURITY POLICY` |
| Column-level security (CLS) | Columns | T-SQL `GRANT`/`DENY` on columns/views |
| Object-level security (OLS) | Whole tables/objects | Deny access, hides from schema browsing |
| OneLake data access roles | Folders/files | Engine-agnostic — enforced for Spark, SQL endpoint, Power BI, external readers alike |
| Dynamic data masking | Column values at query time | `default()`, `email()`, `partial(prefix,padding,suffix)`, `random(low,high)` |
| Sensitivity labels | Classification + optional protection | Microsoft Purview Information Protection taxonomy |

**Git integration** = version control within a workspace/branch.
**Deployment pipelines** = promotion across Dev → Test → Prod stages, with deployment rules to swap environment-specific values.

**Fabric security model — 3-level access evaluation**: Microsoft Entra ID authentication → Fabric access → Data security (workspace roles → item permissions → compute/granular permissions → OneLake security). Granting access at any of the first three layers gives *all* data in the item; OneLake security is the only layer that restricts *within* an item.

**Admin hierarchy (5 levels)**: Tenant → Capacity → Domain → Workspace → Item.
**Admin roles (4 tiers)**: Fabric admin (tenant-wide, = "Power BI Administrator" in Entra ID) → Capacity admin → Domain admin → Workspace admin.

---

## Slowly Changing Dimensions (SCD)

| Type | Behavior |
|---|---|
| 0 | Never changes |
| **1** | Overwrite, no history |
| **2** | New row per change, old row marked expired — the one tested most alongside Type 1 |
| 3 | Previous value in a separate column (limited history) |
| 4 | Changing attributes moved to a separate dimension table |
| 5 | Type 4 + Type 1 |
| 6 | Type 2 + Type 3 |

Fact table loads happen *after* dimension loads (facts reference dimension surrogate keys); for a Type 2 dimension, match the version valid *at the time of the fact event*, not the current version.

---

## Delta Table Maintenance (Optimize a Lakehouse table)

```sql
OPTIMIZE events;                          -- compact small files
OPTIMIZE events ZORDER BY (eventType);    -- compact + co-locate by column for data skipping
VACUUM events RETAIN 168 HOURS;           -- remove stale files (default retention: 7 days)
```

- **OptimizeWrite** (default on): writes fewer, larger files at write time — prevents the small-file problem before it starts. Toggle: `spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", True/False)`.
- **V-Order**: Parquet write-time optimization — **disabled by default in new workspaces** (favors write-heavy engineering workloads); enable deliberately for read-heavy/Direct Lake scenarios. Control via `spark.sql.parquet.vorder.default` (session), table property, or per-write option; in a Warehouse it's whole-database (`ALTER DATABASE CURRENT SET VORDER = OFF`) and **irreversible once turned off**. Real-world tradeoff is workload-dependent (one test: ~50% slower writes, ~2x faster reads when on) — don't anchor on a fixed percentage.
- Don't shorten `VACUUM` retention below what a concurrent reader or time-travel query might still need — 168 hours (7 days) is both the default *and* the minimum Fabric allows.

---

## Structured Streaming Windowing (PySpark)

```python
from pyspark.sql.functions import window

(df.withWatermark("timestamp", "10 minutes")          # how late data may arrive
   .groupBy(window(df.timestamp, "10 minutes", "5 minutes"), df.value)  # size, slide
   .count())
```

- Omit the slide interval → **tumbling** window (fixed, non-overlapping).
- Include a slide smaller than the window → **hopping/sliding** window (overlapping).
- **Session** windows: dynamic length, closes after a gap of inactivity (no fixed size).
- `withWatermark` is required before a windowed aggregation to bound state growth and know when a window can be finalized.

---

## Monitoring & Troubleshooting Quick Map

| Item | First place to look when it fails |
|---|---|
| Pipeline | Run history → activity-level error + input/output JSON |
| Dataflow Gen2 | Refresh history → per-step Power Query error |
| Notebook | Spark UI (stages/tasks) + cell stack trace |
| Eventhouse | `.show ingestion failures`, KQL diagnostics |
| Eventstream | Monitoring view, per-node data preview |
| T-SQL | Query error message, Warehouse query insights |
| OneLake shortcut | Verify source connection/credentials and permissions |
| Capacity-wide slowness | Fabric Capacity Metrics app — check CU smoothing/throttling |

Spark **OOM / executor lost** → check Spark UI stage summary for **data skew** before tuning memory blindly.

Direct Lake semantic model going slow with no visible failure → check for **silent fallback to DirectQuery** (framing limits exceeded, unsupported types).

---

## Handling Data Quality Issues

| Issue | Batch fix | Streaming fix |
|---|---|---|
| Duplicates | `dropDuplicates()`, or `ROW_NUMBER() OVER (PARTITION BY key)` dedupe | Same, applied per micro-batch, or `MERGE` upsert on key |
| Missing data | `na.drop()` / `na.fill()`, or quarantine row | Same, decided per-column |
| Late-arriving facts | Late-arriving dimension pattern (placeholder row, backfill later) | `withWatermark` bounds how long the engine waits |

`MERGE INTO` (Delta) is the general-purpose upsert primitive: `UPDATE SET` on match handles late-arriving updates, `INSERT` when not matched handles new rows.
