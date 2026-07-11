# DP-700 Topic Summaries

One paragraph per topic area — use for a fast final review pass.

## Workspace configuration

Fabric workspaces sit on a capacity and hold items. Spark settings (pools, node family, autoscale, runtime version, high concurrency, environments) control notebook/job compute behavior. Domains group workspaces for tenant-level governance and discovery. OneLake workspace settings govern folder/file-level security (data access roles). Apache Airflow Job settings enable running managed Airflow DAGs inside Fabric for teams migrating from or needing native Airflow orchestration.

## Lifecycle management

Git integration provides version control within a workspace (branch, commit, PR, merge), while deployment pipelines promote items across Dev/Test/Prod stage workspaces with optional deployment rules to swap environment-specific values automatically. Database projects bring schema-as-code to Warehouses via source-controlled `.sql` files. These three mechanisms are complementary, not competing — most mature setups use Git for source control and deployment pipelines for promotion.

## Security and governance

Access control is layered: workspace roles (Admin/Member/Contributor/Viewer) gate the whole workspace; item-level sharing gates one item; RLS/CLS/OLS gate rows/columns/whole-objects inside a Warehouse or Lakehouse SQL endpoint; OneLake data access roles are the engine-agnostic folder/file layer enforced regardless of which engine reads the data. Dynamic data masking hides column values at query time without touching storage. Sensitivity labels (from Microsoft Purview) carry classification and can enforce protection, and propagate downstream to dependent items. Endorsement (Promoted vs Certified) signals quality/trustworthiness for discovery. All Fabric activity is captured in the Purview/M365 audit log.

## Orchestration

Choose Dataflow Gen2 for low-code/business-user transformation, notebooks for code-first large-scale transformation, and pipelines to orchestrate both plus copy/control-flow activities — on a schedule or an event-based trigger (e.g., file arrival). Parameters and dynamic expressions make a single pipeline/notebook reusable across many invocations; the ForEach-over-control-table pattern is the standard way to scale one templated pipeline across many source tables.

## Loading patterns

Full loads truncate-and-overwrite; incremental loads track a watermark or use Delta CDF to load only changes — incremental is the default choice once volume/frequency makes full loads too costly. Dimensional modeling targets a star schema with conformed dimensions and SCD Type 1 (overwrite) or Type 2 (historized) handling. Streaming loads typically land via Eventstream into either a Lakehouse (for BI/Spark-style queries) or an Eventhouse (for high-velocity time-series queries).

## Batch ingestion and transformation

Store choice follows workload shape: Lakehouse for Spark/ML, Warehouse for transactional T-SQL, Eventhouse for streaming/time-series. Transformation tool choice follows the same logic as orchestration (Dataflow Gen2 vs notebook vs T-SQL vs KQL). OneLake shortcuts reference data in place (zero-copy); mirroring continuously replicates external operational databases into OneLake Delta tables. Pipelines ingest primarily via the Copy Data activity. Data quality handling — deduplication, missing-data handling, late-arriving data — commonly converges on Delta's `MERGE INTO` as the upsert primitive.

## Streaming ingestion and transformation

Pick Eventstream for no/low-code ingestion with light transforms, Spark Structured Streaming for complex custom logic unified with batch code, and KQL update policies when the destination is already an Eventhouse. In Real-Time Intelligence, native tables give best performance at the cost of duplicating data; shortcuts avoid duplication at some latency cost, with query-accelerated shortcuts closing most of that gap via a maintained cache. `withWatermark` plus `window()` in PySpark implement tumbling, hopping/sliding, or session windows for time-based aggregation over late-arriving streaming data.

## Monitoring

The Monitoring Hub is the first stop for any item's run history; the Capacity Metrics app is the first stop for capacity-wide slowness (CU smoothing/throttling). Each item type has its own deeper monitoring surface: Spark UI for notebooks, refresh history for Dataflow Gen2 and semantic models, replication status for mirroring, and KQL diagnostic commands for Eventhouse ingestion. Direct Lake semantic models need explicit fallback-to-DirectQuery monitoring since that failure mode doesn't show up as an outright error. Data Activator (Reflex) is the no-code alerting mechanism across these signals.

## Error resolution

Each item type has characteristic failure modes and a corresponding place to look: pipelines (activity-level errors in run history), Dataflow Gen2 (step-level Power Query errors), notebooks (Spark UI stage/task detail and skew diagnosis), Eventhouse (`.show ingestion failures`), Eventstream (per-node monitoring/data preview), T-SQL (query insights), and OneLake shortcuts (source connection/credential/permission checks). The general discipline is to isolate to the smallest failing unit and distinguish data-dependent failures from environment/config failures.

## Performance optimization

Lakehouse tables: `OPTIMIZE` (compaction) and `ZORDER BY` (data skipping via co-location) plus `VACUUM` (stale file cleanup) are the core maintenance commands; V-Order is Fabric's default write-time acceleration. Pipelines benefit from parallelism and avoiding unnecessary sequential/serial ForEach chains. Warehouses benefit from up-to-date statistics, sound distribution, and `MERGE`-based upserts. Eventhouses benefit from update policies, materialized views, and tuned caching/retention policies. Spark performance hinges on right-sized autoscale/executors, high concurrency mode, correct partitioning, broadcast joins for small-side joins, the Native Execution Engine, and skew mitigation (AQE). Query performance generally comes down to pushing filters/projections early and keeping Direct Lake models within their supported limits to avoid DirectQuery fallback.
