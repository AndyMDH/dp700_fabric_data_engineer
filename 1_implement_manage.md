# Implement and Manage an Analytics Solution

<https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700>

## Configure Microsoft Fabric Workspace Settings

A **workspace** is the unit of collaboration and access control in Fabric — it maps to one Fabric/Power BI capacity and holds items (lakehouses, warehouses, notebooks, pipelines, eventhouses, etc.).

### Spark workspace settings

- **Pool**: Starter pool (fast session start, Microsoft-managed) vs custom pool (control over node family/size, autoscale range).
- **Node family**: Memory-optimized (default, best for most Spark workloads) vs compute-optimized.
- **Autoscale**: min/max node count; Fabric adds/removes nodes within the range based on load.
- **Dynamic allocation of executors**: lets Spark scale executors up/down within a session instead of a fixed count.
- **Runtime version**: pins the Spark/Delta/Python/Scala versions available to notebooks and jobs in the workspace — upgrading changes default library versions.
- **High concurrency mode**: multiple notebooks share one Spark session/application to cut cold-start time and improve utilization; available for notebooks and pipelines separately.
- **Environment**: a publishable item bundling Spark properties, libraries (public PyPI or custom wheel/jar), and resources — attach as the workspace default so every notebook/job inherits it without per-notebook config.

### Domain workspace settings

- **Domains** group related workspaces (e.g. by business unit) for governance and discoverability at the tenant level; **subdomains** nest further.
- Assigning a workspace to a domain applies domain-level settings (e.g. certified/endorsed defaults, domain admins) and enables domain-scoped discovery in OneLake catalog.
- Domain admins are distinct from workspace admins — domain admin manages membership/policy across all workspaces in the domain.

### OneLake workspace settings

- Controls workspace-level OneLake behaviors: default storage settings, and enabling/disabling features like **OneLake data access roles** (folder/file-level security, see Security section).
- **OneLake** is the single, tenant-wide logical data lake underlying every Fabric workspace — one copy of data (Delta Parquet) addressable via ADLS Gen2-compatible APIs, shared across every engine (Spark, SQL, KQL, Power BI) with no data movement.

### Apache Airflow workspace settings

- **Apache Airflow Job** is a Fabric item that runs a managed Airflow environment for DAG-based orchestration, useful when you need Airflow-specific operators/sensors or are migrating existing Airflow DAGs into Fabric rather than rewriting as pipelines.
- Workspace-level Airflow settings configure the environment size and allowed connections; DAGs are authored in Python and stored/synced like any Airflow deployment, but scheduling and compute run inside the Fabric-managed environment.
- Use Airflow over a Fabric pipeline when you need complex Python-defined DAG logic, existing Airflow provider packages, or cross-cloud orchestration that a pipeline's GUI/JSON model doesn't cover well.

## Implement Lifecycle Management in Fabric

### Version control (Git integration)

- Workspaces connect to a **Git repository** (Azure DevOps or GitHub) branch; each supported item type serializes to source-controlled files (Notebooks as `.py`/`.ipynb` with metadata, Lakehouses/Warehouses as schema definitions, pipelines as JSON, etc.).
- Workflow: connect workspace → branch out for a feature → **commit** workspace changes to Git, or **update** the workspace from Git → open a PR → merge.
- Supports standard branching workflows: one workspace per branch (e.g., dev/test/prod) is the common pattern, distinct from deployment pipelines (below).

### Database projects

- A **Fabric database project** stores a Warehouse or SQL database's schema (tables, views, procedures) as source-controlled `.sql` files, similar to a SQL Server Database Project (SSDT).
- Enables schema-as-code: diffing, PR review, and automated deployment of schema changes instead of manual DDL execution.

### Deployment pipelines

- A **deployment pipeline** promotes Fabric items across stages — typically **Development → Test → Production** — copying content between workspaces rather than branching.
- Supports **deployment rules** per stage (e.g., swap a connection string or parameter value automatically on deploy) so the same item works unmodified across environments.
- Can be paired with Git integration: Git handles source control within a stage; deployment pipelines handle promotion between stages.
- Contrast: **Git integration** = version control / collaboration history; **deployment pipelines** = environment promotion. Many production setups use both together.

## Configure Security and Governance

### Access control layers (know which layer solves which problem)

| Layer | Grants access to | Example |
|---|---|---|
| **Workspace-level** | The whole workspace (Admin/Member/Contributor/Viewer roles) | Give a team Contributor on the "Sales Analytics" workspace |
| **Item-level** | A single item (share a lakehouse/report without workspace access) | Share one Warehouse with an external analyst |
| **Row-level security (RLS)** | Specific rows in a table, based on the querying user | Regional managers see only their region's rows |
| **Column-level security (CLS)** | Specific columns in a table | Hide a `salary` column from non-HR roles |
| **Object-level security (OLS)** | Whole tables/objects (deny access to a table entirely, hides it from schema browsing) | Hide a staging table from analysts |
| **Folder/file-level security (OneLake data access roles)** | Specific folders/files in OneLake, enforced across every engine that reads OneLake | Restrict a `/raw/hr/` folder to the HR Spark job identity |

- Workspace roles: **Admin** (full control incl. permissions) > **Member** (edit + manage some settings) > **Contributor** (edit content, no permission management) > **Viewer** (read-only).
- RLS/OLS are configured on Warehouses/Lakehouses (SQL analytics endpoint) via T-SQL security policies (`CREATE SECURITY POLICY`) much like Azure SQL / Synapse RLS; CLS via `GRANT`/`DENY` on columns or views.
- **OneLake security (data access roles)** is the newer, engine-agnostic layer: define roles scoped to OneLake folders that apply consistently whether the caller is Spark, SQL endpoint, Power BI, or an external app reading OneLake directly — closes the gap where SQL-level RLS didn't protect direct file access.

### Dynamic data masking

- Masks column values at query time for non-privileged users without altering stored data (e.g., show `XXX-XX-1234` for an SSN column).
- Built-in mask functions: default, email, random (numeric), custom string (partial masking).
- Privileged roles (e.g., table owner, `UNMASK` permission) see unmasked data; everyone else sees the masked pattern.

### Sensitivity labels

- Applied from **Microsoft Purview Information Protection** taxonomy (e.g., Confidential, Highly Confidential) directly onto Fabric items (lakehouses, reports, files).
- Labels flow downstream: a label on a lakehouse can propagate to Power BI reports/semantic models built on it, and can trigger protection (encryption, access restriction) depending on label configuration.
- Distinct from access control — a sensitivity label communicates and can enforce classification/handling policy, RLS/OLS/masking control *who can query what*.

### Endorsement

- **Promoted**: workspace member signals an item is good quality/ready for reuse.
- **Certified**: a designated certifying authority (often domain-scoped) marks an item as an organization-approved, trustworthy source — appears with a certified badge in search/discovery, meant to reduce report/dataset sprawl by steering users to the authoritative version.

### Audit logs

- Fabric activities (item creation/deletion, sharing, exports, access) flow into the **Microsoft Purview audit log** / unified Microsoft 365 audit log, queryable via the compliance portal or Graph API for security investigations and compliance reporting.

## Orchestrate Processes

### Choosing between Dataflow Gen2, a pipeline, and a notebook

| Tool | Best for | Notes |
|---|---|---|
| **Dataflow Gen2** | Low-code/no-code transformation, business-analyst-friendly ETL | Power Query (M) engine; good for smaller-to-medium data, many connectors; can be expensive/slow at very large scale vs Spark |
| **Pipeline** | Orchestration — sequencing activities, moving/copying data, calling other items on a schedule/trigger | The "conductor": Copy Data activity, Notebook activity, Dataflow activity, control flow (If, ForEach, Until) |
| **Notebook** | Code-first transformation at scale (PySpark/Scala/SQL/R) | Full Spark power, best for complex logic, large-scale transforms, ML |

- Common real pattern: a **pipeline** orchestrates the run, calling a **notebook** activity for heavy transformation and/or a **Dataflow Gen2** activity for a self-service step, on a schedule.

### Schedules and event-based triggers

- **Scheduled triggers**: run on a defined cadence (every N minutes/hours, daily at a time, cron-like).
- **Event-based triggers**: fire on an event — e.g., a file arriving in a OneLake/ADLS location (Storage Events), a message on Fabric Event streams/Event Hubs, or completion of another job — enabling near-real-time, arrival-driven pipelines instead of polling on a schedule.

### Orchestration patterns with parameters and dynamic expressions

- **Pipeline parameters**: defined at the pipeline level, passed in at trigger/invocation time (e.g., a date range, a source table name) — enable one parameterized pipeline to serve many scenarios instead of duplicating pipelines.
- **Dynamic expressions**: pipeline expression language (`@pipeline().parameters.x`, `@utcnow()`, `@activity('name').output.value`) used inside activity settings to reference parameters, prior activity outputs, or system variables at runtime.
- **ForEach** activity + parameterized child pipeline/notebook = metadata-driven orchestration: loop over a control table of source tables and invoke the same templated copy/transform logic per row instead of one pipeline per table.
- Notebooks accept parameters too (`%%configure` / the notebook's designated parameters cell), and a pipeline's Notebook activity can pass pipeline parameters/expressions straight into notebook parameters — the standard way to make a single generic notebook reusable across many pipeline invocations.
