# Implement and Manage an Analytics Solution

<https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700>

Sourced from the official DP-700 skills outline plus the actual Microsoft Learn training modules behind it: [Manage a Microsoft Fabric environment](https://learn.microsoft.com/en-us/training/paths/manage-microsoft-fabric-environment/) (CI/CD, monitoring, security, admin) and the relevant units of [Implement a Lakehouse with Microsoft Fabric](https://learn.microsoft.com/en-us/training/paths/implement-lakehouse-microsoft-fabric/).

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

### Domain workspace settings and the Fabric admin hierarchy

*Source: [Administer a Microsoft Fabric environment](https://learn.microsoft.com/en-us/training/modules/administer-fabric/) — Understand the Fabric admin model.*

Fabric operates on a **five-level hierarchy**, each with a narrower scope of control:

1. **Tenant** — the entire Fabric environment, aligned 1:1 with an organization's Microsoft Entra ID directory. Platform-wide settings (security policy, feature availability) live here.
2. **Capacity** — a dedicated compute/storage resource (an F-SKU) that powers Fabric workloads. Users consume capacity when they query, train models, or refresh reports; if the capacity runs out, work stops.
3. **Domain** — a logical grouping of workspaces reflecting business structure (e.g. a Finance domain, a Risk domain). Domains let you apply governance policy and delegate admin rights at the department level, and enable domain-scoped discovery in the OneLake catalog. **Subdomains** nest further.
4. **Workspace** — the collaboration container where teams create lakehouses, notebooks, and reports.
5. **Item** — the data assets themselves (lakehouses, warehouses, notebooks, semantic models, reports).

A matching **four-tier admin delegation model** lets a Fabric admin distribute responsibility instead of managing everything centrally:

| Role | Scope | Responsibility |
|---|---|---|
| **Fabric admin** | Entire tenant | Tenant settings, all capacities/domains, assigns other admins. Listed in Microsoft Entra ID as **Power BI Administrator**. |
| **Capacity admin** | One capacity | Manages workspaces assigned to that capacity, monitors capacity performance |
| **Domain admin** | One domain | Manages workspaces within the domain, enforces domain-specific policy |
| **Workspace admin** | One workspace | Controls access, manages items, assigns member permissions |

The **Fabric admin portal** organizes admin work into tenant settings, capacity settings, domains, users (role assignment), audit logs, and monitoring — plus the Microsoft 365 admin center for licensing, Microsoft Entra ID for identity, and PowerShell/REST APIs for automation.

### OneLake workspace settings

- Controls workspace-level OneLake behaviors: default storage settings, and enabling/disabling features like **OneLake data access roles** (folder/file-level security, see Security section).
- **OneLake** is the single, tenant-wide logical data lake underlying every Fabric workspace — one copy of data (Delta Parquet) addressable via ADLS Gen2-compatible APIs, shared across every engine (Spark, SQL, KQL, Power BI) with no data movement.

### Apache Airflow workspace settings

- **Apache Airflow Job** is a Fabric item that runs a managed Airflow environment for DAG-based orchestration, useful when you need Airflow-specific operators/sensors or are migrating existing Airflow DAGs into Fabric rather than rewriting as pipelines.
- Workspace-level Airflow settings configure the environment size and allowed connections; DAGs are authored in Python and stored/synced like any Airflow deployment, but scheduling and compute run inside the Fabric-managed environment.
- Use Airflow over a Fabric pipeline when you need complex Python-defined DAG logic, existing Airflow provider packages, or cross-cloud orchestration that a pipeline's GUI/JSON model doesn't cover well.

## Implement Lifecycle Management in Fabric

*Source: [Implement CI/CD in Microsoft Fabric](https://learn.microsoft.com/en-us/training/modules/implement-cicd-in-fabric/) — Implement version control and Git integration; Implement deployment pipelines.*

### Version control (Git integration)

- Supported version control systems: **GitHub** and **Azure DevOps**. Integration is at the **workspace level** — you version the items developed within a workspace.
- **Best practice**: don't develop directly in a shared, live workspace — changes there affect every user immediately. Instead, develop in an isolated workspace (or a client tool like Power BI Desktop / VS Code), then merge back.
- Workflow to set up:
  1. Set up a Git repository in GitHub or Azure DevOps.
  2. In the workspace, go to **Workspace settings → Git integration** and connect it to a repository branch. Fabric syncs content between the workspace and Git so they match.
  3. The workspace shows a **Git status** column and a source-control icon indicating how many items differ from the remote branch. Use **Changes** in the Source control window to push workspace edits to Git; use **Updates** to pull new Git commits into the workspace.
- **Branching pattern**: connect a development workspace to the main branch, then use **Source control → Branch out to new workspace** to create an isolated branch *and* a new workspace synced to it in one step. Make changes there, commit them, then open a **Pull Request** in Git to merge into main — the shared development workspace picks up the merged changes on next sync. (When using client tools instead of the web UI, the flow is the same, just with a local clone/push instead of in-browser edits.)

### Database projects

- A **Fabric database project** stores a Warehouse or SQL database's schema (tables, views, procedures) as source-controlled `.sql` files, similar to a SQL Server Database Project (SSDT).
- Enables schema-as-code: diffing, PR review, and automated deployment of schema changes instead of manual DDL execution.

### Deployment pipelines

- A **deployment pipeline** promotes Fabric items across stages — typically **Development → Test → Production** — by *copying* content between workspaces, not branching.
- **Create one**: Workspaces icon → Deployment pipelines → New pipeline → name and define stages → assign a workspace to each stage.
- **Deploy**: select the target stage, pick the source stage in the **Deploy from** dropdown, select **Deploy**. This copies all workspace content into the target stage (e.g. a pipeline that only exists in Development gets copied into Test).
- Supports **deployment rules** per stage (e.g., swap a connection string or parameter value automatically on deploy) so the same item works unmodified across environments.
- **Combining with Git**: a common pattern connects **only the Development workspace** to Git — Git handles version control during development, while the deployment pipeline handles promotion to Test/Production. This avoids Git sync conflicts across multiple stages. Flow: connect Dev workspace to Git → make/commit changes in Dev → use the deployment pipeline's Deploy button to promote Dev → Test → Production (Fabric-side promotion, separate from the Git-side commit history).
- Contrast: **Git integration** = version control / collaboration history; **deployment pipelines** = environment promotion. Many production setups use both together, as above.

## Configure Security and Governance

*Source: [Secure data access in Microsoft Fabric](https://learn.microsoft.com/en-us/training/modules/secure-data-access-in-fabric/) and [Secure a Microsoft Fabric data warehouse](https://learn.microsoft.com/en-us/training/modules/secure-data-warehouse-in-microsoft-fabric/).*

### The Fabric security model: three levels of access evaluation

Fabric evaluates every access request sequentially:

1. **Microsoft Entra ID authentication** — can the user authenticate at all?
2. **Fabric access** — can the user access Fabric?
3. **Data security** — can the user perform the requested action on this table/file? This third level has four configurable controls, each with increasingly specific scope:

| Layer | Scope | Notes |
|---|---|---|
| **Workspace roles** | Every item in a workspace | Admin / Member / Contributor / Viewer. Use when a user needs broad, multi-item access. |
| **Item permissions** | One item (e.g. one lakehouse) | Share an individual item without workspace-wide access. |
| **Compute (granular) permissions** | Within one engine (e.g. SQL analytics endpoint) | T-SQL `GRANT`/`DENY`/`REVOKE`; also where RLS, CLS, and dynamic data masking are applied. |
| **OneLake security** | Specific tables/folders, within an item | Role-based, engine-agnostic — enforced consistently across Spark, SQL, and OneLake APIs. |

A key gotcha: across the first three layers, granting someone access to an item gives them **all** the data in it. **OneLake security** is what adds control *within* an item.

### OneLake data access roles

- Role-based access control (RBAC): each role defines **Data** (which tables/folders), **Permission** (`Read` or `ReadWrite`), **Members**, and optional row/column **Constraints**.
- Configure via the lakehouse's **Manage OneLake security** command: create a role → choose Read or ReadWrite → select tables/folders → save → optionally add members and row/column filters.
- Workspace **Admin**/**Member**/**Contributor** already have full read/write to all OneLake data — OneLake security roles don't restrict them. Roles matter for **Viewer**-role users or users with only item-level **Read** permission, who otherwise have no OneLake data access by default.
- Every lakehouse ships with a built-in **`DefaultReader`** role granting all `ReadAll`-permission users access to all data. If you restrict a user to a custom, narrower role, you must also remove them from `DefaultReader` — otherwise they retain full read access through the default role anyway.
- Only workspace **Admin** or **Member** can create/modify OneLake security roles.

### Access control layers (know which layer solves which problem)

| Layer | Grants access to | Example |
|---|---|---|
| **Workspace-level** | The whole workspace (Admin/Member/Contributor/Viewer roles) | Give a team Contributor on the "Sales Analytics" workspace |
| **Item-level** | A single item (share a lakehouse/report without workspace access) | Share one Warehouse with an external analyst |
| **Row-level security (RLS)** | Specific rows in a table, based on the querying user | Regional managers see only their region's rows |
| **Column-level security (CLS)** | Specific columns in a table | Hide a `salary` column from non-HR roles |
| **Object-level security (OLS)** | Whole tables/objects (deny access to a table entirely, hides it from schema browsing) | Hide a staging table from analysts |
| **Folder/file-level security (OneLake data access roles)** | Specific folders/files in OneLake, enforced across every engine that reads OneLake | Restrict a `/raw/hr/` folder to the HR Spark job identity |

### Row-level security (RLS) — T-SQL

RLS filters which rows a user sees, transparently, without changing application queries. It has two parts: a **filter predicate** (an inline table-valued function returning true/false per row) and a **security policy** (binds the predicate to a table).

```sql
-- Create a schema for security objects
CREATE SCHEMA [Sec];
GO

-- Define the filter predicate
CREATE FUNCTION sec.tvf_SecurityPredicateBySalesPerson(@SalesPerson AS NVARCHAR(50))
    RETURNS TABLE
WITH SCHEMABINDING
AS
    RETURN SELECT 1 AS result
           WHERE @SalesPerson = USER_NAME()
              OR USER_NAME() = 'salesadmin@contoso.com';
GO

-- Apply the policy to the Sales table
CREATE SECURITY POLICY sec.SalesPolicy
ADD FILTER PREDICATE sec.tvf_SecurityPredicateBySalesPerson(SalesPerson) ON [dbo].[Sales]
WITH (STATE = ON);
GO
```

**Exam-relevant gotchas**: RLS predicates apply to *everyone*, including workspace Admins/Members/Contributors — if a predicate doesn't explicitly include an admin condition, admins get filtered too. RLS is also vulnerable to **side-channel attacks** (e.g. a crafted query that triggers a divide-by-zero only when a hidden value matches a guess) — combine RLS with CLS and dynamic data masking to reduce this risk, and restrict `ALTER ANY SECURITY POLICY` to trusted users.

### Column-level security (CLS) — T-SQL

CLS restricts specific columns; users without permission get an error selecting that column, but keep normal access to the rest of the table.

```sql
CREATE ROLE Doctor AUTHORIZATION dbo;
CREATE ROLE Receptionist AUTHORIZATION dbo;
GO

GRANT SELECT ON dbo.Patients TO Doctor;
GRANT SELECT ON dbo.Patients TO Receptionist;
GO

-- Deny SELECT on the sensitive column for roles that shouldn't see it
DENY SELECT ON dbo.Patients (MedicalHistory) TO Receptionist;
GO
```

**Direct Lake gotcha**: when Power BI queries a warehouse in Direct Lake mode and the table has CLS applied, queries **automatically fall back to DirectQuery** — security is still enforced, but at DirectQuery performance instead of the Direct Lake baseline.

### Dynamic data masking — T-SQL

Masks column values at query time for non-privileged users; the stored data is untouched, so DDM can be added to an existing warehouse without schema changes.

| Masking type | What it shows | Function |
|---|---|---|
| **Default** | Type-based full replacement — numbers→0, strings→XXXX, dates→1900-01-01 | `default()` |
| **Email** | First character + fixed `.com` suffix, e.g. `j*****@contoso.com` | `email()` |
| **Custom text (partial)** | Configurable prefix/suffix characters exposed, custom padding between | `partial(prefix, padding, suffix)` |
| **Random** | Random number in a specified range, for numeric/binary columns | `random(low, high)` |

```sql
ALTER TABLE Customers
ALTER COLUMN Email ADD MASKED WITH (FUNCTION = 'email()');

ALTER TABLE Customers
ALTER COLUMN CreditCardNumber ADD MASKED WITH (FUNCTION = 'partial(0,"XXXX-XXXX-XXXX-",4)');

-- Grant a specific non-admin user the ability to see unmasked data
GRANT UNMASK ON dbo.Customers TO [user@contoso.com];

-- Let an engineer manage masks without full admin rights
GRANT ALTER ANY MASK TO [engineer@contoso.com];
```

Users with `CONTROL` permission (Admins, Members, Contributors) always see unmasked data by default. **Gotcha**: DDM doesn't prevent querying the column — it only obscures the displayed value — so a masked user can still infer real values through side-channel queries (e.g. forcing an error that only occurs for a specific hidden value). Treat masking as one layer, not the only control.

### Sensitivity labels

- Applied from **Microsoft Purview Information Protection** taxonomy (e.g., Confidential, Highly Confidential) directly onto Fabric items (lakehouses, reports, files).
- Labels flow downstream: a label on a lakehouse can propagate to Power BI reports/semantic models built on it, and can trigger protection (encryption, access restriction) depending on label configuration.
- Distinct from access control — a sensitivity label communicates and can enforce classification/handling policy, RLS/OLS/masking control *who can query what*.

### Endorsement

- **Promoted**: workspace member signals an item is good quality/ready for reuse.
- **Certified**: a designated certifying authority (often domain-scoped) marks an item as an organization-approved, trustworthy source — appears with a certified badge in search/discovery, meant to reduce report/dataset sprawl by steering users to the authoritative version.

### Audit logs

- Fabric activities (item creation/deletion, sharing, exports, access) flow into the **Microsoft Purview audit log** / unified Microsoft 365 audit log, queryable via the compliance portal or Graph API for security investigations and compliance reporting. The Fabric admin portal also has a dedicated **Audit logs** section.

### Securing a medallion lakehouse (applied example)

*Source: [Organize a Fabric lakehouse using medallion architecture design](https://learn.microsoft.com/en-us/training/modules/describe-medallion-architecture/) — Secure and govern your medallion lakehouse.*

A medallion architecture creates a natural access boundary: data engineers work in bronze/silver, analysts and business users consume gold. Two approaches enforce that boundary:

| Approach | When to use |
|---|---|
| OneLake data access roles (single lakehouse, role scoped to gold tables) | Teams sharing a workspace who need different table access per layer |
| Separate workspace per medallion layer | Strong isolation, compliance boundaries, or separate capacity required |

Change management matters here too: Git integration versions pipeline/notebook/lakehouse definitions together so a bad transformation can be reverted; deployment pipelines then promote the whole medallion workspace through Dev → Test → Production in a controlled sequence.

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
