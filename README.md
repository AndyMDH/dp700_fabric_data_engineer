# Microsoft Fabric Data Engineer Associate (DP-700) — Study Notes

Study notes and reference material for [Exam DP-700: Implementing Data Engineering Solutions Using Microsoft Fabric](https://learn.microsoft.com/en-us/credentials/certifications/fabric-data-engineer-associate/).

## What's in this repo

Content is generated in two layers:

1. **Skills outline** — Microsoft's official [DP-700 study guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700), fetched directly (WebSearch + WebFetch), gives the exact domains/weights/bullets the exam measures.
2. **Teaching depth** — the actual [Microsoft Learn training modules](https://learn.microsoft.com/en-us/training/courses/dp-700t00) behind the official DP-700 course (found via the course page's module manifest: `dp-700t00`'s five constituent learning paths — Ingest Data, Implement a Lakehouse, Implement a Data Warehouse, Implement Real-Time Intelligence, Manage a Fabric Environment). Each module's individual **unit pages** (not just the module landing page, which is only a table of contents) were fetched for their real content — concepts, terminology, exact T-SQL/KQL/PySpark syntax, and numeric specifics (e.g. V-Order's ~15% write overhead, VACUUM's 168-hour default retention). Every major subsection cites its source module inline.

Technical syntax was additionally cross-checked against live API docs via the Context7 MCP tool (`delta-io/delta` for `OPTIMIZE`/`VACUUM`/`ZORDER`, `pyspark` for structured-streaming windowing) so it stays accurate independent of the Fabric-specific docs.

The skills outline reflects the exam version effective **July 21, 2026** — Microsoft publishes the incoming skills-measured revision ahead of its effective date on the same study guide page, alongside a change log against the prior version.

| File | Description |
|------|-------------|
| [1_implement_manage.md](1_implement_manage.md) | Workspace config, lifecycle management, security & governance, orchestration |
| [2_ingest_transform.md](2_ingest_transform.md) | Loading patterns, batch ingestion/transformation, streaming ingestion/transformation |
| [3_monitor_optimize.md](3_monitor_optimize.md) | Monitoring, error resolution per item type, performance optimization |
| [exam_sections.md](exam_sections.md) | Full exam domain breakdown with weightings, straight from the official skills outline |
| [study_cheat_sheet.md](study_cheat_sheet.md) | Quick-reference cheat sheet — decision trees, security layers, Delta/streaming syntax |
| [study_flashcards.md](study_flashcards.md) | Flashcards for key concepts |
| [study_practice_questions.md](study_practice_questions.md) | 20 practice exam questions with explanations |
| [study_topic_summaries.md](study_topic_summaries.md) | Topic summaries for a final review pass |

## Exam domains

| Domain | Weight |
|--------|--------|
| Implement and manage an analytics solution | 30–35% |
| Ingest and transform data | 30–35% |
| Monitor and optimize an analytics solution | 30–35% |

## Updating this content

The exam refreshes periodically — re-check the study guide before your exam date and regenerate notes if the skills outline has changed.

### Step 1 — Pull the current skills outline

In Claude Code, use WebSearch to find the current study guide URL, then WebFetch it directly:

```
https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700
```

Watch for a "Change log" table on that page comparing the prior vs. current skills-measured version — the "as of [date]" column is the one to study from if a revision is upcoming.

If you have an Exa API key and want broader web coverage beyond the one canonical page (e.g. community writeups, exam-readiness blog posts), the same `exa-py` workflow from the Databricks repo applies:

```bash
pip install exa-py
export EXA_API_KEY="your_key_here"
```

```python
from exa_py import Exa
import os

exa = Exa(api_key=os.environ["EXA_API_KEY"])

results = exa.search(
    "DP-700 Microsoft Fabric Data Engineer exam study guide 2026",
    type="deep",
    num_results=10,
    contents={"highlights": True},
)

for r in results.results:
    print(r.title, r.url)
    print(r.highlights)
```

### Step 2 — Pull the real teaching content from Microsoft Learn training modules

The skills outline alone is thin — it's a bullet list, not an explanation. For actual depth:

1. WebFetch the course page (`https://learn.microsoft.com/en-us/training/courses/dp-700t00`) — its raw YAML frontmatter includes a `learn_item` list of the five constituent learning-path IDs.
2. WebFetch each learning path's index page (`https://learn.microsoft.com/en-us/training/paths/<path-id>/`) to get its module list.
3. **Important**: a module's own index page (`.../modules/<module-id>/`) is only a table of contents — it lists unit titles but not their content. Fetch the **individual unit pages** (`.../modules/<module-id>/<unit-slug>`, e.g. `.../modules/work-delta-lake-tables-fabric/3b-optimize-delta-tables`) for the actual teaching text, code examples, and terminology. Skip the boilerplate units (Introduction, Exercise, Module assessment, Summary) — the content units are what matter.
4. Prioritize units that map directly to graded exam bullets over generic/overview units you likely already know well from trained knowledge.

### Step 3 — Supplement with Context7

In Claude Code, use the Context7 MCP tool to pull current API documentation for the specific libraries referenced in the notes — `delta-io/delta` for `OPTIMIZE`/`VACUUM`/`ZORDER`, `pyspark` for structured streaming/windowing syntax. This keeps code snippets accurate as those APIs evolve, independent of the Fabric-specific product documentation.

### Step 4 — Refresh the markdown files

Paste the WebFetch/Exa/Context7 output into Claude Code and ask it to update the relevant section files. The files map to the exam's three skill areas:

| File | Skill area |
|------|-------------|
| [1_implement_manage.md](1_implement_manage.md) | Implement and manage an analytics solution |
| [2_ingest_transform.md](2_ingest_transform.md) | Ingest and transform data |
| [3_monitor_optimize.md](3_monitor_optimize.md) | Monitor and optimize an analytics solution |

Update the section files first, then regenerate the derived study files last:
- [study_cheat_sheet.md](study_cheat_sheet.md)
- [study_flashcards.md](study_flashcards.md)
- [study_practice_questions.md](study_practice_questions.md)
- [study_topic_summaries.md](study_topic_summaries.md)
