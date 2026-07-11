# Microsoft Fabric Data Engineer Associate (DP-700) — Study Notes

Study notes and reference material for [Exam DP-700: Implementing Data Engineering Solutions Using Microsoft Fabric](https://learn.microsoft.com/en-us/credentials/certifications/fabric-data-engineer-associate/).

## What's in this repo

Content is generated in two layers:

1. **Skills outline** — Microsoft's official [DP-700 study guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700), fetched directly (WebSearch + WebFetch), gives the exact domains/weights/bullets the exam measures.
2. **Teaching depth** — the actual [Microsoft Learn training modules](https://learn.microsoft.com/en-us/training/courses/dp-700t00) behind the official DP-700 course (found via the course page's module manifest: `dp-700t00`'s five constituent learning paths — Ingest Data, Implement a Lakehouse, Implement a Data Warehouse, Implement Real-Time Intelligence, Manage a Fabric Environment). Each module's individual **unit pages** (not just the module landing page, which is only a table of contents) were fetched for their real content — concepts, terminology, exact T-SQL/KQL/PySpark syntax, and numeric specifics (e.g. VACUUM's 168-hour default/minimum retention). Every major subsection cites its source module inline.

Technical syntax was additionally cross-checked against live API docs via the Context7 MCP tool (`delta-io/delta` for `OPTIMIZE`/`VACUUM`/`ZORDER`, `pyspark` for structured-streaming windowing) so it stays accurate independent of the Fabric-specific docs.

3. **Independent verification pass with Exa** — real `exa-py` calls (`search`, `search_and_contents`, `get_contents` with `livecrawl="always"`), not just Microsoft's own pages, against real-world blogs, Microsoft Fabric Community forum threads, and actual exam-taker writeups. This caught one genuine staleness bug the training-module pass alone missed (**V-Order's default state changed from on to off** in newer Fabric workspaces — Microsoft's current docs override what an older training module said) and filled a real gap (**deployment pipeline "item pairing," data-not-copied, and gateway-remapping behavior**) flagged by an actual exam-taker's writeup as tested but undocumented in the training modules. Content sourced from Microsoft's own docs is authoritative by default; independent/community sources were used to catch drift and fill gaps, not to override official Microsoft material where the two agree.

The skills outline reflects the exam version effective **July 21, 2026** — Microsoft publishes the incoming skills-measured revision ahead of its effective date on the same study guide page, alongside a change log against the prior version.

| File | Description |
|------|-------------|
| [1_implement_manage.md](1_implement_manage.md) | Workspace config, lifecycle management, security & governance, orchestration |
| [2_ingest_transform.md](2_ingest_transform.md) | Loading patterns, batch ingestion/transformation, streaming ingestion/transformation |
| [3_monitor_optimize.md](3_monitor_optimize.md) | Monitoring, error resolution per item type, performance optimization |
| [exam_sections.md](exam_sections.md) | Full exam domain breakdown with weightings, straight from the official skills outline |
| [study_cheat_sheet.md](study_cheat_sheet.md) | Quick-reference cheat sheet — decision trees, security layers, Delta/streaming syntax |
| [study_flashcards.md](study_flashcards.md) | Flashcards for key concepts |
| [study_practice_questions.md](study_practice_questions.md) | 30 practice exam questions with explanations |
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

If you have an Exa API key, use it as an **independent verification pass** on top of the training-module content, not a replacement for it — Microsoft's own docs are authoritative, but training modules can lag behind product changes (this is exactly how the V-Order default-state staleness bug below was caught).

Store the key in macOS Keychain rather than a plaintext `.env` (see `security add-generic-password -a "$USER" -s "EXA_API_KEY" -w "your-key"`), then in an isolated venv:

```bash
python3 -m venv venv && source venv/bin/activate
pip install exa-py
```

```python
import os, subprocess
from exa_py import Exa

key = subprocess.check_output(
    ["security", "find-generic-password", "-a", os.environ["USER"], "-s", "EXA_API_KEY", "-w"]
).decode().strip()
exa = Exa(api_key=key)

# Broad sweep: find community writeups, exam-taker experiences, and drift from official docs
r = exa.search("DP-700 Microsoft Fabric exam gotchas tricky questions", type="auto", num_results=6)
for res in r.results:
    print(res.title, res.url)

# Targeted verification: live-crawl a specific claim you're not fully sure of
c = exa.get_contents(
    ["https://learn.microsoft.com/en-us/fabric/data-engineering/delta-optimization-and-v-order"],
    text={"max_characters": 5000},
    livecrawl="always",  # bypasses Exa's cache, forces a fresh fetch
)
print(c.results[0].text)
```

Good query patterns for this repo specifically: `"<topic> exam experience/gotchas"` surfaces real test-taker writeups (often catches emphasis/nuance official docs don't state directly); `"<specific claim> Microsoft Fabric"` plus a live-crawled `get_contents` on the top official-docs hit is the fastest way to check whether a training-module-sourced fact has gone stale.

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
