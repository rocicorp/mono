# Zero stability review — 2026-06

A stability analysis of the last 90 days of incidents (17 Mar – 24 Jun 2026), sourced from
incident.io (org `rocicorp`) to find the worst offenders by failure mode and stack.

## Contents

| File                         | What it is                                                                                       |
| ---------------------------- | ------------------------------------------------------------------------------------------------ |
| `FINDINGS.md`                | The full analysis in markdown (renders on GitHub). **Start here.**                               |
| `zero-stability-report.html` | Visual report — charts, collapsible theme sections, clickable incident links. Open in a browser. |
| `report-data.json`           | Structured source data behind the HTML report (metrics, charts, tables, recommendations).        |
| `build-report.cjs`           | Rebuilds the HTML from `report-data.json`.                                                       |

## Viewing

- **Markdown:** open `FINDINGS.md` (or read it on GitHub).
- **Visual report:** open `zero-stability-report.html` in a browser. It is self-contained
  (Tailwind + Chart.js load from CDN, so you need an internet connection on first open).
  Print to PDF from the browser for a shareable snapshot.

## Rebuilding the HTML

Edit `report-data.json`, then:

```bash
node build-report.cjs   # re-injects report-data.json into zero-stability-report.html
```

The script replaces the `<script id="report-data">` block in place, so it is idempotent.

## How it was generated

Produced from the incident.io MCP `analysis_start` workspace (operational-review playbook):
`incident_stats` / `incident_list` for the shape, `follow_up_list` + per-incident
`incident_show` (investigation/postmortem) for fix-status, then synthesised into
`report-data.json` and rendered with the branded template.

## Caveats

Severity is under-graded and AI urgency was never assessed, so triage is by incident content.
Theme and per-stack counts are analyst categorisations / name-match approximations (marked `~`),
not exact aggregates. "Fix status" is drawn from follow-ups, investigations, and fix-language in
summaries; unconfirmed where none existed. See the "Known gaps" section in `FINDINGS.md`.
