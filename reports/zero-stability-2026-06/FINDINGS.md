# Zero stability review — worst offenders

**Window:** 17 Mar – 24 Jun 2026 (90 days) · **Org:** rocicorp · **Source:** incident.io

> Companion to `zero-stability-report.html` (open in a browser for charts + clickable
> incident links). This file is the same analysis in plain markdown so it renders on GitHub.

## Headline numbers

| Metric                | Value            | Notes                                           |
| --------------------- | ---------------- | ----------------------------------------------- |
| Real incidents        | **123**          | of 159 filed; 36 triage-only dropped            |
| Major / Critical      | **24** (~20%)    | 1 Critical, 23 Major — severity is under-graded |
| Responder time        | **~95h**         | Apr 34h · May 43h · Jun 12h                     |
| Follow-up closure     | **11%**          | 2 of 18 done; 16 open, avg 41 days, oldest 90   |
| From one alert source | **95%**          | `cloudzero logs + amp` (151/159)                |
| Concentration         | **2 components** | `replication-manager` + `view-syncer`           |

Monthly volume: Mar 5 → Apr 40 → May 58 → Jun 56 (June partial). Rising, but this
tracks customer/stack onboarding more than a reliability regression — the important
signal is how _repetitive_ the failure mix is.

## The meta-finding

**Most incidents are mitigated or worked around, not root-fixed — and the post-incident
loop is barely used.** Only 18 follow-ups exist across 123 incidents, 2 are closed, and
most Majors generated none. As a result, the same failure classes recur:

- Slot loss **INC-331 → INC-556**
- Missing PG event-triggers **INC-485 → INC-487**
- ddl-ordering **INC-143** repeated

Recurrence is downstream of an unused post-incident process. Fixing the process is the
highest-leverage non-code change available.

## Root-cause themes, ranked

Counts are analyst categorisations (`~`), not exact aggregates — incident.io name search
tokenises too loosely for precise totals. Treat as proportions. "Fix status" reflects the
deepest fix found, not severity.

| Theme                                                      | ~Incidents | Fix status                                | Representative incidents         |
| ---------------------------------------------------------- | ---------: | ----------------------------------------- | -------------------------------- |
| Managed-PG failover / slot loss / auth / connection limits |        ~26 | Mitigated, recurring                      | INC-331→556, 476, 351, 347       |
| DDL & schema-change handling                               |        ~16 | Mostly mitigated / in-progress, recurring | INC-477, 485→487, 143, 494, 146  |
| Recoverable error → crash or false page                    |        ~13 | Partially fixed                           | INC-40 (done), 239, 16, 783, 209 |
| IVM / ZQL engine bugs                                      |        ~10 | Partially fixed in 1.6                    | INC-217 (done), 200, 234, 417    |
| Customer DB reset / user error                             |         ~8 | N/A (mostly not our bug)                  | INC-222, 156, 151, 162           |
| S3 / litestream backup & restore                           |         ~7 | Some fixes in flight                      | INC-783, 34, 33, 19, 192         |
| Replication lag from write bursts                          |         ~4 | Root cause open                           | INC-793, 43, 21, 38              |
| WebSocket payload size                                     |         ~2 | Config bump                               | INC-297, 111                     |

> Note: "Container restart loop" (alert fired 13×) and "replication lag" are _symptom_
> views that overlap the root-cause rows above, so the counts are not mutually exclusive.

## Theme detail

### 1. Managed-Postgres instability (~26) — _highest leverage_

Zero's dependence on customer-managed Postgres (PlanetScale / Supabase / Neon) is the
single largest incident generator. On failover, resize, or reset the replication slot is
lost and Zero is forced into a full resync → lag alerts, restart loops, real downtime
(INC-331 ~30 min, INC-476 ~2.5h). Recurring and only mitigated; two root-cause follow-ups
are open: slot-cleanup logic on interrupted initial sync (INC-351) and automatic
subscription retry (INC-347). Also includes password-auth failures (INC-393/391/388/305/12/8),
connection-refused/unreachable DB (INC-617/474/287), and superuser slot exhaustion
(INC-743/600).

### 2. Replica can't keep up with upstream write bursts

Large upstream transactions build lag → slot accumulates WAL until managed-PG caps/invalidates
it → recovery replaying the backlog overwhelms writes → loops back into lag. INC-793 captured
backpressure hitting a ~184MB buffer cap with 300K+ queued changes; it carries the **only open
High-priority follow-up**. INC-43 notes two consecutive days of giant-transaction lag; INC-21
traced a 16h stall to `statement_timeout=0` allowing an indefinite deadlock.

### 3. DDL & schema-change fragility (~16)

The change-streamer's DDL handling is brittle: `ddlUpdate received without ddlStart` halting
replication (INC-143/121); `missing property event` TypeErrors (INC-166/154/153); new column
arriving without DDL because PG event triggers weren't installed (INC-485 → recurred INC-487);
array/enum column SQL-syntax errors (INC-149/148/146/140); schema-change + backfill crash-looping
until an RM resync (INC-477). Mostly mitigated by resync, not root-fixed.

### 4. Recoverable errors that crash or false-page (~13) — _high ROI_

Recoverable/expected conditions surface as unhandled rejections / ERROR logs that crash a
worker or page on-call: change-streamer write timeouts → `unhandledRejection` → exit 13
(INC-793); intentional resync logged as exit 255 (INC-40, **fixed** — proves the pattern);
Supavisor crashes paging unnecessarily (INC-239); bad connection string → unhandled exception
(INC-209). An error-classification pass removes incidents _and_ noise simultaneously.

### 5. IVM / ZQL engine bugs (~10) — _purest product signal_

Bugs in the query engine itself, not the environment. INC-217 **fixed** (#5930, 1.6,
CVR concurrent-modify); INC-200 symptom-trapped (#5910/#5916) with root cause open;
INC-234 `UnionFanIn requires sorted input` (fix unconfirmed); INC-417 Take operator bug.

## Worst-offender stacks (name match, approx)

| Stack       | ~Incidents | Backend / note                                               |
| ----------- | ---------: | ------------------------------------------------------------ |
| Goblins     |        ~17 | PlanetScale — failover/slot-loss poster child                |
| Margins     |        ~13 | Supabase — backup-replicator crashes, lag, connection limits |
| Hinoki      |         ~7 | DDL/schema-change failures                                   |
| Productlane |         ~6 | auth + restart-loop churn                                    |
| Chief       |         ~4 |                                                              |
| Interfere   |         ~4 |                                                              |

## Recommendations (prioritised)

1. **[High] Survive managed-Postgres failover / slot loss without a full resync.** Retain or
   auto-recreate the slot across failover/resize, auto-retry the subscription, stop dropping
   the slot on transient initial-sync interruption. Root-cause follow-ups INC-351 / INC-347
   already exist but are open.
2. **[High] Fix replica throughput & backpressure under upstream write bursts.** Investigate
   change-streamer write throughput vs upstream volume, buffer/slot sizing, smoothing large
   transactions; make slow backfills stream incrementally instead of blocking the stream.
   (INC-793 — only open High-priority follow-up.)
3. **[Medium] Harden the DDL / schema-change path.** Make constant/enum-default `ADD COLUMN`
   metadata-only (no full backfill), fix array-column SQL generation, make ddl ordering and
   missing event-triggers self-healing rather than replication-halting.
4. **[Medium] Run an error-classification & alert-noise sweep.** Downgrade recoverable/transient
   cases (Supavisor crashes, transient `database is locked`, intentional resync exits,
   slow-upstream write timeouts) to retry-or-warn.
5. **[Medium] Close the post-incident loop.** Require a follow-up on every Major, assign
   priorities (15 of 16 open items are unprioritised), and burn down the backlog — several open
   items already have fixes in PRs and just need closing.
6. **[Medium] Burn down IVM / ZQL engine bugs and confirm 1.6 fixes landed.** Verify #5930
   shipped and that band-aided cases (INC-200, #5910/#5916) get a real root-cause fix.

## Known gaps — what this report does not tell you

- Severity is under-graded (~20% Major+) and AI escalation-urgency was never assessed → triage
  by incident content, not the severity field.
- Root-cause theme counts and per-stack counts are categorisations / name-match approximations
  (`~`), not exact query aggregates.
- Postmortems are sparse and follow-ups cover only ~15% of incidents, so "fix status" is drawn
  from investigation findings + fix-language in summaries; where neither existed it is marked
  unconfirmed.
- June is partial (through the 24th); 90-day window with no prior baseline, so month-over-month
  trend is directional, not seasonal.
