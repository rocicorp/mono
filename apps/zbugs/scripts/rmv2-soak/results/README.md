# Recorded soak results

This directory preserves reports from important soak runs. The files in
`.soak/` are ignored and are not a permanent record.

## `2026-09-02-post-fix-full-isolated.json`

- Integration branch: `mlaw/soak-post-fix-run` at `f3afadbc4`.
- Backup fix: `425ced06a`.
- Command: `node scripts/rmv2-soak.ts --chaos all --baseline --seed --run-id post-fix-full-isolated-2026-09-02`.
- Duration: 2,180 seconds.
- Result: `FAIL` because C9 reported one harness finding.
- Correctness: all 18 oracle checks passed.
- Coverage: all required SQLite routes ran.
- Safety: no reservation demotions, barrier timeouts, or purge-floor violations occurred.

C9 stopped MinIO for 120 seconds in this run. The soak then reported that the
physical SQLite file did not shrink. SQLite reused the freed pages instead of
truncating the file. The C9 assertion therefore did not measure purge recovery
correctly.

The next harness change measures live and free SQLite pages. It also limits the
WAL measurement to replication slots that start with `rmv2_soak_0_`.

## `2026-09-02-c9-corrected-post-fix.json`

- Integration branch: `mlaw/soak-post-fix-run` at `879016fb6`.
- Backup fix: `425ced06a`.
- Command: `node scripts/rmv2-soak.ts --chaos C9 --seed --run-id c9-corrected-post-fix-2026-09-02`.
- Duration: 1,026 seconds.
- Result: `FAIL` because C9 reported one harness finding.
- Correctness: all five oracle checks passed.
- Purge: 30,604 rows were removed after MinIO recovered.
- WAL: the app slot grew from 1.05 MB to 56.29 MB, then decreased to 0.86 MB.
- Live pages: usage decreased from 13.43 MB to 1.49 MB after recovery.

The pre-outage sample still contained data from the 8 KB payload phase. Its
live-page usage was 20.41 MB. Thus, the outage sample was smaller even though
MinIO pinned the purge floor. The app-slot growth proves that the outage pinned
the upstream acknowledgment. C9 must check the post-recovery decrease, not the
change from the pre-outage sample.

## `2026-09-02-c9-final-post-fix.json`

- Integration branch: `mlaw/soak-post-fix-run` at `716690ffc`.
- Backup fix: `425ced06a`.
- Command: `node scripts/rmv2-soak.ts --chaos C9 --seed --run-id c9-final-post-fix-2026-09-02`.
- Duration: 1,026 seconds.
- Result: `PASS` with no findings.
- Correctness: all five oracle checks passed.
- Purge: 29,897 rows were removed after MinIO recovered.
- WAL: the app slot grew from 5.31 MB to 66.00 MB, then decreased to 176 bytes.
- Live pages: usage decreased from 13.86 MB to 1.88 MB after recovery.
- Purge bounds: no pass reached the batch limit or requested an immediate continuation.

This run confirms C9 recovery after the planned five-minute MinIO outage. The
physical SQLite file stayed at 42.37 MB and reused 31.62 MB of free pages.
