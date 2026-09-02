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
