# Mutations

Each patch brings back a bug the simulation must find (§5 of the replication
DST plan). Apply one on top of the working tree, sweep, and revert it:

```sh
git apply src/test/sim/mutations/row-03-no-reservation-cap.patch
ZERO_SIM_RUNS=500 ZERO_SIM_SEED=1 \
  npx vitest --project='*no-pg*' run src/test/sim/rm.sim.test.ts -t sweep
git apply -R src/test/sim/mutations/row-03-no-reservation-cap.patch
```

Rows 6 and 7 sweep `src/test/sim/backfill.sim.test.ts` instead.

A sweep that passes has missed the bug. Add `ZERO_SIM_SHRINK=1` with the
reported `ZERO_SIM_SEED` and `ZERO_SIM_PATH` to shrink a counterexample. A
replay by `ZERO_SIM_PATH` keeps sweeping up to `ZERO_SIM_RUNS`, so set that to
one past the reported path to stop at the failing run.

| Row | Patch                                      | Brings back                                                                      | Caught by                                                                             | Seeds 1–5: runs to failure |
| --- | ------------------------------------------ | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | -------------------------- |
| 1   | `row-01-purge-floor-inclusive`             | Purge at the confirmed backup watermark, not strictly below it                   | The purge scheduler's own floor probe (an alarm)                                      | 1, 1, 1, 2, 1              |
| 2   | `row-02-no-invalidation-on-reseed`         | A reseed that leaves open reservations in place                                  | Not reachable with the PG change log off; see below                                   | none in 1,000 runs         |
| 3   | `row-03-no-reservation-cap`                | A reservation that never expires                                                 | `Prop_NoPermanentPin` after heal: a wedged restore holds its reservation past the cap | 7, 2, 13, 4, 9             |
| 4   | `row-04-lease-shorter-than-restore`        | A lease shorter than a restore (a config, not code)                              | `Prop_RestoreCompletes` while commits arrive: the restore loops                       | 1, 1, 2, 2, 1              |
| 5   | `row-05-replace-from-older-backup`         | A replication-manager restored below the slot's ACK (an illegal step)            | The silent gap: a replicator applies DDL for a table it never received                | 199, 165, 33, 42, 8        |
| 6a  | `row-06a-untracked-table-backfill-ignored` | A tracker that ignores a backfill the stream starts on a table it does not track | "Completes" at heal: a replica that stops following a run never requests another      | 237, 1358, 944, 671, 412   |
| 6b  | `row-06b-no-tracker-without-declarations`  | No tracker for a subscriber that declared nothing                                | "Completes" at heal, as for 6a                                                        | 237, 1358, 944, 1028, 412  |
| 7   | `row-07-v17-rollback-keeps-marks`          | Rolling forward from v17 keeps the marks that v17 left stale                     | Not by the sweep; see below                                                           | none in 2,000 runs each    |
| 8   | `row-08-purge-floor-at-minor`              | A purge floor at the minimum ACK itself, not at its major (B§13.5)               | Oracle 4, the purge floor probe, or a backup replicator that can no longer resume     | 2134, 1639, 40, 434, 282   |
| 9   | `row-09-no-seed-catchup-start`             | No seed standing in for a follower's major (`NoSeedStandIn`)                     | A backup replicator restored at a minor, which its own new log cannot resume          | 28, 5, 81, 3, 24           |
| 10  | `row-10-unrenamed-declarations-dropped`    | Every declaration discarded as `dropped-table` (B§13.9)                          | "Completes" at heal: a replica left backfilling, which nothing requests a run for     | 2356, 4460, 1137, 3, 587   |

Every run that failed under a mutation was replayed on the tree without it, to
check that the failure was the mutation's, and every replay passed.

**Rows 1–5 were measured again after D5** put backfills, backfill faults, and
slot takeovers into the sweep's generator. Each failure still came within 30
seconds of wall time. Rows 6a and 6b are from the backfill sweep, where each came
within 20 seconds.

**Rows 8–10 sweep the composed system**, where backfills run through the real
change-streamer, backup replicator, and view-syncers, and they take longer: row
8's seed 1 needed 2,134 runs, and row 10's seed 2 needed 4,460, about 11
minutes.

**Row 7 needs a pinned case**, as it does in the model test. A hole takes a mark
on a column in flight, a row key change that moves a row from above the mark to
below it and leaves the column out (a TOASTed value), replicated by v17, and
then a run resumed from the stale mark. The sweep's v17 step moves random rows
of random tables, and 10,000 runs never lined those up. `backfill.sim.test.ts`
pins it ("rolling forward from v17 forgets a mark a row key change passed"),
and under this patch the pin fails at once on oracle 7: the moved row's column
is empty after the run completes.

**Row 2 cannot be reached with the PG change log off.** A reseed only harms a
reservation that was confirmed, or pinned, against the log it replaces. With
the PG log off:

- A live replication-manager never reseeds. A writer that fails soft stays
  disabled until the process restarts, and a restart ends every reservation.
- A new incarnation reseeds only at its first reconcile. A `created` log has no
  file for a reservation to be pinned to. A `schema-mismatch` or
  `identity-mismatch` log is rejected by `inspectSQLiteChangeLog`, so its
  reservation holds a PG route, which is re-pinned against the reseeded log at
  the next backup.

It needs the PG change log on, which the simulation leaves out, or a fault the
simulation does not have.

**Row 5's first measurement**, before D5, failed on a real bug for seed 2
instead: its run masked the step group that holds `rmReplace`, so the mutation
never ran. That bug is pinned in `rm.sim.test.ts` as "a disconnect while a
forwarded commit awaits flow control does not roll it back".
