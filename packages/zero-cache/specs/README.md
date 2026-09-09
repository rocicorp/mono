# TLA+ specs

## `ChangeLogCoverage.tla`

Models the SQLite change log's **coverage protocol**: what a restoring
view-syncer is promised by a snapshot reservation, what the purger may delete,
and how the reservation cap added by
`feat(zero-cache): cap how long a snapshot reservation may hold the change log`
interacts with both.

It models the **post-retirement** topology — no PG change log — because that is
the configuration the properties are about. With PG still enabled every hold
below becomes a demotion and every catchup succeeds.

Run everything with `./check.sh`. It fetches `tla2tools.jar` on first use and
asserts the expected outcome of each configuration; a `FAIL` line means the
code's behaviour and the spec's have diverged.

### What is modelled

| Spec                                                        | Code                                                                                                                  |
| ----------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `logMin`, `head`, `seedWm`                                  | `SQLiteChangeLogCoverage` (`sqlite-change-log-read-router.ts:14`)                                                     |
| `Spans(w)`                                                  | `spansInterval` (`change-log-initializer.ts:462`) — reaches back that far **and** holds a `commit` row at exactly `w` |
| `Floor`                                                     | `#getCleanupFloor` (`change-streamer-service.ts:1525`)                                                                |
| `PurgeDispatch` / `PurgeApply`                              | `SQLiteChangeLogPurgeScheduler.purge`, split so a batch can be in flight                                              |
| `OpenReservation` / `PinReservation` / `ConfirmReservation` | `startSnapshotReservation` and `#confirmReservations`                                                                 |
| `ExpireReservation`                                         | `SnapshotReservations.#expire`                                                                                        |
| `Reseed`                                                    | `reconcileChangeLog`, which runs on **every change-stream connection** (`change-streamer-service.ts:765`)             |

Two deliberate over-approximations, both sound for safety: purge deletes to the
floor itself rather than to `majorVersionOf(floor)`, which retains strictly
more; and a crashed follower's ACK leaves the floor at once rather than after
the cleanup grace period.

### Properties

- `Inv_PurgeNeverPassesBackup` — invariant 14 in its mechanical form.
- `Inv_LiveReservationCovered` — a confirmed reservation still covers what it
  promised.
- `Inv_PromiseKept` — a follower restoring under a reservation that has not
  been taken back can always catch up from the watermark it was given.
  `lost` exempts the followers whose cap expired: the deliberate sacrifice.
- `Prop_NoPermanentPin` — no reservation holds the log forever.
- `Prop_RestoreCompletes` — a restore that is not wedged ends up serving again.

### Results

**The reseed bug, found here and since fixed.** `NoInvalidation` fails: nothing
in the reservation machinery observed a reseed, and reconciliation runs on every
change-stream connection. Two windows:

1. `peek()` returns the route stored by `pin()`, coverage and all
   (`sqlite-change-log-read-router.ts:101`), so a reseed between the pin and
   the confirmation confirms against a minimum the log no longer has.
2. A reseed **after** confirmation silently voids a promise already made.

Both ended the same way: the follower restores, subscribes at the watermark it
was handed, and is answered `WatermarkTooOld`. No silent gap — invariant 12
holds — but the reservation bought nothing, which is the one thing it exists
to do. `RevalidateOnly` shows that re-reading the bounds at confirmation time
closes only the first window; taking reservations back on a reseed closes both
and is sufficient on its own.

The fix is `SQLiteChangeLogWriterOptions.onReseeded` →
`ChangeStreamerService.#invalidateReservations` →
`SnapshotReservations.closeAll()`. `Fixed` is the shipped behaviour and is the
baseline every other configuration below varies from.

**A `truncated` reconcile deliberately does _not_ invalidate**, because it
deletes above the resume watermark and every reservation is advertised at the
confirmed backup watermark, which is at or below it. That relationship is load
bearing rather than incidental: `TruncateLow` drops it and safety fails. It
holds because a backup covers only what the replica has applied, the replica
holds only what was forwarded, and invariant 2 puts forwarding after the log's
commit. Worth an assertion if that chain ever changes.

**`SeedConfirm` fails**, which answers the open question in the soak plan
§1.5. Confirming on `seedWatermark <= backupWatermark` instead of
`minWatermark` is not conservatism that can be tightened away: `seedWatermark`
lives in the meta row and writes no stream row (`change-log-db.ts:191`), so a
log that has committed nothing since its seed has no catchup boundary at that
watermark and never will. The four-step counter-example confirms a reservation
whose subscribe is then rejected. `minWatermark` is exact; the wait it causes
is real.

**`NoPause` passes**, which is a hypothesis rather than a recommendation: at
this abstraction the purge pause has no safety role, because the floor is
already min'd with `backupWatermark` and that is exactly the watermark
`confirmFor` advertises. Before acting on that, check the two things the model
abstracts away — the torn minimum transaction (invariant 5) and
`headWatermark`.

**The cap is sound and load-bearing.** `Liveness-NoLease` violates
`Prop_NoPermanentPin`: a wedged restore pins the log forever. `Liveness-Lease`
holds. The cap does **not** weaken invariant 14 — reserved watermarks only
ever lower the floor, so taking one back lets the floor rise to the backup and
no further. `Liveness-ShortLease` reproduces the loop that
`snapshot-reservations.ts:26` warns about: with a cap shorter than a restore,
expire → `WatermarkTooOld` → restore → expire, forever.
