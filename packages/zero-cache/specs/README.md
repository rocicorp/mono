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

A watermark has a **major** and a **minor**. A major is an upstream commit. A
minor is a backfill transaction that the replication-manager mints under the
last one, and it is local to the replica that applies it, so a follower that
follows backfill runs (protocol v7) subscribes at the major of its state
version rather than at the version itself. That difference is the one the
model needs to see the seed-boundary bug below.

Run everything with `./check.sh`. It fetches `tla2tools.jar` on first use and
asserts the expected outcome of each configuration; a `FAIL` line means the
code's behaviour and the spec's have diverged. The shipped configuration has
about a million distinct states, and all twelve configurations take a little
over a minute.

### What is modelled

| Spec                                                        | Code                                                                                                   |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `head`, `log`, `seedWm`                                     | `SQLiteChangeLogCoverage` (`sqlite-change-log-read-router.ts`): the log's head, its rows, and its seed |
| `hist`                                                      | every watermark committed at or below the head: where a replica or a backup can be                     |
| `Major(w)`                                                  | `majorVersionOf` (`types/state-version.ts`)                                                            |
| `Commit` / `BackfillCommit`                                 | an upstream commit, and a backfill transaction at the next minor (`beginTxFor`, `backfill-manager.ts`) |
| `Spans(w)`                                                  | `spansInterval` — a `commit` row at exactly `w`, in a log that is only ever purged by prefix           |
| `SubscribeAt` / `SeedCovers` / `CanCatchUp`                 | the subscribe watermark (`incremental-sync.ts`) and `seedCatchupStart` (`sqlite-change-log-reader.ts`) |
| `Floor` / `PurgeFloor`                                      | `#getCleanupFloor`, which purges to the major of the floor                                             |
| `PurgeDispatch` / `PurgeApply`                              | `SQLiteChangeLogPurgeScheduler.purge`, split so a batch can be in flight                               |
| `OpenReservation` / `PinReservation` / `ConfirmReservation` | `startSnapshotReservation` and `#confirmReservations`                                                  |
| `ExpireReservation`                                         | `SnapshotReservations.#expire`                                                                         |
| `Reseed`                                                    | `reconcileChangeLog`, which runs on **every change-stream connection**, and `seedChangeLogStream`      |

A reseed writes a real transaction at the resume watermark, so a freshly seeded
log serves a subscriber at exactly that watermark. An earlier version of this
spec modelled the seed as writing no row, which the code has never done.

One deliberate over-approximation, sound for safety: a crashed follower's ACK
leaves the floor at once rather than after the cleanup grace period. One
simplification: a reseed lands at the head, not anywhere between the backup and
the head.

### Properties

- `Inv_PurgeNeverPassesBackup` — invariant 14 in its mechanical form.
- `Inv_LiveReservationCovered` — a confirmed reservation still covers what it
  promised.
- `Inv_PromiseKept` — a follower restoring under a reservation that has not
  been taken back can always catch up from the watermark it was given.
  `lost` exempts the followers whose cap expired: the deliberate sacrifice.
- `Prop_NoPermanentPin` — no reservation holds the log forever.
- `Prop_RestoreCompletes` — a restore that is not wedged ends up serving again.

"Catch up from the watermark it was given" means from where the follower then
subscribes (`CanCatchUp`): its major, or the seed standing in for it.

### Results

**The reseed bug, found here and since fixed.** `NoInvalidation` fails: nothing
in the reservation machinery observed a reseed, and reconciliation runs on every
change-stream connection. Two windows:

1. `peek()` returns the route stored by `pin()`, coverage and all
   (`sqlite-change-log-read-router.ts`), so a reseed between the pin and the
   confirmation confirms against a minimum the log no longer has.
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

**The seed-boundary bug, found in review and modelled since.** `NoSeedStandIn`
fails in seven steps: a backfill transaction takes the head to a minor, a
backup lands on it, a follower crashes, the log is reseeded at that minor, and
the follower's reservation is confirmed there. Subscribing at the major, which
this log has never held, the follower is refused, and restoring the same backup
repeats it. The earlier spec could not express this: watermarks had no minor,
every follower subscribed at exactly the watermark it restored to, and the seed
wrote no row. The fix is `seedCatchupStart` (`SeedStandsIn`): a follower that
follows runs starts from the seed while the seed is still the log's first
transaction. `SubscribeExact` passes without it, so it is subscribing at the
major that needs the stand-in.

**A `truncated` reconcile deliberately does _not_ invalidate**, because it
deletes above the resume watermark and every reservation is advertised at the
confirmed backup watermark, which is at or below it. That relationship is load
bearing rather than incidental: `TruncateLow` drops it and safety fails. It
holds because a backup covers only what the replica has applied, the replica
holds only what was forwarded, and invariant 2 puts forwarding after the log's
commit. Worth an assertion if that chain ever changes.

**`SeedConfirm` passes**, which reverses this spec's earlier answer to soak plan
§1.5. That answer rested on the seed writing no stream row. With the row, the
log's minimum is the seed until a purge passes it, and a purge never passes the
backup, so a seed at or below the backup always means a minimum at or below it
too. Confirming on `seedWatermark <= backupWatermark` is therefore as safe as
confirming on `minWatermark`. It does not shorten the wait for a backup at or
above the seed, which is still real.

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
