--------------------------- MODULE ChangeLogCoverage ---------------------------
(***************************************************************************)
(* The SQLite change log's coverage protocol: what a restoring view-syncer *)
(* is promised by a snapshot reservation, what the purger is allowed to    *)
(* delete, and how the reservation lease added by                          *)
(* `snapshot-reservations.ts` interacts with both.                         *)
(*                                                                        *)
(* Models the *post-retirement* topology: the PG change log is gone, so    *)
(* the SQLite log is the only thing a restoring follower can catch up      *)
(* from. With PG still enabled every hold below becomes a demotion and     *)
(* every catchup succeeds, so that configuration has nothing to check.     *)
(*                                                                        *)
(* A watermark has a major and a minor. A major is an upstream commit. A   *)
(* minor is a backfill transaction that the replication-manager mints      *)
(* under the last one, and is local to the replica that applies it, so a   *)
(* follower that follows backfill runs (protocol v7) subscribes at the     *)
(* major of its state version rather than at the state version itself.   *)
(*                                                                        *)
(* Invariant numbers refer to plans/sqlite-change-log-plan.md section 4.   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Tasks,               \* view-syncer task IDs
    MaxMajor,            \* bound on upstream commits, for finiteness
    MaxMinor,            \* bound on backfill transactions under one commit
    ConfirmUsesSeed,     \* soak plan 1.5: confirm on seedWatermark, not minWatermark
    PauseWaitsForBatch,  \* whether pause() waits out an in-flight purge batch
    RevalidateOnConfirm, \* whether confirmation re-reads the log's bounds
    InvalidateOnReseed,  \* whether a reseed takes back open reservations
    LeaseCoversRestore,  \* whether the cap outlasts a restore that is moving
    TruncateBelowBackup, \* whether a reconcile may truncate below the backup
    SubscribeAtMajor,    \* whether a follower subscribes at its major (v7)
    SeedStandsIn         \* whether the seed serves a subscriber at its major

ASSUME MaxMajor \in Nat /\ MaxMajor > 0
ASSUME MaxMinor \in Nat
ASSUME ConfirmUsesSeed \in BOOLEAN
ASSUME PauseWaitsForBatch \in BOOLEAN
ASSUME RevalidateOnConfirm \in BOOLEAN
ASSUME InvalidateOnReseed \in BOOLEAN
ASSUME LeaseCoversRestore \in BOOLEAN
ASSUME TruncateBelowBackup \in BOOLEAN
ASSUME SubscribeAtMajor \in BOOLEAN
ASSUME SeedStandsIn \in BOOLEAN

\* A watermark is major * K + minor, so integer order is watermark order.
K == MaxMinor + 1
MajorOf(w) == w \div K
MinorOf(w) == w % K
\* The watermark of w's major: `majorVersionOf` in `types/state-version.ts`.
Major(w) == MajorOf(w) * K

MaxWatermark == MaxMajor * K + MaxMinor
Watermarks == 0..MaxWatermark
NoBatch  == MaxWatermark + 1   \* no purge batch in flight
NoMin    == MaxWatermark + 2   \* the log advertised no minimum (it is empty)

MinOfSet(S) == CHOOSE x \in S : \A y \in S : x =< y

VARIABLES
    head,     \* the newest committed watermark
    hist,     \* every watermark committed at or below head
    log,      \* the watermarks whose transactions the log still holds
    seedWm,   \* the watermark the log was (re)seeded at
    backup,   \* the confirmed durable litestream watermark
    batch,    \* the purge floor of an in-flight batch, or NoBatch
    paused,   \* tasks holding a purge pause (an open reservation)
    task,     \* Tasks -> follower state
    resv      \* Tasks -> reservation state

vars == <<head, hist, log, seedWm, backup, batch, paused, task, resv>>

(***************************************************************************)
(* Coverage.                                                               *)
(*                                                                         *)
(* A follower at watermark w needs every transaction after w, and needs w  *)
(* itself present as the catchup boundary -- `spansInterval` requires a    *)
(* `commit` row at exactly `fromWatermark`                                 *)
(* (sqlite-change-log-reader.ts:50). Purging is a prefix delete, so a      *)
(* boundary that is still in the log has everything after it too.          *)
(*                                                                         *)
(* A seed is a transaction like any other: `seedChangeLogStream` writes a  *)
(* `begin` and a `commit` at the resume watermark, so a freshly seeded log *)
(* serves a subscriber at exactly that watermark.                          *)
(***************************************************************************)
LogEmpty == log = {}
LogMin == IF LogEmpty THEN NoMin ELSE MinOfSet(log)

Spans(w) == w \in log

\* Where a follower restored to w subscribes (`incremental-sync.ts`).
SubscribeAt(w) == IF SubscribeAtMajor THEN Major(w) ELSE w

\* `seedCatchupStart`: a follower subscribing at the seed's major is caught up
\* from the seed, while the seed is still the log's first transaction. What it
\* is not sent is the backfill transactions between the two, which a follower
\* that follows runs does not need replayed.
SeedCovers(s) ==
    /\ SeedStandsIn
    /\ SubscribeAtMajor
    /\ ~LogEmpty
    /\ seedWm = LogMin
    /\ s < seedWm
    /\ Major(seedWm) = s

\* What catchup actually does, and so what any promise has to match.
CanCatchUp(w) == Spans(SubscribeAt(w)) \/ SeedCovers(SubscribeAt(w))

\* The minimum a pinned route advertises to `#confirmReservations`.
AdvertisedMin == IF ConfirmUsesSeed THEN seedWm ELSE LogMin

TaskStates == {"synced", "down", "reserving", "restoring", "subscribing"}
ResvStates == {"none", "open", "pinned", "confirmed"}

NoResv == [st |-> "none", wm |-> 0, pinMin |-> 0]

Synced(w) == [st |-> "synced", at |-> w, lost |-> FALSE, stalled |-> FALSE]
Down      == [st |-> "down", at |-> 0, lost |-> FALSE, stalled |-> FALSE]

TypeOK ==
    /\ head \in Watermarks
    /\ hist \subseteq Watermarks
    /\ head \in hist
    /\ log \subseteq hist
    /\ seedWm \in Watermarks
    /\ backup \in Watermarks
    /\ batch \in Watermarks \cup {NoBatch}
    /\ paused \subseteq Tasks
    /\ task \in [Tasks -> [st: TaskStates, at: Watermarks,
                          lost: BOOLEAN, stalled: BOOLEAN]]
    /\ resv \in [Tasks -> [st: ResvStates,
                           wm: Watermarks,
                           pinMin: Watermarks \cup {NoMin}]]

Init ==
    /\ head = 0
    /\ hist = {0}
    /\ log = {0}             \* the seed's own transaction
    /\ seedWm = 0
    /\ backup = 0
    /\ batch = NoBatch
    /\ paused = {}
    /\ task = [t \in Tasks |-> Synced(0)]
    /\ resv = [t \in Tasks |-> NoResv]

-----------------------------------------------------------------------------
(* The change stream and the backup monitor. *)

\* An upstream commit: the next major.
Commit ==
    /\ MajorOf(head) < MaxMajor
    /\ head' = Major(head) + K
    /\ hist' = hist \cup {head'}
    /\ log' = log \cup {head'}
    /\ UNCHANGED <<seedWm, backup, batch, paused, task, resv>>

\* A backfill transaction: the next minor under the last upstream commit
\* (`beginTxFor` in `backfill-manager.ts`).
BackfillCommit ==
    /\ MinorOf(head) < MaxMinor
    /\ head' = head + 1
    /\ hist' = hist \cup {head'}
    /\ log' = log \cup {head'}
    /\ UNCHANGED <<seedWm, backup, batch, paused, task, resv>>

BackupAdvance ==
    /\ \E w \in hist : w > backup /\ backup' = w
    /\ UNCHANGED <<head, hist, log, seedWm, batch, paused, task, resv>>

\* The routine outcome of reconciliation. Invariant 1 puts the log's commit
\* before anything that can advance the resume watermark, so after a dropped
\* connection the log normally holds transactions above the point upstream
\* will resume from, and they are deleted before the stream restarts
\* (invariant 18: truncate above the resume watermark, never dedupe).
\*
\* The resume watermark is at or above the confirmed backup, because a backup
\* covers only what the replica has applied and the replica has only what was
\* forwarded, which invariant 2 puts after the log's commit. Setting
\* TruncateBelowBackup drops that assumption, to see whether it is load
\* bearing.
TruncateFloor == IF TruncateBelowBackup THEN LogMin ELSE backup

Truncate ==
    /\ ~LogEmpty
    /\ \E r \in log :
          /\ r >= TruncateFloor
          /\ r < head
          /\ head' = r
          /\ hist' = {w \in hist : w =< r}
          /\ log' = {w \in log : w =< r}
    /\ UNCHANGED <<seedWm, backup, batch, paused, task, resv>>

\* Reconciliation runs on every change-stream connection
\* (change-streamer-service.ts:765), so a reseed can land under a reservation
\* that is already open, already pinned, or already confirmed.
ReseedEffect ==
    IF InvalidateOnReseed
    THEN /\ resv' = [t \in Tasks |-> NoResv]
         /\ paused' = {}
         /\ task' = [t \in Tasks |->
               IF resv[t].st = "none"
               THEN task[t]
               ELSE IF task[t].st \in {"restoring", "subscribing"}
               THEN [st |-> task[t].st, at |-> task[t].at,
                     lost |-> TRUE, stalled |-> task[t].stalled]
               ELSE Down]
    ELSE UNCHANGED <<paused, task, resv>>

\* The log is wiped and reseeded at the replica's state version, which can be
\* a backfill minor: one of the five ReseedReasons (created / schema-mismatch /
\* identity-mismatch / gap / oversized-truncate). It keeps no history but the
\* seed's own transaction.
Reseed ==
    /\ ~(seedWm = head /\ log = {head})
    /\ seedWm' = head
    /\ log' = {head}
    /\ ReseedEffect
    /\ UNCHANGED <<head, hist, backup, batch>>

-----------------------------------------------------------------------------
(* Followers. *)

Ack(t) ==
    /\ task[t].st = "synced"
    /\ \E w \in hist :
          /\ w > task[t].at
          /\ task' = [task EXCEPT ![t] = Synced(w)]
    /\ UNCHANGED <<head, hist, log, seedWm, backup, batch, paused, resv>>

\* The view-syncer loses its replica volume (soak case C3).
Crash(t) ==
    /\ task[t].st = "synced"
    /\ task' = [task EXCEPT ![t] = Down]
    /\ UNCHANGED <<head, hist, log, seedWm, backup, batch, paused, resv>>

\* GET /snapshot: open the reservation, then await purgeScheduler.pause().
OpenReservation(t) ==
    /\ task[t].st = "down"
    /\ resv[t].st = "none"
    /\ task' = [task EXCEPT ![t] =
          [st |-> "reserving", at |-> 0, lost |-> FALSE, stalled |-> FALSE]]
    /\ resv' = [resv EXCEPT ![t] = [st |-> "open", wm |-> 0, pinMin |-> 0]]
    /\ paused' = paused \cup {t}
    /\ UNCHANGED <<head, hist, log, seedWm, backup, batch>>

\* The pause has settled, so pin the read source and capture its bounds.
\* PauseWaitsForBatch is the `#lock.withLock` in `pause()`: without it the
\* bounds can be captured while a dispatched batch is still about to apply.
PinReservation(t) ==
    /\ resv[t].st = "open"
    /\ (PauseWaitsForBatch => batch = NoBatch)
    /\ resv' = [resv EXCEPT ![t] = [st |-> "pinned", wm |-> 0, pinMin |-> AdvertisedMin]]
    /\ UNCHANGED <<head, hist, log, seedWm, backup, batch, paused, task>>

\* #confirmReservations. Not enabled when the advertised minimum is above the
\* backup: with no PG log to demote to, the reservation is held pending until
\* a later backup moves the durable watermark into the log's covered range.
ConfirmReservation(t) ==
    /\ resv[t].st = "pinned"
    \* `peek()` returns the route stored by `pin()`, coverage and all, so
    \* today's check reads a minimum captured before the pause even settled.
    \* RevalidateOnConfirm re-reads it instead.
    /\ (IF RevalidateOnConfirm THEN AdvertisedMin ELSE resv[t].pinMin) =< backup
    /\ resv' = [resv EXCEPT ![t] = [st |-> "confirmed", wm |-> backup, pinMin |-> 0]]
    /\ task' = [task EXCEPT ![t] =
          [st |-> "restoring", at |-> backup, lost |-> FALSE, stalled |-> FALSE]]
    /\ UNCHANGED <<head, hist, log, seedWm, backup, batch, paused>>

\* litestream restore finished.
FinishRestore(t) ==
    /\ task[t].st = "restoring"
    /\ ~task[t].stalled
    /\ task' = [task EXCEPT ![t] = [st |-> "subscribing", at |-> task[t].at,
                                    lost |-> task[t].lost, stalled |-> FALSE]]
    /\ UNCHANGED <<head, hist, log, seedWm, backup, batch, paused, resv>>

\* The restore this cap exists for: a client that is alive, holds its socket,
\* and never finishes. Its liveness pings keep the reservation open, so only
\* the cap can end it.
StallRestore(t) ==
    /\ task[t].st = "restoring"
    /\ ~task[t].stalled
    /\ task' = [task EXCEPT ![t] = [st |-> "restoring", at |-> task[t].at,
                                    lost |-> task[t].lost, stalled |-> TRUE]]
    /\ UNCHANGED <<head, hist, log, seedWm, backup, batch, paused, resv>>

\* The follower subscribes at the watermark it restored to, or at its major
\* (`SubscribeAt`). #subscribe closes the task's reservation either way.
Subscribe(t) ==
    /\ task[t].st = "subscribing"
    /\ resv' = [resv EXCEPT ![t] = NoResv]
    /\ paused' = paused \ {t}
    /\ task' = [task EXCEPT ![t] =
          IF CanCatchUp(task[t].at) THEN Synced(task[t].at)
                                    ELSE Down]   \* WatermarkTooOld
    /\ UNCHANGED <<head, hist, log, seedWm, backup, batch>>

\* The lease added by `feat(zero-cache): cap how long a snapshot reservation
\* may hold the change log`. The follower is not told; it keeps restoring with
\* bounds that are no longer guaranteed.
ExpireReservation(t) ==
    /\ resv[t].st # "none"
    \* A cap that outlasts any moving restore can only fire on a reservation
    \* that is still waiting for its bounds, or on a wedged restore. Setting
    \* LeaseCoversRestore to FALSE models a cap shorter than a real restore.
    /\ \/ ~LeaseCoversRestore
       \/ resv[t].st # "confirmed"
       \/ task[t].stalled
    /\ resv' = [resv EXCEPT ![t] = NoResv]
    /\ paused' = paused \ {t}
    /\ task' = [task EXCEPT ![t] =
          IF task[t].st \in {"restoring", "subscribing"}
          THEN [st |-> task[t].st, at |-> task[t].at,
                lost |-> TRUE, stalled |-> task[t].stalled]
          ELSE Down]
    /\ UNCHANGED <<head, hist, log, seedWm, backup, batch>>

-----------------------------------------------------------------------------
(* The purger. *)

SyncedAcks == {task[t].at : t \in {t \in Tasks : task[t].st = "synced"}}
HeldWatermarks == {resv[t].wm : t \in {t \in Tasks : resv[t].st = "confirmed"}}

Floor == MinOfSet({backup} \cup SyncedAcks \cup HeldWatermarks)

\* #getCleanupFloor purges to the major of the floor: a follower resumes at
\* the major, so the transaction there has to outlive it.
PurgeFloor == Major(Floor)

PurgeDispatch ==
    /\ paused = {}
    /\ batch = NoBatch
    /\ ~LogEmpty
    /\ PurgeFloor > LogMin
    /\ batch' = PurgeFloor
    /\ UNCHANGED <<head, hist, log, seedWm, backup, paused, task, resv>>

PurgeApply ==
    /\ batch # NoBatch
    /\ log' = {w \in log : w >= batch}
    /\ batch' = NoBatch
    /\ UNCHANGED <<head, hist, seedWm, backup, paused, task, resv>>

-----------------------------------------------------------------------------

TaskStep(t) ==
    \/ Ack(t)
    \/ Crash(t)
    \/ OpenReservation(t)
    \/ PinReservation(t)
    \/ ConfirmReservation(t)
    \/ FinishRestore(t)
    \/ StallRestore(t)
    \/ Subscribe(t)

Base == Commit \/ BackfillCommit \/ BackupAdvance \/ Truncate
           \/ PurgeDispatch \/ PurgeApply
           \/ \E t \in Tasks : TaskStep(t)

Expire == \E t \in Tasks : ExpireReservation(t)

Next         == Base \/ Reseed \/ Expire
NextNoExpire == Base \/ Reseed
NextNoReseed == Base \/ Expire

-----------------------------------------------------------------------------
(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

\* Invariant 14, in the mechanical form the code implements: the floor a
\* purge batch runs with is min'd with the confirmed backup watermark, so no
\* transaction at or above a durable backup is ever deleted. The reservation
\* lease does NOT weaken this: reserved watermarks only ever lower the floor,
\* so taking one back lets the floor rise to the backup and no further.
Inv_PurgeNeverPassesBackup ==
    batch # NoBatch => batch =< backup

\* A confirmed, unexpired reservation still covers what it promised.
Inv_LiveReservationCovered ==
    \A t \in Tasks : resv[t].st = "confirmed" => CanCatchUp(resv[t].wm)

\* The headline property: a follower restoring under a reservation that has
\* not been taken back can always catch up from the watermark it was given.
\* `lost` marks the followers whose lease expired -- the deliberate sacrifice
\* the cap makes, and the only ones this exempts.
Inv_PromiseKept ==
    \A t \in Tasks :
        (task[t].st \in {"restoring", "subscribing"} /\ ~task[t].lost)
            => CanCatchUp(task[t].at)

Safety ==
    /\ TypeOK
    /\ Inv_PurgeNeverPassesBackup
    /\ Inv_LiveReservationCovered
    /\ Inv_PromiseKept

-----------------------------------------------------------------------------
(***************************************************************************)
(* Liveness.                                                               *)
(*                                                                        *)
(* A restore that is not wedged is fair; StallRestore is the only way to   *)
(* hang, and nothing forces it. That is the wedged client the cap exists   *)
(* for -- alive, holding its socket, never finishing.                     *)
(***************************************************************************)

\* No reservation pins the log forever.
Prop_NoPermanentPin ==
    \A t \in Tasks : (resv[t].st = "confirmed") ~> (resv[t].st = "none")

\* A restore that is not wedged always ends up serving again.
Prop_RestoreCompletes ==
    \A t \in Tasks :
        (task[t].st = "restoring") ~> (task[t].st = "synced" \/ task[t].stalled)

\* Progress of the change stream and the backup monitor is assumed; progress
\* of the follower's own restore is not.
EnvFairness ==
    /\ WF_vars(Commit)
    /\ WF_vars(BackupAdvance)
    /\ \A t \in Tasks : WF_vars(Ack(t))

SystemFairness ==
    /\ WF_vars(PurgeDispatch)
    /\ WF_vars(PurgeApply)
    /\ \A t \in Tasks :
          /\ WF_vars(OpenReservation(t))
          /\ WF_vars(PinReservation(t))
          /\ WF_vars(ConfirmReservation(t))
          /\ WF_vars(Subscribe(t))
          \* A restore that is not wedged makes progress; StallRestore is the
          \* only way to hang, and it is never forced.
          /\ WF_vars(FinishRestore(t))

Spec == Init /\ [][Next]_vars

\* The lease is armed: a stalled restore is eventually taken back.
SpecWithLease ==
    /\ Spec /\ EnvFairness /\ SystemFairness
    /\ \A t \in Tasks : WF_vars(ExpireReservation(t))

\* No lease: nothing ever takes a reservation back.
SpecNoLease ==
    /\ Init /\ [][NextNoExpire]_vars
    /\ EnvFairness /\ SystemFairness

\* A lease longer than any real restore (Expire is left unfair), a restore
\* that does finish, and no reseed storm. The tuning claim behind
\* DEFAULT_MAX_RESERVATION_AGE_MS: under it, followers still come back.
SpecHealthyRestore ==
    /\ Init /\ [][NextNoReseed]_vars
    /\ EnvFairness /\ SystemFairness

=============================================================================
