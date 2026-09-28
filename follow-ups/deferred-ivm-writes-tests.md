# Deferred IVM writes: missing tests

Written 2026-09-23 for `mlaw/defer-ivm-writes` at `23b0b1f07`
(`feat(zero-cache): budget deferred IVM writes per worker`).

That commit made the write mode a per-advancement choice. Each advancement
reserves its change count from a `DeferredWritesBudget` shared by the syncer
process. If the reservation fits, the changes are held in memory. If it does
not, the advancement is written through to the `prev` snapshot.

## What is covered

- Unit tests in `pipeline-driver.test.ts`, `describe('deferred writes budget')`:
  - One driver switching modes between advancements, compared against
    drivers that always write through or always defer.
  - The row bound (held rows never exceed the reserved changes).
  - Release of the reservation when an advancement is abandoned and when the
    byte backstop resets.
- The `pipeline-driver` and view-syncer Postgres suites, run once per mode.
  With `ZERO_TEST_DEFER_IVM_WRITES=1` every advancement fits the test budget
  (1 GiB, about 1M rows), so these runs never write through.
- `table-source.test.ts`: switching modes with changes pending throws, and
  switching after `setDB` works.

## The gap

No test runs two client groups in different modes at the same version while
they share a `SnapshotRowCache`.

- A deferring group reads with `'none'` and fills the cache with
  `p:<prev>` entries.
- A group that fell back reads with `'divergent'` from a `prev` it writes to.

The argument that mixing them is safe: a write-through group skips the cache
for tables with more than one unique key, and for single-unique-key tables no
earlier change in the advancement can affect its read. This is the setting of
#6647. Items 3 and 4 below now cover it with hand-written cases; the fuzzer
(item 1) is still needed for broad coverage.

## Tests to add, in order of value

### 1. Fuzzer groups lane (ws3, `mlaw/more-fuzz`) -- done

Done 2026-09-23 after rebasing onto `mlaw/more-fuzz`. The lane's worker
shares a `SeededDeferredWritesBudget`
(`chinook-zero-cache-fuzzer-groups.test.helpers.ts`): for each advancement,
a generator separate from the lane's schedule picks held in memory, written
through, or held and then switched to write-through after 1-3 changes. The
real budget still does the accounting, and the test checks it is empty once
every view-syncer has stopped. Seed 1 ran 103 held, 108 written through and
27 switched. It catches both switch bugs from the unit tests: skipping the
`'divergent'` switch failed 2 of 6 seeds ("Row not found", or a divergence on
`customers-by-email`), and inserting before deleting failed 3 of 6 (UNIQUE
on the email swap). That is at `ZERO_FUZZ_BUDGET=1`; the nightly runs 4.

The lane no longer runs with every advancement written through (about 45%
of its advancements still are). `startSyncWorker` now also takes
`ZERO_TEST_DEFER_IVM_WRITES` for the other zero-cache fuzzer lanes: all six
files pass with it unset, `1` and `mixed` (about 33 s each). The nightly does
not set it.

### 2. Row-bound invariant under the fuzzer -- done

Done 2026-09-23. After each change, the driver compares the rows it holds
with its reservation. If they exceed it, it records
`DeferredWritesBudget.recordRowOverrun()`, logs an error and writes through
the rest (no assert). The groups lane expects no overruns after every
barrier.

Writing it turned up a real gap: when the change log keys a table by a
unique key other than the client schema's primary key (e.g. upstream PK
`name`, Zero PK `id`), an entry that changes the primary key is a remove
plus an add, so 2 rows for 1 entry. The reservation now counts such tables'
entries twice; `changesByTable()` reads the key columns from the entries'
JSON row keys. The chinook lanes cannot reach this case (their Zero and
upstream keys match). To exercise it, a lane could give `customer`
`REPLICA IDENTITY USING INDEX customer_email_key`, but only a Zero primary-key
change with a stable upstream key breaks the old bound, and the chinook
foreign keys make that awkward.

### 3. A mixed mode for the existing suites (this branch) -- done

Done 2026-09-23: `deferred-writes-test-util.ts` reads the variable (and
throws on an unknown value). The projects are `vitest.config.mixed-ivm.ts`
(`pipeline-driver.test.ts`) and `vitest.config.pg-18-mixed-ivm.ts` (the
view-syncer Postgres suites, about 20 s, run on pull requests). In one run,
50 advancements with changes were deferred and 14 written through.

Add `ZERO_TEST_DEFER_IVM_WRITES=mixed`, which gives each driver a test budget
that alternates between fitting and not fitting. This runs every
`pipeline-driver` test and every view-syncer Postgres test through a mode
switch, including the reset and timeout paths. Only the test helpers in
`pipeline-driver.test.ts` and `view-syncer-test-util.ts` change.

### 4. Two unit tests (this branch) -- done

Done 2026-09-23 in `pipeline-driver.test.ts`,
`describe('deferred writes budget')`. The row-cache test fails, in the case
where the deferring group advances first, if a write-through group reads the
unwritten-`prev` cache entries.

- **Mixed modes, shared row cache.** Two groups share a row cache; one
  defers and one writes through; the transaction swaps a unique key. Run it
  in both orders, because the group that advances first fills the cache.
- **Competing reservations.** Group A is partway through an advancement and
  holds the budget, so group B falls back. After A finishes, a third
  advancement fits again. In production, groups interleave this way because
  they yield mid-advancement.

## Not worth testing

- The wiring in `server/syncer.ts` (5 lines).
- The `ivm.deferred-writes-fallbacks` counter.

## Other follow-ups from the same change

- Sizing (2026-09-23): the budget is `deferIvmWritesHeapProportion`
  (default 0.25) of the heap limit, converted to rows at an assumed 1 KiB per
  row. Groups reserve only the change-log entries of the tables they read
  (`SnapshotDiff.changesByTable()`, a `GROUP BY` that runs only when the flag
  is on). A version that learned bytes per row for each table and grew
  reservations mid-advancement was built and dropped as too much machinery
  (its diff is `follow-ups/deferred-ivm-byte-reservation.diff`, without its
  new unit test file).
- Bytes (2026-09-23): each advancement adds the growth in its estimated bytes
  to a worker-wide total after each change and removes it when it ends. This
  covers fan-out: when many groups hold the same transaction of wide rows,
  the total sees all of the copies. The total lags each group by at most the
  change it is partway through.
- Over the byte budget (2026-09-23): the group that takes the total past the
  budget no longer resets. It writes the rows it holds into its `prev`
  snapshot (`TableSource.writePendingChanges()`: delete every touched key,
  then insert the live rows, yielding as it goes), switches the diff to
  `'divergent'`, releases its reservation, and writes through the rest of the
  advancement. The `ivm-delta-overflow` reset reason is gone.
  `ivm.deferred-writes-fallbacks` has a `stage` attribute (`start` or
  `partway`). Mixed mode now cycles three ways, including this switch after
  the first change; in one run the two mixed suites switched 11 times, a few
  of them with changes still to go.
- The switch needs the fuzzer groups lane (item 1): a switching group sharing
  a row cache with deferring and write-through groups, over many shapes. The
  unit tests cover one shape (a unique-key displacement) in both orders; the
  wrong diff mode there fails with "Row not found".
- The switch writes the held rows in one pass per table. It yields, and the
  advancement time limit still applies, but it is about as many writes as
  writing through from the start would have made, done at once.
- The byte estimates count text values once per group. Text read through a
  row-cache hit is shared across groups (`fromSQLiteTypes` passes strings
  through), so the estimates overcount on the safe side.
- Done 2026-09-23: an abandoned advancement (a reset, an error, or a
  generator `return`) drops what its sources hold in `#advance`'s `finally`
  (`TableSource.discardPendingChanges()`), when it releases the budget, rather
  than when the view-syncer calls `reset()` after `await pokers?.cancel()`.
- Done 2026-09-23: gauges `zero.sync.ivm.deferred-writes-reserved-rows` and
  `zero.sync.ivm.deferred-writes-held-bytes` per worker, and
  `ivm.deferred-writes-fallbacks` has a `reason` (`bytes` or `row-overrun`)
  for `stage: partway`.
- The defaults (a quarter of the heap, 1 KiB per row) are guesses. The next
  A/B run should include many groups on one table with wide rows, and watch
  the two gauges and the fallback counter by stage.
- The write-through fallback goes away with 004 step 2 (one snapshot pair per
  worker). Holding one copy of the delta per worker without spilling has to
  be acceptable by then.
