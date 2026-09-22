# Zero Query Fuzzer Expansion Design

## Purpose

Zero already has a coverage-driven fuzzer for the core ZQL IVM engine. It checks
that generated Zero queries hydrate and maintain the same results as the same
query evaluated against PostgreSQL.

The same correctness principle should apply at every layer of Zero:

```text
query through Zero == same query over PostgreSQL
```

For maintained queries, the invariant is:

```text
incrementally maintained Zero result == fresh PostgreSQL result
after the same committed writes
```

This design expands the fuzzer in concentric circles, starting with the current
IVM engine fuzzer and then adding the production layers that stand between
PostgreSQL and an application-visible result.

## Goals

- Reuse the current fuzzer's query generation, coverage strategy, shrinking, and
  regression corpus wherever possible.
- Test each Zero layer against the same PostgreSQL oracle.
- Keep failures reproducible by recording seeds, generated schemas, queries,
  writes, barriers, and observed results.
- Add layers incrementally so failures remain debuggable.
- Catch bugs in replication, storage, protocol translation, client
  materialization, and subscription lifecycle that the current IVM-only fuzzer
  cannot see.

## Non-Goals

- This is not a replacement for focused unit tests or integration tests.
- This is not initially a performance benchmark. The fuzzer may collect timing
  data to debug barriers, but its primary output is correctness.
- This is not one monolithic end-to-end fuzzer. Each layer should remain
  independently runnable.
- This does not require every outer layer to support the full random surface on
  day one.

## Current State

The current fuzzer lives under:

```text
packages/zql-integration-tests/src/chinook/
```

It already provides several useful building blocks:

- a deterministic mini Chinook fixture
- generated query skeletons and decorations
- pairwise coverage over query axes
- randomized sweep and scale lanes
- push histories for incremental maintenance checks
- PostgreSQL oracle comparison
- shrink-to-repro support
- committed regression replay

Today, the fuzzer primarily exercises the core query engine:

```text
PostgreSQL oracle
        ^
        |
generated query
        |
        v
IVM memory/sqlite views
```

That is the right innermost layer. The expansion should preserve it as the
fastest and most debuggable signal.

## Layered Model

The fuzzer should grow outward in layers. Each layer has the same high-level
shape:

1. Generate a schema, data fixture, query, and write sequence.
2. Apply the writes to PostgreSQL.
3. Wait until the layer under test has observed a declared consistency barrier.
4. Read the result through the layer under test.
5. Read the same query freshly from PostgreSQL.
6. Normalize both results.
7. Compare.
8. On divergence, write a replayable artifact.

### L0: IVM Engine

This is the existing fuzzer.

Path under test:

```text
generated rows and pushes -> IVM memory/sqlite views
```

Oracle:

```text
fresh PostgreSQL query through z2s
```

Purpose:

- Keep the fastest correctness signal.
- Preserve high-quality shrinking.
- Validate query semantics and incremental maintenance independent of
  replication, networking, and client state.

Expected cadence:

- Backbone on every PR.
- Larger sweeps in slower CI or nightly jobs.

### L1: Replicator + zero-cache Storage

This is the highest-value next layer.

Path under test:

```text
PostgreSQL writes
  -> logical replication
  -> zero-cache replica storage
  -> server-side query/view evaluation
```

Oracle:

```text
fresh PostgreSQL query after the same committed writes
```

Purpose:

- Catch WAL decoding and replication ordering bugs.
- Catch schema/name mapping mismatches.
- Catch type conversion bugs between PostgreSQL and the replica.
- Catch SQLite replica storage differences.
- Catch transaction boundary and delete/update/insert sequencing issues.
- Catch server-side query evaluation bugs in the context zero-cache actually
  runs.

The key new requirement is a reliable replication barrier. After a PostgreSQL
transaction commits, the harness must be able to wait until zero-cache has
applied at least that transaction before comparing results.

Useful barrier forms, in order of preference:

- source commit LSN observed and applied by zero-cache
- explicit high-water marker row written in the same transaction
- zero-cache debug or test hook reporting applied replication position
- polling a generated marker query through zero-cache until visible

The LSN-based barrier is preferred because it is precise and independent of the
generated query. Marker rows are simpler but can accidentally share query bugs
with the layer being tested.

### L2: zero-cache Protocol

This layer adds the server protocol without requiring a full browser or
application client.

Path under test:

```text
PostgreSQL writes
  -> logical replication
  -> zero-cache replica storage
  -> view-syncer
  -> Zero protocol messages
  -> synthetic protocol client result
```

Oracle:

```text
fresh PostgreSQL query after the same committed writes
```

Purpose:

- Catch protocol serialization bugs.
- Catch patch/diff construction bugs.
- Catch query registration and unsubscribe lifecycle issues.
- Catch initial hydrate versus subsequent diff mismatches.
- Catch reconnect and resume issues if enabled for a given run.

This layer needs two barriers:

- a replication barrier proving zero-cache has applied the PostgreSQL writes
- a client observation barrier proving the synthetic client has received all
  protocol changes for that applied position

The synthetic client should expose a materialized result set per subscribed
query. The fuzzer should compare that materialized result to PostgreSQL after
initial sync and after every generated write step.

### L3: zero-client

This layer adds the public client data path.

Path under test:

```text
PostgreSQL writes
  -> logical replication
  -> zero-cache
  -> Zero protocol
  -> zero-client
  -> application-visible query result
```

Oracle:

```text
fresh PostgreSQL query after the same committed writes
```

Purpose:

- Catch client-side materialization bugs.
- Catch subscription lifecycle bugs.
- Catch local cache application bugs.
- Catch client-side type conversion and normalization bugs.
- Catch reconnect, resume, and hydration edge cases.

This layer should start in a Node or headless test environment if possible. A
browser lane can come later for IndexedDB and real browser scheduling behavior.

### L4: Full Client Behavior

This is the broadest and slowest layer.

Path under test:

```text
PostgreSQL writes and client mutations
  -> server mutation handling
  -> PostgreSQL
  -> logical replication
  -> zero-cache
  -> Zero protocol
  -> zero-client
  -> application-visible query result
```

Oracle:

```text
fresh PostgreSQL query after committed server state
```

Purpose:

- Catch optimistic mutation and rebase bugs.
- Catch multi-client interaction bugs.
- Catch auth and permission-filter bugs.
- Catch disconnect, reconnect, and resume bugs under writes.
- Catch application-like workflows that combine reads and writes.

This should be a later layer. It has the largest state space and the weakest
shrinking story, so it should reuse minimized cases from inner layers whenever
possible.

## Shared Harness Concepts

### Generated Case

Every layer should consume the same logical case format:

```text
schema
initial data
query set
write sequence
normalization policy
seed and generator labels
```

Early implementations can continue using the Chinook schema and mini fixture.
Later implementations can add generated schemas as a separate axis.

### PostgreSQL Oracle

PostgreSQL remains the source of truth. For every comparison point, the harness
should evaluate the equivalent query freshly against PostgreSQL after the
expected writes have committed.

The oracle path should be as direct as possible:

```text
generated ZQL/AST -> SQL -> PostgreSQL result
```

The oracle should not pass through zero-cache, protocol code, or client code.

### Barriers

Outer-layer fuzzing is only meaningful if comparisons happen after the layer has
caught up to the intended PostgreSQL state.

The harness should model barriers explicitly:

```typescript
type Barrier = {
  readonly pgCommit: unknown;
  readonly replicaApplied?: unknown | undefined;
  readonly clientObserved?: unknown | undefined;
};
```

The exact representation can vary by layer, but every comparison should say
which barrier it waited for.

Required barriers:

- L0: none beyond the direct push step
- L1: PostgreSQL commit observed by zero-cache
- L2: PostgreSQL commit observed by zero-cache and protocol client
- L3: PostgreSQL commit observed by zero-client materialization
- L4: committed server state observed by every participating client

### Normalization

Some differences between PostgreSQL and Zero are not correctness bugs unless the
query requested semantics that make them observable. The fuzzer should normalize
or reject cases where comparison would be ambiguous.

Important rules:

- Require explicit ordering before comparing ordered result arrays.
- Treat unordered results as sets or multisets with stable row identity.
- Normalize timestamp precision.
- Normalize numeric representations where PostgreSQL and JavaScript differ.
- Normalize JSON object key order.
- Make NULL and undefined handling explicit.
- Avoid locale-sensitive collation unless it is the axis being tested.
- Preserve duplicate rows if the query semantics allow duplicates.
- Include permission filters in the PostgreSQL oracle when testing auth.

The normalization policy should be stored in the replay artifact so a failure
can be reproduced exactly.

### Replay Artifact

Every failing outer-layer case should produce a durable artifact. It should be
possible to replay the artifact without rerunning the original random search.

Suggested fields:

```json
{
  "layer": "zero-cache",
  "seed": 12648430,
  "label": "L1|album|filter:eq|push:edit",
  "schema": {},
  "initialData": {},
  "queries": [],
  "writes": [],
  "barriers": [],
  "normalization": {},
  "expectedPgSnapshots": [],
  "observedZeroSnapshots": [],
  "notes": "short human context"
}
```

Inner-layer regressions can stay close to the current compact AST format. Outer
layers need more operational data because process state, replication position,
and client observation points matter.

## Test Lanes

The fuzzer should keep separate lanes with different cost and blast radius.

### Backbone

Runs on every PR.

- deterministic mini fixture
- bounded query depth
- small write sequences
- L0 always
- L1 once stable
- L2 smoke once stable

Backbone should favor structure and coverage over randomness.

### Sweep

Runs in slower CI or manually.

- seeded randomized cases
- broader query decorations
- larger write sequences
- L0 and L1 by default
- L2 and L3 as budgets allow

Sweep should report all failures in a batch, then shrink or minimize within a
bounded budget.

### Scale

Runs nightly or manually.

- full Chinook fixture or larger generated fixtures
- more relationships and deeper queries
- larger write histories
- reconnect/resume axes for client layers

Scale is allowed to be slower and less shrink-friendly, but failures must still
produce replay artifacts.

### Regression Replay

Runs on every PR.

- committed minimized repros
- no randomness
- stable fixture
- layer-specific replay entry points

Regression replay is the permanent guardrail for bugs found by any lane.

## Query and Write Generation

The current query generator should remain the core. Outer layers should add
axes gradually:

- PostgreSQL type coverage: integers, floats, text, booleans, timestamps, JSON,
  nullable columns
- primary key shapes: single-column and compound keys
- relationship shapes: one-to-one, one-to-many, many-to-one
- write kinds: insert, update, delete
- transaction shapes: single-row and multi-row transactions
- update shapes: membership-preserving, membership-entering, membership-leaving,
  order-boundary-crossing
- subscription shapes: initial hydrate, maintained push, unsubscribe,
  resubscribe
- client shapes: one client, multiple clients, reconnect, resume

The generator should keep a cost model so outer layers do not accidentally
create cases that are too large to debug.

## Debugging Workflow

When a divergence is found:

1. Save the replay artifact.
2. Try to reproduce at the same layer.
3. If the failure is in an outer layer, attempt to project the case inward:
   - L3 failure -> replay through L2
   - L2 failure -> replay through L1
   - L1 failure -> replay through L0 if it can be represented as direct pushes
4. Shrink the innermost layer that still reproduces.
5. Commit the smallest useful regression artifact.

This keeps outer-layer fuzzing from turning into opaque end-to-end failures.

## Implementation Plan

### Phase 1: Shared Case Format

- Extract or define a serializable fuzzer case model.
- Keep compatibility with the current Chinook fuzzer.
- Add replay plumbing that can target different layers.
- Record normalization policy with each case.

### Phase 2: L1 zero-cache Harness

- Start PostgreSQL and zero-cache in a test-owned lifecycle.
- Apply the mini schema and seed data.
- Register or execute generated queries through the server-side zero-cache path.
- Apply generated writes to PostgreSQL.
- Wait for a replication barrier.
- Compare zero-cache results to PostgreSQL snapshots.
- Save replay artifacts on divergence.

This phase should be the first expansion target because it covers the largest
new production surface while remaining easier to debug than a client test.

### Phase 3: L2 Synthetic Protocol Client

- Add a protocol-level synthetic client.
- Subscribe to generated queries.
- Materialize protocol messages into result sets.
- Compare after initial hydrate and after each write barrier.
- Add reconnect/resubscribe only after the steady-state path is stable.

### Phase 4: L3 zero-client Harness

- Run generated subscriptions through `zero-client`.
- Compare application-visible results to PostgreSQL.
- Add browser-backed storage as a separate lane after Node or headless support
  is stable.

### Phase 5: L4 Client Mutations and Multi-Client

- Add generated client mutations.
- Compare only after server commit semantics are clear.
- Add multiple clients and reconnect/resume axes.
- Add auth and permission-filter axes with oracle-side permission evaluation.

## Risks

- Barrier bugs can look like query bugs. The harness should report barrier state
  separately from result divergence.
- PostgreSQL and JavaScript type differences can create false positives.
  Normalization must be explicit and conservative.
- Outer-layer failures are harder to shrink. Projecting failures inward should
  be part of the workflow, not an afterthought.
- Process lifecycle can make tests flaky. Each lane needs strict startup,
  readiness, timeout, and cleanup handling.
- A generated query without explicit order can produce unstable row order.
  Comparison must respect query semantics rather than array incidental order.

## Open Questions

- What is the best zero-cache test hook for an applied replication LSN?
- Should L1 execute queries through an internal server API, the same
  view-syncer path used by protocol clients, or both as separate lanes?
- How much of the current Chinook fuzzer case model can be serialized without
  loss?
- Where should outer-layer regression artifacts live?
- Which subset of generated PostgreSQL types should be enabled first?
- Should generated schemas be a near-term axis, or should the first outer-layer
  harness stay on deterministic Chinook fixtures?

## Recommendation

Build the expansion in this order:

1. Preserve and continue improving the current L0 IVM fuzzer.
2. Add L1 for replicator plus zero-cache with an explicit replication barrier.
3. Add L2 with a synthetic protocol client.
4. Add L3 with `zero-client`.
5. Add L4 for multi-client, optimistic mutations, reconnect, resume, and auth.

This gives the best correctness coverage per debugging cost. The shared
PostgreSQL oracle remains the center of the design, while each outer layer adds
one production boundary at a time.
