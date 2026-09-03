# View-Syncer Hydration Budget Implementation Plan

Status: Proposed

## Summary

The view-syncer will apply a time budget to each query hydration pass.

The view-syncer will always hydrate active queries and internal queries. The budget cannot remove these queries.

After required hydration, the view-syncer will use the remaining budget for inactive queries. It will remove each unstarted inactive query after budget exhaustion.

The budget is soft. The view-syncer will finish a query that starts before budget exhaustion.

This behavior makes TTL retention best-effort. A TTL remains the maximum retention time, not a guaranteed minimum retention time.

## Goals

- Reduce connection latency when a CVR contains expensive inactive queries.
- Give active queries priority over inactive queries.
- Preserve all CVR, row-reference, and poke invariants.
- Use the normal query-expiration result for budget evictions.
- Keep query hydration interruptible only at query boundaries.
- Provide metrics for budget use, eviction, and cache churn.

## Non-Goals

- Do not interrupt hydration inside one query.
- Do not apply this budget to normal pipeline advancement.
- Do not remove an active client query because of the budget.
- Do not remove an internal query because of the budget.
- Do not change TTL-clock progression or natural TTL expiration.
- Do not predict query cost before hydration.

## Current Behavior

Pipeline initialization uses two hydration paths in `ViewSyncerService.run()`:

1. `#hydrateUnchangedQueries()` hydrates gotten queries at the current replica version.
2. `#syncQueryPipelineSet()` reconciles all missing, changed, errored, and expired queries.

`#hydrateUnchangedQueries()` excludes fully inactive queries. This path gives active queries an initial priority.

`#syncQueryPipelineSet()` does not make the same priority explicit. It builds `addQueries` from CVR iteration order.

`#addAndRemoveQueries()` calls `CVRQueryDrivenUpdater.trackQueries()` before hydration. The updater records every `addQueries` entry as executed at that point.

Therefore, an early generator exit is unsafe. The CVR will record unhydrated queries as executed unless the updater converts them to removals.

## Definitions

### Active query

A client query is active when at least one client state has no `inactivatedAt` value.

A custom query uses the same rule. An internal query is always active for this plan.

### Inactive query

A non-internal query is inactive when no client state is active. A query with no client state is also inactive.

For multiple client states, the effective expiration is the latest client expiration. This rule matches natural TTL expiration.

### Required hydration

Required hydration contains all active queries and all internal queries that need a pipeline.

### Optional hydration

Optional hydration contains unexpired inactive queries that need a pipeline. The budget controls only this group.

### Hydration pass

A hydration pass is one logical reconciliation of the query pipeline set.

During initialization, the pass includes `#hydrateUnchangedQueries()` and the next `#syncQueryPipelineSet()` call.

For later reconciliation calls, the pass starts in `#syncQueryPipelineSet()`.

## Required Invariants

The implementation must preserve these invariants:

- Every required query finishes hydration.
- An inactive query is either fully hydrated or fully removed.
- Every CVR query marked as gotten has a matching pipeline and reconciled row state.
- Removed query IDs do not remain in row reference counts.
- Each poke declares its final CVR version before it sends poke parts.
- A reconnect can request a budget-evicted query again.
- The TTL clock does not move because of a budget eviction.
- A query that is active for one client remains active for the client group.

## Configuration

Add a root Zero Cache configuration value named `viewSyncerHydrationBudgetMs`.

The generated interfaces are:

- CLI flag: `--view-syncer-hydration-budget-ms`
- Environment variable: `ZERO_VIEW_SYNCER_HYDRATION_BUDGET_MS`

Use a nonnegative integer value in milliseconds. A value of `0` disables budget eviction.

Keep the first code default at `0`. Select a nonzero production value during rollout.

Add the value near `yieldThresholdMs` in `packages/zero-cache/src/config/zero-config.ts`.

Update the configuration snapshots in `packages/zero-cache/src/config/zero-config.test.ts`.

## Budget Measurement

Use `performance.now()` through an injected monotonic clock. The injected clock gives deterministic tests.

Start the budget immediately before the first query transformation or hydration in a pass.

Count these intervals:

- Active custom-query transformation time.
- Active pipeline hydration time.
- Inactive custom-query transformation time that starts before exhaustion.
- Inactive pipeline hydration time.
- Row processing time during hydration.
- Event-loop yield time during the pass.

Do not count the wait for a replica version. Do not count the initial CVR load.

The final CVR flush, catchup, and `pokeEnd` occur after query selection. Their time cannot cause more query eviction.

The budget object will expose these operations:

```ts
class HydrationBudget {
  readonly limitMs: number;
  elapsedMs(): number;
  exhausted(): boolean;
}
```

The disabled form always returns `false` from `exhausted()`.

## Query Classification and Order

Add one shared query-classification helper. Use it in both hydration paths.

The helper will apply these rules:

1. Classify internal queries as required.
2. Classify queries with an active client state as required.
3. Classify all other non-internal queries as optional.

Do not depend on object insertion order for priority.

Process required queries before optional queries. Keep the present order inside the required group unless a protocol dependency requires another order.

Order optional queries by effective expiration from latest to earliest. This order retains the queries with the most TTL time.

Give a query with no client state the lowest priority. This query has no active client or TTL owner.

If the shared helper needs the effective expiration calculation, refactor `getInactiveQueries()`. Keep natural eviction ordered from earliest to latest.

## Hydration Algorithm

Use this algorithm for each hydration pass:

```text
create one hydration budget

classify all queries
remove naturally expired queries from the candidate set

transform and hydrate every required query

if the budget is exhausted:
  remove every unstarted optional query
else:
  order optional queries by descending effective expiration
  for each optional query:
    if the budget is exhausted:
      remove this query and all remaining optional queries
      stop optional hydration
    transform and hydrate the query

reconcile rows for hydrated and removed queries
flush one CVR update
finish one poke
```

Evaluate the budget before each optional query. Evaluate it again after each optional query finishes.

Retain the optional query that causes the elapsed time to cross the limit. Remove only queries that did not start.

If active hydration crosses the limit, remove all optional queries without hydration.

If all hydration finishes within the limit, retain all unexpired inactive queries.

## Custom Queries

Partition custom queries before remote transformation. Transform required custom queries first.

If the budget is exhausted after required hydration, do not transform optional custom queries. Remove those queries through the budget-eviction path.

For the first implementation, transform remaining optional custom queries in one batch. Reevaluate the budget after that batch.

If the batch crosses the limit, remove all optional queries that did not start hydration. A later change can add smaller transformation batches.

Never skip authentication or authorization transformation for a query that remains hydrated.

Keep the current error behavior for active custom queries. An active transformation failure still reports the existing protocol error.

An inactive query with no gotten state provides no warm data. Remove this query before optional hydration.

## CVR Updater Changes

Keep one `CVRQueryDrivenUpdater`, one CVR flush, and one poke for the complete reconciliation.

Continue to call `trackQueries()` before row hydration. Include required and optional hydration candidates in its executed set.

Add an updater operation that converts tracked inactive queries into removals. A possible name is `removeTrackedQueries()`.

The new operation will perform these actions for each query:

1. Make sure that the query was tracked as executed in this update.
2. Make sure that the query is non-internal and inactive.
3. Remove the query from the mutable CVR snapshot.
4. Mark the stored query as deleted at the current CVR version.
5. Return a global query-delete patch.

Keep each converted query ID in `#removedOrExecutedQueryIDs`. This set removes its old row references during reconciliation.

Do not emit an initial query-put patch for an optional query that can later become a removal.

Only gotten inactive queries qualify for optional hydration. Route inactive queries without gotten state directly to removal.

The current same-hash version bump provides a final poke version before hydration. Add an explicit assertion for this requirement.

## `#addAndRemoveQueries()` Changes

Change the method input to contain separate required and optional query arrays.

Pass one shared `HydrationBudget` to the method. Preserve the budget from `#hydrateUnchangedQueries()` during initialization.

Build the row-change generator in this order:

1. Generate changes for every required query.
2. Generate changes for optional queries while the budget has time.
3. Record all unstarted optional query IDs.

Keep one row-change generator for hydrated queries. This design preserves cross-query row deduplication.

After `#processChanges()` finishes, convert the recorded IDs with `removeTrackedQueries()`.

Then perform these actions:

1. Send each global query-delete patch.
2. Remove any old pipeline for each evicted query.
3. Remove inspector data for each evicted query.
4. Remove each query from replacement tracking.
5. Call `deleteUnreferencedRows()`.
6. Flush the updater.
7. Catch up clients.
8. Send `pokeEnd`.

Pass only hydrated query IDs as hydration exclusions to `#catchupClients()`. Treat budget-evicted IDs like normal removed query IDs.

Count query-coverage metrics only for queries that start hydration.

## `#hydrateUnchangedQueries()` Changes

Accept the shared `HydrationBudget` as an argument.

Continue to hydrate only required unchanged queries in this method. Add each completed query time to existing hydration metrics.

Do not hydrate optional unchanged queries in this method. The reconciliation path controls optional order, budget use, and eviction.

Return the present drift set. The following reconciliation call will process each drifted active query as required.

## Natural Expiration

Keep the current `expired()` logic and expiration timer.

A natural expiration and a budget eviction produce the same client-visible result:

- The global gotten query receives a delete patch.
- The query pipeline is removed.
- Query row references are removed.
- Unreferenced rows receive delete patches.
- The query record becomes a tombstone in CVR storage.

Record a different internal reason for metrics and logs. Use `ttl` and `hydration-budget` as the reason values.

After a budget eviction, schedule the TTL timer from the updated CVR. Do not schedule timers for removed queries.

## Observability

Add these metrics:

- `sync.hydration_budget_exhaustions`: Number of hydration passes that exhaust the budget.
- `sync.hydration_budget_evictions`: Number of inactive queries removed by the budget.
- `sync.hydration_budget_elapsed`: Elapsed time when optional hydration stops.
- `sync.hydration_budget_overshoot`: Elapsed time beyond the configured budget.

Use milliseconds for elapsed-time metrics. Use `{query}` for query counters and `{pass}` for pass counters.

Add one structured information log for an exhausted pass. Include these fields:

- `clientGroupID`
- `hydrationBudgetMs`
- `hydrationElapsedMs`
- `activeHydratedQueries`
- `inactiveHydratedQueries`
- `inactiveEvictedQueries`
- `firstEvictedQueryHash`

Do not log a line for each evicted query. Large CVRs can make per-query logging expensive.

Use existing per-query hydration metrics only for queries that finish hydration.

## Tests

### Unit tests

Add tests for the shared classification helper:

- An internal query is required.
- A single-client active query is required.
- A multi-client query with one active client is required.
- A fully inactive query is optional.
- A query with no client state is optional.
- Optional order uses descending effective expiration.
- Multiple client states use the latest effective expiration.

Add tests for `HydrationBudget` with an injected clock:

- A disabled budget never exhausts.
- A budget does not exhaust before its limit.
- A budget exhausts at its limit.
- Elapsed time uses the monotonic clock.

Add tests for `CVRQueryDrivenUpdater.removeTrackedQueries()`:

- The operation removes a tracked inactive query.
- The operation returns a global query-delete patch.
- The operation retains the query ID in row reconciliation.
- The operation rejects an active query.
- The operation rejects an internal query.
- The operation rejects an untracked query.

### View-syncer integration tests

Add focused PostgreSQL tests for these cases:

1. Active hydration exceeds the budget. All active queries finish, and all inactive queries are removed.
2. The budget expires between inactive queries. Completed queries remain, and unstarted queries are removed.
3. All queries finish within the budget. No inactive query is removed.
4. The budget is zero. Existing behavior remains unchanged.
5. One client keeps a shared query active. The budget does not remove that query.
6. An internal query remains after budget exhaustion.
7. A removed query loses all row references. Shared rows remain when another query references them.
8. A client requests an evicted query again. The view-syncer hydrates it as an active query.
9. An inactive custom query is not transformed after active hydration exhausts the budget.
10. A custom query that remains hydrated receives the required authorization transformation.
11. Query drift keeps the active query in the required group.
12. Event-loop yields consume wall-clock budget but do not interrupt a query.

Use an injected monotonic clock and controlled pipeline work. Do not depend on real elapsed time.

Make sure that each test inspects downstream pokes and persisted CVR state.

## Implementation Sequence

1. Add the configuration value and configuration tests.
2. Add `HydrationBudget` and its unit tests.
3. Add the shared query-classification helper and its unit tests.
4. Add the updater conversion operation and CVR tests.
5. Partition required and optional queries before transformation.
6. Pass one budget through both initialization hydration paths.
7. Add the optional-query cutoff to `#addAndRemoveQueries()`.
8. Add eviction cleanup, row reconciliation, and poke handling.
9. Add metrics and structured logs.
10. Add view-syncer integration tests.
11. Run package tests with coverage.
12. Run formatting, linting, and type checks.

## Validation Commands

Run these commands from the repository root:

```bash
pnpm --filter zero-cache run test view-syncer --coverage
pnpm --filter zero-cache run format
pnpm --filter zero-cache run lint
pnpm --filter zero-cache run check-types
pnpm run format
pnpm run lint
pnpm run check-types
```

## Rollout

1. Deploy the code with `viewSyncerHydrationBudgetMs` set to `0`.
2. Record baseline hydration latency and inactive-query counts.
3. Enable a nonzero value on a canary deployment.
4. Compare active-query latency, eviction counts, and query re-request rates.
5. Increase the deployment scope after the canary remains stable.
6. If cache churn or query errors increase, set the value to `0`.

## Acceptance Criteria

The implementation is complete when all these statements are true:

- Active and internal queries always finish hydration.
- Inactive hydration stops at the first query boundary after budget exhaustion.
- The query that crosses the budget remains hydrated.
- Every unstarted inactive query follows the normal removal result.
- The CVR contains no gotten query without a matching hydrated pipeline.
- Row reference counts contain no budget-evicted query ID.
- A re-request of an evicted query succeeds.
- A disabled budget preserves current behavior.
- Metrics distinguish natural expiration from budget eviction.
- All required tests, formatting, linting, and type checks pass.

## Open Decision

Select the first nonzero production budget from observed hydration latency. The code and tests do not depend on that value.
