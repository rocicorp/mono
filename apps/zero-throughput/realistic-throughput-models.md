# Realistic Throughput Models

This note captures the intended next step for the `zero-throughput` benchmark:
keep the existing harness and add alternate workload models that preserve the
same scenarios while avoiding the current "every write hits every active query"
shape.

## Goal

The current profiles are useful pathological hot-partition tests. They should
remain available. We also want realistic variants where:

- clients do not all subscribe to the same owner/category/org/bucket;
- writes are distributed across active and inactive partitions;
- some writes go by silently for a given client group;
- the same benchmark harness, metrics, result format, process management, and
  sweep machinery are reused.

The preferred structure is to keep the harness stable and swap a workload model
that owns query predicates, initial seed shape, and writer distribution.

## Proposed Harness Shape

Add a workload-model layer rather than duplicating the app:

```ts
type ThroughputModel = {
  readonly name: string;
  buildQuery(
    builder: SchemaQuery<ThroughputSchema>,
    queryIndex: number,
    rowsPerQuery: number,
    clientIndex: number,
  ): BuiltProfileQuery;
  writeOne(sql: WriteSQL, seq: number): Promise<WriteImpact>;
};
```

The existing profiles become the `hot`/`pathological` model. The new variants
become `realistic`.

Possible CLI shapes:

```text
--profile relational --model hot
--profile relational --model realistic
```

or:

```text
--profile relational:hot
--profile relational:realistic
```

Prefer the first form if it keeps existing profile parsing and sweep output
simpler.

The synthetic client should pass `clientIndex` into query construction, so
realistic models can spread clients over owners/categories/orgs/buckets while
reusing the same client lifecycle and metrics.

## Metrics To Add

Alongside existing p95/p99/max-seq metrics, record write impact metadata:

- total logical writes;
- writes targeting active partitions;
- writes expected to affect zero active client groups;
- writes expected to affect at least one active client group;
- optionally writes expected to affect visible rows vs only non-visible rows.

This will make runs interpretable. A realistic profile should report an
approximate active-query impact rate, not just p99 lag.

## Feed Append

Current hot model:

- all clients watch `bucket = 0`;
- every write inserts into `bucket = 0`;
- every write affects every active query set.

Realistic model:

- create many feed buckets;
- spread clients across buckets or small bucket sets;
- writer chooses buckets with a mix of active and inactive targets;
- optionally include one hot global bucket to model a popular feed.

Initial target distribution:

```text
64 buckets
100 clients spread across active buckets
30% writes to client-watched buckets
70% writes to cold/unwatched buckets
```

Queries stay structurally similar:

```ts
builder.event
  .where('bucket', clientBucket)
  .orderBy('seq', 'desc')
  .limit(rowsPerQuery);
```

## Email

Current hot model:

- all clients watch `SHARED_OWNER_ID`;
- all writes insert unread inbox messages for that owner;
- each write also bumps the visible thread.

Realistic model:

- create many owners;
- each client watches its own owner inbox, or a small set of owners;
- writer mixes active owner inbox writes, inactive owner writes, non-inbox
  writes, and metadata/read-state updates;
- some updates should target threads outside the visible top-N window.

Initial target distribution:

```text
100 active owners plus cold owners
20% active owner inbox writes
40% cold owner inbox writes
20% active owner non-inbox writes
20% read/unread/thread metadata updates, often outside visible top N
```

Queries stay structurally similar, but replace `SHARED_OWNER_ID` with the
client's assigned owner:

```ts
builder.emailThread
  .where('ownerID', ownerIDForClient(clientIndex))
  .where('mailbox', 'inbox')
  ...
```

## Forum

Current hot model:

- all clients watch one category;
- every write inserts into that category;
- every write updates a thread and the category row.

Realistic model:

- create many categories;
- spread clients across categories;
- writer distributes posts across categories and threads with a skew;
- many writes should hit unwatched categories;
- within watched categories, some writes should hit threads outside the visible
  top-N window.

Initial target distribution:

```text
32 categories
clients spread across categories
25% writes to active watched categories
75% writes to cold/unwatched categories
within a category, skew toward recent threads but not exclusively top N
```

Queries stay structurally similar, but replace the single category constant with
the client's assigned category:

```ts
builder.forumThread
  .where('categoryID', categoryIDForClient(clientIndex))
  ...
```

## Relational

Current hot model:

- all clients watch one org;
- every write inserts an activity into that org;
- every write updates an account;
- every write updates the org row, making all org-scoped queries hot.

Realistic model:

- create many orgs;
- clients watch one org or a small account subset;
- writer distributes activity across active and inactive orgs;
- do not update the org row on every activity insert;
- model org aggregate updates as lower-frequency writes.

Initial target distribution:

```text
100 active orgs plus cold orgs
20% writes to active watched orgs
70% writes to cold/unwatched orgs
10% metadata/status updates
org aggregate update every 10th or 20th write, not every write
```

The most important change is reducing the synchronous `UPDATE rel_org` rate.
Real applications may denormalize last-activity/count fields, but doing that on
every activity insert makes all org-scoped queries hot and dominates the
benchmark.

Queries stay structurally similar, but replace `REL_ORG_ID` with the client's
assigned org:

```ts
builder.relActivity
  .where('orgID', orgIDForClient(clientIndex))
  ...
```

## Implementation Notes

- Preserve the existing hot profiles exactly for regression and worst-case
  testing.
- Add model-aware helpers instead of branching throughout the harness.
- Keep seed data reusable where possible, but realistic models will need more
  owners/categories/orgs/buckets than the current constants.
- Prefer deterministic distributions based on `seq` so runs are reproducible.
- Use simple deterministic weighting first; add random/Zipf only if needed.
- Record the model name and impact counters in the result JSON.
- Extend sweep support so it can run `hot` and `realistic` models for the same
  profile matrix.

## Open Questions

- Should realistic clients watch one partition each, or multiple partitions?
- What active-write ratio should be the default: 10%, 20%, or 30%?
- Should the writer target active partitions uniformly or with hot-spot skew?
- Should visible-row impact be estimated by the writer, or measured from client
  observations after the fact?
- Should realistic mode include deletes and edits, or start with inserts and
  metadata updates only?
