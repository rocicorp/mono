# Standalone Queries API

## Overview

This spike revisits the idea of defining custom queries and mutators as standalone objects rather than invoking them through the Zero constructor.

### Problem

The current API has custom queries registered on the Zero constructor and accessed via `z.query.queryName(args)`. However, Zero continues to support ad-hoc queries in two places:

- Inside a transaction (tx.query.tableName...)
- Local-only queries (z.run, z.materialize, and z.preload)

Exposing custom queries at z.query.<queryName> creates the question of how to handle these ad-hoc queries:

- creates an awkward inconsistency in how custom vs ad-hoc queries are run: custom are z.query.<queryName>.run() and ad-hoc are z.run() or tx.run()
- It muddies what is possible to do with queries with the queries themselves. Outside of transactions, it makes sense to .materialize(), .run(), or .preload() custom queries. Inside of tx, it still makes sense to be able to use custom queries, but only .run() makes sense. But with this functionality being a method of query, it makes it harder to make these distinctions. It would be much more convenient if the queries themselves were separate from what you can do with them.
- We already knew this but it kind of doubles the api surface area for running queries. Like for running queries and waiting for complet we have both q.run({type: "complete"}) and z.run(q, {type: "complete"}). Or for materializing we have q.materialize() and z.materialize(q). This also interacts weirdly with useQuery.

## Why Standalone Queries Didn't Work

Two reasons:

1. Namespace collisions. We need every query and mutator to have a unique name. Passing the tree of objects to Zero to create names provided a handy chokepoint to do this.
2. Mutators _have_ to be known to Zero for offline/persistent support.

## New Proposal

Queries become "dumb data" again:

```ts
const issuesByLabel = defineQuery<ZeroContext>(
  z.string(),
  ({args: label, ctx: {userID}}) => {
    return zql.issue
      .whereExists('label', q => q.where('name', label))
      .whereExists('viewer', q => q.where('userID', userID));
  },
);
```

To solve the naming issue, we factor out that functionality into a new `defineQueries()` helper:

```ts
const queries = defineQueries({
  issues: {
    byID: defineQuery(z.string(), ({args: id, ctx: {userID}}) => {
      return viewableIssues(userID).where('id', id).one();
    }),
    byLabel: defineQuery(z.string(), ({args: label, ctx: {userID}}) => {
      return viewableIssues(userID).whereExists('label', q =>
        q.where('name', label),
      );
    }),
  },
});
```

These queries can now be used with `z.run()`, `z.materialize()`, or anywhere else ZQL queries can be used today:

```ts
const [issues] = await zero.run(queries.issues.byLabel('important'), {
  type: 'complete',
});

const [issue] = useQuery(queries.issues.byID('i1'));
```

Note that the `Context` is still passed into these queries automatically. Just as in `mono` right now. The examples above rely on having previously done:

```tsx
<ZeroProvider context={userID: "u1"}>
...
</ZeroProvider>
```

This means that the return value from `queries.issues.byID("i1")` is not a ZQL query. It's: `(ctx) => ZQL`.

Also note this also means that `tx` no longer needs `tx.query.tableName` (except maybe for backward compat), because you can just say:

```ts
tx.run(zql.query.tableName);
```

## Mutators

Mutators are set up exactly the same way:

```ts
const mutators = defineMutators({
  issue: {
    addLabel: defineMutator(
      z.object({
        issueID: z.string(),
        labelID: z.string(),
      }),
      async (tx, {args: {issueID, labelID}, ctx: {userID}}) => {
        must(
          await tx.query.issue
            .where('id', 'issueID')
            .whereExists('editors', userID),
          'Access denied',
        );
        await tx.mutate.issueLabel.upsert({
          issueID,
          labelID,
        });
      },
    ),
  },
});
```

The only difference is that before you can invoke a mutator you must also register the set of them with Zero:

```tsx
<ZeroProvider mutators={mutators} context={context}>
  ...
</ZeroProvider>
```

Once you've done that, you call mutators similarly to queries:

```ts
z.mutate(mutators.addLabel('i1', 'l1'));
```

If you call a mutator that hasn't been registered, it's a runtime error.

Note that `z.mutate(query)` implies that you should also be able to do do something like `z.mutate(crud.issue.insert({...}))`. This would enable getting rid of `tx.query.tableName`.

## Design Decisions

### No Chaining

I explicitly choose to remove chaining from synced queries for now:

```ts
// This is NOT allowed
queries.labels({projectName}).orderBy('name', 'asc'); // Error: thunk is not a Query
```

Implementation is complex (it means these query factory functions have to have entire query interface too) and the semantics are unclear server-side.

We can revisit this later.

### Optional Args for No-Input Queries

Queries defined with `z.undefined()` can be called without arguments:

```typescript
const [users] = useQuery(queries.allUsers()); // No need to pass undefined
```

## Cleanup TODO

If this direction is accepted, we need to:

1. **Naming** - Review and improve names for:
   - `defineQuery` / `defineQueries` / `defineQueriesWithContextType`
   - `QueryRegistry` (return type of defineQueries)
   - `QueryDefinition` (the tagged function)
   - The "thunk" concept (currently unnamed in API)

2. **Remove `wrapCustomQuery`** - Delete the constructor-based version, rename `wrapCustomQuery2` to `wrapCustomQuery`

3. **Remove queries from Zero constructor** - Eliminate the `QD` type parameter and `queries` option entirely

4. **Clean up exports** - Remove now-unnecessary exports related to constructor-based queries

5. **Move to correct package** - `defineQuery`, `defineMutator`, and related code belong in `zero-client`, not `zql`. ZQL should just be about the query language itself.

6. **Mutator registration validation** - Add runtime or type check that mutators passed to `z.mutate()` were registered with Zero ahead of time

7. **Remove QueryRegistry and MutatorRegistry classes** - These server-side classes that build maps from definitions are no longer needed with the new thunk-based pattern. Use `getQuery` and `getMutation` directly instead
