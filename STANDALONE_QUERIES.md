# Standalone Queries API

## Overview

This spike explores a new pattern for defining synced queries as standalone objects rather than registering them on the Zero constructor.

### Problem

The current API has queries registered on the Zero constructor and accessed via `z.query.queryName(args)`. This creates several issues:

1. **Namespace collision** - Table names and query names share the same namespace on `z.query`
2. **Inconsistency with mutators** - Custom mutators are standalone, but queries are tied to Zero instance
3. **Context coupling** - Queries capture context at definition time rather than execution time

### Solution

Queries become "dumb data" - pure objects that can be passed around:

```typescript
// Define queries as standalone registry
const queries = defineQueries({
  labels: defineQuery(
    z.object({ projectName: z.string() }),
    ({args: {projectName}, ctx}) =>
      builder.label.whereExists('project', q =>
        q.where('lowerCaseName', projectName.toLocaleLowerCase())
      ).orderBy('name', 'asc')
  ),

  user: defineQuery(z.string(), ({args: userID}) =>
    builder.user.where('id', userID).one()
  ),
});

// Usage - clean and simple
const [labels] = useQuery(queries.labels({projectName}));
const [user] = useQuery(queries.user(currentUserID));
```

### How It Works

1. `defineQueries()` stamps fully-qualified names from the object tree
2. `queries.foo(args)` returns a thunk: `(ctx) => Query`
3. `useQuery()` / `z.run()` resolves the thunk by injecting context
4. No chaining allowed - thunks prevent it by design (this is intentional)

### Benefits

- Queries are portable - pass them around, store them, test them
- Clear separation between synced queries (named) and ad-hoc queries (unnamed)
- Server only sees `{name, args}` - chaining is client-local only
- Cleaner component code: `useQuery(queries.foo(args))` vs `useQuery(z.query.foo(args))`

## Design Decisions

### No Chaining

We explicitly chose not to support chaining on synced queries:

```typescript
// This is NOT allowed
queries.labels({projectName}).orderBy('name', 'asc')  // Error: thunk is not a Query
```

Reasoning:
- On the server (queries endpoint), chaining doesn't make sense - the server just resolves `{name, args}` to ZQL
- Chaining blurs the line between "what syncs" and "how I view it locally"
- If you need a sort, put it in the query definition

### Optional Args for No-Input Queries

Queries defined with `z.undefined()` can be called without arguments:

```typescript
const [users] = useQuery(queries.allUsers());  // No need to pass undefined
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

6. **Documentation** - Update docs to reflect new pattern

7. **Migration guide** - Document how to migrate from `z.query.foo()` to `queries.foo()`

8. **Mutator registration validation** - Add runtime or type check that mutators passed to `z.mutate()` were registered with Zero ahead of time

## Files Changed

### Core Implementation
- `packages/zql/src/query/define-query.ts` - defineQueries, wrapCustomQuery2, QueryRegistry type
- `packages/zero-client/src/client/zero.ts` - run/preload/materialize handle thunks
- `packages/zero-react/src/use-query.tsx` - useQuery resolves thunks
- `packages/zero-client/src/mod.ts` - exports

### Zbugs Migration
- `apps/zbugs/shared/queries.ts` - Uses defineQueriesWithContextType
- `apps/zbugs/shared/zero-type.ts` - Removed Queries type parameter
- `apps/zbugs/src/hooks/use-zero.ts` - Removed Queries type parameter
- All components updated to use `queries.foo(args)` pattern
