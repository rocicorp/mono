# ast-to-ivm

Given an AST (as defined by `ZqlAst.ts`), build up an IVM pipeline.

There are a few stages towards implementation here --

# Implementation Stages

## Stage 1: Pipeline Per ZQL Query

This is the simplest conversion. Each ZQL query gets a unique pipeline. This means if we have duplicate queries then we get duplicate pipelines.

E.g.,

```ts
for (let i = 0; i < 1000; ++i) {
  Issue.select('id').prepare();
}
```

Would prepare 1k identical pipelines and a write would visit all 1k pipelines rather than the statement being collapsed to a single ref-counted pipeline.

## Stage 2: Pipeline per Unique Bound ZQL Query

This ref counts so identical queries (with identical bindings) share the same pipeline. Identical queries with differing bindings still get unique pipelines.

E.g.,

```ts
for (let i = 0; i < 1000; ++i) {
  Issue.select('id').prepare();
}
```

Would create 1 pipeline with a refcount of 1k.

```ts
for (let i = 0; i < 1000; ++i) {
  Issue.select('id').where('x', '=', slot('y')).prepare();
}
```

Would create 1k pipelines since we don't collapse over slots in stage 2.

Slots represent binding positions. E.g., the `?` from a SQL statement: `SELECT * FROM foo WHERE x = ?` is a bind slot.

## Stage 3: Pipeline per Unique Unbound ZQL Query
