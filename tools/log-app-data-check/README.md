# log-app-data-check

Flags log calls that pass customer app data.

```bash
pnpm run check-logs                      # from the repo root
pnpm run check-logs -- packages/zero-cache packages/zero-client
pnpm run check-logs -- --json
```

Exits non-zero when anything is flagged, so CI can gate on it.

## How it works

At a log sink the useful question is _what is the static type of this
expression_, not _where did this value come from_. So this reads types from the
TypeScript checker instead of doing dataflow analysis. The sinks are enumerable
— every `.info`/`.debug`/`.warn`/`.error` call — and the checker answers the
rest.

It also unwraps **laundering**. A value that becomes a `string` via
`JSON.stringify(...)` or a `` `${...}` `` template span hides its type at the
sink, so those are followed inward up to three hops. Without that,
`change-source.ts:687` is invisible.

Matching descends into nested types, so logging a protocol message that happens
to contain an `AST` is caught even though the outer type is anonymous.

## What it cannot see

Data baked into an `Error` message at construction, thrown, and logged from a
different function:

```ts
throw new Error(`Invalid _0_version in ${stringify(row)}`);
```

At the sink that type is `Error`. Catching it needs real interprocedural
analysis — see `log-taint-analysis-evaluation.md` in the `cloudzero` repo.

## Suppressing a reviewed site

Put a comment on the line or the line above, with a reason:

```ts
// log-allow: pg_replication_slots rows, not customer data
lc.info?.('dropped slots', {dropped});
```

The default is deliberately broad — `postgres.Row` is included, because a query
against a customer table returns those. Infrastructure queries get suppressed
one at a time, so the default stays fail-safe.
