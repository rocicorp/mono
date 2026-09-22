# Fuzzer regressions

Committed minimized repros from the coverage-driven fuzzer
(`src/chinook/fuzz/`). Each `*.json` here is replayed on every run by
`chinook-fuzz-regressions.pg.test.ts` (the "corpus-first" guard, design §9), so
a once-found divergence can never silently return.

## Format

Each file is one `Regression` (`src/chinook/fuzz/regressions.ts`):

```json
{
  "note": "what bug this guards — and the original seed/label",
  "ast": { "table": "track", "where": { ... } },
  "pushes": [ { "table": "track", "kind": "edit", "row": { ... }, "old": { ... } } ]
}
```

- `ast` — the shrunk query (the JSON wire format; the driver's `minimizeRepro`
  produces the minimal AST).
- `pushes` — optional four-phase push history (client-named mutations). Omit for
  a hydrate-only repro.

The fixture is always the deterministic `mini` fixture, so sources are not
stored. Replay re-wraps the `ast` and routes to hydrate parity, or per-step push
parity when `pushes` is present.

## Filing one

When the fuzzer reports a divergence, take the shrunk AST from the failure
output, wrap it as a `Regression`, `serializeRegression(...)`, and write it here
as `regression-<short-description>.json`.
