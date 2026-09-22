# zql-benchmarks-rn

Host app for running the ZQL benchmarks on a React Native device. The runner,
device drivers and Hermes profiling live in `tools/rn-bench`; this is just the
app they drive.

Like `packages/replicache-perf/rn`, it is **deliberately not a pnpm workspace
package** — `pnpm-workspace.yaml` globs a single path segment, so nesting it
here keeps it in git while leaving it out of the workspace. It needs a one-time
`pnpm install` in this directory, and the empty `pnpm-workspace.yaml` stops
`pnpm install` walking up and reinstalling the monorepo.

Two root-level checks walk the tree by path rather than by workspace membership
and need this app excluded: `oxlint --type-aware` (via `oxlint.base.ts`) and
`syncpack` (via a version group in `.syncpackrc`).

## Running

From the monorepo root:

```bash
pnpm --filter zql-benchmarks run perf:rn -- --platform android
pnpm --filter zql-benchmarks run perf:rn -- --run 'hydrate' --profile /tmp/zql.json
```

## How the benchmarks get here

`src/rn.ts` imports the `.bench.ts` files that run without Node. They call
`bench`/`describe` from `shared/src/bench.ts`, which wraps mitata and Vitest;
the RN build rewrites that import to `tools/rn-bench/src/mitata-shim.ts`, so the
same files feed both the Vitest suite and the device. There is no second copy of
the benchmarks to keep in step, and the Node side reads its benchmark list from
the built bundle for the same reason.

Note the reported unit: each figure is **N iterations** (a benchmark's
`max_samples`, or 100), not one operation, because mitata measures per-iteration
while this harness times a whole rep. Comparable across runs, not directly
comparable to mitata's per-op numbers.
