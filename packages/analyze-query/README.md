# Analyze ZQL

This package contains:

1. `transform-query`: a script that transforms query hashes to their full
   AST and ZQL representation, with permissions applied.
2. `runAnalyzeCLI`: a library entry point for building a project-specific
   `analyze` CLI that analyzes ZQL queries against a running `zero-cache`
   via the inspector protocol. See `apps/zbugs/scripts/analyze.ts` for a
   minimal example, or import it as:

   ```ts
   import {runAnalyzeCLI} from '@rocicorp/zero/analyze';
   import {schema} from './schema.ts';
   await runAnalyzeCLI({schema});
   ```

## Usage

Run `transform-query` from the folder that contains the `.env` for your
product; it needs access to the schema, permissions, replica, and cvr db.

```bash
npx transform-query --hash=hash --schema=path_to_schema.ts
```

## Measuring pipeline advance against an exported replica

For the Margins investigation, `zql-benchmarks` has a standalone runner that
loads the exported server query definitions, samples high-row-count users and
referenced work covers from the replica, and times each long-lived background
query while a 500-row `catalog.work_covers` transaction advances its pipeline.

```bash
pnpm --filter zql-benchmarks margins:perf
```

The defaults point at `../investigate/margins-zero-queries` and
`../investigate/margins-base.db` relative to this repository. Use `--help` for
path, sample-count, query-filter, JSON-output, and legacy-relationship options.
The source replica is never opened for writes: the runner requires a filesystem
copy-on-write clone and removes that clone after the run.

Use `--workload cover` to exercise the catalog queries whose graphs reach
`work_covers`, including the ISBN-to-edition-cover relationship. Its work IDs,
ISBNs, and ISBN arrays are sampled from the replica. The default `background`
workload uses the highest-row-count users in the replica.

The exported query bundle is newer than the replica schema, so the runner adds
the missing nullable cover dimensions and zero-valued catalog count columns to
the clone. It also applies the production `work_isbn13s.cover` relationship fix
while compiling the bundle: `(work_id, cover_id)` is joined to the matching
composite key on `work_covers`, and reverse invalidation can anchor on the
indexed `work_isbn13s.work_id` instead of scanning by unindexed `cover_id`. Pass
`--legacy-cover-join` only to measure the old relationship for comparison.
