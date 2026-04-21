# ZQL Benchmarks

## Query Scenario Comparison

Use `compare-query-scenarios` when you want to compare the SQL and runtime of one ZQL scenario across two local Zero commits, branches, worktrees, or paths.

The command never fetches from the network. Git refs must already exist locally. When a ref is not a filesystem path, the script creates a detached worktree under `.tmp/query-scenario-worktrees` and imports the runtime modules from that checkout.

Compare the current working tree against `origin/main`:

```sh
npm --workspace=zql-benchmarks run compare-query-scenarios -- \
  --left origin/main \
  --right . \
  --scenario student-membership-mixed-or
```

Compare two local commits:

```sh
npm --workspace=zql-benchmarks run compare-query-scenarios -- \
  --left 9587e11 \
  --right 04dc506 \
  --scenario permission-and-class-filter-intersection
```

Compare two different query shapes on the same checkout:

```sh
npm --workspace=zql-benchmarks run compare-query-scenarios -- \
  --left . \
  --right . \
  --left-scenario parent-or-exists-union-roots \
  --right-scenario student-membership-mixed-or
```

Run the full committed scenario suite and keep machine-readable output:

```sh
npm --workspace=zql-benchmarks run compare-query-scenarios -- \
  --left origin/main \
  --right . \
  --format json > .tmp/query-scenario-compare.json
```

JSON output includes the SQL shapes and planner debug text captured from the final measured iteration on each side. This is usually the best format when you want to paste an exact before and after SQL comparison into a PR.

Scenario selection accepts the full scenario name, `all`, or a short slug from the scenario filename. The default scenario source is `packages/zqlite/src/test/query-scenarios/scenarios/index.ts`.

You can point the runner at a custom scenario module:

```sh
npm --workspace=zql-benchmarks run compare-query-scenarios -- \
  --left origin/main \
  --right . \
  --scenario-source ./packages/zqlite/src/test/query-scenarios/scenarios/index.ts \
  --scenario all
```

A custom scenario module should default-export either one scenario or an array of scenarios with this shape:

```ts
export default {
  name: 'my query',
  schema,
  seed: db => {
    db.exec('CREATE TABLE example (id TEXT PRIMARY KEY)');
    db.prepare('INSERT INTO example (id) VALUES (?)').run('1');
  },
  query: builder => builder.example,
};
```

The script reports median wall time, SQL call count, distinct SQL shapes, and row count for each side. Different row counts are allowed when comparing two different queries, but they usually mean a correctness regression when comparing the same scenario across commits.
