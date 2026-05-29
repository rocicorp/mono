import {createSilentLogContext} from 'shared/src/logging-test-utils.ts';
/* oxlint-disable no-console */
import {describe, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {relationships} from '../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {number, table} from '../../zero-schema/src/builder/table-builder.ts';
import {buildPipeline} from '../../zql/src/builder/builder.ts';
import {TestBuilderDelegate} from '../../zql/src/builder/test-builder-delegate.ts';
import {Catch, type CaughtNode} from '../../zql/src/ivm/catch.ts';
import {asQueryImpl, newQuery} from '../../zql/src/query/query-impl.ts';
import {Database} from './db.ts';
import {TableSource} from './table-source.ts';

/**
 * Wall-clock perf for a flipped EXISTS query against a real zqlite
 * TableSource at the 1:1 parent:child shape, sweeping N. Each child has
 * its own parent-key value, so K (the number of distinct parent-key
 * values) is equal to N — the shape where the batched-fetch path most
 * clearly outperforms a per-key-cursor merge.
 *
 * The pipeline is constructed via ZQL (`parent.whereExists('children',
 * {flip: true})`) and `buildPipeline` rather than by hand-instantiating
 * `FlippedJoin`, so this exercises the same wiring zero-cache uses in
 * prod.
 *
 * Gated on PERF=1 so it doesn't run in CI. To run:
 *
 *   PERF=1 pnpm --filter zqlite run test flipped-join-merge.perf
 *
 * To compare against an earlier revision, check it out in a worktree
 * and port this file across — the FlippedJoin and TableSource
 * constructor signatures haven't changed.
 */

const lc = createSilentLogContext();

const parentTable = table('parent')
  .columns({
    id: number(),
    bucket: number(),
  })
  .primaryKey('id');

const childTable = table('child')
  .columns({
    id: number(),
    bucket: number(),
  })
  .primaryKey('id');

const parentRelationships = relationships(parentTable, ({many}) => ({
  children: many({
    sourceField: ['bucket'],
    destField: ['bucket'],
    destSchema: childTable,
  }),
}));

const schema = createSchema({
  tables: [parentTable, childTable],
  relationships: [parentRelationships],
});

function setupDelegate(numChildren: number): TestBuilderDelegate {
  const db = new Database(lc, ':memory:');
  // parent.bucket is intentionally NOT declared unique in the schema —
  // FlippedJoin keys off schema-declared uniqueness, not observed data,
  // so this keeps the operator on the merge-sort path even when each
  // bucket value happens to be unique.
  db.exec(/* sql */ `
    CREATE TABLE parent (
      id INTEGER NOT NULL,
      bucket INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX parent_id_idx ON parent (id);
    CREATE INDEX parent_bucket_idx ON parent (bucket);
    CREATE TABLE child (
      id INTEGER NOT NULL,
      bucket INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX child_id_idx ON child (id);
    CREATE INDEX child_bucket_idx ON child (bucket);
  `);

  // 1:1 parent:child — each child has its own bucket value.
  const insertParent = db.prepare(
    'INSERT INTO parent (id, bucket) VALUES (?,?)',
  );
  const insertChild = db.prepare('INSERT INTO child (id, bucket) VALUES (?,?)');
  db.transaction(() => {
    for (let i = 1; i <= numChildren; i++) {
      insertParent.run(i, i);
      insertChild.run(i, i);
    }
  });

  const parent = new TableSource(
    lc,
    testLogConfig,
    db,
    'parent',
    {id: {type: 'number'}, bucket: {type: 'number'}},
    ['id'],
  );
  const child = new TableSource(
    lc,
    testLogConfig,
    db,
    'child',
    {id: {type: 'number'}, bucket: {type: 'number'}},
    ['id'],
  );
  return new TestBuilderDelegate({parent, child});
}

type RunResult = {
  numChildren: number;
  rowsOut: number;
  elapsedMs: number;
};

function runOnce(numChildren: number): RunResult {
  const delegate = setupDelegate(numChildren);

  // ZQL: parent rows that have at least one matching child, with the
  // join forced to flip (child drives the parent fetch). The builder
  // turns this into a FlippedJoin over the two TableSources — same
  // shape as the prior hand-built pipeline, but constructed the way
  // zero-cache constructs it from a user query.
  const q = newQuery(schema, 'parent').whereExists('children', {flip: true});
  const input = buildPipeline(asQueryImpl(q).ast, delegate, 'perf-test');

  const start = performance.now();
  const result: CaughtNode[] = new Catch(input).fetch({});
  const elapsedMs = performance.now() - start;

  return {
    numChildren,
    rowsOut: result.length,
    elapsedMs,
  };
}

function logHeader() {
  console.log(
    'children'.padStart(10) +
      'rowsOut'.padStart(10) +
      'elapsedMs'.padStart(12) +
      'us/row'.padStart(10),
  );
}

function logRow(r: RunResult) {
  const usPerRow = (r.elapsedMs * 1000) / Math.max(1, r.rowsOut);
  console.log(
    r.numChildren.toString().padStart(10) +
      r.rowsOut.toString().padStart(10) +
      r.elapsedMs.toFixed(1).padStart(12) +
      usPerRow.toFixed(1).padStart(10),
  );
}

describe.skipIf(!process.env.PERF)(
  'FlippedJoin perf — scaling N at 1:1 parent:child',
  {timeout: 600_000},
  () => {
    test('sweep N from 100 to 30k', () => {
      // Warm-up so JIT compilation is amortized away from the timing.
      runOnce(500);

      const cases = [100, 500, 1_000, 2_500, 5_000, 10_000, 20_000, 30_000];

      console.log(`\n=== FlippedJoin scaling: 1:1 parent:child ===`);
      logHeader();
      for (const n of cases) {
        logRow(runOnce(n));
      }
    });

    test('N=2,500, repeated for variance', () => {
      runOnce(500);
      console.log(`\n=== FlippedJoin: N=2,500, 1:1 parent:child (3 runs) ===`);
      logHeader();
      for (let i = 0; i < 3; i++) {
        logRow(runOnce(2_500));
      }
    });

    test('result fingerprint sweep', async () => {
      // Fingerprint the full emitted result so two builds can be
      // compared row-by-row. Sort by parent id, JSON-stringify the
      // bucket-of-each-parent + its emitted children rows, sha256 it.
      const {createHash} = await import('node:crypto');
      const cases = [100, 500, 1_000, 2_500, 5_000];
      console.log(`\n=== FlippedJoin result fingerprint ===`);
      console.log(
        'N'.padStart(8) +
          'rowsOut'.padStart(10) +
          '  fingerprint (sha256 first 16 hex)',
      );
      for (const n of cases) {
        const delegate = setupDelegate(n);
        const q = newQuery(schema, 'parent').whereExists('children', {
          flip: true,
        });
        const input = buildPipeline(
          asQueryImpl(q).ast,
          delegate,
          'fingerprint-test',
        );
        const rows = new Catch(input)
          .fetch({})
          .filter((n): n is Exclude<CaughtNode, 'yield'> => n !== 'yield');
        const serialized = rows
          .map(node => ({
            row: node.row,
            relationships: Object.fromEntries(
              Object.entries(node.relationships).map(([k, v]) => [
                k,
                v
                  .filter(
                    (c): c is Exclude<CaughtNode, 'yield'> => c !== 'yield',
                  )
                  .map(c => c.row),
              ]),
            ),
          }))
          .sort((a, b) =>
            JSON.stringify(a.row).localeCompare(JSON.stringify(b.row)),
          );
        const hash = createHash('sha256')
          .update(JSON.stringify(serialized))
          .digest('hex')
          .slice(0, 16);
        console.log(
          n.toString().padStart(8) +
            rows.length.toString().padStart(10) +
            '  ' +
            hash,
        );
      }
    });
  },
);
