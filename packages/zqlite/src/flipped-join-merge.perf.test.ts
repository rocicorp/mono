/* oxlint-disable no-console */
import {describe, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Catch, type CaughtNode} from '../../zql/src/ivm/catch.ts';
import {FlippedJoin} from '../../zql/src/ivm/flipped-join.ts';
import {Database} from './db.ts';
import {TableSource} from './table-source.ts';

/**
 * Wall-clock perf for FlippedJoin against a real zqlite TableSource at
 * the 1:1 parent:child shape, sweeping N. Each child has its own
 * parent-key value, so K (the number of distinct parent-key values) is
 * equal to N — the shape where the batched-fetch path most clearly
 * outperforms a per-key-cursor merge.
 *
 * Gated on PERF=1 so it doesn't run in CI. To run:
 *
 *   PERF=1 npm --workspace=zqlite run test -- flipped-join-merge.perf
 *
 * To compare against an earlier revision, check it out in a worktree
 * and port this file across — the FlippedJoin and TableSource
 * constructor signatures haven't changed.
 */

const lc = createSilentLogContext();

function setupDb(numChildren: number): {
  parent: TableSource;
  child: TableSource;
} {
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
  return {parent, child};
}

type RunResult = {
  numChildren: number;
  rowsOut: number;
  elapsedMs: number;
};

function runOnce(numChildren: number): RunResult {
  const {parent, child} = setupDb(numChildren);

  const fj = new FlippedJoin({
    parent: parent.connect([['id', 'asc']]),
    child: child.connect([['id', 'asc']]),
    parentKey: ['bucket'],
    childKey: ['bucket'],
    relationshipName: 'parents',
    hidden: false,
    system: 'client',
  });

  const start = performance.now();
  const result: CaughtNode[] = new Catch(fj).fetch({});
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
    test('sweep N from 100 to 10k', () => {
      // Warm-up so JIT compilation is amortized away from the timing.
      runOnce(500);

      const cases = [100, 500, 1_000, 2_500, 5_000, 10_000];

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
  },
);
