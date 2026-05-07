/* oxlint-disable no-console */
import {describe, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Catch, type CaughtNode} from '../../zql/src/ivm/catch.ts';
import {FlippedJoin} from '../../zql/src/ivm/flipped-join.ts';
import {Database} from './db.ts';
import {TableSource} from './table-source.ts';

/**
 * Wall-clock perf for FlippedJoin against a real zqlite TableSource on
 * the non-unique parentKey path — the path optimized by the heap-merge
 * + dedup changes.
 *
 * Two compounding wins this test surfaces:
 *  - **Dedup of redundant parent fetches.** Children sharing a
 *    parent-key value now produce one parent cursor, not N. Pre-fix the
 *    code opened one cursor per child and each cursor refetched the
 *    same parent rows.
 *  - **Heap-based K-way merge.** O(log K) per emit instead of O(K) per
 *    emit (linear scan of every iterator's head row). K = number of
 *    open per-key cursors, so K = #childNodes pre-dedup vs #unique-keys
 *    post-dedup — the dedup win shrinks K too.
 *
 * Gated on PERF=1 so it doesn't run in CI. To run:
 *
 *   PERF=1 npm --workspace=zqlite run test -- flipped-join-merge.perf
 *
 * To compare against the pre-heap-merge / pre-dedup algorithm, check
 * out a revision before those changes landed in a worktree and port
 * this file across — the FlippedJoin and TableSource constructor
 * signatures are identical at that revision, so no test-side changes
 * are needed.
 */

const lc = createSilentLogContext();

function setupDb(
  numChildren: number,
  uniqueBuckets: number,
): {parent: TableSource; child: TableSource} {
  const db = new Database(lc, ':memory:');
  // parent.bucket is intentionally NOT unique — that's what forces
  // FlippedJoin down the merge-sort path (i.e. the path the heap-merge
  // + dedup changes optimized). With a unique parent key the operator
  // takes the quicksort path instead, which doesn't exercise either of
  // the wins we want to measure.
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

  // 1:1 parents-to-children-per-bucket so total emitted-row count is
  // independent of the dedup factor — only the merge K and per-cursor
  // work change across cases.
  const numParents = numChildren;
  const insertParent = db.prepare(
    'INSERT INTO parent (id, bucket) VALUES (?,?)',
  );
  const insertChild = db.prepare('INSERT INTO child (id, bucket) VALUES (?,?)');
  db.transaction(() => {
    for (let i = 1; i <= numParents; i++) {
      insertParent.run(i, ((i - 1) % uniqueBuckets) + 1);
    }
    for (let i = 1; i <= numChildren; i++) {
      insertChild.run(i, ((i - 1) % uniqueBuckets) + 1);
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
  uniqueBuckets: number;
  childrenPerBucket: number;
  rowsOut: number;
  elapsedMs: number;
};

function runOnce(numChildren: number, uniqueBuckets: number): RunResult {
  const {parent, child} = setupDb(numChildren, uniqueBuckets);

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
    uniqueBuckets,
    childrenPerBucket: numChildren / uniqueBuckets,
    rowsOut: result.length,
    elapsedMs,
  };
}

function logHeader() {
  console.log(
    'children'.padStart(10) +
      'buckets'.padStart(10) +
      'kidsPerBkt'.padStart(12) +
      'rowsOut'.padStart(10) +
      'elapsedMs'.padStart(12) +
      'ms/row'.padStart(11),
  );
}

function logRow(r: RunResult) {
  console.log(
    r.numChildren.toString().padStart(10) +
      r.uniqueBuckets.toString().padStart(10) +
      r.childrenPerBucket.toString().padStart(12) +
      r.rowsOut.toString().padStart(10) +
      r.elapsedMs.toFixed(1).padStart(12) +
      (r.elapsedMs / Math.max(1, r.rowsOut)).toFixed(4).padStart(11),
  );
}

describe.skipIf(!process.env.PERF)(
  'FlippedJoin perf — non-unique parentKey (merge-sort path)',
  {timeout: 600_000},
  () => {
    test('2.5k children, sweep dedup factor', () => {
      const N = 2_500;
      // Warm-up so JIT compilation is amortized away from the timing.
      runOnce(500, 50);

      // dedup factor = N / uniqueBuckets. Pre-fix the code always
      // opened N cursors regardless of dedup, so cases with high dedup
      // show the largest cursor-count delta. K = uniqueBuckets is also
      // the merge fan-in, so high-K rows show the heap vs linear-scan
      // win. Scale is intentionally small so the pre-fix algorithm (no
      // batching, K = N cursors at dedup=1) finishes in seconds, not
      // minutes — useful for A/B against the previous algorithm.
      const cases = [N, N / 5, N / 25, N / 125, N / 625];

      console.log(
        `\n=== FlippedJoin merge-sort: ${N.toLocaleString()} children, 1:1 parents-per-bucket ===`,
      );
      logHeader();
      for (const buckets of cases) {
        logRow(runOnce(N, buckets));
      }
    });

    test('2.5k children, dedup=25, repeated for variance', () => {
      const N = 2_500;
      runOnce(500, 50);
      console.log(
        `\n=== FlippedJoin merge-sort: ${N.toLocaleString()} children, dedup=25 (3 runs) ===`,
      );
      logHeader();
      for (let i = 0; i < 3; i++) {
        logRow(runOnce(N, N / 25));
      }
    });
  },
);
