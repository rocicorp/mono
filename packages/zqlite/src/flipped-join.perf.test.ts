/* oxlint-disable no-console */
import {afterEach, describe, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Catch, type CaughtNode} from '../../zql/src/ivm/catch.ts';
import {
  FlippedJoin,
  setMultiConstraintChunkSizeForTest,
} from '../../zql/src/ivm/flipped-join.ts';
import {Database} from './db.ts';
import {TableSource} from './table-source.ts';

/**
 * Wall-clock perf for FlippedJoin against a real zqlite TableSource at
 * scales where the chunked multi-IN path opens many sub-streams.
 *
 * Gated on PERF=1 so it doesn't run in CI. To run:
 *
 *   PERF=1 npm --workspace=zqlite run test -- flipped-join.perf
 *
 * What we're measuring:
 *  - Steady-state throughput materializing N joined rows from N children.
 *  - Sensitivity to MULTI_CONSTRAINT_CHUNK_SIZE: smaller chunks → more
 *    sub-streams in mergeSortedStreams (current production: 256).
 *
 * Why this case matters: 1:1 inner join with N children produces ⌈N/chunk⌉
 * simultaneous open prepared-statement cursors during merge. The N-way
 * merge cost (heap vs prior linear-scan) shows up here.
 *
 * To compare against the pre-chunked-IN `#fetchQuicksort` algorithm,
 * run this file's equivalent at the parent of commit `9b0904976`
 * ("in batching") via a git worktree — see the README of that commit
 * for the old unique-key path. The same setupDb fixture works against
 * the old API; just skip the chunkSize sweep (the constant didn't
 * exist) and time `Catch(fj).fetch({})`.
 */

const lc = createSilentLogContext();

const N_LARGE = 100_000;

let restoreChunkSize: (() => void) | undefined;

afterEach(() => {
  restoreChunkSize?.();
  restoreChunkSize = undefined;
});

function setupDb(n: number): {parent: TableSource; child: TableSource} {
  const db = new Database(lc, ':memory:');
  // INTEGER PRIMARY KEY is an alias for the rowid in SQLite and does not
  // create a separate unique index, which TableSource requires. Add
  // explicit unique indexes so the Source contract is satisfied.
  db.exec(/* sql */ `
    CREATE TABLE parent (id INTEGER NOT NULL, label TEXT NOT NULL);
    CREATE UNIQUE INDEX parent_id_idx ON parent (id);
    CREATE TABLE child (id INTEGER NOT NULL, parentId INTEGER NOT NULL);
    CREATE UNIQUE INDEX child_id_idx ON child (id);
    CREATE INDEX child_parent_idx ON child (parentId);
  `);

  // Bulk insert with a single transaction — TableSource reads rows
  // directly from the table on fetch, so there's no need to drive
  // inserts through `source.push()` (which would also be ~100x slower
  // per row at this scale due to the IVM dispatch).
  const insertParent = db.prepare(
    'INSERT INTO parent (id, label) VALUES (?,?)',
  );
  const insertChild = db.prepare(
    'INSERT INTO child (id, parentId) VALUES (?,?)',
  );
  db.transaction(() => {
    for (let i = 1; i <= n; i++) {
      insertParent.run(i, `p${i}`);
      insertChild.run(i, i);
    }
  });

  const parent = new TableSource(
    lc,
    testLogConfig,
    db,
    'parent',
    {id: {type: 'number'}, label: {type: 'string'}},
    ['id'],
  );
  const child = new TableSource(
    lc,
    testLogConfig,
    db,
    'child',
    {id: {type: 'number'}, parentId: {type: 'number'}},
    ['id'],
  );
  return {parent, child};
}

type RunResult = {
  chunkSize: number;
  numChunks: number;
  rows: number;
  elapsedMs: number;
};

function runOnce(n: number, chunkSize: number): RunResult {
  restoreChunkSize?.();
  restoreChunkSize = setMultiConstraintChunkSizeForTest(chunkSize);

  const {parent, child} = setupDb(n);

  const fj = new FlippedJoin({
    parent: parent.connect([['id', 'asc']]),
    child: child.connect([['id', 'asc']]),
    parentKey: ['id'],
    childKey: ['parentId'],
    relationshipName: 'children',
    hidden: false,
    system: 'client',
  });

  const start = performance.now();
  const result: CaughtNode[] = new Catch(fj).fetch({});
  const elapsedMs = performance.now() - start;

  return {
    chunkSize,
    numChunks: Math.ceil(n / chunkSize),
    rows: result.length,
    elapsedMs,
  };
}

function logRow(r: RunResult) {
  const rowsPerSec = (r.rows / r.elapsedMs) * 1000;
  console.log(
    r.chunkSize.toString().padStart(10) +
      r.numChunks.toString().padStart(12) +
      r.rows.toString().padStart(10) +
      r.elapsedMs.toFixed(1).padStart(12) +
      (r.elapsedMs / r.rows).toFixed(4).padStart(11) +
      Math.round(rowsPerSec).toString().padStart(12),
  );
}

function logHeader() {
  console.log(
    'chunkSize'.padStart(10) +
      'numChunks'.padStart(12) +
      'rows'.padStart(10) +
      'elapsedMs'.padStart(12) +
      'ms/row'.padStart(11) +
      'rows/sec'.padStart(12),
  );
}

describe.skipIf(!process.env.PERF)(
  'FlippedJoin perf — many children → many merge streams',
  {timeout: 600_000},
  () => {
    test(`100k children, sweep over chunk sizes`, () => {
      // Warm-up so JIT compilation is amortized away from the timing.
      runOnce(1_000, 256);

      // Production chunk size is 256. We sweep tighter and looser to see
      // how merge-stream count affects total cost. With N=100k:
      //   chunk=32   → 3125 streams
      //   chunk=256  →  391 streams
      //   chunk=2048 →   49 streams
      //
      // The single-chunk (N) case is intentionally absent: SQLite caps
      // bind parameters at 32,766 (SQLITE_MAX_VARIABLE_NUMBER), and the
      // SQL builder's recursive `sql.join` blows the stack well before
      // that, so the path isn't reachable.
      const cases = [32, 64, 128, 256, 512, 1024, 2048, 4096];

      console.log(
        `\n=== FlippedJoin: ${N_LARGE.toLocaleString()} children, 1:1 inner join ===`,
      );
      logHeader();
      for (const chunkSize of cases) {
        const r = runOnce(N_LARGE, chunkSize);
        logRow(r);
      }
    });

    test(`100k children, default chunk=256, repeated for variance`, () => {
      runOnce(1_000, 256); // warm-up
      console.log(
        `\n=== FlippedJoin: ${N_LARGE.toLocaleString()} children, chunk=256 (3 runs) ===`,
      );
      logHeader();
      for (let i = 0; i < 3; i++) {
        logRow(runOnce(N_LARGE, 256));
      }
    });

    test(`25k children, baseline at smaller scale`, () => {
      const N = 25_000;
      runOnce(1_000, 256);

      const cases = [32, 64, 128, 256, 1024, 4096];

      console.log(
        `\n=== FlippedJoin: ${N.toLocaleString()} children, 1:1 inner join ===`,
      );
      logHeader();
      for (const chunkSize of cases) {
        const r = runOnce(N, chunkSize);
        logRow(r);
      }
    });

    test(`25k children, default chunk=256, repeated for variance`, () => {
      const N = 25_000;
      runOnce(1_000, 256);
      console.log(
        `\n=== FlippedJoin: ${N.toLocaleString()} children, chunk=256 (3 runs) ===`,
      );
      logHeader();
      for (let i = 0; i < 3; i++) {
        logRow(runOnce(N, 256));
      }
    });
  },
);
