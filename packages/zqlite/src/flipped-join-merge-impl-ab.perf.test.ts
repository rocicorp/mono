/* oxlint-disable no-console */
import {afterEach, describe, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Catch, type CaughtNode} from '../../zql/src/ivm/catch.ts';
import type {Node} from '../../zql/src/ivm/data.ts';
import {FlippedJoin} from '../../zql/src/ivm/flipped-join.ts';
import {setMergeSortedStreamsImplForTest} from '../../zql/src/ivm/memory-source.ts';
import type {Stream} from '../../zql/src/ivm/stream.ts';
import {Database} from './db.ts';
import {TableSource} from './table-source.ts';

/**
 * End-to-end A/B: heap-based mergeSortedStreams vs linear-scan, run
 * through real FlippedJoin + SQLite at scales where post-batching K
 * matters. Companion to merge-sorted-streams.perf.test.ts (microbench)
 * — confirms whether the microbench's per-emit savings show up
 * end-to-end against SQLite IO and IVM node-shaping overhead.
 *
 * Setup: 1:1 parents-per-bucket, dedup=1 (every child has its own
 * unique parent-key value). With multiConstraintChunkSize=256:
 *   uniqueKeys = N → K = ⌈N/256⌉
 *   N=10k  → K=40
 *   N=25k  → K=98
 *   N=100k → K=391
 *
 * Gated on PERF=1:
 *
 *   PERF=1 npm --workspace=zqlite run test -- flipped-join-merge-impl-ab.perf
 */

const lc = createSilentLogContext();

let restoreImpl: (() => void) | undefined;

afterEach(() => {
  restoreImpl?.();
  restoreImpl = undefined;
});

/**
 * Linear-scan K-way merge — same external contract as the production
 * heap implementation. Lifted from merge-sorted-streams.perf.test.ts so
 * this file is self-contained.
 */
function* mergeSortedStreamsLinear(
  streams: readonly Stream<Node | 'yield'>[],
  compare: (a: Node, b: Node) => number,
): Stream<Node | 'yield'> {
  const iterators: Iterator<Node | 'yield'>[] = streams.map(s =>
    s[Symbol.iterator](),
  );
  const active: boolean[] = new Array(iterators.length).fill(true);
  const heads: (Node | null)[] = new Array(iterators.length).fill(null);

  const pullNext = function* (
    idx: number,
  ): Generator<'yield', Node | undefined, undefined> {
    while (true) {
      const r = iterators[idx].next();
      if (r.done) {
        active[idx] = false;
        return undefined;
      }
      if (r.value === 'yield') {
        yield 'yield';
        continue;
      }
      return r.value;
    }
  };

  try {
    for (let i = 0; i < iterators.length; i++) {
      const v = yield* pullNext(i);
      heads[i] = v ?? null;
    }
    while (true) {
      let minIdx = -1;
      let minNode: Node | null = null;
      for (let i = 0; i < heads.length; i++) {
        const h = heads[i];
        if (h === null) continue;
        if (minNode === null || compare(h, minNode) < 0) {
          minIdx = i;
          minNode = h;
        }
      }
      if (minIdx === -1) return;
      yield minNode!;
      const v = yield* pullNext(minIdx);
      heads[minIdx] = v ?? null;
    }
  } finally {
    for (let i = 0; i < iterators.length; i++) {
      if (active[i]) iterators[i].return?.();
    }
  }
}

function setupDb(numChildren: number): {
  parent: TableSource;
  child: TableSource;
} {
  const db = new Database(lc, ':memory:');
  // dedup=1 case: every child's bucket is unique, so the merge fan-in
  // K = ⌈numChildren / chunkSize⌉. parent.bucket is non-unique to force
  // the merge-sort path (#fetchBatched → #fetchChunked → mergeSortedStreams).
  db.exec(/* sql */ `
    CREATE TABLE parent (id INTEGER NOT NULL, bucket INTEGER NOT NULL);
    CREATE UNIQUE INDEX parent_id_idx ON parent (id);
    CREATE INDEX parent_bucket_idx ON parent (bucket);
    CREATE TABLE child (id INTEGER NOT NULL, bucket INTEGER NOT NULL);
    CREATE UNIQUE INDEX child_id_idx ON child (id);
    CREATE INDEX child_bucket_idx ON child (bucket);
  `);

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

function runOnce(numChildren: number): {rowsOut: number; elapsedMs: number} {
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
  return {rowsOut: result.length, elapsedMs};
}

const median = (xs: number[]): number =>
  xs.toSorted((a, b) => a - b)[Math.floor(xs.length / 2)];

function logHeader() {
  console.log(
    'N'.padStart(8) +
      'K@256'.padStart(8) +
      'rowsOut'.padStart(10) +
      'heapMs'.padStart(12) +
      'linearMs'.padStart(12) +
      'heap/linear'.padStart(14) +
      'winner'.padStart(10),
  );
}

function logRow(N: number, rowsOut: number, heap: number, linear: number) {
  const ratio = heap / linear;
  const winner = ratio < 0.95 ? 'heap' : ratio > 1.05 ? 'linear' : 'tie';
  console.log(
    N.toString().padStart(8) +
      Math.ceil(N / 256)
        .toString()
        .padStart(8) +
      rowsOut.toString().padStart(10) +
      heap.toFixed(1).padStart(12) +
      linear.toFixed(1).padStart(12) +
      ratio.toFixed(2).padStart(14) +
      winner.padStart(10),
  );
}

describe.skipIf(!process.env.PERF)(
  'FlippedJoin merge impl A/B — heap vs linear, end-to-end through SQLite',
  {timeout: 600_000},
  () => {
    test('sweep N at dedup=1 (forces high merge fan-in)', () => {
      // Warm-up so JIT compilation is amortized away from the timing.
      runOnce(1_000);

      const Ns = [2_500, 10_000, 25_000, 50_000, 100_000];
      const REPS = 3;

      console.log(
        `\n=== FlippedJoin A/B: dedup=1, chunkSize=256, ${REPS} reps (median) ===`,
      );
      logHeader();
      for (const N of Ns) {
        const heap: number[] = [];
        const lin: number[] = [];
        let rowsOut = 0;
        for (let i = 0; i < REPS; i++) {
          // Heap (default).
          restoreImpl?.();
          restoreImpl = undefined;
          const h = runOnce(N);
          heap.push(h.elapsedMs);
          rowsOut = h.rowsOut;

          // Linear.
          restoreImpl = setMergeSortedStreamsImplForTest(
            mergeSortedStreamsLinear,
          );
          const l = runOnce(N);
          lin.push(l.elapsedMs);
          restoreImpl();
          restoreImpl = undefined;
        }
        logRow(N, rowsOut, median(heap), median(lin));
      }
    });
  },
);
