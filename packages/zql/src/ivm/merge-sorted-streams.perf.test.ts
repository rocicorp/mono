/* oxlint-disable no-console */
import {describe, test} from 'vitest';
import type {Node} from './data.ts';
import {mergeSortedStreams} from './memory-source.ts';
import type {Stream} from './stream.ts';

/**
 * Microbench: heap-based `mergeSortedStreams` vs a linear-scan
 * equivalent. Per-row work and stream contents are held constant so the
 * only variable is the merge algorithm.
 *
 * Why this exists: post-batching, FlippedJoin's merge fan-in is K =
 * ⌈uniqueParentKeys / chunkSize⌉. With chunkSize=256 and 10k unique
 * keys, K=40. The heap is O(log K) per emit vs linear's O(K), but at
 * small K the heap's per-op constant cost (array swaps, sift-down,
 * closure-comparator calls) can wipe out the asymptotic edge. This
 * bench finds the empirical crossover.
 *
 * Gated on PERF=1 so it doesn't run in CI:
 *
 *   PERF=1 npm --workspace=zql run test -- merge-sorted-streams.perf
 */

/**
 * Linear-scan K-way merge with the same external contract as
 * `mergeSortedStreams`: forward 'yield's, propagate `.return()` to
 * sub-iterators in `finally`. O(K) per emit.
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

/**
 * Build K presorted streams whose rows are interleaved (round-robin) on
 * the merge key. Worst case for both algorithms: every emit pulls from
 * a different stream, so the heap pays a full sift-down on every step
 * and the linear scan can never short-circuit early.
 */
function buildStreams(K: number, totalRows: number): Stream<Node | 'yield'>[] {
  const streams: Node[][] = Array.from({length: K}, () => []);
  for (let i = 0; i < totalRows; i++) {
    streams[i % K].push({row: {id: i, bucket: i % K}, relationships: {}});
  }
  return streams as unknown as Stream<Node | 'yield'>[];
}

const compare = (a: Node, b: Node): number =>
  (a.row.id as number) - (b.row.id as number);

type MergeFn = (
  streams: readonly Stream<Node | 'yield'>[],
  compare: (a: Node, b: Node) => number,
) => Stream<Node | 'yield'>;

function timeMerge(fn: MergeFn, K: number, totalRows: number): number {
  // Rebuild streams each time — array iterators are single-use.
  const streams = buildStreams(K, totalRows);
  const start = performance.now();
  let count = 0;
  for (const node of fn(streams, compare)) {
    if (node !== 'yield') count++;
  }
  const elapsedMs = performance.now() - start;
  if (count !== totalRows) {
    throw new Error(`expected ${totalRows} emits, got ${count}`);
  }
  return elapsedMs;
}

const median = (xs: number[]): number =>
  xs.toSorted((a, b) => a - b)[Math.floor(xs.length / 2)];

function logHeader() {
  console.log(
    'K'.padStart(6) +
      'totalRows'.padStart(12) +
      'heapMs'.padStart(12) +
      'linearMs'.padStart(12) +
      'heap/linear'.padStart(14) +
      'winner'.padStart(10),
  );
}

function logRow(K: number, totalRows: number, heap: number, linear: number) {
  const ratio = heap / linear;
  const winner = ratio < 0.95 ? 'heap' : ratio > 1.05 ? 'linear' : 'tie';
  console.log(
    K.toString().padStart(6) +
      totalRows.toString().padStart(12) +
      heap.toFixed(2).padStart(12) +
      linear.toFixed(2).padStart(12) +
      ratio.toFixed(2).padStart(14) +
      winner.padStart(10),
  );
}

describe.skipIf(!process.env.PERF)(
  'mergeSortedStreams microbench — heap vs linear',
  {timeout: 600_000},
  () => {
    test('sweep K with fixed total emits, interleaved streams', () => {
      const TOTAL = 100_000;

      // Warm-up so JIT compilation is amortized away from the timing.
      for (let i = 0; i < 3; i++) {
        timeMerge(mergeSortedStreams, 10, 10_000);
        timeMerge(mergeSortedStreamsLinear, 10, 10_000);
      }

      // K values cover realistic post-batching fan-in. With
      // chunkSize=256, uniqueKeys = K · 256, so:
      //   K=4   →  ~1k unique keys
      //   K=40  →  ~10k unique keys
      //   K=400 → ~100k unique keys
      const Ks = [2, 4, 10, 40, 100, 200, 400];

      console.log(
        `\n=== mergeSortedStreams microbench: ${TOTAL.toLocaleString()} total emits, interleaved (worst-case) ===`,
      );
      logHeader();
      for (const K of Ks) {
        const heap: number[] = [];
        const lin: number[] = [];
        for (let i = 0; i < 5; i++) {
          heap.push(timeMerge(mergeSortedStreams, K, TOTAL));
          lin.push(timeMerge(mergeSortedStreamsLinear, K, TOTAL));
        }
        logRow(K, TOTAL, median(heap), median(lin));
      }
    });
  },
);
