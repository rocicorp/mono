/**
 * Where the time goes when ~21 queries are registered before a large replica
 * has loaded (Margins: ~24MB replica, ~40s of unbroken JS thread occupancy on a
 * mid-range Android phone).
 *
 * Everything here is one synchronous task on the client today, so the sum is
 * the event loop stall. The phases are split to show what a yield *between*
 * pipelines can and cannot break up:
 *
 * - "load replica": rows into sources with no pipelines attached, one push
 *   per row. "pushAdds" is what the client does now; "rows only" is the cost
 *   of generating the rows, to subtract from both.
 * - "pushAdds + hydrate, cold indexes": fresh sources every call (indexes
 *   outlive their connections, and mitata warms a body up before sampling it,
 *   so sources cannot be shared between calls). The deferred pipelines attach;
 *   each new sort builds its index on demand inside the first fetch that needs
 *   it. Subtract "load replica, pushAdds" for the hydration alone.
 * - "hydrate, warm indexes": hydration with the indexes already built. Cold
 *   hydration minus warm is the index build cost.
 *
 * A per-query table is logged once, from the first cold and warm passes; the
 * largest single entry is the longest stall that remains with yields only
 * between pipelines.
 *
 * Opt-in, since every sample is a 180k row load: it is too heavy and too
 * coarse for the tracked runs, and hydration benchmarks degrade an emulator
 * for whatever runs after them.
 *
 *   COLD_BOOT_BREAKDOWN=1 pnpm --filter zql-benchmarks run bench cold-boot-breakdown
 *   pnpm --filter zql-benchmarks run perf:rn --platform android --run 'cold boot breakdown'
 */

import {bench, describe} from '../../shared/src/bench.ts';
import type {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import {QueryDelegateImpl} from '../../zql/src/query/test/query-delegate.ts';
import {
  LIBRARY_QUERIES,
  load,
  loadBulk,
  rows as rowsOnly,
  makeSources,
  QUERIES,
} from './cold-boot-data.ts';

// 30k issues, 90k comments, 60k issueLabels: ~180k rows.
const SCALE = 6;

const ALL_QUERIES = [...QUERIES, ...LIBRARY_QUERIES];

function hydrateAll(sources: Record<string, MemorySource>): number[] {
  const delegate = new QueryDelegateImpl({sources});
  const times: number[] = [];
  for (const q of ALL_QUERIES) {
    const t0 = performance.now();
    const view = delegate.materialize(q());
    times.push(performance.now() - t0);
    view.destroy();
  }
  return times;
}

let coldTimes: number[] | undefined;
let warmTimes: number[] | undefined;
let logged = false;

function maybeLogTable() {
  if (logged || !coldTimes || !warmTimes) {
    return;
  }
  logged = true;
  const cold = coldTimes;
  const warm = warmTimes;
  const sum = (a: number[]) => a.reduce((x, y) => x + y, 0);
  const f = (n: number) => n.toFixed(1).padStart(9);
  const lines = [
    `COLD_BOOT_BREAKDOWN`,
    `  query      cold ms   warm ms`,
    ...cold.map((c, i) => `  q${String(i).padEnd(3)} ${f(c)} ${f(warm[i])}`),
    `  sum  ${f(sum(cold))} ${f(sum(warm))}`,
    `  max  ${f(Math.max(...cold))} ${f(Math.max(...warm))}`,
  ];
  // oxlint-disable-next-line no-console
  console.log(lines.join('\n'));
}

const opts = {max_samples: 1, min_samples: 1};

// On a device the runner leaves this group out unless `--run` asks for it
// (see perf-rn.ts); there is no environment to read there.
const isReactNative =
  typeof navigator !== 'undefined' && navigator.product === 'ReactNative';
const enabled =
  isReactNative ||
  (typeof process !== 'undefined' && !!process.env.COLD_BOOT_BREAKDOWN);

if (enabled) {
  describe('cold boot breakdown', () => {
    bench(
      'load replica',
      () => {
        load(makeSources(), SCALE);
      },
      opts,
    );

    bench(
      'load replica, rows only (no sources)',
      () => {
        let n = 0;
        for (const _ of rowsOnly(SCALE)) {
          n++;
        }
        return n;
      },
      opts,
    );

    bench('load replica, pushAdds', () => loadBulk(SCALE), opts);

    bench(
      'pushAdds + hydrate 21 queries, cold indexes',
      () => {
        const times = hydrateAll(loadBulk(SCALE));
        coldTimes ??= times;
      },
      opts,
    );

    bench(
      'hydrate 21 queries, warm indexes',
      function* () {
        const sources = loadBulk(SCALE);
        hydrateAll(sources);
        yield () => {
          const times = hydrateAll(sources);
          warmTimes ??= times;
          maybeLogTable();
        };
      },
      opts,
    );
  });
}
