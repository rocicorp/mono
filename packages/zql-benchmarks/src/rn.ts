/**
 * React Native entry point for the ZQL benchmarks.
 *
 * Importing a `.bench.ts` module registers its benchmarks as a side effect —
 * they call `bench`/`describe` from `shared/src/bench.ts`, which the RN build
 * rewrites to `tools/rn-bench/src/mitata-shim.ts` (see tool/build-rn.ts). So
 * the same files feed both the Vitest/mitata suite and the device harness, and
 * there is no second copy to keep in step.
 *
 * Only the modules that run without Node are listed. The rest of the suite
 * needs PostgreSQL (`chinook-*`, `planner-*`, `zbugs`), `zero-cache`, or
 * ZQLite's native `@rocicorp/zero-sqlite3`, none of which exist on a phone.
 */
import {formatAsReplicache} from '../../../tools/rn-bench/src/format.ts';
import {makeHarness} from '../../../tools/rn-bench/src/harness.ts';
import {collectedBenchmarks} from '../../../tools/rn-bench/src/mitata-shim.ts';

// Side-effecting imports, in the order their groups should appear.
import './ivm-memory.bench.ts';
import './array-view-relationships.bench.ts';
import './array-view-transaction.bench.ts';
import './cold-boot-hydration.bench.ts';
import './memory-ivm-deopt.bench.ts';
import './debug-row-vended.bench.ts';
import './query-hash.bench.ts';

export type {BenchmarkResult} from '../../../tools/rn-bench/src/benchmark.ts';
export {
  formatAsBenchmarkJS,
  formatAsReplicache,
} from '../../../tools/rn-bench/src/format.ts';

const harness = makeHarness(collectedBenchmarks());

export const {benchmarks, findBenchmarks, runBenchmarkByNameAndGroup} = harness;

/**
 * Present for symmetry with the Replicache harness, which uses it to pick a
 * key/value backend. These benchmarks run entirely in memory and have nothing
 * to configure, but the host app and the profiler both call it.
 */
export function configure(_opts?: unknown): void {
  // Nothing to configure.
}

// Referenced so the formatter is not dropped from the bundle when the app only
// reaches it through the profiler's injected expression.
export const __formatter = formatAsReplicache;
