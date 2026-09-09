import {makeHarness} from '../../../tools/rn-bench/src/harness.ts';
/**
 * React Native entry point for the perf harness.
 *
 * Deliberately free of DOM, Node and `react-native` imports: `tool/build.ts`
 * bundles this into a single self-contained `out/rn.js` that the Expo app in
 * `replicache-perf-rn` copies in and imports. Only `expo-sqlite` and
 * `@op-engineering/op-sqlite` stay external, supplied by the app.
 */
import {setBenchKVStore} from '../../replicache/src/bench-util.ts';
import {expoSQLiteStoreProvider} from '../../replicache/src/kv/expo-sqlite/store.ts';
import {opSQLiteStoreProvider} from '../../replicache/src/kv/op-sqlite/store.ts';
import type {StoreProvider} from '../../replicache/src/kv/store.ts';
import {benchmarks as mapLoopBenchmarks} from './benchmarks/map-loop.ts';
import {benchmarks as replicacheBenchmarks} from './benchmarks/replicache.ts';
import {setTmcwUrl} from './data.ts';

export type {BenchmarkResult} from '../../../tools/rn-bench/src/benchmark.ts';
export {setTmcwUrl};
export {
  formatAsBenchmarkJS,
  formatAsReplicache,
} from '../../../tools/rn-bench/src/format.ts';

// `replicache` plus the pure-JS `map-loop` group, which is useful for
// separating JS-engine cost from storage cost on a device. The rest are out:
// `idb` and `storage` need IndexedDB/localStorage, `hash` needs
// `crypto.subtle`, and `compare-utf8` needs TextEncoder. Importing perf.ts
// would pull all of them (and hash-wasm) into the bundle.
const harness = makeHarness([
  ...replicacheBenchmarks(),
  ...mapLoopBenchmarks(),
]);

export const {benchmarks, findBenchmarks, runBenchmarkByNameAndGroup} = harness;

/** The key/value backends the on-device harness can benchmark. */
export type Backend = 'expo' | 'op' | 'mem';

export const backends: readonly Backend[] = ['expo', 'op', 'mem'];

function storeProvider(backend: Backend): StoreProvider | 'mem' {
  switch (backend) {
    case 'expo':
      return expoSQLiteStoreProvider();
    case 'op':
      return opSQLiteStoreProvider();
    case 'mem':
      return 'mem';
  }
}

/**
 * Points every benchmark rep at `backend`. Must be called before running a
 * benchmark; there is no meaningful default on React Native, since the harness
 * otherwise falls back to IndexedDB.
 */
export function setBackend(backend: Backend): void {
  setBenchKVStore(storeProvider(backend));
}

/**
 * Configures the harness from the control server's `/next` response: which
 * backend to use, and where to fetch the tmcw fixture from (it is 9.7 MB, far
 * too large to bundle into the app).
 */
export function configure(opts: {backend: Backend; tmcwUrl: string}): void {
  setBackend(opts.backend);
  setTmcwUrl(opts.tmcwUrl);
}
