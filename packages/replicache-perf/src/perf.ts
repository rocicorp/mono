import {benchmarks as compareBenchmarks} from './benchmarks/compare-utf8.ts';
import {benchmarks as hashBenchmarks} from './benchmarks/hash.ts';
import {benchmarks as idbBenchmarks} from './benchmarks/idb.ts';
import {benchmarks as mapLoopBenchmarks} from './benchmarks/map-loop.ts';
import {benchmarks as replicacheBenchmarks} from './benchmarks/replicache.ts';
import {benchmarks as storageBenchmarks} from './benchmarks/storage.ts';
import {makeHarness} from './harness.ts';

const harness = makeHarness([
  ...replicacheBenchmarks(),
  ...hashBenchmarks(),
  ...storageBenchmarks(),
  ...compareBenchmarks(),
  ...mapLoopBenchmarks(),
  ...idbBenchmarks(),
]);

export const {benchmarks, findBenchmarks, runBenchmarkByNameAndGroup} = harness;
