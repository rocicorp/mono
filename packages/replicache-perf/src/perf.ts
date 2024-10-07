import {type Benchmark, runBenchmark} from './benchmark.js';
import {benchmarks as compareBenchmarks} from './benchmarks/compare-utf8.js';
import {benchmarks as hashBenchmarks} from './benchmarks/hash.js';
import {benchmarks as idbBenchmarks} from './benchmarks/idb.js';
import {benchmarks as mapLoopBenchmarks} from './benchmarks/map-loop.js';
import {benchmarks as replicacheBenchmarks} from './benchmarks/replicache.js';
import {benchmarks as storageBenchmarks} from './benchmarks/storage.js';
import {formatAsReplicache} from './format.js';

export const benchmarks = [
  ...replicacheBenchmarks(),
  ...hashBenchmarks(),
  ...storageBenchmarks(),
  ...compareBenchmarks(),
  ...mapLoopBenchmarks(),
  ...idbBenchmarks(),
];

function findBenchmark(name: string, group: string): Benchmark {
  for (const b of benchmarks) {
    if (b.name === name && b.group === group) {
      return b;
    }
  }
  throw new Error(`No benchmark named "${name}" in group "${group}"`);
}

export async function runBenchmarkByNameAndGroup(
  name: string,
  group: string,
): Promise<['result', unknown] | ['error', unknown] | undefined> {
  const b = findBenchmark(name, group);
  try {
    const result = await runBenchmark(b);
    if (!result) {
      return ['error', 'no result'];
    }
    return ['result', result];
  } catch (e) {
    return ['error', e];
  }
}

export function findBenchmarks(groups: string[], runs: string[]): Benchmark[] {
  const bs = benchmarks.filter(b => groups.includes(b.group));
  if (runs.length > 0) {
    const runRegExps = runs.map(r => new RegExp(r));
    return bs.filter(b => runRegExps.every(re => re.test(b.name)));
  }
  return benchmarks.filter(b => groups.includes(b.group));
}

export async function runAll(groups: string[], runs: string[]): Promise<void> {
  const out: HTMLElement | null = document.getElementById('out');
  if (!out) {
    return;
  }
  const benchmarks = findBenchmarks(groups, runs);
  for (const b of benchmarks) {
    try {
      const result = await runBenchmark(b);
      if (result) {
        out.textContent += formatAsReplicache(result) + '\n';
      }
    } catch (e) {
      out.textContent += `${b.name} had an error: ${e}` + '\n';
    }
  }
  out.textContent += 'Done!\n';
}
