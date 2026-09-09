/**
 * Runs the ZQL benchmarks on a React Native device.
 *
 * Everything generic — device drivers, the HTTP control server, launching Expo,
 * Hermes profiling — lives in `tools/rn-bench`. There is nothing to configure
 * per run here: these benchmarks are pure in-memory IVM, so unlike the
 * Replicache harness there is no storage backend to select and no fixture to
 * serve.
 */
import * as path from 'node:path';
import {fileURLToPath} from 'node:url';
import {runRnBench} from '../../../tools/rn-bench/src/runner.ts';

const rootDir = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');

await runRnBench({
  rootDir,
  cliName: 'perf:rn',
  buildScript: 'tool/build-rn.ts',
  globalName: '__zqlPerf',

  // Read from the built bundle rather than re-imported here, so the list can
  // never drift from what the device actually runs. The bundle is plain ESM
  // with no React Native imports, so Node can load it directly.
  listBenchmarks: async () => {
    const bundle = (await import(path.join(rootDir, 'out', 'rn.js'))) as {
      benchmarks: {name: string; group: string}[];
    };
    return bundle.benchmarks.map(({name, group}) => ({name, group}));
  },

  configureExpr: () => '__zqlPerf.configure()',
});
