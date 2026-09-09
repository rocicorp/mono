/**
 * Runs the Replicache benchmarks on a React Native device.
 *
 * Everything generic — device drivers, the HTTP control server, launching Expo,
 * Hermes profiling — lives in `tools/rn-bench`. This file supplies only what is
 * specific to this package: which benchmarks exist, the key/value backend axis,
 * and the 9.7 MB tmcw fixture, which is served rather than bundled.
 */
import {createReadStream} from 'node:fs';
import * as path from 'node:path';
import {fileURLToPath} from 'node:url';
import {runRnBench} from '../../../tools/rn-bench/src/runner.ts';
import {benchmarks as mapLoopBenchmarks} from './benchmarks/map-loop.ts';
import {benchmarks as replicacheBenchmarks} from './benchmarks/replicache.ts';

const rootDir = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');

await runRnBench({
  rootDir,
  cliName: 'perf:rn',
  globalName: '__replicachePerf',

  // Must match the harness in rn.ts.
  listBenchmarks: () =>
    [...replicacheBenchmarks(), ...mapLoopBenchmarks()].map(b => ({
      name: b.name,
      group: b.group,
    })),

  variants: {
    flag: 'backend',
    values: ['expo', 'op', 'mem'],
    describe: 'Key/value backends',
  },

  nextExtras: port => ({tmcwUrl: `http://localhost:${port}/tmcw.json`}),

  extraRoutes: {
    '/tmcw.json': (_req, res) => {
      res.writeHead(200, {'content-type': 'application/json'});
      createReadStream(path.join(rootDir, 'resources', 'tmcw.json')).pipe(res);
    },
  },

  configureExpr: (variant, port) =>
    `__replicachePerf.configure({backend: ${JSON.stringify(
      variant,
    )}, tmcwUrl: ${JSON.stringify(`http://localhost:${port}/tmcw.json`)}})`,
});
