// @ts-check

import * as path from 'node:path';
import {fileURLToPath} from 'node:url';
import * as esbuild from 'esbuild';
import {makeDefine, sharedOptions} from '../../shared/src/build.ts';

const dirname = path.dirname(fileURLToPath(import.meta.url));

async function buildIndex(): Promise<void> {
  const minify = true;
  const define = {
    ...makeDefine('release'),
    'import.meta.env': 'undefined',
  };
  await esbuild.build({
    ...sharedOptions(minify),
    external: ['node:*', 'expo*'],
    format: 'esm',
    platform: 'browser',
    splitting: true,
    define,
    outdir: path.join(dirname, '..', 'out'),
    entryPoints: [path.join(dirname, '..', 'src', 'index.ts')],
  });
}

/**
 * A single self-contained ESM file for React Native. Metro never resolves any
 * monorepo TypeScript: the runner copies this one file into the Expo app.
 */
async function buildRN(): Promise<void> {
  // Minified by default to match what ships, but Hermes CPU profiles are
  // unreadable without real function names — PERF_RN_NO_MINIFY=1 keeps them.
  const minify = process.env.PERF_RN_NO_MINIFY !== '1';
  const define = {
    ...makeDefine('release'),
    'import.meta.env': 'undefined',
    // Hermes cannot parse `import.meta` at all. The only use is the default
    // tmcw fixture URL, which the RN runner overrides via setTmcwUrl().
    'import.meta.url': '""',
  };
  await esbuild.build({
    ...sharedOptions(minify),
    external: ['node:*', 'expo-sqlite', '@op-engineering/op-sqlite'],
    format: 'esm',
    platform: 'neutral',
    splitting: false,
    define,
    outfile: path.join(dirname, '..', 'out', 'rn.js'),
    entryPoints: [path.join(dirname, '..', 'src', 'rn.ts')],
  });
}

await buildIndex();
await buildRN();
