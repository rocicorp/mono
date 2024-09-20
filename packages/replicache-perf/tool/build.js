// @ts-check

import * as esbuild from 'esbuild';
import * as path from 'path';
import {makeDefine, sharedOptions} from 'shared/src/build.js';
import {fileURLToPath} from 'url';

const dirname = path.dirname(fileURLToPath(import.meta.url));

/**
 * @typedef {'unknown'|'debug'|'release'} BuildMode
 */

async function buildIndex() {
  const minify = true;
  const define = makeDefine('release');
  const outfile = path.join(dirname, '..', 'out', 'index.js');
  await esbuild.build({
    ...sharedOptions(minify),
    external: [],
    format: 'esm',
    platform: 'browser',
    define,
    outfile,
    entryPoints: [path.join(dirname, '..', 'src', 'index.ts')],
  });
}

async function buildRunner() {
  const define = makeDefine('release');
  const outfile = path.join(dirname, '..', 'out', 'runner.js');
  await esbuild.build({
    external: ['node:*', '@web/*', 'command-line-*', 'get-port', 'playwright'],
    bundle: true,
    target: 'esNext',
    format: 'esm',
    platform: 'node',
    define,
    outfile,
    entryPoints: [path.join(dirname, '..', 'src', 'runner.ts')],
  });
}

await buildIndex();
await buildRunner();
