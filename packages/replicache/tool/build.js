// @ts-check

import * as esbuild from 'esbuild';
import {writeFile} from 'fs/promises';
import * as path from 'path';
import {fileURLToPath} from 'url';
import {sharedOptions} from '../../shared/src/build.js';
import {makeDefine} from './make-define.js';
import {readPackageJSON} from './read-package-json.js';

const forBundleSizeDashboard = process.argv.includes('--bundle-sizes');
const perf = process.argv.includes('--perf');
const debug = process.argv.includes('--debug');
// You can then visualize the metafile at https://esbuild.github.io/analyze/
const metafile = process.argv.includes('--metafile');

const dirname = path.dirname(fileURLToPath(import.meta.url));

/**
 * @typedef {'unknown'|'debug'|'release'} BuildMode
 */

/**
 * @typedef {{
 *   minify: boolean,
 *   ext: string,
 *   mode: BuildMode,
 *   external?: string[] | undefined,
 * }} BuildOptions
 */

/**
 * @param {BuildOptions} options
 */
async function buildReplicache(options) {
  const define = await makeDefine(options.mode);
  const {ext, mode, external, ...restOfOptions} = options;
  const outfile = path.join(dirname, '..', 'out', 'replicache.' + ext);
  const result = await esbuild.build({
    ...sharedOptions(options.minify, metafile),
    ...(external ? {external} : {}),
    ...restOfOptions,
    format: 'esm',
    // Use neutral to remove the automatic define for process.env.NODE_ENV
    platform: 'neutral',
    define: {
      ...define,
      ['TESTING']: 'false',
    },
    outfile,
    entryPoints: [path.join(dirname, '..', 'src', 'mod.ts')],
  });
  if (metafile) {
    await writeFile(outfile + '.meta.json', JSON.stringify(result.metafile));
  }
}

/**
 * @param {Partial<BuildOptions>} options
 */
async function buildMJS({
  minify = true,
  ext = 'js',
  mode = 'unknown',
  external,
} = {}) {
  await buildReplicache({minify, ext, mode, external});
}

async function buildCLI() {
  await esbuild.build({
    ...sharedOptions(true),
    platform: 'node',
    outfile: path.join(dirname, '..', 'out', 'cli.cjs'),
    entryPoints: [path.join(dirname, 'cli.ts')],
    format: 'cjs',
  });
}

async function isRocicorpPackage() {
  const packageJSON = await readPackageJSON();
  return packageJSON.name.startsWith('@rocicorp/');
}

if (perf) {
  await buildMJS({mode: 'release'});
} else if (forBundleSizeDashboard) {
  // Bundle external modules for the bundle size dashboard
  const external = ['node:*'];
  // We keep mjs as mjs so the dashboard does not get reset
  await Promise.all([
    buildMJS({minify: false, ext: 'mjs', external}),
    buildMJS({minify: true, ext: 'min.mjs', external}),
    buildCLI(),
  ]);
} else {
  let opts = {};
  if (debug || (await isRocicorpPackage())) {
    opts = {minify: false};
  }
  await Promise.all([buildMJS(opts), buildCLI()]);
}
