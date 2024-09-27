// @ts-check

import * as esbuild from 'esbuild';
import {writeFile} from 'node:fs/promises';
import * as path from 'node:path';
import process from 'node:process';
import {fileURLToPath} from 'node:url';
import {makeDefine, sharedOptions} from '../../shared/src/build.js';
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
 * @param {string[]} parts
 * @returns {string}
 */
function basePath(...parts) {
  return path.join(dirname, '..', ...parts);
}

/**
 * @param {BuildOptions} options
 */
async function buildReplicache(options) {
  const define = makeDefine(options.mode);
  const {ext, mode, external, ...restOfOptions} = options;
  const outfile = basePath('out', 'replicache.' + ext);
  const entryPoints = {
    replicache: basePath('src', 'mod.ts'),
    ...(forBundleSizeDashboard
      ? {}
      : {impl: basePath('src', 'replicache-impl.ts')}),
  };
  const result = await esbuild.build({
    ...sharedOptions(options.minify, metafile),
    ...(external ? {external} : {}),
    ...restOfOptions,
    bundle: true,
    splitting: !forBundleSizeDashboard,
    format: 'esm',
    // Use neutral to remove the automatic define for process.env.NODE_ENV
    platform: 'neutral',
    define,
    outdir: basePath('out'),
    entryPoints,
    outExtension: {'.js': `.${ext}`},
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
    outfile: basePath('out', 'cli.cjs'),
    entryPoints: [basePath('tool', 'cli.ts')],
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
