// @ts-check

import * as esbuild from 'esbuild';
import {makeDefine} from './make-define.mjs';

const forBundleSizeDashboard = process.argv.includes('--bundle-sizes');
const perf = process.argv.includes('--perf');
const debug = process.argv.includes('--debug');
const dd31 = process.argv.includes('--dd31');

/**
 * @param {boolean} minify
 * @returns {{
 *   bundle: boolean;
 *   target: string;
 *   mangleProps?: RegExp;
 *   reserveProps?: RegExp;
 * }}
 */
function sharedOptions(minify) {
  const opts = {
    bundle: true,
    target: 'es2018',
  };
  if (minify) {
    return {...opts, mangleProps: /^_./, reserveProps: /^__.*__$/};
  }
  return opts;
}

/**
 * @param {{
 *   format: "esm" | "cjs";
 *   minify: boolean;
 *   ext: string;
 *   sourcemap: boolean;
 * }} options
 */
async function buildReplicache(options) {
  const {ext, ...restOfOptions} = options;
  await esbuild.build({
    ...sharedOptions(options.minify),
    ...restOfOptions,
    // Use neutral to remove the automatic define for process.env.NODE_ENV
    platform: 'neutral',
    outfile: 'out/replicache.' + ext,
    entryPoints: ['src/mod.ts'],
    define: await makeDefine('release', dd31),
  });
}

async function buildMJS({minify = true, ext = 'mjs', sourcemap = true} = {}) {
  await buildReplicache({format: 'esm', minify, ext, sourcemap});
}

async function buildCJS({minify = true, ext = 'js', sourcemap = true} = {}) {
  await buildReplicache({format: 'cjs', minify, ext, sourcemap});
}

async function buildCLI() {
  await esbuild.build({
    ...sharedOptions(true),
    platform: 'node',
    external: ['node:*'],
    outfile: 'out/cli.cjs',
    entryPoints: ['tool/cli.ts'],
    minify: true,
  });
}

if (perf) {
  await buildMJS();
} else if (forBundleSizeDashboard) {
  await Promise.all([
    buildMJS({minify: false, ext: 'mjs'}),
    buildMJS({minify: true, ext: 'min.mjs'}),
    buildCJS({minify: false, ext: 'js'}),
    buildCJS({minify: true, ext: 'min.js'}),
    buildCLI(),
  ]);
} else {
  let opts = {};
  if (debug) {
    opts = {minify: false};
  }
  await Promise.all([buildMJS(opts), buildCJS(opts), buildCLI()]);
}
