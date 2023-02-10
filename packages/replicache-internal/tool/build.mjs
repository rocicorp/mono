// @ts-check

import * as esbuild from 'esbuild';
import {readFile} from 'fs/promises';

const forBundleSizeDashboard = process.argv.includes('--bundle-sizes');
const perf = process.argv.includes('--perf');

const sharedOptions = {
  bundle: true,
  target: 'es2018',
  mangleProps: /^_./,
  reserveProps: /^__.*__$/,
};

async function readPackageJSON() {
  const url = new URL('../package.json', import.meta.url);
  const s = await readFile(url, 'utf-8');
  return JSON.parse(s);
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
    ...sharedOptions,
    ...restOfOptions,
    // Use neutral to remove the automatic define for process.env.NODE_ENV
    platform: 'neutral',
    outfile: 'out/replicache.' + ext,
    entryPoints: ['src/mod.ts'],
    define: {
      REPLICACHE_VERSION: JSON.stringify((await readPackageJSON()).version),
    },
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
    ...sharedOptions,
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
  await Promise.all([buildMJS(), buildCJS(), buildCLI()]);
}
