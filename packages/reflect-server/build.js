// @ts-check
/* eslint-env node, es2022 */

import * as esbuild from 'esbuild';
import {writeFile} from 'fs/promises';
import * as path from 'path';
import {fileURLToPath} from 'url';
import {sharedOptions} from '../shared/src/build.js';

const metafile = process.argv.includes('--metafile');

const dirname = path.dirname(fileURLToPath(import.meta.url));

async function buildESM() {
  const outfile = path.join(dirname, 'out', 'reflect-server.js');
  const result = await buildInternal({
    entryPoints: [path.join(dirname, 'src', 'mod.ts')],
    outfile,
  });
  if (metafile) {
    await writeFile(outfile + '.meta.json', JSON.stringify(result.metafile));
  }
}

function buildExample() {
  return buildInternal({
    entryPoints: [path.join(dirname, 'example', 'index.ts')],
    outdir: path.join(dirname, 'out', 'example'),

    // Jest builds this target and tries to load it... Maybe related to
    // `testEnvironment: 'miniflare'`
    external: [],
  });
}

function buildCLI() {
  return buildInternal({
    entryPoints: [path.join(dirname, 'tool', 'cli.ts')],
    outfile: path.join(dirname, 'out', 'cli.js'),
  });
}

/**
 * @param {Partial<import("esbuild").BuildOptions>} options
 */
function buildInternal(options) {
  const shared = sharedOptions(true, metafile);
  return esbuild.build({
    ...shared,
    ...options,
  });
}

try {
  await Promise.all([buildESM(), buildExample(), buildCLI()]);
} catch {
  process.exitCode = 1;
}
