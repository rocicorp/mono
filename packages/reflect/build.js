// @ts-check
/* eslint-env node, es2022 */

import * as esbuild from 'esbuild';
import {writeFile} from 'fs/promises';
import * as path from 'path';
import {fileURLToPath} from 'url';
import {makeDefine, sharedOptions} from '../shared/src/build.js';

// You can then visualize the metafile at https://esbuild.github.io/analyze/
const metafile = process.argv.includes('--metafile');
const debug = process.argv.includes('--debug');

const dirname = path.dirname(fileURLToPath(import.meta.url));

/**
 * @typedef {'unknown'|'debug'|'release'} BuildMode
 */

/**
 * @typedef {{
 *   minify?: boolean,
 *   mode?: BuildMode | undefined,
 * }} BuildOptions
 */

/**
 * @param {Partial<BuildOptions>} options
 */
async function buildESM({mode, minify = true} = {}) {
  if (debug) {
    mode = 'debug';
  } else {
    mode = 'unknown';
  }
  const shared = sharedOptions(minify, metafile);
  const result = await esbuild.build({
    ...shared,
    // Use neutral to remove the automatic define for process.env.NODE_ENV
    platform: 'neutral',
    define: makeDefine(mode),
    format: 'esm',
    entryPoints: [path.join(dirname, 'src', 'mod.ts')],
    outfile: path.join(dirname, 'out/reflect.js'),
    metafile,
  });
  if (metafile) {
    await writeFile(
      path.join(dirname, 'out/reflect.js.meta.json'),
      JSON.stringify(result.metafile),
    );
  }
}

try {
  await buildESM();
} catch {
  process.exitCode = 1;
}
