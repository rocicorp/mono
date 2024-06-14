// @ts-check

import * as esbuild from 'esbuild';
import {writeFile} from 'fs/promises';
import * as path from 'path';
import {makeDefine, sharedOptions} from 'shared/src/build.js';
import {fileURLToPath} from 'url';

// You can then visualize the metafile at https://esbuild.github.io/analyze/
const metafile = process.argv.includes('--metafile');

const dirname = path.dirname(fileURLToPath(import.meta.url));

/**
 * @typedef {'unknown'|'debug'|'release'} BuildMode
 */

async function build() {
  const minify = true;
  const define = makeDefine('release');
  const outfile = path.join(dirname, '..', 'out', 'index.js');
  const result = await esbuild.build({
    ...sharedOptions(minify, metafile),
    external: [],
    format: 'esm',
    platform: 'browser',
    define,
    outfile,
    entryPoints: [path.join(dirname, '..', 'src', 'index.ts')],
  });
  if (metafile) {
    await writeFile(outfile + '.meta.json', JSON.stringify(result.metafile));
  }
}

await build();
