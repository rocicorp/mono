// @ts-check

import * as esbuild from 'esbuild';
import * as path from 'node:path';
import {fileURLToPath} from 'node:url';
import {makeDefine, sharedOptions} from '../../shared/src/build.ts';

const dirname = path.dirname(fileURLToPath(import.meta.url));

async function buildIndex(): Promise<void> {
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

await buildIndex();
