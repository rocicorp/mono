// @ts-check

import * as esbuild from 'esbuild';
import * as path from 'node:path';
import {fileURLToPath} from 'node:url';
import {makeDefine, sharedOptions} from '../../shared/src/build.ts';

const dirname = path.dirname(fileURLToPath(import.meta.url));

async function buildIndex(): Promise<void> {
  const minify = true;
  const define = makeDefine('release');
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

await buildIndex();
