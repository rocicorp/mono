// @ts-check

import * as esbuild from 'esbuild';
import * as fs from 'node:fs';
import * as path from 'node:path';
import {fileURLToPath} from 'node:url';
import {makeDefine, sharedOptions} from '../../shared/src/build.js';
import {getExternalFromPackageJSON} from '../../shared/src/tool/get-external-from-package-json.js';

/** @param {string[]} parts */
function basePath(...parts) {
  return path.join(
    path.dirname(fileURLToPath(import.meta.url)),
    '..',
    ...parts,
  );
}

async function buildPackages() {
  // let's just never minify for now, it's constantly getting in our and our
  // user's way. When we have an automated way to do both minified and non-
  // minified builds we can re-enable this.
  const minify = false;
  let shared = sharedOptions(minify, false);
  const define = makeDefine();

  fs.rmSync(basePath('out'), {recursive: true, force: true});

  await esbuild.build({
    ...shared,
    external: await getExternalFromPackageJSON(import.meta.url),
    platform: 'browser',
    define: {
      ...define,
      ['TESTING']: 'false',
    },
    format: 'esm',
    entryPoints: [basePath('src', 'index.ts')],
    bundle: true,
    outfile: 'out/zql.js',
  });
}

await buildPackages();
