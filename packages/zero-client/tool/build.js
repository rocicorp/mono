// @ts-check

import * as esbuild from 'esbuild';
import * as path from 'node:path';
import {makeDefine, sharedOptions} from '../../shared/src/build.js';
import {getExternalFromPackageJSON} from '../../shared/src/tool/get-external-from-package-json.js';

/** @param {string[]} parts */
function basePath(...parts) {
  return path.join(
    path.dirname(new URL(import.meta.url).pathname),
    '..',
    ...parts,
  );
}

async function buildPackages() {
  let shared = sharedOptions(false, false);
  const define = makeDefine();

  await esbuild.build({
    ...shared,
    external: await getExternalFromPackageJSON(import.meta.url),
    platform: 'browser',
    define,
    entryPoints: [basePath('src', 'mod.ts')],
    outfile: 'out/zero-client.js',
  });
}

await buildPackages();
