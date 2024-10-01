// @ts-check

import * as esbuild from 'esbuild';
import * as path from 'node:path';
import {makeDefine, sharedOptions} from '../../shared/src/build.js';
import {getExternalFromPackageJSON} from '../../shared/src/tool/get-external-from-package-json.js';

const dirname = path.dirname(new URL(import.meta.url).pathname);

/**
 * @param {string[]} parts
 * @returns {string}
 */
function basePath(...parts) {
  return path.join(dirname, '..', ...parts);
}

const external = await getExternalFromPackageJSON(import.meta.url);

async function buildZeroClient() {
  const define = makeDefine('unknown');
  const entryPoints = {
    zero: basePath('src', 'zero.ts'),
  };
  await esbuild.build({
    ...sharedOptions(false, false),
    external,
    splitting: true,
    // Use neutral to remove the automatic define for process.env.NODE_ENV
    platform: 'neutral',
    define,
    outdir: basePath('out'),
    entryPoints,
  });
}

await buildZeroClient();
