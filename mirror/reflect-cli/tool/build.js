// @ts-check

import * as esbuild from 'esbuild';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {getVersion} from '../../../packages/reflect-shared/tool/get-version.js';
import {getExternalFromPackageJSON} from '../../../packages/shared/src/tool/get-external-from-package-json.js';
import {injectRequire} from '../../../packages/shared/src/tool/inject-require.js';

/**
 * @returns {Promise<{name: string; version: string}>}
 * @param {string | URL} relPath
 */
async function packageJSON(relPath) {
  const s = await readFile(
    fileURLToPath(new URL(relPath, import.meta.url)),
    'utf-8',
  );
  return JSON.parse(s);
}

const reflectVersion = getVersion();
const reflectCliName = (await packageJSON('../package.json')).name;

async function main() {
  const outfile = 'out/index.mjs';
  await esbuild.build({
    entryPoints: ['src/index.ts'],
    bundle: true,
    outfile,
    external: await getExternalFromPackageJSON(import.meta.url),
    platform: 'node',
    target: 'esnext',
    format: 'esm',
    sourcemap: false,
    banner: {
      js: injectRequire(),
    },
    define: {
      REFLECT_VERSION: JSON.stringify(reflectVersion),
      REFLECT_CLI_NAME: JSON.stringify(reflectCliName),
    },
  });
}

await main();
