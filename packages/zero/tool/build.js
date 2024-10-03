// @ts-check

import * as esbuild from 'esbuild';
import {readFile} from 'node:fs/promises';
import {builtinModules} from 'node:module';
import * as path from 'node:path';
import {makeDefine, sharedOptions} from '../../shared/src/build.js';
import {getExternalFromPackageJSON} from '../../shared/src/tool/get-external-from-package-json.js';
import {isInternalPackage} from '../../shared/src/tool/internal-packages.js';

const dirname = path.dirname(new URL(import.meta.url).pathname);

/**
 * @param {string[]} parts
 * @returns {string}
 */
function basePath(...parts) {
  return path.join(dirname, '..', ...parts);
}

const externalSet = new Set([
  ...(await getExternalFromPackageJSON(import.meta.url)),
  'node:*',
  ...builtinModules,
  // better-sqlite3 is installed using a preinstall script.
  'better-sqlite3',
]);

async function getInternalDeps() {
  const s = await readFile(basePath('package.json'), 'utf-8');
  const json = JSON.parse(s);
  const packages = new Set();
  for (const key of ['dependencies', 'peerDependencies', 'devDependencies']) {
    if (json[key]) {
      for (const dep of Object.keys(json[key])) {
        if (isInternalPackage(dep)) {
          packages.add(dep);
        }
      }
    }
  }
  return [...packages];
}

/**
 * @param {string} internalPackageName
 */
async function addExternalDepsFor(internalPackageName) {
  for (const dep of await getExternalFromPackageJSON(
    basePath('..', internalPackageName),
  )) {
    externalSet.add(dep);
  }
}

// We also need to add the external dependencies of the internal
// devDependencies. This is because the devDependencies contains zero-cache and
// zero-client etc.
for (const internalDep of await getInternalDeps()) {
  await addExternalDepsFor(internalDep);
}

for (const dep of [
  'shared',
  'zero-cache',
  'zero-client',
  'zero-react',
  'zero-sqlite',
]) {
  await addExternalDepsFor(dep);
}

// temporary hack to remove replicache from the externals. This is because we
// are using the replicache source directly in the zero-client.
// This is only until we get things running.
// externalSet.delete('replicache');

const external = [...externalSet];

async function buildZeroClient() {
  const define = makeDefine('unknown');
  const entryPoints = {
    zero: basePath('src', 'zero.ts'),
    react: basePath('src', 'react.ts'),
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
