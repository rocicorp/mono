// @ts-check

/* eslint-env es2022 */

import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import {pkgUp} from 'pkg-up';

/**
 * @param {string} basePath
 * @returns {Promise<string[]>}
 */
export async function getExternalFromPackageJSON(basePath) {
  const path = await pkgUp({cwd: basePath});
  if (!path) {
    throw new Error('Could not find package.json');
  }
  const x = await readFile(path, 'utf-8');
  const pkg = JSON.parse(x);

  const deps = new Set();
  for (const dep of Object.keys({
    ...pkg.dependencies,
    ...pkg.peerDependencies,
  })) {
    if (isInternalPackage(dep)) {
      for (const depDep of await getRecursiveExternals(dep)) {
        deps.add(depDep);
      }
    } else {
      deps.add(dep);
    }
  }
  return [...deps];
}

const internalPackages = [
  'cloudflare-api',
  'datadog',
  'mirror-protocol',
  'mirror-schema',
  'reflect-client',
  'reflect-protocol',
  'reflect-server',
  'reflect-shared',
  'reflect-react',
  'shared',
  'reflect-cli',
];

/**
 * @param {string} name
 */
function isInternalPackage(name) {
  // A better solution might be to check if the package is in the monorepo
  return internalPackages.includes(name);
}

/**
 * @param {string} name
 */
function getRecursiveExternals(name) {
  if (name === 'shared') {
    return getExternalFromPackageJSON(fileURLToPath(import.meta.url));
  }

  const require = createRequire(import.meta.url);
  const depPath = require.resolve(name);
  return getExternalFromPackageJSON(depPath);
}
