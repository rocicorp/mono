// @ts-check

/* eslint-env es2022 */

import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {pkgUp} from 'pkg-up';
import {isInternalPackage} from './internal-packages.js';

/**
 * @param {string} basePath
 * @returns {Promise<string[]>}
 */
export async function getExternalFromPackageJSON(basePath) {
  const result = new Set();
  await getExternalFromPackageJSONInternal(basePath, new Set(), result);
  return [...result];
}

/**
 * @param {string} basePath
 * @param {Set<string>} visited
 * @param {Set<string>} result
 * @returns {Promise<void>}
 */
async function getExternalFromPackageJSONInternal(basePath, visited, result) {
  const path = await pkgUp({cwd: basePath});
  if (!path) {
    throw new Error('Could not find package.json');
  }
  const pkg = JSON.parse(await readFile(path, 'utf-8'));

  if (visited.has(pkg.name)) {
    return;
  }
  visited.add(pkg.name);

  for (const dep of Object.keys({
    ...pkg.dependencies,
    ...pkg.peerDependencies,
  })) {
    if (isInternalPackage(dep)) {
      await getRecursiveExternals(dep, visited, result);
    } else {
      result.add(dep);
    }
  }
}

/**
 * @param {string} name
 * @param {Set<string>} visited
 * @param {Set<string>} result
 */
async function getRecursiveExternals(name, visited, result) {
  if (name === 'shared') {
    await getExternalFromPackageJSONInternal(import.meta.url, visited, result);
    return;
  }

  const require = createRequire(import.meta.url);
  const depPath = require.resolve(name);
  await getExternalFromPackageJSONInternal(depPath, visited, result);
}
