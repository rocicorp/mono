// @ts-check

/* eslint-env es2022 */

import {readFile} from 'node:fs/promises';
import {pkgUp} from 'pkg-up';
import {isInternalPackage} from './internal-packages.js';

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

/**
 * @param {string} name
 */
function getRecursiveExternals(name) {
  if (name === 'shared') {
    return getExternalFromPackageJSON(new URL(import.meta.url).pathname);
  }
  const depPath = new URL(`../../../${name}/package.json`, import.meta.url)
    .pathname;
  return getExternalFromPackageJSON(depPath);
}
