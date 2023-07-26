// @ts-check

/* eslint-env es2022 */

import {readFile} from 'fs/promises';
import {pkgUp} from 'pkg-up';

const internalPackages = ['shared', 'reflect-cli'];
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

  const deps = new Set(Object.keys(pkg.dependencies));
  for (const internalPackage of internalPackages) {
    deps.delete(internalPackage);
  }
  return [...deps];
}
