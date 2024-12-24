// @ts-check

/* eslint-env es2022 */

import {readFile} from 'node:fs/promises';
import {packageUp} from 'package-up';
import {resolve as resolvePath} from 'node:path';
import {fileURLToPath} from 'node:url';
import {isInternalPackage} from './internal-packages.js';

/**
 * @param {string} basePath
 * @param {boolean} includePeerDeps
 * @returns {Promise<string[]>}
 */
export async function getExternalFromPackageJSON(basePath, includePeerDeps) {
  const cwd = basePath.startsWith('file:') ? fileURLToPath(basePath) : basePath;
  const path = await packageUp({cwd});

  if (!path) {
    throw new Error('Could not find package.json');
  }
  const x = await readFile(path, 'utf-8');
  const pkg = JSON.parse(x);

  const deps = new Set();
  for (const dep of Object.keys({
    ...pkg.dependencies,
    ...(includePeerDeps ? pkg.peerDependencies : {}),
  })) {
    if (isInternalPackage(dep)) {
      for (const depDep of await getRecursiveExternals(dep, includePeerDeps)) {
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
 * @param {boolean} includePeerDeps
 */
function getRecursiveExternals(name, includePeerDeps) {
  if (name === 'shared') {
    return getExternalFromPackageJSON(
      new URL(import.meta.url).pathname,
      includePeerDeps,
    );
  }
  const depPath = resolvePath(`../${name}`);

  return getExternalFromPackageJSON(depPath, includePeerDeps);
}
