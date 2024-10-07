import {readFile, writeFile} from 'node:fs/promises';
import {internalDeps} from './internal-deps.js';

// The reason for this script (and postpack) is to remove internal dependencies
// from the package.json file. We need to do this because we do not publish the
// internal dependencies to npm. We still need to keep the dependencies in the
// package.json file so that turbo can build and run things in the right order.
//
// We cannot keep the internal packages in `devDependencies` because we have a
// `preinstall` script to install better-sqlite3 from source and this is using
// `npm install` which resoves and tries to download the internal packages from
// npm.
//
// The correct solution is to prebuild the better-sqlite3 binary and upload it
// to npm.

/**
 * @param {URL} path
 * @returns {Promise<void>}
 */
async function removeZeroInternalPackages(path) {
  const x = await readFile(path, 'utf-8');
  const pkg = JSON.parse(x);

  for (const key of internalDeps) {
    delete pkg.devDependencies[key];
  }

  await writeFile(path, JSON.stringify(pkg, null, 2) + '\n');
}

await removeZeroInternalPackages(new URL('../package.json', import.meta.url));
