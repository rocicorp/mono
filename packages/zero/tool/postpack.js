import {readFile, writeFile} from 'node:fs/promises';
import {internalDeps} from './internal-deps.js';

// See comment in postpack.js for why we need to add internal dependencies to
// the package.json file.

/**
 * @param {URL} path
 * @returns {Promise<void>}
 */
async function addZeroInternalPackages(path) {
  const x = await readFile(path, 'utf-8');
  const pkg = JSON.parse(x);

  for (const key of internalDeps) {
    pkg.devDependencies[key] = '0.0.0';
  }

  await writeFile(path, JSON.stringify(pkg, null, 2) + '\n');
}

await addZeroInternalPackages(new URL('../package.json', import.meta.url));
