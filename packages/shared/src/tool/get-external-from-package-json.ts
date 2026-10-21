import {readFile} from 'node:fs/promises';
import {dirname, join, resolve as resolvePath} from 'node:path';
import {fileURLToPath} from 'node:url';
import {isInternalPackage} from './internal-packages.ts';

export async function getExternalFromPackageJSON(
  basePath: string,
  includePeerDeps = false,
): Promise<string[]> {
  const cwd = basePath.startsWith('file:') ? fileURLToPath(basePath) : basePath;
  const pkg = JSON.parse(await readClosestPackageJSON(cwd));

  const deps: Set<string> = new Set();
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

async function readClosestPackageJSON(cwd: string): Promise<string> {
  for (let dir = resolvePath(cwd); ; dir = dirname(dir)) {
    try {
      return await readFile(join(dir, 'package.json'), 'utf-8');
    } catch (e) {
      // ENOTDIR when the starting path is a file.
      const {code} = e as NodeJS.ErrnoException;
      if (code !== 'ENOENT' && code !== 'ENOTDIR') {
        throw e;
      }
      if (dir === dirname(dir)) {
        throw new Error('Could not find package.json');
      }
    }
  }
}

function getRecursiveExternals(
  name: string,
  includePeerDeps: boolean,
): Promise<string[]> {
  if (name === 'shared') {
    return getExternalFromPackageJSON(
      new URL(import.meta.url).pathname,
      includePeerDeps,
    );
  }
  const depPath = resolvePath(`../${name}`);

  return getExternalFromPackageJSON(depPath, includePeerDeps);
}
