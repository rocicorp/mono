// @ts-check

import {readFileSync, statSync} from 'node:fs';
import * as nodePath from 'node:path';
import {fileURLToPath} from 'node:url';
import colors from 'picocolors';

const internalPackages = [
  ...getGenericDependencies('packages/reflect', 'devDependencies').entries(),
].flatMap(([name, version]) => {
  if (version === '0.0.0') {
    for (const packageDir of ['packages', 'mirror']) {
      if (dirExists(path(packageDir, name))) {
        return [`${packageDir}/${name}`];
      }
    }
  }
  return [];
});

/**
 * @param {string[]} args
 */
function path(...args) {
  return nodePath.join(
    fileURLToPath(import.meta.url),
    '..',
    '..',
    '..',
    '..',
    ...args,
  );
}

/**
 * @param {string} p
 * @param {string} propName
 * @return {Map<string, string>}
 */
function getGenericDependencies(p, propName) {
  const {[propName]: prop = {}} = JSON.parse(
    readFileSync(path(p, 'package.json'), 'utf-8'),
  );
  return new Map(Object.entries(prop));
}

/**
 * @param {string} p
 * @return {Set<string>}
 */
function getDependencies(p) {
  return new Set(getGenericDependencies(p, 'dependencies').keys());
}

function getDependenciesFromCurrentPackage() {
  return getDependencies('packages/reflect');
}

/**
 * @param {string} p
 */
function dirExists(p) {
  try {
    const s = statSync(p);
    return s.isDirectory();
  } catch {
    return false;
  }
}

/**
 * @param {string} name
 */
function isInternalPackage(name) {
  return dirExists(path('packages', name)) || dirExists(path('mirror', name));
}

function main() {
  const currentDependencies = getDependenciesFromCurrentPackage();
  for (const ip of internalPackages) {
    const deps = getDependencies(ip);
    for (const dep of deps) {
      if (isInternalPackage(dep)) {
        continue;
      }
      if (!currentDependencies.has(dep)) {
        console.error(
          `Missing dependency ${colors.bold(
            dep,
          )} in packages/reflect/package.json but present in ${ip}/package.json`,
        );
        process.exit(1);
      }
    }
  }

  for (const cd of currentDependencies) {
    if (isInternalPackage(cd)) {
      console.error(
        `reflect/package.json should not have an internal dependency. Found ${colors.bold(
          cd,
        )}`,
      );
      process.exit(1);
    }

    let found = false;
    for (const ip of internalPackages) {
      const deps = getDependencies(ip);
      if (deps.has(cd)) {
        found = true;
        break;
      }
    }
    if (!found) {
      console.error(
        `Extra dependency ${colors.bold(cd)} in ${colors.bold(
          'packages/reflect/package.json',
        )} but not present in any internal package`,
      );
      process.exit(1);
    }
  }
}

main();
