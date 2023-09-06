// @ts-check

import {readFileSync, statSync} from 'node:fs';
import * as nodePath from 'node:path';
import {fileURLToPath} from 'node:url';
import colors from 'picocolors';

const internalPackages = /** @type const */ ([
  'mirror/reflect-cli',
  'packages/reflect-client',
  'packages/reflect-server',
  'packages/reflect-shared',
]);

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
 * @return {Set<string>}
 */
function getDependencies(p) {
  const {dependencies = {}} = JSON.parse(
    readFileSync(path(p, 'package.json'), 'utf-8'),
  );
  return new Set(Object.keys(dependencies));
}

function getDepsFromCurrentPackage() {
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
  const currentDependencies = getDepsFromCurrentPackage();
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
