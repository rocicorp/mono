import {readFile} from 'fs/promises';
import path from 'node:path';
import {pkgUp} from 'pkg-up';
import {Range, validRange} from 'semver';
import {isSupportedSemverRange} from 'shared/src/is-supported-semver-range.js';

/**
 * This finds the version of the reflect-server that an app is depending on. It
 * does this by finding the first package.json that has @rocicorp/reflect as a
 * dependency and returns the version from there.
 */
export function findServerVersionRange(appPath: string): Promise<Range> {
  return findServerVersionRangeInternal(appPath, appPath);
}

async function findServerVersionRangeInternal(
  appPath: string,
  startAppPath: string,
): Promise<Range> {
  const packageName = '@rocicorp/reflect';
  const pkg = await pkgUp({cwd: appPath});
  if (!pkg) {
    throw new Error(
      `No dependency on "${packageName}" found for \`${startAppPath}\`. Make sure you have a \`package.json\` file with "${packageName}" in your "dependencies".`,
    );
  }

  const {dependencies = {}, devDependencies = {}} = JSON.parse(
    await readFile(pkg, 'utf-8'),
  );
  const v = dependencies[packageName] || devDependencies[packageName];
  if (v) {
    if (validRange(v) === null) {
      throw new Error(
        `Invalid version range "${v}" for "${packageName}" in \`${pkg}\`.`,
      );
    }
    const range = new Range(v);
    if (!isSupportedSemverRange(range)) {
      throw new Error(
        `Unsupported version range "${v}" for "${packageName}" in \`${pkg}\`. We do not support pinning the version.`,
      );
    }
    return range;
  }

  return findServerVersionRangeInternal(
    path.join(path.dirname(pkg), '..'),
    startAppPath,
  );
}
