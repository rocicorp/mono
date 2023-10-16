// @ts-check

import {opendir, readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import * as path from 'path';
import colors from 'picocolors';

const {bold} = colors;

/**
 * @returns {Promise<string>}
 */
async function versionFromPackageJSON() {
  const s = await readFile(
    fileURLToPath(new URL('../package.json', import.meta.url)),
    'utf-8',
  );
  return JSON.parse(s).version;
}

/**
 * @param {string} fileName
 * @param {string} version
 */
async function fileContainsVersionString(fileName, version) {
  const js = await readFile(fileName, 'utf-8');
  return new RegExp(
    'var [a-zA-Z0-9]+ ?= ?"' + version.replace(/([+.])/g, '\\$1') + '";',
    'g',
  ).test(js);
}

/**
 * Checks that there is a file in dir that contains the version string
 * @param {string} dir
 * @param {string} version
 */
async function checkFilesForVersion(dir, version) {
  /** @type string[] */
  const files = [];
  for await (const entry of await opendir(dir)) {
    if (!entry.isFile() || !entry.name.endsWith('.js')) {
      continue;
    }
    if (
      await fileContainsVersionString(path.join(dir, '/', entry.name), version)
    ) {
      return;
    }
    files.push(entry.name);
  }

  console.error(
    `Version string ${bold(version)} not found in any of these files in ${bold(
      dir,
    )} dir:\n  ${files.join('\n  ')}`,
  );
  process.exit(1);
}

await checkFilesForVersion('out', await versionFromPackageJSON());
