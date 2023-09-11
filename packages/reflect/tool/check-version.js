// @ts-check

import {readFileSync} from 'node:fs';
import {opendir} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import * as path from 'path';
import colors from 'picocolors';

const {bold} = colors;

/**
 * @param {string} fileName
 * @returns {string}
 */
function read(fileName) {
  return readFileSync(
    path.join(path.dirname(fileURLToPath(import.meta.url)), '..', fileName),
    'utf-8',
  );
}

/**
 * @param {string} fileName
 * @param {string} version
 */
function fileContainsVersionString(fileName, version) {
  const js = readFileSync(fileName, 'utf-8');
  return new RegExp('var [a-zA-Z0-9]+ ?= ?"' + version + '";', 'g').test(js);
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
    if (fileContainsVersionString(path.join(dir, '/', entry.name), version)) {
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

const {version} = JSON.parse(read('package.json'));

checkFilesForVersion('out', version);
