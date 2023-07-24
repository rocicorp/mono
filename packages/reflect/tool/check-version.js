// @ts-check

import {readFileSync} from 'node:fs';
import * as path from 'path';
import {fileURLToPath} from 'node:url';

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
function checkFileForVersion(fileName, version) {
  const js = read(fileName);
  if (!new RegExp('var [a-zA-Z0-9]+ ?= ?"' + version + '";', 'g').test(js)) {
    console.error(`Did not find expected version ${version} in ${fileName}`);
    process.exit(1);
  }
}

const {version} = JSON.parse(read('package.json'));

checkFileForVersion('index.js', version);
checkFileForVersion('client.js', version);
checkFileForVersion('server.js', version);
