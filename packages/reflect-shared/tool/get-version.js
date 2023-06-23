// @ts-check

import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';

/**
 * @returns {string}
 */
export function getVersion() {
  const url = new URL('../../reflect/package.json', import.meta.url);
  return JSON.parse(readFileSync(fileURLToPath(url), 'utf-8')).version;
}
