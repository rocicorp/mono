// @ts-check

import {readFileSync} from 'node:fs';

/**
 * @returns {string}
 */
export function getVersion() {
  const url = new URL('../../reflect/package.json', import.meta.url);
  return JSON.parse(readFileSync(url, 'utf-8')).version;
}
