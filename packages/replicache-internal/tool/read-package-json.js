// @ts-check

import {readFile} from 'fs/promises';

/**
 * @typedef  {{
 *   [key: string]: any;
 *   name: string;
 *   version: string;
 * }} PackageJSON
 */

/** @returns {Promise<PackageJSON>} */
export async function readPackageJSON() {
  const url = new URL('../package.json', import.meta.url);
  const s = await readFile(url, 'utf-8');
  return JSON.parse(s);
}
