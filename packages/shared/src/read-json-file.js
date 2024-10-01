// @ts-check

import {readFile} from 'node:fs/promises';

/**
 * @typedef  {{
 *   [key: string]: any;
 *   name: string;
 *   version: string;
 * }} PackageJSON
 */

/**
 * @param {string} pathLike
 * @returns {Promise<PackageJSON>}
 */
export async function readJSONFile(pathLike) {
  const s = await readFile(pathLike, 'utf-8');
  return JSON.parse(s);
}
