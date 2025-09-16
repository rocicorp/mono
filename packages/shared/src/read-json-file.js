/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-base-to-string, @typescript-eslint/restrict-template-expressions */
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
