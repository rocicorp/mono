import {readPackageJSON} from './read-package-json.js';
import {makeDefine as makeBasicDefine} from '../../shared/src/build.js';

/**
 * @param {'debug'|'release'|'unknown'} mode
 * @return {Promise<Record<string, string>>}
 */

export async function makeDefine(mode) {
  return {
    ...makeBasicDefine(mode),
    REPLICACHE_VERSION: JSON.stringify((await readPackageJSON()).version),
  };
}
