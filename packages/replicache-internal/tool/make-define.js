import {readPackageJSON} from './read-package-json.js';

/**
 * @param {'debug'|'release'|'unknown'} mode
 * @return {Promise<Record<string, string>>}
 */

export async function makeDefine(mode) {
  const define = {
    REPLICACHE_VERSION: JSON.stringify((await readPackageJSON()).version),
  };
  if (mode === 'unknown') {
    return define;
  }
  return {
    ...define,
    'process.env.NODE_ENV': mode === 'debug' ? '"development"' : '"production"',
  };
}
