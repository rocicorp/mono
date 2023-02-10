import {readFile} from 'fs/promises';

/**
 * @param {'debug'|'release'|'unknown'} mode
 * @param {boolean} dd31
 * @return {Promise<Record<string, string>>}
 */

export async function makeDefine(mode, dd31) {
  const define = {
    REPLICACHE_VERSION: JSON.stringify((await readPackageJSON()).version),
    DD31: JSON.stringify(dd31),
  };
  if (mode === 'unknown') {
    return define;
  }
  return {
    ...define,
    'process.env.NODE_ENV': mode === 'debug' ? '"development"' : '"production"',
  };
}

async function readPackageJSON() {
  const url = new URL('../package.json', import.meta.url);
  const s = await readFile(url, 'utf-8');
  return JSON.parse(s);
}
