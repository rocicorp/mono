import {readFile} from 'fs/promises';

/**
 * @param {'debug'|'release'} mode
 * @param {boolean} dd31
 * @return {Promise<Record<string, string>>}
 */

export async function makeDefine(mode, dd31) {
  return {
    'process.env.NODE_ENV': mode === 'debug' ? '"development"' : '"production"',
    'REPLICACHE_VERSION': JSON.stringify((await readPackageJSON()).version),
    'DD31': JSON.stringify(dd31),
  };
}

async function readPackageJSON() {
  const url = new URL('../package.json', import.meta.url);
  const s = await readFile(url, 'utf-8');
  return JSON.parse(s);
}
