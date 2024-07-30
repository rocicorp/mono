// @ts-check
/* eslint-env node, es2022 */

import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';

const external = [
  'node:*',
  '@badrap/valita',
  '@rocicorp/datadog-util',
  '@rocicorp/lock',
  '@rocicorp/logger',
  '@rocicorp/resolver',
  'replicache',
];

/**
 * @param {boolean=} minify
 * @param {boolean=} metafile
 */
export function sharedOptions(minify = true, metafile = false) {
  const opts = /** @type {const} */ ({
    bundle: true,
    target: 'es2022',
    format: 'esm',
    external,
    minify,
    sourcemap: true,
    metafile,
  });
  if (minify) {
    return /** @type {const} */ ({
      ...opts,
      mangleProps: /^_./,
      reserveProps: /^__.*__$/,
    });
  }
  return opts;
}

/**
 * @param {string} name
 * @return {string}
 */

function getVersion(name) {
  const url = new URL(`../../${name}/package.json`, import.meta.url);
  const s = readFileSync(fileURLToPath(url), 'utf-8');
  return JSON.parse(s).version;
}

/**
 * @param {'debug'|'release'|'unknown'} mode
 * @return {Record<string, string>}
 */
export function makeDefine(mode = 'unknown') {
  /** @type {Record<string, string>} */
  const define = {
    ['REPLICACHE_VERSION']: JSON.stringify(getVersion('replicache')),
    ['ZERO_VERSION']: JSON.stringify(getVersion('zero-client')),
    ['TESTING']: 'false',
  };
  if (mode === 'unknown') {
    return define;
  }
  return {
    ...define,
    'process.env.NODE_ENV': mode === 'debug' ? '"development"' : '"production"',
  };
}
