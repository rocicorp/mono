// @ts-check
/* eslint-env node, es2022 */

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
 * @param {'debug'|'release'|'unknown'} mode
 * @return {Record<string, string>}
 */
export function makeDefine(mode) {
  /** @type {Record<string, string>} */
  const define = {};
  if (mode === 'unknown') {
    return define;
  }
  return {
    ...define,
    'process.env.NODE_ENV': mode === 'debug' ? '"development"' : '"production"',
  };
}
