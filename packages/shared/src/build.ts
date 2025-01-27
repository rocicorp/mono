import {readFileSync} from 'node:fs';

const external = [
  'node:*',
  '@badrap/valita',
  '@rocicorp/datadog-util',
  '@rocicorp/lock',
  '@rocicorp/logger',
  '@rocicorp/resolver',
  'replicache',
];

export function sharedOptions(
  minify: boolean | undefined = true,
  metafile: boolean | undefined = false,
) {
  const opts = {
    bundle: true,
    target: 'es2022',
    format: 'esm',
    external,
    minify,
    sourcemap: true,
    metafile,
  } as const;
  if (minify) {
    return /** @type {const} */ {
      ...opts,
      mangleProps: /^_./,
      reserveProps: /^__.*__$/,
    } as const;
  }
  return opts;
}

function getVersion(name: string): string {
  const url = new URL(`../../${name}/package.json`, import.meta.url);
  const s = readFileSync(url, 'utf-8');
  return JSON.parse(s).version;
}

export function makeDefine(
  mode: 'debug' | 'release' | 'unknown' = 'unknown',
): Record<string, string> {
  /** @type {Record<string, string>} */
  const define: Record<string, string> = {
    ['process.env.REPLICACHE_VERSION']: JSON.stringify(
      getVersion('replicache'),
    ),
    ['process.env.ZERO_VERSION']: JSON.stringify(getVersion('zero')),
    ['process.env.DISABLE_MUTATION_RECOVERY']: 'false',
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
