import * as esbuild from 'esbuild';

function createRandomIdentifier(name) {
  return `${name}_${Math.random() * 10000}`.replace('.', '');
}

/**
 * Injects a global `require` function into the bundle.
 *
 *  @returns {esbuild.BuildOptions}
 */
function injectRequire() {
  const createRequireAlias = createRandomIdentifier('createRequire');
  return `import {createRequire as ${createRequireAlias}} from 'module';
var require = ${createRequireAlias}(import.meta.url);
`;
}

const external = [
  '@badrap/valita',
  '@google-cloud/firestore',
  '@rocicorp/resolver',
  'esbuild',
  'firebase',
  'miniflare',
  'nanoid',
  'open',
  'picocolors',
  'pkg-up',
  'semver',
  'validate-npm-package-name',
  'yargs',
];

async function main() {
  await esbuild.build({
    entryPoints: ['src/index.ts'],
    bundle: true,
    outfile: 'out/index.mjs',
    external,
    platform: 'node',
    target: 'esnext',
    format: 'esm',
    sourcemap: false,
    banner: {
      js: injectRequire(),
    },
  });
}

await main();
