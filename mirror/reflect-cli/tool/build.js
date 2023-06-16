import * as esbuild from 'esbuild';

// npx esbuild src/index.ts --bundle --outfile=lib/index2.js --external:firebase-functions --external:firebase-admin --external:busboy --platform=node --target=esnext --format=esm

/** quasi-random identifier generator, good for less than 10k values in series */
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
  'busboy',
  'esbuild',
  'firebase-admin',
  'firebase-functions',
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
