import * as esbuild from 'esbuild';
import {checkOutfileForNodeModules} from 'shared/src/tool/check-outfile-for-node-modules.js';
import {getExternalFromPackageJSON} from 'shared/src/tool/get-external-from-package-json.js';

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

async function main() {
  const outfile = 'out/index.mjs';
  await esbuild.build({
    entryPoints: ['src/index.ts'],
    bundle: true,
    outfile,
    external: await getExternalFromPackageJSON(import.meta.url),
    platform: 'node',
    target: 'esnext',
    format: 'esm',
    sourcemap: false,
    banner: {
      js: injectRequire(),
    },
  });
  await checkOutfileForNodeModules(outfile);
}

await main();
