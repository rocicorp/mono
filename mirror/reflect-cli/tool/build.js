import * as esbuild from 'esbuild';
import {checkOutfileForNodeModules} from 'shared/src/tool/check-outfile-for-node-modules.js';
import {getExternalFromPackageJSON} from 'shared/src/tool/get-external-from-package-json.js';
import {injectRequire} from 'shared/src/tool/inject-require.js';

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
