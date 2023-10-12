import * as esbuild from 'esbuild';
import {getExternalFromPackageJSON} from 'shared/src/tool/get-external-from-package-json.js';
import {injectRequire} from 'shared/src/tool/inject-require.js';

async function main() {
  await esbuild.build({
    entryPoints: ['src/index.ts'],
    bundle: true,
    outfile: 'out/index.mjs',
    external: await getExternalFromPackageJSON(import.meta.url),
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
