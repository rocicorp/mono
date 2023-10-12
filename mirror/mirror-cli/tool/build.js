import * as esbuild from 'esbuild';
import {getExternalFromPackageJSON} from 'shared/src/tool/get-external-from-package-json.js';
import {injectRequire} from 'shared/src/tool/inject-require.js';

async function main() {
  const cli = 'out/index.mjs';
  await esbuild.build({
    entryPoints: ['src/index.ts'],
    bundle: true,
    outfile: cli,
    external: await getExternalFromPackageJSON(import.meta.url),
    platform: 'node',
    target: 'esnext',
    format: 'esm',
    sourcemap: false,
    banner: {
      js: injectRequire(),
    },
  });

  const dispatcher = 'out/dispatcher.js';
  await esbuild.build({
    entryPoints: ['dispatcher/index.ts'],
    conditions: ['workerd', 'worker', 'browser'],
    bundle: true,
    outfile: dispatcher,
    external: [],
    platform: 'browser',
    target: 'esnext',
    format: 'esm',
    sourcemap: false,
  });
}

await main();
