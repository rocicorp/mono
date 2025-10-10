/* eslint-disable no-console */
import * as esbuild from 'esbuild';
import pkg from '../package.json' with {type: 'json'};

async function build() {
  const external = Object.keys(pkg.dependencies);

  await esbuild.build({
    bundle: true,
    target: 'es2022',
    format: 'esm',
    platform: 'neutral',
    external,
    outdir: 'out',
    entryPoints: ['src/mod.ts'],
  });
}

await build();
