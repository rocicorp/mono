/* oxlint-disable no-console */
import * as esbuild from 'esbuild';
import pkg from '../package.json' with {type: 'json'};

async function build() {
  const external = Object.keys(
    (pkg as unknown as Record<string, string>).dependencies ?? {},
  );

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
