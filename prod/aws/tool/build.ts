/* eslint-disable no-console */
import {resolver} from '@rocicorp/resolver';
import * as esbuild from 'esbuild';
import {fork} from 'node:child_process';
import {readdirSync} from 'node:fs';
import {join} from 'node:path';
import pkg from '../package.json' with {type: 'json'};

/**
 * bundles each lambda entrypoint in the src/* directory and
 * writes the entrypoints to out/.
 */
async function build() {
  const deps = [
    ...pkg.bundleDependencies,
    ...Object.keys(pkg.peerDependencies),
  ];

  await esbuild.build({
    bundle: true,
    target: 'node22',
    format: 'esm',
    minify: true,
    sourcemap: true,
    platform: 'node',
    external: deps,
    outdir: 'out',
    // Lambda requires the .mjs extension for es modules
    outExtension: {'.js': '.mjs'},
    entryPoints: ['src/handlers/*.handler.ts'],
  });
}

await build();

// Verify that each module was bundled correctly (and do not
// pull in any dynamic require() calls) by loading each one.
for (const module of readdirSync('out')) {
  if (module.endsWith('.js')) {
    const {promise, resolve, reject} = resolver();
    fork(join('out', module), {stdio: 'inherit'})
      .on('error', reject)
      .on('close', code => (code === 0 ? resolve() : reject(code)));
    await promise;
    console.info(`Verified ${module}`);
  }
}
