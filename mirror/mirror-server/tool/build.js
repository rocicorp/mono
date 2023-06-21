import * as esbuild from 'esbuild';
import packageJSON from '../package.json' assert {type: 'json'};

const {dependencies, devDependencies, bundleDependencies} = packageJSON;
const external = new Set(
  Object.keys({
    ...dependencies,
    ...devDependencies,
  }),
);

// See comment in tool/process-deps.js
for (const dep of bundleDependencies) {
  external.delete(dep);
}

const ctx = await esbuild.context({
  entryPoints: ['src/index.ts'],
  bundle: true,
  outfile: 'out/index.js',
  external: [...external],
  platform: 'node',
  target: 'esnext',
  format: 'esm',
  sourcemap: false,
});

if (process.argv.includes('--watch')) {
  await ctx.watch();
} else {
  await ctx.rebuild();
  ctx.dispose();
}
