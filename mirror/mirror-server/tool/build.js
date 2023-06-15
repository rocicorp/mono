import * as esbuild from 'esbuild';
import packageJSON from '../package.json' assert {type: 'json'};

const {dependencies, devDependencies} = packageJSON;
const external = Object.keys({
  ...dependencies,
  ...devDependencies,
});

const ctx = await esbuild.context({
  entryPoints: ['src/index.ts'],
  bundle: true,
  outfile: 'out/index.js',
  external,
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
