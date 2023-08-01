import * as esbuild from 'esbuild';
import {injectRequire} from 'shared/src/tool/inject-require.js';
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

const indexCtx = await esbuild.context({
  entryPoints: ['src/index.ts'],
  bundle: true,
  outfile: 'out/index.js',
  external: [...external],
  platform: 'node',
  target: 'esnext',
  format: 'esm',
  sourcemap: false,
  banner: {
    js: injectRequire(),
  },
});

if (process.argv.includes('--watch')) {
  await indexCtx.watch();
} else {
  await indexCtx.rebuild();
  indexCtx.dispose();
}
