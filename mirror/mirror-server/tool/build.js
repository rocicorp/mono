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
