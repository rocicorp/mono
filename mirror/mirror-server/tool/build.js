import * as esbuild from 'esbuild';
import * as path from 'node:path';
import {fileURLToPath} from 'node:url';
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
});

// HACK: We compile reflect server into this package so that the file gets
// published with the firebase function. This is is so that we can get the
// source when we publish things to Cloudflare. We read this file in
// mirror-server/src/cloudflare/publish.ts.
//
// In the end we want to store the reflect server source in firestore.
async function buildReflectServer() {
  const dirname = path.dirname(fileURLToPath(import.meta.url));
  const serverPath = path.join(
    dirname,
    '..',
    '..',
    '..',
    'packages',
    'reflect-server',
    'src',
    'mod.ts',
  );
  return esbuild.context({
    entryPoints: [serverPath],
    bundle: true,
    outfile: 'out/data/reflect-server.js',
    external: [],
    // Remove process.env. It does not exist in CF workers and we have npm
    // packages that use it.
    define: {'process.env': '{}'},
    platform: 'browser',
    target: 'esnext',
    format: 'esm',
    sourcemap: false,
  });
}

const reflectServerContext = await buildReflectServer();

if (process.argv.includes('--watch')) {
  await Promise.all[(indexCtx.watch(), reflectServerContext.watch())];
} else {
  await Promise.all([indexCtx.rebuild(), reflectServerContext.rebuild()]);
  indexCtx.dispose();
  reflectServerContext.dispose();
}
