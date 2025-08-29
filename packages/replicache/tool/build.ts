import * as esbuild from 'esbuild';
import {writeFile} from 'node:fs/promises';
import * as path from 'node:path';
import process from 'node:process';
import {fileURLToPath} from 'node:url';
import {makeDefine, sharedOptions} from '../../shared/src/build.ts';

const forBundleSizeDashboard = process.argv.includes('--bundle-sizes');
const perf = process.argv.includes('--perf');
// You can then visualize the metafile at https://esbuild.github.io/analyze/
const metafile = process.argv.includes('--metafile');

const dirname = path.dirname(fileURLToPath(import.meta.url));

type BuildMode = 'unknown' | 'debug' | 'release';

type BuildOptions = {
  minify: boolean;
  ext: string;
  mode: BuildMode;
  external?: string[] | undefined;
};

function basePath(...parts: string[]): string {
  return path.join(dirname, '..', ...parts);
}

async function buildReplicache(options: BuildOptions) {
  const define = makeDefine(options.mode);
  const {ext, mode, external, ...restOfOptions} = options;
  const outfile = basePath('out', 'replicache.' + ext);
  const entryPoints = {
    replicache: basePath('src', 'mod.ts'),
    ...(forBundleSizeDashboard ? {} : {impl: basePath('src', 'impl.ts')}),
  };
  const result = await esbuild.build({
    ...sharedOptions(options.minify, metafile),
    ...(external ? {external} : {}),
    ...restOfOptions,
    bundle: true,
    splitting: !forBundleSizeDashboard,
    format: 'esm',
    // Use neutral to remove the automatic define for process.env.NODE_ENV
    platform: 'neutral',
    define,
    outdir: basePath('out'),
    entryPoints,
    outExtension: {'.js': `.${ext}`},
  });
  if (metafile) {
    await writeFile(outfile + '.meta.json', JSON.stringify(result.metafile));
  }
}

async function buildCLI() {
  await esbuild.build({
    ...sharedOptions(true),
    platform: 'node',
    outfile: basePath('out', 'cli.cjs'),
    entryPoints: [basePath('tool', 'cli.ts')],
    format: 'cjs',
  });
}

if (perf) {
  await buildReplicache({minify: true, ext: 'js', mode: 'release'});
} else if (forBundleSizeDashboard) {
  // Bundle external modules for the bundle size dashboard
  const external = ['node:*'];
  // We keep mjs as mjs so the dashboard does not get reset
  await Promise.all([
    buildReplicache({minify: false, ext: 'mjs', mode: 'unknown', external}),
    buildReplicache({minify: true, ext: 'min.mjs', mode: 'unknown', external}),
    buildCLI(),
  ]);
} else {
  await buildCLI();
  await buildReplicache({minify: false, ext: 'js', mode: 'unknown'});
}
