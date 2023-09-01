import * as esbuild from 'esbuild';
import {createRequire} from 'node:module';
import {watchFiles} from './watch-files.js';

const reflectServerFileName = 'reflect-server.js';

const replaceReflectServerPlugin: esbuild.Plugin = {
  name: 'replace-reflect',
  setup(build) {
    build.onResolve({filter: /^@rocicorp\/reflect\/server$/}, () => ({
      path: `./${reflectServerFileName}`,
      external: true,
    }));
  },
};

function getEsbuildOptions(
  entryPoint: string,
  sourcemap: esbuild.BuildOptions['sourcemap'] = 'external',
) {
  return {
    bundle: true,
    conditions: ['workerd', 'worker', 'browser'],
    // Remove process.env. It does not exist in CF workers and we have npm
    // packages that use it.
    define: {'process.env': '{}'},
    external: [],
    format: 'esm',
    outdir: '.',
    platform: 'browser',
    plugins: [replaceReflectServerPlugin],
    target: 'esnext',
    write: false,
    metafile: true,
    entryPoints: [entryPoint],
    sourcemap,
  } as esbuild.BuildOptions & {metafile: true; write: false};
}

export type CompileResult = {
  code: esbuild.OutputFile;
  sourcemap: esbuild.OutputFile | undefined;
};

export async function compile(
  entryPoint: string,
  sourcemap?: esbuild.BuildOptions['sourcemap'],
): Promise<CompileResult> {
  const res = await esbuild.build(getEsbuildOptions(entryPoint, sourcemap));
  return getResultFromEsbuildResult(res, sourcemap);
}

function getResultFromEsbuildResult(
  res: esbuild.BuildResult<
    esbuild.BuildOptions & {
      metafile: true;
      write: false;
    }
  >,
  sourcemap: esbuild.BuildOptions['sourcemap'] = 'external',
): CompileResult {
  const {errors, outputFiles} = res;
  if (errors.length > 0) {
    throw new Error(res.errors.join('\n'));
  }

  const expectedCount = shouldHaveSourcemapFile(sourcemap) ? 2 : 1;
  if (expectedCount !== outputFiles.length) {
    throw new Error('Unexpected output from esbuild');
  }

  if (expectedCount === 1) {
    return {code: outputFiles[0], sourcemap: undefined};
  }

  // Not clear if the order in outputFiles is guaranteed.
  if (outputFiles[0].path.endsWith('.map')) {
    outputFiles.reverse();
  }
  return {code: outputFiles[0], sourcemap: outputFiles[1]};
}

export async function* watch(
  entryPoint: string,
  sourcemap: esbuild.BuildOptions['sourcemap'] | undefined,
  signal: AbortSignal,
): AsyncGenerator<CompileResult> {
  const buildContext = await esbuild.context(
    getEsbuildOptions(entryPoint, sourcemap),
  );

  const hashes = new Map<string, string>();
  let first = true;
  try {
    while (!signal.aborted) {
      if (first) {
        process.stdout.write('Building...');

        first = false;
      } else {
        process.stdout.write('Rebuilding...');
      }
      const start = Date.now();
      const res = await buildContext.rebuild();
      if (signal.aborted) {
        break;
      }

      process.stdout.write(` Done in ${Date.now() - start}ms.\n`);

      yield getResultFromEsbuildResult(res, sourcemap);

      if (signal.aborted) {
        return;
      }

      const filesToWatch = Object.keys(res.metafile.inputs).filter(
        input => !input.startsWith('<define:'),
      );

      await watchFiles(filesToWatch, signal, hashes);
      if (!signal.aborted) {
        process.stdout.write('Files changed. ');
      }
    }
  } finally {
    await buildContext.dispose();
  }
}

function shouldHaveSourcemapFile(
  v: esbuild.BuildOptions['sourcemap'] | undefined,
): boolean {
  switch (v) {
    case true:
    case 'both':
    case 'external':
    case 'linked':
      return true;
    case false:
    case undefined:
    case 'inline':
      return false;
  }
}

export async function buildReflectServerContent(): Promise<string> {
  const require = createRequire(import.meta.url);
  const serverPath = require.resolve('@rocicorp/reflect/server');
  const {code} = await compile(serverPath, false);
  return code.text;
}
