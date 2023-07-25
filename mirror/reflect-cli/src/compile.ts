import * as esbuild from 'esbuild';
import {createRequire} from 'node:module';

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

export async function compile(
  entryPoint: string,
  sourcemap: true | 'both' | 'external' | 'linked',
): Promise<{
  code: esbuild.OutputFile;
  sourcemap: esbuild.OutputFile;
}>;
export async function compile(
  entryPoint: string,
  sourcemap: false | undefined | 'inline',
): Promise<{
  code: esbuild.OutputFile;
  sourcemap: undefined;
}>;
export async function compile(
  entryPoint: string,
  sourcemap: esbuild.BuildOptions['sourcemap'] = 'external',
): Promise<{
  code: esbuild.OutputFile;
  sourcemap: esbuild.OutputFile | undefined;
}> {
  const res = await esbuild.build({
    bundle: true,
    conditions: ['workerd', 'worker', 'browser'],
    // Remove process.env. It does not exist in CF workers and we have npm
    // packages that use it.
    define: {'process.env': '{}'},
    entryPoints: [entryPoint],
    external: [],
    format: 'esm',
    outdir: '.',
    platform: 'browser',
    plugins: [replaceReflectServerPlugin],
    sourcemap,
    target: 'esnext',
    write: false,
  });
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
