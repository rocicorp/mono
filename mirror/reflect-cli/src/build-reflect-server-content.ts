import * as esbuild from 'esbuild';
import {createRequire} from 'node:module';

// await startDevServer();
export async function buildReflectServerContent(): Promise<string> {
  const require = createRequire(import.meta.url);
  const serverPath = require.resolve('@rocicorp/reflect/server');

  const result = await esbuild.build({
    entryPoints: [serverPath],
    bundle: true,
    external: [],
    // Remove process.env. It does not exist in CF workers and we have npm
    // packages that use it.
    define: {'process.env': '{}'},
    platform: 'browser',
    conditions: ['workerd', 'worker', 'browser'],
    target: 'esnext',
    format: 'esm',
    sourcemap: false,
    write: false,
  });

  const {errors, warnings, outputFiles} = result;
  for (const error of errors) {
    console.error(error);
  }
  for (const warning of warnings) {
    console.warn(warning);
  }
  if (errors.length > 0) {
    throw new Error(errors.join('\n'));
  }

  if (outputFiles.length !== 1) {
    throw new Error(`Expected 1 output file, got ${outputFiles.length}`);
  }

  return outputFiles[0].text;
}
