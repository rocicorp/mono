import * as esbuild from 'esbuild';
import {createRequire} from 'node:module';
import type {CommonYargsArgv, YargvToInterface} from '../yarg-types.js';

const require = createRequire(import.meta.url);

export function uploadReflectServerOptions(yargs: CommonYargsArgv) {
  return yargs.option('semver', {
    describe: 'The semver of @rocicorp/reflect',
    type: 'string',
    demandOption: true,
  });
}

type UploadReflectServerHandlerArgs = YargvToInterface<
  ReturnType<typeof uploadReflectServerOptions>
>;

export async function uploadReflectServerHandler(
  yargs: UploadReflectServerHandlerArgs,
) {
  console.log(
    'Make sure you run `npm run build` from the root of the repo first',
  );
  console.log('TODO: Implement upload-reflect-server');
  console.log('yargs', yargs.semver);

  const source = await buildReflectServerContent();
  console.log('source', source);
}

async function buildReflectServerContent() {
  const serverPath = require.resolve('@rocicorp/reflect-server');

  const result = await esbuild.build({
    entryPoints: [serverPath],
    bundle: true,
    external: [],
    // Remove process.env. It does not exist in CF workers and we have npm
    // packages that use it.
    define: {'process.env': '{}'},
    platform: 'browser',
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
