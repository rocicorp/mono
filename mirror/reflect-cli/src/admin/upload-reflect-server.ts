import * as esbuild from 'esbuild';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {pkgUp} from 'pkg-up';
import {assert, assertObject, assertString} from 'shared/src/asserts.js';
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

  const source = await buildReflectServerContent();
  console.log('Version (from @rocicorp/reflect):', await findVersion());
  if (yargs.semver) {
    console.log('Version (from --semver):         ', yargs.semver);
  }
  console.log('Source: ...\n', source.split('\n').slice(-30).join('\n'));

  console.log('TODO: Implement upload-reflect-server');
}

async function findVersion() {
  const serverPath = require.resolve('@rocicorp/reflect');
  const pkg = await pkgUp({cwd: serverPath});
  console.log('pkg', pkg);
  assert(pkg);
  const s = await readFile(pkg, 'utf8');
  const v = JSON.parse(s);
  assertObject(v);
  assertString(v.version);
  return v.version;
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
