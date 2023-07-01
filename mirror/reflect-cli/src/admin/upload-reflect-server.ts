import * as esbuild from 'esbuild';
import {
  uploadResponseSchema,
  type UploadRequest,
} from 'mirror-protocol/src/reflect-server.js';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import * as path from 'node:path';
import {pkgUp} from 'pkg-up';
import {assert, assertObject, assertString} from 'shared/src/asserts.js';
import {FirebaseError, callFirebase} from 'shared/src/call-firebase.js';
import {makeRequester} from '../requester.js';
import type {CommonYargsArgv, YargvToInterface} from '../yarg-types.js';

const require = createRequire(import.meta.url);

export function uploadReflectServerOptions(yargs: CommonYargsArgv) {
  return yargs.option('force', {
    describe: 'Overwrite existing version',
    type: 'boolean',
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
  const versionFromPackage = await findVersion();
  const workerTemplate = await getWorkerTemplate();
  console.log('Version (from @rocicorp/reflect):', versionFromPackage);

  // TODO(arv) Implement userID
  const userID = 'USERID';
  const data: UploadRequest = {
    requester: makeRequester(userID),
    version: versionFromPackage,
    main: {
      content: source,
      name: 'reflect-server.js',
      type: 'esm',
    },
    modules: [
      {
        content: workerTemplate,
        name: 'worker.template.js',
        type: 'text',
      },
    ],
    force: yargs.force,
  };

  try {
    await callFirebase('reflectServer-upload', data, uploadResponseSchema);
  } catch (e) {
    if (e instanceof FirebaseError && e.status === 'ALREADY_EXISTS') {
      console.log(e.message);
      console.log('Use --force to overwrite');
      process.exit(1);
    }

    throw e;
  }
  console.log(`Uploaded version ${versionFromPackage} successfully`);
}

async function findVersion() {
  const serverPath = require.resolve('@rocicorp/reflect');
  const pkg = await pkgUp({cwd: serverPath});
  assert(pkg);
  const s = await readFile(pkg, 'utf8');
  const v = JSON.parse(s);
  assertObject(v);
  assertString(v.version);
  return v.version;
}

async function buildReflectServerContent() {
  const serverPath = require.resolve('reflect-server');

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

async function getWorkerTemplate() {
  const serverPath = require.resolve('reflect-server');
  const pkg = await pkgUp({cwd: serverPath});
  assert(pkg);
  const templatePath = path.join(
    path.dirname(pkg),
    'templates',
    'worker.template.js',
  );
  return readFile(templatePath, 'utf-8');
}
