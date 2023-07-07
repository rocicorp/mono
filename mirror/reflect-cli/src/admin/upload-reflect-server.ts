import type {Bucket} from '@google-cloud/storage';
import * as esbuild from 'esbuild';
import {initializeApp} from 'firebase-admin/app';
import type {Firestore} from 'firebase-admin/firestore';
import {getFirestore} from 'firebase-admin/firestore';
import type {Storage} from 'firebase-admin/storage';
import {getStorage} from 'firebase-admin/storage';
import * as schema from 'mirror-schema/src/server.js';
import {nanoid} from 'nanoid';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import * as path from 'node:path';
import {pkgUp} from 'pkg-up';
import {SemVer} from 'semver';
import {assert, assertObject, assertString} from 'shared/src/asserts.js';
import type {CommonYargsArgv, YargvToInterface} from '../yarg-types.js';

const require = createRequire(import.meta.url);

// TODO(arv): This should be a config value
const bucketName = 'reflect-mirror-staging-servers';
// TODO(arv): This should be a config value
const projectId = 'reflect-mirror-staging';

export function uploadReflectServerOptions(yargs: CommonYargsArgv) {
  return yargs.option('force', {
    describe: 'Overwrite existing version',
    type: 'boolean',
  });
}

type UploadReflectServerHandlerArgs = YargvToInterface<
  ReturnType<typeof uploadReflectServerOptions>
>;

type Module = {
  name: string;
  content: string;
  type: 'esm' | 'text';
};

export async function uploadReflectServerHandler(
  yargs: UploadReflectServerHandlerArgs,
) {
  console.log(
    'Make sure you run `npm run build` from the root of the repo first',
  );

  initializeApp({projectId});
  const firestore = getFirestore();
  const storage = getStorage();

  const source = await buildReflectServerContent();
  const version = await findVersion();
  const workerTemplate = await getWorkerTemplate();
  console.log('Version (from @rocicorp/reflect):', version.toString());

  // TODO(arv): Where should this come from? Config or CLI arg?
  const channel: schema.ReleaseChannel = 'canary';

  await upload(
    firestore,
    storage,
    bucketName,
    !!yargs.force,
    version,
    source,
    workerTemplate,
    channel,
  );

  console.log(`Uploaded version ${version} successfully`);
}

async function findVersion(): Promise<SemVer> {
  const serverPath = require.resolve('@rocicorp/reflect');
  const pkg = await pkgUp({cwd: serverPath});
  assert(pkg);
  const s = await readFile(pkg, 'utf8');
  const v = JSON.parse(s);
  assertObject(v);
  assertString(v.version);
  return new SemVer(v.version);
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

async function upload(
  firestore: Firestore,
  storage: Storage,
  bucketName: string,
  force: boolean,
  version: SemVer,
  source: string,
  workerTemplate: string,
  channel: schema.ReleaseChannel,
) {
  const main: Module = {
    content: source,
    name: 'reflect-server.js',
    type: 'esm',
  };
  const workerTemplateModule: Module = {
    content: workerTemplate,
    name: 'worker.template.js',
    type: 'text',
  };
  const bucket = storage.bucket(bucketName);

  const [mainFilename, workerTemplateFilename] = await Promise.all([
    storeModule(bucket, main),
    storeModule(bucket, workerTemplateModule),
  ]);

  const docRef = firestore
    .doc(schema.serverPath(version.toString()))
    .withConverter(schema.serverDataConverter);

  await firestore.runTransaction(async txn => {
    const doc = await txn.get(docRef);
    if (doc.exists && !force) {
      console.error(`Version ${version} has already been uploaded`);
      console.error('Use --force to overwrite');
      process.exit(1);
    }

    const newDoc: schema.Server = {
      major: version.major,
      minor: version.minor,
      patch: version.patch,
      modules: [
        {
          name: main.name,
          filename: mainFilename,
          type: main.type,
        },
        {
          name: workerTemplateModule.name,
          filename: workerTemplateFilename,
          type: workerTemplateModule.type,
        },
      ],
      channel,
    };

    txn.set(docRef, newDoc);
  });
}

async function storeModule(bucket: Bucket, module: Module) {
  const filename = `${encodeURIComponent(module.name)}-${nanoid()}`;
  await bucket.file(filename).save(module.content);
  return filename;
}
