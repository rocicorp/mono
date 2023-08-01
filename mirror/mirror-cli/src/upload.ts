import {initializeApp} from 'firebase-admin/app';
import type {Firestore} from 'firebase-admin/firestore';
import {getFirestore} from 'firebase-admin/firestore';
import type {Storage} from 'firebase-admin/storage';
import {getStorage} from 'firebase-admin/storage';
import * as schema from 'mirror-schema/src/server.js';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {pkgUp} from 'pkg-up';
import {buildReflectServerContent} from 'reflect-cli/src/compile.js';
import {getWorkerTemplate} from 'reflect-cli/src/get-worker-template.js';
import type {
  CommonYargsArgv,
  YargvToInterface,
} from 'reflect-cli/src/yarg-types.js';
import {SemVer} from 'semver';
import {assert, assertObject, assertString} from 'shared/src/asserts.js';
import {storeModule} from 'mirror-schema/src/module.js';

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
  const workerTemplate = getWorkerTemplate('<APP>', '<REFLECT_SERVER>');
  console.log('Version (from @rocicorp/reflect):', version.toString());

  // TODO(arv): Where should this come from? Config or CLI arg?
  const channel: schema.ReleaseChannel = 'canary';

  console.log('Uploading...');
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
  const s = await readFile(pkg, 'utf-8');
  const v = JSON.parse(s);
  assertObject(v);
  assertString(v.version);
  return new SemVer(v.version);
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
    type: 'esm',
  };
  const bucket = storage.bucket(bucketName);

  const [mainURL, workerTemplateURL] = await Promise.all([
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
          url: mainURL,
          type: main.type,
        },
        {
          name: workerTemplateModule.name,
          url: workerTemplateURL,
          type: workerTemplateModule.type,
        },
      ],
      channel,
    };

    txn.set(docRef, newDoc);
  });
}
