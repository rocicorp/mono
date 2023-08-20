import type {Firestore} from 'firebase-admin/firestore';
import {getFirestore} from 'firebase-admin/firestore';
import type {Storage} from 'firebase-admin/storage';
import {getStorage} from 'firebase-admin/storage';
import * as schema from 'mirror-schema/src/server.js';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {pkgUp} from 'pkg-up';
import {buildReflectServerContent} from 'reflect-cli/src/compile.js';
import {getScriptTemplate} from 'reflect-cli/src/get-script-template.js';
import type {
  CommonYargsArgv,
  YargvToInterface,
} from 'reflect-cli/src/yarg-types.js';
import {SemVer} from 'semver';
import {assert, assertObject, assertString} from 'shared/src/asserts.js';
import {storeModule, type Module} from 'mirror-schema/src/module.js';

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

  const firestore = getFirestore();
  const storage = getStorage();
  const bucketName =
    yargs.stack === 'prod'
      ? 'reflect-mirror-prod-modules'
      : 'reflect-mirror-staging-modules';

  const source = await buildReflectServerContent();
  const version = await findVersion();
  const scriptTemplate = await getScriptTemplate('prod');
  console.log('Script template:\n', scriptTemplate);
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
    scriptTemplate,
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
  scriptTemplate: string,
  channel: schema.ReleaseChannel,
) {
  const main: Module = {
    content: source,
    name: 'reflect-server.js',
    type: 'esm',
  };
  const scriptTemplateModule: Module = {
    content: scriptTemplate,
    name: 'script.template.js',
    type: 'esm',
  };
  const bucket = storage.bucket(bucketName);

  console.log(`Uploading modules to ${bucketName}`);
  const [mainModuleRef, scriptTemplateModuleRef] = await Promise.all([
    storeModule(bucket, main),
    storeModule(bucket, scriptTemplateModule),
  ]);

  const docRef = firestore
    .doc(schema.serverPath(version.toString()))
    .withConverter(schema.serverDataConverter);

  console.log('Writing server to firestore');
  await firestore.runTransaction(async txn => {
    const doc = await txn.get(docRef);
    if (doc.exists) {
      if (force) {
        console.info(`Overwriting existing module at ${version} with --force`);
      } else {
        console.error(`Version ${version} has already been uploaded`);
        console.error('Use --force to overwrite');
        process.exit(1);
      }
    }

    const newDoc: schema.Server = {
      major: version.major,
      minor: version.minor,
      patch: version.patch,
      modules: [mainModuleRef, scriptTemplateModuleRef],
      channel,
    };

    txn.set(docRef, newDoc);
  });
}
