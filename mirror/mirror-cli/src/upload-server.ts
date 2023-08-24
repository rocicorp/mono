import type {Firestore} from 'firebase-admin/firestore';
import {getFirestore, GrpcStatus} from 'firebase-admin/firestore';
import type {Storage} from 'firebase-admin/storage';
import {getStorage} from 'firebase-admin/storage';
import * as schema from 'mirror-schema/src/server.js';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {pkgUp} from 'pkg-up';
import {compile} from 'reflect-cli/src/compile.js';
import {getScriptTemplate} from 'reflect-cli/src/get-script-template.js';
import type {
  CommonYargsArgv,
  YargvToInterface,
} from 'reflect-cli/src/yarg-types.js';
import {SemVer} from 'semver';
import {assert, assertObject, assertString} from 'shared/src/asserts.js';
import {storeModule, type Module} from 'mirror-schema/src/module.js';
import {CANARY_RELEASE_CHANNEL} from 'mirror-schema/src/server.js';

const require = createRequire(import.meta.url);

export function uploadReflectServerOptions(yargs: CommonYargsArgv) {
  return yargs
    .option('force', {
      describe: 'Overwrite existing version',
      type: 'boolean',
    })
    .option('channels', {
      describe: 'The channels to which the server is immediately deployed',
      type: 'array',
      string: true,
      default: [CANARY_RELEASE_CHANNEL],
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

  console.log('Uploading...');
  await upload(
    firestore,
    storage,
    bucketName,
    !!yargs.force,
    version,
    source,
    scriptTemplate,
    yargs.channels,
  );

  console.log(`Uploaded version ${version} successfully`);
}

async function buildReflectServerContent(): Promise<string> {
  const serverPath = require.resolve('@rocicorp/reflect/server');
  assert(
    // Note: Don't include the full directory name because that trips up some
    // unrelated build checks.
    serverPath.indexOf('/node_module') >= 0,
    `Must reference a published npm and not a monorepo source directory: ${serverPath}.\n` +
      `Try temporarily bumping the version in 'packages/reflect/package.json' and re-running 'npm install' from the repo root.`,
  );
  console.info(`Building server from ${serverPath}`);
  const {code} = await compile(serverPath, false);
  return code.text;
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
  channels: string[],
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

  const newDoc: schema.Server = {
    major: version.major,
    minor: version.minor,
    patch: version.patch,
    modules: [mainModuleRef, scriptTemplateModuleRef],
    channels,
  };

  console.log('Writing server to firestore');
  try {
    await docRef.create(newDoc);
  } catch (e) {
    if ((e as {code: GrpcStatus}).code === GrpcStatus.ALREADY_EXISTS) {
      if (force) {
        console.info(`Overwriting existing module at ${version} with --force`);
        await docRef.set(newDoc);
      } else {
        console.error(`Version ${version} has already been uploaded`);
        console.error('Use --force to overwrite');
        process.exit(1);
      }
    } else {
      throw e;
    }
  }
}
