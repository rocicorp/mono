import type {Firestore} from 'firebase-admin/firestore';
import {getFirestore, GrpcStatus} from 'firebase-admin/firestore';
import type {Storage} from 'firebase-admin/storage';
import {getStorage} from 'firebase-admin/storage';
import {storeModule, type Module} from 'mirror-schema/src/module.js';
import * as schema from 'mirror-schema/src/server.js';
import {
  CANARY_RELEASE_CHANNEL,
  STABLE_RELEASE_CHANNEL,
} from 'mirror-schema/src/server.js';
import {execSync} from 'node:child_process';
import {mkdtemp} from 'node:fs/promises';
import {createRequire} from 'node:module';
import * as os from 'node:os';
import * as path from 'node:path';
import colors from 'picocolors';
import {compile} from 'reflect-cli/src/compile.js';
import {getScriptTemplate} from 'reflect-cli/src/get-script-template.js';
import {SemVer} from 'semver';
import {assert} from 'shared/src/asserts.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

const {bold} = colors;

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
    })
    .option('version', {
      describe: 'The version of the server from npm to upload.',
      type: 'string',
      requiresArg: true,
    })
    .option('build-from-source-version', {
      describe:
        'Build the server from source and give it the specified version string. ' +
        'This is only intended for debugging and experiments, and requires that non-standard --channels be specified.',
      type: 'string',
      requiresArg: true,
    });
}

type UploadReflectServerHandlerArgs = YargvToInterface<
  ReturnType<typeof uploadReflectServerOptions>
>;

export async function uploadReflectServerHandler(
  yargs: UploadReflectServerHandlerArgs,
) {
  const {version, buildFromSourceVersion, channels} = yargs;
  if (
    buildFromSourceVersion &&
    (channels.includes(STABLE_RELEASE_CHANNEL) ||
      channels.includes(CANARY_RELEASE_CHANNEL))
  ) {
    console.error(
      '--build-from-source-version may only be used with a non-standard --channels',
    );
    process.exit(1);
  }
  const firestore = getFirestore();
  const storage = getStorage();
  const bucketName = `reflect-mirror-${yargs.stack}-modules`;

  let serverPath: string;
  let semver: SemVer;
  if (buildFromSourceVersion) {
    console.log(
      'Make sure you run `npm run build` from the root of the repo first',
    );
    serverPath = getServerPathFromCurrentRepo();
    semver = new SemVer(buildFromSourceVersion);
  } else if (!version) {
    console.error('Either --version or --build-from-source is required');
    process.exit(1);
  } else {
    semver = new SemVer(version);
    serverPath = await installReflectPackageFromNpm(semver);
  }

  console.info(`Building server from ${bold(serverPath)}`);
  const {code} = await compile(serverPath, false, 'production');
  const source = code.text;

  const scriptTemplate = await getScriptTemplate('prod');
  console.log(
    `Version (from ${
      buildFromSourceVersion
        ? '--build-from-source-version'
        : '@rocicorp/reflect'
    }): ${version}`,
  );

  console.log('Uploading...');
  await upload(
    firestore,
    storage,
    bucketName,
    !!yargs.force,
    semver,
    source,
    scriptTemplate,
    channels,
  );

  console.log(`Uploaded version ${version} successfully`);
}

function getServerPathFromCurrentRepo() {
  const require = createRequire(import.meta.url);
  const serverPath = require.resolve('@rocicorp/reflect/server');
  assert(
    // Note: Don't include the full directory name because that trips up some
    // unrelated build checks.
    serverPath.indexOf('/node_module') < 0,
    `mirror-cli is referencing a published node package. Make sure the package.json version of @rocicorp/reflect ` +
      `matches the version in packages/reflect/package.json, and rerun 'npm install' from the repo root.`,
  );
  return serverPath;
}

/**
 * Installs the latest version of @rocicorp/reflect from npm into a temporary
 * directory and returns the path to the server file as well as the version.
 */
async function installReflectPackageFromNpm(v: SemVer): Promise<string> {
  const dir = await mkdtemp(
    path.join(os.tmpdir(), 'mirror-cli-upload-reflect-'),
  );

  execSync('npm init -y', {cwd: dir, stdio: ['ignore', 'pipe', 'ignore']});
  console.log(
    `Installing ${bold('@rocicorp/reflect')} version ${bold(
      v.toString(),
    )} into ${bold(dir)}`,
  );

  execSync(`npm install @rocicorp/reflect@${v}`, {
    cwd: dir,
    stdio: ['ignore', 'pipe', 'ignore'],
  });

  // Use node module resolve to find server file in case it moves around.
  const require = createRequire(path.join(dir, 'dummy.js'));
  const serverPath = require.resolve('@rocicorp/reflect/server');
  return serverPath;
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
