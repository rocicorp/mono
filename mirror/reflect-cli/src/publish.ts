import {
  publish as publishCaller,
  type PublishRequest,
} from 'mirror-protocol/src/publish.js';
import {deploymentViewDataConverter} from 'mirror-schema/src/deployment.js';
import {watch} from 'mirror-schema/src/watch.js';
import assert from 'node:assert';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import {
  ensureAppInstantiated,
  writeTemplatedFilePlaceholders,
} from './app-config.js';
import {authenticate} from './auth-config.js';
import {compile} from './compile.js';
import {findServerVersionRange} from './find-reflect-server-version.js';
import {Firestore, getFirestore} from './firebase.js';
import {makeRequester} from './requester.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {checkForServerDeprecation} from './version.js';

export function publishOptions(yargs: CommonYargsArgv) {
  return yargs.option('reflect-channel', {
    desc: 'Set the Reflect Channel for server updates',
    type: 'string',
  });
}

async function exists(path: string) {
  try {
    await fs.access(path);
    return true;
  } catch {
    return false;
  }
}

type PublishHandlerArgs = YargvToInterface<ReturnType<typeof publishOptions>>;

export type PublishCaller = typeof publishCaller;

export async function publishHandler(
  yargs: PublishHandlerArgs,
  publish: PublishCaller = publishCaller, // Overridden in tests.
  firestore: Firestore = getFirestore(), // Overridden in tests.
) {
  const {reflectChannel} = yargs;
  const {appID, server: script} = await ensureAppInstantiated(yargs);

  const absPath = path.resolve(script);
  if (!(await exists(absPath))) {
    throw new Error(`File not found: ${absPath}`);
  }

  const range = await findServerVersionRange(absPath);
  await checkForServerDeprecation(yargs, range);
  const serverVersionRange = range.raw;

  console.log(`Compiling ${script}`);
  const {code, sourcemap} = await compile(absPath, 'linked', 'production');
  assert(sourcemap);

  const {userID} = await authenticate(yargs);

  const data: PublishRequest = {
    requester: makeRequester(userID),
    source: {
      content: code.text,
      name: path.basename(code.path),
    },
    sourcemap: {
      content: sourcemap.text,
      name: path.basename(sourcemap.path),
    },
    serverVersionRange,
    appID,
  };
  if (reflectChannel) {
    data.serverReleaseChannel = reflectChannel;
  }

  console.log('Requesting deployment');
  const {deploymentPath} = await publish(data);

  const deploymentDoc = firestore
    .doc(deploymentPath)
    .withConverter(deploymentViewDataConverter);

  for await (const snapshot of watch(deploymentDoc)) {
    const deployment = snapshot.data();
    if (!deployment) {
      console.error(`Deployment not found`);
      break;
    }
    if (deployment?.status === 'RUNNING') {
      console.log(`🎁 Published successfully to:`);
      console.log(`https://${deployment.spec.hostname}`);
      writeTemplatedFilePlaceholders({
        appHostname: deployment.spec.hostname,
      });
      break;
    }
    console.info(
      `Status: ${deployment.status}${
        deployment.statusMessage ? ': ' + deployment.statusMessage : ''
      }`,
    );
    if (deployment.status === 'FAILED' || deployment.status === 'STOPPED') {
      break;
    }
  }
}
