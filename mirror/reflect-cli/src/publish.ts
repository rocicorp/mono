import type * as esbuild from 'esbuild';
import {Firestore, getFirestore} from 'firebase/firestore';
import {
  publish as publishCaller,
  type PublishRequest,
} from 'mirror-protocol/src/publish.js';
import assert from 'node:assert';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import {ensureAppInstantiated} from './app-config.js';
import {authenticate} from './auth-config.js';
import {CompileResult, compile} from './compile.js';
import {ErrorWrapper} from './error.js';
import {findServerVersionRange} from './find-reflect-server-version.js';
import {logErrorAndExit} from './log-error-and-exit.js';
import {makeRequester} from './requester.js';
import {checkForServerDeprecation} from './version.js';
import {watchDeployment} from './watch-deployment.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function publishOptions(yargs: CommonYargsArgv) {
  return yargs
    .option('reflect-channel', {
      desc: 'Set the Reflect Channel for server updates',
      type: 'string',
      hidden: true,
    })
    .option('force-version-range', {
      describe: 'Force the version range',
      type: 'string',
      requiresArg: true,
      hidden: true,
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
    logErrorAndExit(`File not found: ${absPath}`);
  }

  let serverVersionRange;
  if (yargs.forceVersionRange) {
    serverVersionRange = yargs.forceVersionRange;
  } else {
    const range = await findServerVersionRange(absPath);
    await checkForServerDeprecation(yargs, range);
    serverVersionRange = yargs.forceVersionRange ?? range.raw;
  }

  console.log(`Compiling ${script}`);
  const {code, sourcemap} = await compileOrReportWarning(
    absPath,
    'linked',
    'production',
  );
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

  await watchDeployment(firestore, deploymentPath, 'Published');
}

async function compileOrReportWarning(
  entryPoint: string,
  sourcemap: esbuild.BuildOptions['sourcemap'],
  mode: 'production' | 'development',
): Promise<CompileResult> {
  try {
    // await to catch errors.
    return await compile(entryPoint, sourcemap, mode);
  } catch (e) {
    throw new ErrorWrapper(e, 'WARNING');
  }
}
