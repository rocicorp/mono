import type * as esbuild from 'esbuild';
import {Firestore, getFirestore} from 'firebase/firestore';
import {
  publish as publishCaller,
  type PublishRequest,
} from 'mirror-protocol/src/publish.js';
import assert from 'node:assert';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import {
  getDefaultApp,
  getDefaultServerPath,
  getAppID,
  mustReadAppConfig,
  DEFAULT_FROM_REFLECT_CONFIG,
} from './app-config.js';
import {CompileResult, compile} from './compile.js';
import {ErrorWrapper} from './error.js';
import {findServerVersionRange} from './find-reflect-server-version.js';
import type {AuthContext} from './handler.js';
import {logErrorAndExit} from './log-error-and-exit.js';
import {checkForServerDeprecation} from './version.js';
import {watchDeployment} from './watch-deployment.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {getLogger} from './logger.js';
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
    })
    .option('server-path', {
      describe: 'Path to the Reflect server entry file',
      type: 'string',
      requiresArg: true,
      default: getDefaultServerPath(),
      required: true,
    })
    .option('app', {
      describe: 'The name of the App',
      type: 'string',
      requiresArg: true,
      default: getDefaultApp(),
      required: true,
    })
    .option('output', {
      describe: 'Output the result in a specified format',
      type: 'string',
      requiresArg: true,
      choices: ['json', 'text'],
      default: 'text',
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
  authContext: AuthContext,
  publish: PublishCaller = publishCaller, // Overridden in tests.
  firestore: Firestore = getFirestore(), // Overridden in tests.
) {
  const {reflectChannel, app} = yargs;

  let {serverPath} = yargs;
  if (serverPath === DEFAULT_FROM_REFLECT_CONFIG) {
    serverPath = mustReadAppConfig().server;
  }
  if (!serverPath) logErrorAndExit('No server path found');
  const absPath = path.resolve(serverPath);
  if (!absPath || !(await exists(absPath))) {
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

  getLogger().log(`Compiling ${serverPath}`);

  const {code, sourcemap} = await compileOrReportWarning(
    absPath,
    'linked',
    'production',
  );
  assert(sourcemap);

  const appID = await getAppID(authContext, app, true);

  const data: PublishRequest = {
    requester: authContext.requester,
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

  getLogger().log('Requesting deployment');

  const {deploymentPath} = await publish.call(data);

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
