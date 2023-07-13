import type {PublishRequest} from 'mirror-protocol/src/publish.js';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import {callFirebase} from 'shared/src/mirror/call-firebase.js';
import {getUserIDFromConfig, mustReadAuthConfigFile} from './auth-config.js';
import {compile} from './compile.js';
import {findServerVersionRange} from './find-reflect-server-version.js';
import {makeRequester} from './requester.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function publishOptions(yargs: CommonYargsArgv) {
  return yargs
    .option('name', {
      describe: 'Name of the worker',
      type: 'string',
      demandOption: true,
    })
    .positional('script', {
      describe: 'Path to the worker script',
      type: 'string',
      demandOption: true,
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

export async function publishHandler(yargs: PublishHandlerArgs) {
  const config = mustReadAuthConfigFile();
  const userID = getUserIDFromConfig(config);
  const {script, name} = yargs;

  // TODO(arv): This should be part of the config.
  const appID = 'temp-app-id';

  const absPath = path.resolve(script);

  if (!(await exists(absPath))) {
    throw new Error(`File not found: ${absPath}`);
  }

  const range = await findServerVersionRange(absPath);
  const serverVersionRange = range.raw;

  const {code, sourcemap} = await compile(absPath);

  const data: PublishRequest = {
    requester: makeRequester(userID),
    name,
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

  await callFirebase('publish', data, config.idToken);

  console.log(`🎁 Published successfully to:`);
  console.log(`https://${name}.reflect-server.net/`);
}
