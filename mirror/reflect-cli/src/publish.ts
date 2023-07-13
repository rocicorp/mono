import {
  publish as publishCaller,
  type PublishRequest,
} from 'mirror-protocol/src/publish.js';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import {authenticate} from './auth-config.js';
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

export async function publishHandler(
  yargs: PublishHandlerArgs,
  publish = publishCaller, // Overridden in tests.
) {
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

  const user = await authenticate();
  const userID = user.uid;

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

  await publish(data);

  console.log(`🎁 Published successfully to:`);
  console.log(`https://${name}.reflect-server.net/`);
}
