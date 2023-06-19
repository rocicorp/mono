import type {PublishRequest} from 'mirror-protocol/publish.js';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import {callFirebase} from './call-firebase.js';
import {compile} from './compile.js';
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
  const {script, name} = yargs;

  const absPath = path.resolve(script);

  if (!(await exists(absPath))) {
    throw new Error(`File not found: ${absPath}`);
  }

  const {code, sourcemap} = await compile(absPath);

  // TODO(arv): Implement userID
  const userID = 'USERID';

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
  };

  await callFirebase('publish', data);

  console.log(`🎁 Published successfully to:`);
  console.log(`https://${name}.replicache.workers.dev/`);
}
