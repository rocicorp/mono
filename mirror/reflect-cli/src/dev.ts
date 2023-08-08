import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import {compile} from './compile.js';
import {startDevServer} from './dev/start-dev-server.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function devOptions(yargs: CommonYargsArgv) {
  return yargs
    .option('port', {
      alias: 'p',
      describe: 'Port to run the dev server on',
      type: 'number',
      default: 8080,
      requiresArg: true,
    })
    .positional('script', {
      describe: 'Path to the worker script',
      type: 'string',
      demandOption: true,
    });
}

async function exists(path: string) {
  try {
    await fs.stat(path);
    return true;
  } catch {
    return false;
  }
}

type DevHandlerArgs = YargvToInterface<ReturnType<typeof devOptions>>;

export async function devHandler(yargs: DevHandlerArgs) {
  const {script, port} = yargs;

  const absPath = path.resolve(script);
  if (!(await exists(absPath))) {
    throw new Error(`File not found: ${absPath}`);
  }

  const {code, sourcemap} = await compile(absPath, 'linked');

  const ac = new AbortController();
  const {href} = await startDevServer(code, sourcemap, port, ac.signal);
  console.log(`Dev server running at:
  ${href}
  ${href.replace(/^http/, 'ws')}`);
}
