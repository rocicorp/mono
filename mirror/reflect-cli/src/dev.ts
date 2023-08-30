import getPort, {portNumbers} from 'get-port';
import isPortReachable from 'is-port-reachable';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import {compile} from './compile.js';
import {startDevServer} from './dev/start-dev-server.js';
import type {CommonYargsArgv} from './yarg-types.js';

const DEFAULT_PORT = 8080;
const DEFAULT_END_PORT = 65535;

export function devOptions(yargs: CommonYargsArgv) {
  return (
    yargs
      // `port` is done in a pretty strange way to be able to detect if port was
      // provided or not
      .option('port', {
        alias: 'p',
        describe: 'Port to run the dev server on',
        type: 'number',
        requiresArg: true,
      })
      .default('port', undefined, '' + DEFAULT_PORT)
      .positional('script', {
        describe: 'Path to the worker script',
        type: 'string',
        demandOption: true,
      })
  );
}

async function exists(path: string) {
  try {
    await fs.stat(path);
    return true;
  } catch {
    return false;
  }
}

type DevHandlerArgs = {
  script: string;
  port?: number | undefined;
};

export async function devHandler(yargs: DevHandlerArgs) {
  const {script} = yargs;

  const absPath = path.resolve(script);
  if (!(await exists(absPath))) {
    throw new Error(`File not found: ${absPath}`);
  }

  const {code, sourcemap} = await compile(absPath, 'linked');

  const port = await findPort(yargs.port);
  const ac = new AbortController();
  const {href} = await startDevServer(code, sourcemap, port, ac.signal);
  console.log(`Dev server running at:
  ${href}
  ${href.replace(/^http/, 'ws')}`);
}

async function findPort(port: undefined | number) {
  // If port was provided, check if it's available and exit if it's not.
  if (port !== undefined) {
    if (await isPortReachable(port, {host: '0.0.0.0'})) {
      console.error(`Port ${port} is already in use`);
      process.exit(1);
    }
    return port;
  }

  // Otherwise, find a free port.
  return getPort({
    port: portNumbers(DEFAULT_PORT, DEFAULT_END_PORT),
  });
}
