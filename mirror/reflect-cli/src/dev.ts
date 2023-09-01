import getPort, {portNumbers} from 'get-port';
import isPortReachable from 'is-port-reachable';
import assert from 'node:assert';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import {mustReadAppConfig} from './app-config.js';
import {watch} from './compile.js';
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
  port?: number | undefined;
};

export async function devHandler(yargs: DevHandlerArgs) {
  const {server: script} = mustReadAppConfig();

  const absPath = path.resolve(script);
  if (!(await exists(absPath))) {
    throw new Error(`File not found: ${absPath}`);
  }

  let first = true;
  const port = await findPort(yargs.port);
  const ac = new AbortController();
  let mfAc: AbortController | undefined;
  for await (const {code, sourcemap} of watch(absPath, 'linked', ac.signal)) {
    assert(sourcemap);
    const start = Date.now();
    process.stdout.write(
      (first ? 'Starting' : 'Restarting') + ' dev server...',
    );

    mfAc?.abort();
    mfAc = new AbortController();

    const {href} = await startDevServer(code, sourcemap, port, mfAc.signal);
    process.stdout.write(` Done in ${Date.now() - start}ms.\n`);
    if (first) {
      console.log(`
Dev server running at:
  ${href}
  ${href.replace(/^http/, 'ws')}
`);

      first = false;
    }
  }

  mfAc?.abort();
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
