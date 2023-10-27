import isPortReachable from 'is-port-reachable';
import assert from 'node:assert';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import {mustReadAppConfig} from './app-config.js';
import {watch} from './compile.js';
import {startDevServer} from './dev/start-dev-server.js';
import {ErrorWrapper} from './error.js';
import {logErrorAndExit} from './log-error-and-exit.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

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
        default: 8080,
      })
      .option('silence-startup-message', {
        describe: 'Silence startup message',
        type: 'boolean',
        default: false,
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

type DevHandlerArgs = YargvToInterface<ReturnType<typeof devOptions>>;

export async function devHandler(yargs: DevHandlerArgs) {
  const {server: script} = mustReadAppConfig();

  const absPath = path.resolve(script);
  if (!(await exists(absPath))) {
    logErrorAndExit(`File not found: ${absPath}`);
  }

  const {port, silenceStartupMessage} = yargs;
  if (await isPortReachable(port, {host: '0.0.0.0'})) {
    logErrorAndExit(`Port ${port} is already in use`);
  }

  let first = true;
  const ac = new AbortController();
  let mfAc: AbortController | undefined;
  const mode = 'development';

  try {
    for await (const {code, sourcemap} of watch(
      absPath,
      'linked',
      mode,
      ac.signal,
    )) {
      assert(sourcemap);
      const start = Date.now();
      process.stdout.write(
        (first ? 'Starting' : 'Restarting') + ' dev server...',
      );

      mfAc?.abort();
      mfAc = new AbortController();

      const {href} = await startDevServer(
        code,
        sourcemap,
        port,
        mode,
        mfAc.signal,
      );
      process.stdout.write(` Done in ${Date.now() - start}ms.\n`);
      if (first && !silenceStartupMessage) {
        console.log(`
Dev server running at:
  ${href}
`);

        first = false;
      }
    }
  } catch (e) {
    throw new ErrorWrapper(e, 'WARNING');
  } finally {
    mfAc?.abort();
  }
}
