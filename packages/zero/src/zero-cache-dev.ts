#!/usr/bin/env node

import {spawn, type ChildProcess} from 'node:child_process';
import {watch} from 'chokidar';
import {parseOptionsAdvanced} from '../../shared/src/options.js';
import {resolver} from '@rocicorp/resolver';
import {buildSchemaOptions} from '../../zero-schema/src/build-schema-options.js';
import {
  ZERO_ENV_VAR_PREFIX,
  zeroOptions,
} from '../../zero-cache/src/config/zero-config.js';
import 'dotenv/config';

const buildSchemaScript = 'zero-build-schema';
const zeroCacheScript = 'zero-cache';

function killProcess(childProcess: ChildProcess | undefined) {
  if (!childProcess || childProcess.exitCode !== null) {
    return Promise.resolve();
  }
  const {resolve, promise} = resolver();
  childProcess.on('exit', resolve);
  // Use SIGQUIT in particular since this will cause
  // a fast zero-cache shutdown instead of a graceful drain.
  childProcess.kill('SIGQUIT');
  return promise;
}

const LOG_COLOR = {
  red: 31,
  green: 32,
} as const;

function log(
  msg: string,
  color: keyof typeof LOG_COLOR = 'green',
  method: 'log' | 'error' = 'log',
) {
  console[method](`\x1b[${LOG_COLOR[color]}m> ${msg}\x1b[0m`);
}

function logError(msg: string) {
  log(msg, 'red', 'error');
}

async function main() {
  const {config} = parseOptionsAdvanced(
    {
      ...buildSchemaOptions,
      ...zeroOptions,
    },
    process.argv.slice(2),
    ZERO_ENV_VAR_PREFIX,
  );

  const {unknown: zeroArgs} = parseOptionsAdvanced(
    buildSchemaOptions,
    process.argv.slice(2),
    undefined,
    true,
  );

  const {unknown: schemaArgs} = parseOptionsAdvanced(
    zeroOptions,
    process.argv.slice(2),
    ZERO_ENV_VAR_PREFIX,
    true,
  );

  const {path} = config;

  let schemaProcess: ChildProcess | undefined;
  let zeroCacheProcess: ChildProcess | undefined;

  // Ensure child processes are killed when the main process exits
  process.on('exit', () => {
    schemaProcess?.kill('SIGQUIT');
    zeroCacheProcess?.kill('SIGQUIT');
  });

  async function buildSchemaAndStartZeroCache() {
    schemaProcess?.removeAllListeners('exit');
    zeroCacheProcess?.removeAllListeners('exit');
    await killProcess(schemaProcess);
    schemaProcess = undefined;
    await killProcess(zeroCacheProcess);
    zeroCacheProcess = undefined;

    log(`Running ${buildSchemaScript}.`);
    schemaProcess = spawn(buildSchemaScript, schemaArgs ?? [], {
      stdio: 'inherit',
    });

    schemaProcess.on('exit', (code: number) => {
      if (code === 0) {
        log(`${buildSchemaScript} completed successfully.`);
        log(`Running ${zeroCacheScript}.`);
        zeroCacheProcess = spawn(zeroCacheScript, zeroArgs || [], {
          stdio: 'inherit',
        });
        zeroCacheProcess.on('exit', () => {
          logError(`${zeroCacheScript} exited. Exiting.`);
          process.exit(-1);
        });
      } else {
        logError(
          `Errors in ${path} must be fixed before zero-cache can be started.`,
        );
      }
    });
  }

  await buildSchemaAndStartZeroCache();

  // Watch for file changes
  const watcher = watch(path, {
    ignoreInitial: true,
    awaitWriteFinish: {stabilityThreshold: 500, pollInterval: 100},
  });
  const onFileChange = async () => {
    log(`Detected ${path} change.`, 'green');
    await buildSchemaAndStartZeroCache();
  };
  watcher.on('add', onFileChange);
  watcher.on('change', onFileChange);
  watcher.on('unlink', onFileChange);
}

process.on('unhandledRejection', reason => {
  logError('Unexpected unhandled rejection.');
  console.error(reason);
  logError('Exiting');
  process.exit(-1);
});

main().catch(e => {
  logError(`Unexpected unhandled error.`);
  console.error(e);
  logError('Exiting.');
  process.exit(-1);
});
