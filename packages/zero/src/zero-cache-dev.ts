#!/usr/bin/env node

import {spawn, type ChildProcess} from 'node:child_process';
import {watch} from 'chokidar';
import {parseOptions} from '../../shared/src/options.js';
import {resolver} from '@rocicorp/resolver';
import {schemaOptions} from '../../zero-schema/src/build-schema.js';

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
  const options = parseOptions(
    schemaOptions,
    process.argv.slice(2),
    'ZERO_SCHEMA_',
  );

  let schemaProcess: ChildProcess | undefined;
  let zeroCacheProcess: ChildProcess | undefined;

  // Ensure child processes are killed when the main process exits
  process.on('exit', () => {
    schemaProcess?.kill('SIGQUIT');
    zeroCacheProcess?.kill('SIGQUIT');
  });

  async function buildSchemaAndStartZeroCache() {
    // If schemaProcess is running remove the listener waiting
    // for its completion before we kill it.
    schemaProcess?.removeAllListeners('exit');
    await killProcess(schemaProcess);
    schemaProcess = undefined;
    await killProcess(zeroCacheProcess);
    zeroCacheProcess = undefined;

    log(`Running ${buildSchemaScript}.`);
    schemaProcess = spawn(buildSchemaScript, process.argv.slice(2), {
      stdio: 'inherit',
    });

    schemaProcess.on('exit', (code: number) => {
      if (code === 0) {
        // Start zero cache
        log(`${buildSchemaScript} completed successfully.`);
        log(`Running ${zeroCacheScript}.`);
        zeroCacheProcess = spawn(zeroCacheScript, {stdio: 'inherit'});
      } else {
        logError(
          `Errors in ${options.path} must be fixed before zero-cache can be started.`,
        );
      }
    });
  }

  await buildSchemaAndStartZeroCache();

  // Watch for file changes
  const watcher = watch(options.path);
  const onFileChange = async () => {
    log(`Detected ${options.path} change.`, 'green');
    await buildSchemaAndStartZeroCache();
  };
  watcher.on('add', onFileChange);
  watcher.on('change', onFileChange);
  watcher.on('unlink', onFileChange);
}

process.on('unhandledRejection', reason => {
  logError(`Unexpected unhandled rejection.\n${reason}\nExiting.`);
  process.exit(-1);
});

main().catch(e => {
  logError(`Unexpected unhandled error.\n${e}\nExiting.`);
  process.exit(-1);
});
