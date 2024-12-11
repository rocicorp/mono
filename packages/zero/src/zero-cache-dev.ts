#!/usr/bin/env node

import {spawn, type ChildProcess} from 'child_process';
import * as fs from 'fs';
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
  childProcess.removeAllListeners('exit');
  childProcess.on('exit', resolve);
  childProcess.kill('SIGQUIT');
  return promise;
}

enum LogColor {
  Red = 31,
  Green = 32,
}

function log(msg: string, color: LogColor, method: 'log' | 'error' = 'log') {
  console[method](`\x1b[${color}m ${msg} \x1b[0m`);
}

function main() {
  const options = parseOptions(
    schemaOptions,
    process.argv.slice(2),
    'ZERO_SCHEMA_',
  );

  let schemaProcess: ChildProcess | undefined;
  let zeroCacheProcess: ChildProcess | undefined;

  function startZeroCache() {
    log(`Running ${zeroCacheScript}.`, LogColor.Green);
    zeroCacheProcess = spawn(zeroCacheScript, {stdio: 'inherit'});
  }

  async function killProcesses(): Promise<void> {
    await killProcess(schemaProcess);
    schemaProcess = undefined;
    await killProcess(zeroCacheProcess);
    zeroCacheProcess = undefined;
  }

  function buildSchemaAndStartZeroCache() {
    schemaProcess = spawn(buildSchemaScript, process.argv.slice(2), {
      stdio: 'inherit',
    });
    log(`Running ${buildSchemaScript}.`, LogColor.Green);

    schemaProcess.on('exit', (code: number) => {
      if (code === 0) {
        startZeroCache();
      } else {
        log(
          `Errors in ${options.path} must be fixed before zero-cache can be started.`,
          LogColor.Red,
          'error',
        );
      }
    });
  }

  buildSchemaAndStartZeroCache();

  // Watch for file changes
  fs.watch(options.path, async () => {
    log(`Detected ${options.path} change.`, LogColor.Green);
    await killProcesses();
    buildSchemaAndStartZeroCache();
  });

  // Ensure child processes are killed when the main process exits
  process.on('exit', () => {
    schemaProcess?.kill('SIGQUIT');
    zeroCacheProcess?.kill('SIGQUIT');
  });
}

main();
