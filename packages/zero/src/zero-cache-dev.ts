#!/usr/bin/env node
/* eslint-disable no-console */

import '../../shared/src/dotenv.ts';
import {resolver} from '@rocicorp/resolver';
import chalk from 'chalk';
import {watch} from 'chokidar';
import {spawn, type ChildProcess} from 'node:child_process';
import {parseOptionsAdvanced} from '../../shared/src/options.ts';
import {
  ZERO_ENV_VAR_PREFIX,
  zeroOptions,
} from '../../zero-cache/src/config/zero-config.ts';
import {deployPermissionsOptions} from '../../zero-cache/src/scripts/permissions.ts';

const deployPermissionsScript = 'zero-deploy-permissions';
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

function log(msg: string) {
  console.log(chalk.green('> ' + msg));
}

function logWarn(msg: string) {
  console.log(chalk.yellow('> ' + msg));
}

function logError(msg: string) {
  console.error(chalk.red('> ' + msg));
}

async function main() {
  const {config} = parseOptionsAdvanced(
    {
      ...deployPermissionsOptions,
      ...zeroOptions,
    },
    process.argv.slice(2),
    ZERO_ENV_VAR_PREFIX,
    false,
    true, // allowPartial, required by server/runner/config.ts
  );

  const {unknown: zeroCacheArgs} = parseOptionsAdvanced(
    deployPermissionsOptions,
    process.argv.slice(2),
    ZERO_ENV_VAR_PREFIX,
    true,
  );

  const {unknown: deployPermissionsArgs} = parseOptionsAdvanced(
    zeroOptions,
    process.argv.slice(2),
    ZERO_ENV_VAR_PREFIX,
    true,
  );

  const {path} = config.schema;

  let permissionsProcess: ChildProcess | undefined;
  let zeroCacheProcess: ChildProcess | undefined;

  // Ensure child processes are killed when the main process exits
  process.on('exit', () => {
    permissionsProcess?.kill('SIGQUIT');
    zeroCacheProcess?.kill('SIGQUIT');
  });

  async function deployPermissions(): Promise<boolean> {
    if (config.upstream.type !== 'pg') {
      logWarn(
        `Skipping permissions deployment for ${config.upstream.type} upstream`,
      );
      return true;
    }
    permissionsProcess?.removeAllListeners('exit');
    await killProcess(permissionsProcess);
    permissionsProcess = undefined;

    log(`Running ${deployPermissionsScript}.`);
    permissionsProcess = spawn(
      deployPermissionsScript,
      deployPermissionsArgs ?? [],
      {
        stdio: 'inherit',
        shell: true,
      },
    );

    const {promise: code, resolve} = resolver<number>();
    permissionsProcess.on('exit', resolve);
    if ((await code) === 0) {
      log(`${deployPermissionsScript} completed successfully.`);
      return true;
    }
    logError(`Failed to deploy permissions from ${path}.`);
    return false;
  }

  async function deployPermissionsAndStartZeroCache() {
    zeroCacheProcess?.removeAllListeners('exit');
    await killProcess(zeroCacheProcess);
    zeroCacheProcess = undefined;

    if (await deployPermissions()) {
      log(
        `Running ${zeroCacheScript} at\n\n\thttp://localhost:${config.port}\n`,
      );
      const env: Record<string, string> = {
        // Set some low defaults so as to use fewer resources and not trip up,
        // e.g. developers sharing a database.
        ['ZERO_NUM_SYNC_WORKERS']: '3',
        ['ZERO_CVR_MAX_CONNS']: '6',
        ['ZERO_UPSTREAM_MAX_CONNS']: '6',
        // But let the developer override any of these dev defaults.
        ...process.env,
      };
      if (process.env.OTEL_EXPORTER_OTLP_ENDPOINT) {
        log('Starting OpenTelemetry auto instrumentation.');
        env['NODE_OPTIONS'] =
          (env['NODE_OPTIONS'] || '') +
          ' --require @opentelemetry/auto-instrumentations-node/register';
      }
      zeroCacheProcess = spawn(zeroCacheScript, zeroCacheArgs || [], {
        env,
        stdio: 'inherit',
        shell: true,
      });
      zeroCacheProcess.on('exit', () => {
        logError(`${zeroCacheScript} exited. Exiting.`);
        process.exit(-1);
      });
    }
  }

  await deployPermissionsAndStartZeroCache();

  // Watch for file changes
  const watcher = watch(path, {
    ignoreInitial: true,
    awaitWriteFinish: {stabilityThreshold: 500, pollInterval: 100},
  });
  const onFileChange = async () => {
    log(`Detected ${path} change.`);
    await deployPermissions();
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
