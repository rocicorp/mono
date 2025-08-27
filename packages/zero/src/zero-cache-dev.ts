#!/usr/bin/env node
/* eslint-disable no-console */

import {resolver} from '@rocicorp/resolver';
import {watch} from 'chokidar';
import {spawn, type ChildProcess} from 'node:child_process';
import '../../shared/src/dotenv.ts';
import {createLogContext} from '../../shared/src/logging.ts';
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

async function main() {
  const {config} = parseOptionsAdvanced(
    {
      ...deployPermissionsOptions,
      ...zeroOptions,
    },
    {
      envNamePrefix: ZERO_ENV_VAR_PREFIX,
      // TODO: This may no longer be necessary since multi-tenant was removed.
      allowPartial: true, // required by server/runner/config.ts
    },
  );

  const lc = createLogContext(config);

  process.on('unhandledRejection', reason => {
    lc.error?.('Unexpected unhandled rejection.', reason);
    lc.error?.('Exiting');
    process.exit(-1);
  });

  const {unknown: zeroCacheArgs} = parseOptionsAdvanced(
    deployPermissionsOptions,
    {envNamePrefix: ZERO_ENV_VAR_PREFIX, allowUnknown: true},
  );

  const {unknown: deployPermissionsArgs} = parseOptionsAdvanced(zeroOptions, {
    envNamePrefix: ZERO_ENV_VAR_PREFIX,
    allowUnknown: true,
  });

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
      lc.warn?.(
        `Skipping permissions deployment for ${config.upstream.type} upstream`,
      );
      return true;
    }
    permissionsProcess?.removeAllListeners('exit');
    await killProcess(permissionsProcess);
    permissionsProcess = undefined;

    lc.info?.(`Running ${deployPermissionsScript}.`);
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
      lc.info?.(`${deployPermissionsScript} completed successfully.`);
      return true;
    }
    lc.error?.(`Failed to deploy permissions from ${path}.`);
    return false;
  }

  async function deployPermissionsAndStartZeroCache() {
    zeroCacheProcess?.removeAllListeners('exit');
    await killProcess(zeroCacheProcess);
    zeroCacheProcess = undefined;

    if (await deployPermissions()) {
      lc.info?.(
        `Running ${zeroCacheScript} at\n\n\thttp://localhost:${config.port}\n`,
      );
      const env: NodeJS.ProcessEnv = {
        // Set some low defaults so as to use fewer resources and not trip up,
        // e.g. developers sharing a database.
        ['ZERO_NUM_SYNC_WORKERS']: '3',
        ['ZERO_CVR_MAX_CONNS']: '6',
        ['ZERO_UPSTREAM_MAX_CONNS']: '6',
        // But let the developer override any of these dev defaults.
        ...process.env,
      };
      zeroCacheProcess = spawn(zeroCacheScript, zeroCacheArgs || [], {
        env,
        stdio: 'inherit',
        shell: true,
      });
      zeroCacheProcess.on('exit', () => {
        lc.error?.(`${zeroCacheScript} exited. Exiting.`);
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
    lc.info?.(`Detected ${path} change.`);
    await deployPermissions();
  };
  watcher.on('add', onFileChange);
  watcher.on('change', onFileChange);
  watcher.on('unlink', onFileChange);
}

void main();
