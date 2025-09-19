import type {LogContext} from '@rocicorp/logger';
import {nanoid} from 'nanoid';
import {availableParallelism} from 'node:os';
import {assert, assertNotUndefined} from '../../../shared/src/asserts.ts';
import {getHostIp} from './network.ts';
import type {ZeroConfig} from './zero-config.ts';

/** {@link ZeroConfig} with defaults set per option documentation. */
export type NormalizedZeroConfig = ZeroConfig & {
  taskID: string;
  changeStreamer: {
    port: number;
    address: string;
  };
  change: {
    db: string;
  };
  cvr: {
    db: string;
  };
  litestream: {
    port: number;
  };
  numSyncWorkers: number;
};

export function isDevelopmentMode(): boolean {
  return process.env.NODE_ENV === 'development';
}

export function assertNormalized(
  config: ZeroConfig,
): asserts config is NormalizedZeroConfig {
  assert(config.taskID, 'missing --task-id');
  assert(config.changeStreamer.port, 'missing --change-streamer-port');
  assert(config.changeStreamer.address, 'missing --change-streamer-address');
  assert(config.litestream.port, 'missing --litestream-port');
  assert(config.change.db, 'missing --change-db');
  assert(config.cvr.db, 'missing --cvr-db');
  assertNotUndefined(config.numSyncWorkers, 'missing --num-sync-workers');

  if (!isDevelopmentMode()) {
    assert(
      config.adminPassword,
      'missing --admin-password: required in production mode',
    );
  }
}

/**
 * Normalizes the parsed `config` by setting defaults from the environment
 * or from other options as documented. When defaults are applied, the
 * corresponding `env` variable is updated so that the settings are propagated
 * to spawned child workers. Child workers can then call
 * {@link assertNormalized} to verify that the expected defaults have been set.
 */
export function normalizeZeroConfig(
  lc: LogContext,
  config: ZeroConfig,
  env: NodeJS.ProcessEnv,
  defaultTaskID?: string,
): NormalizedZeroConfig {
  if (!config.taskID) {
    const taskID = defaultTaskID ?? nanoid();
    config.taskID = taskID;
    env['ZERO_TASK_ID'] = taskID;
  }
  if (!config.changeStreamer.port) {
    const port = config.port + 1;
    config.changeStreamer.port = port;
    env['ZERO_CHANGE_STREAMER_PORT'] = String(port);
  }
  if (!config.litestream.port) {
    const port = config.port + 2;
    config.litestream.port = port;
    env['ZERO_LITESTREAM_PORT'] = String(port);
  }
  if (config.numSyncWorkers === undefined) {
    // Reserve 1 core for the replicator. The change-streamer is not CPU heavy.
    const numSyncers = Math.max(1, availableParallelism() - 1);
    config.numSyncWorkers = numSyncers;
    env['ZERO_NUM_SYNC_WORKERS'] = String(numSyncers);
  }

  const hostIP = getHostIp(
    lc,
    config.changeStreamer.discoveryInterfacePreferences,
  );
  if (!config.changeStreamer.address) {
    const {port} = config.changeStreamer;
    const address = `${hostIP}:${port}`;
    config.changeStreamer.address = address;
    env['ZERO_CHANGE_STREAMER_ADDRESS'] = address;
  }

  if (!config.change.db) {
    config.change.db = config.upstream.db;
    env['ZERO_CHANGE_DB'] = config.upstream.db;
  }

  if (!config.cvr.db) {
    config.cvr.db = config.upstream.db;
    env['ZERO_CVR_DB'] = config.upstream.db;
  }

  lc.info?.(`runtime env: taskID=${config.taskID}, hostIP=${hostIP}`);

  return {
    ...config,
    taskID: config.taskID,

    changeStreamer: {
      ...config.changeStreamer,
      port: config.changeStreamer.port,
      address: config.changeStreamer.address,
    },

    litestream: {
      ...config.litestream,
      port: config.litestream.port,
    },

    change: {
      ...config.change,
      db: config.change.db,
    },

    cvr: {
      ...config.cvr,
      db: config.cvr.db,
    },

    numSyncWorkers: config.numSyncWorkers,
  };
}
