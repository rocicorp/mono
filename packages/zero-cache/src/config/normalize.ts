import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {getHostIp, getTaskID} from './runtime.ts';
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
};

export function assertNormalized(
  config: ZeroConfig,
): asserts config is NormalizedZeroConfig {
  assert(config.taskID, 'missing --task-id');
  assert(config.changeStreamer.port, 'missing --change-streamer-port');
  assert(config.changeStreamer.address, 'missing --change-streamer-address');
  assert(config.litestream.port, 'missing --litestream-port');
  assert(config.change.db, 'missing --change-db');
  assert(config.cvr.db, 'missing --cvr-db');
}

/**
 * Normalizes the parsed `config` by setting defaults from the environment
 * or from other options as documented. When defaults are applied, the
 * corresponding `env` variable is updated so that the settings are propagated
 * to spawned child workers. Child workers can then call
 * {@link assertNormalized} to verify that the expected defaults have been set.
 */
// TODO: Merge / unify this with zero-config.ts:normalizeZeroConfig()
export async function normalizeConfig(
  lc: LogContext,
  config: ZeroConfig,
  env: NodeJS.ProcessEnv,
): Promise<NormalizedZeroConfig> {
  const autoTaskID = await getTaskID(lc);
  if (!config.taskID) {
    config.taskID = autoTaskID;
    env['ZERO_TASK_ID'] = autoTaskID;
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

  const hostIP = getHostIp(lc);
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

  lc.info?.(`taskID=${config.taskID}, hostIP=${hostIP}`);

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
  };
}
