import type {LogContext} from '@rocicorp/logger';
import {logOptions} from '../../../otel/src/log-options.ts';
import type {Config} from '../../../shared/src/options.ts';
import {appOptions, shardOptions, zeroOptions} from '../config/zero-config.ts';
import {decommissionShard} from '../services/change-source/pg/decommission.ts';
import {pgClient, type PostgresDB} from '../types/pg.ts';
import {cdcSchema, cvrSchema, getShardID} from '../types/shards.ts';
import {id} from '../types/sql.ts';

export const decommissionOptions = {
  app: {
    id: appOptions.id,
  },

  shard: {
    num: shardOptions.num,
  },

  upstream: {
    db: zeroOptions.upstream.db,
    type: zeroOptions.upstream.type,
  },

  cvr: {
    db: zeroOptions.cvr.db,
  },

  change: {
    db: zeroOptions.change.db,
  },

  log: {level: logOptions.level, format: logOptions.format},
};

export type DecommissionConfig = Config<typeof decommissionOptions>;

export async function decommissionZero(
  lc: LogContext,
  cfg: DecommissionConfig,
) {
  const {app, shard} = cfg;
  const shardID = getShardID(cfg);
  lc.info?.(`Decommissioning app "${app.id}"`);

  if (cfg.upstream.type === 'pg') {
    const upstream = pgClient(lc, cfg.upstream.db);
    await decommissionShard(lc, upstream, app.id, shard.num);

    lc.debug?.(`Cleaning up upstream metadata from ${hostPort(upstream)}`);
    await upstream.unsafe(`DROP SCHEMA IF EXISTS ${id(app.id)} CASCADE`);
    await upstream.end();
  }

  const cvr = pgClient(lc, cfg.cvr.db ?? cfg.upstream.db);
  lc.debug?.(`Cleaning up cvc data from ${hostPort(cvr)}`);
  await cvr.unsafe(`DROP SCHEMA IF EXISTS ${id(cvrSchema(shardID))} CASCADE`);
  await cvr.end();

  const cdc = pgClient(lc, cfg.change.db ?? cfg.upstream.db);
  lc.debug?.(`Cleaning up cdc data from ${hostPort(cdc)}`);
  await cdc.unsafe(`DROP SCHEMA IF EXISTS ${id(cdcSchema(shardID))} CASCADE`);
  await cdc.end();

  lc.info?.(`App "${app.id}" decommissioned`);
}

function hostPort(db: PostgresDB) {
  const {host, port} = db.options;
  return `${host.join(',')}:${port?.at(0) ?? 5432}`;
}
