/**
 * These types represent the _compiled_ config whereas `define-config` types represent the _source_ config.
 */

import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {tsImport} from 'tsx/esm/api';
import * as v from '../../../shared/src/valita.js';
import {astSchema} from '../../../zero-protocol/src/ast.js';
import {parseOptions, type Config} from './config.js';

export type Action = 'select' | 'insert' | 'update' | 'delete';

const ruleSchema = v.tuple([v.literal('allow'), astSchema]);
export type Rule = v.Infer<typeof ruleSchema>;
const policySchema = v.array(ruleSchema);
export type Policy = v.Infer<typeof policySchema>;

const assetSchema = v.object({
  select: policySchema.optional(),
  insert: policySchema.optional(),
  update: policySchema.optional(),
  delete: policySchema.optional(),
});

export type AssetAuthorization = v.Infer<typeof assetSchema>;

const authorizationConfigSchema = v.record(
  v.object({
    table: assetSchema.optional(),
    column: v.record(assetSchema).optional(),
    row: assetSchema.optional(),
    cell: v.record(assetSchema).optional(),
  }),
);

// TODO: This will be moved into schema.ts
export type AuthorizationConfig = v.Infer<typeof authorizationConfigSchema>;

/**
 * Configures the view of the upstream database replicated to this zero-cache.
 */
const shardOptions = {
  id: {
    type: v.string().default('0'),
    desc: [
      'Unique identifier for the zero-cache shard.',
      '',
      'A shard presents a logical partition of the upstream database, delineated',
      'by a set of publications and managed by a dedicated replication slot.',
      '',
      `A shard's zero {bold clients} table and shard-internal functions are stored in`,
      `the {bold zero_\\{id\\}} schema in the upstream database.`,
    ],
    allCaps: true, // so that the flag is --shardID
  },

  publications: {
    type: v.array(v.string()).optional(() => []),
    desc: [
      `Postgres {bold PUBLICATION}s that define the partition of the upstream`,
      `replicated to the shard. All publication names must begin with the prefix`,
      `{bold zero_}, and all tables must be in the {bold public} schema.`,
      ``,
      `If unspecified, zero-cache will create and use a {bold zero_public} publication that`,
      `publishes all tables in the {bold public} schema, i.e.:`,
      ``,
      `CREATE PUBLICATION zero_public FOR TABLES IN SCHEMA public;`,
      ``,
      `Note that once a shard has begun syncing data, this list of publications`,
      `cannot be changed, and zero-cache will refuse to start if a specified`,
      `value differs from what was originally synced.`,
      ``,
      `To use a different set of publications, a new shard should be created.`,
    ],
  },
};

export type ShardConfig = Config<typeof shardOptions>;

const logOptions = {
  level: v
    .union(
      v.literal('debug'),
      v.literal('info'),
      v.literal('warn'),
      v.literal('error'),
    )
    .default('info'),

  format: {
    type: v.union(v.literal('text'), v.literal('json')).default('text'),
    desc: [
      `Use {bold text} for developer-friendly console logging`,
      `and {bold json} for consumption by structured-logging services`,
    ],
  },
};

export type LogConfig = Config<typeof logOptions>;

const perUserMutationLimit = {
  max: {
    type: v.number().optional(),
    desc: [
      `The maximum mutations per user within the specified {bold windowMs}.`,
      `If unset, no rate limiting is enforced.`,
    ],
  },
  windowMs: {
    type: v.number().default(60_000),
    desc: [
      `The sliding window over which the {bold perUserMutationLimitMax} is enforced.`,
    ],
  },
};

export type RateLimit = Config<typeof perUserMutationLimit>;

// Note: --help will list flags in the order in which they are defined here,
// so order the fields such that the important (e.g. required) ones are first.
// (Exported for testing)
export const zeroOptions = {
  upstream: {
    db: {
      type: v.string(),
      desc: [
        `The "upstream" authoritative postgres database.`,
        `In the future we will support other types of upstream besides PG.`,
      ],
    },

    maxConns: {
      type: v.number().default(20),
      desc: [
        `The maximum number of connections to open to the upstream database`,
        `for committing mutations. This is divided evenly amongst sync workers.`,
        `In addition to this number, zero-cache uses one connection for the`,
        `replication stream.`,
        ``,
        `Note that this number must allow for at least one connection per`,
        `sync worker, or zero-cache will fail to start. See {bold --numSyncWorkers}`,
      ],
    },

    maxConnsPerWorker: {
      type: v.number().optional(),
      hidden: true, // Passed from main thread to sync workers
    },
  },

  cvr: {
    db: {
      type: v.string(),
      desc: [
        `A separate Postgres database we use to store CVRs. CVRs (client view records)`,
        `keep track of which clients have which data. This is how we know what diff to`,
        `send on reconnect. It can be same database as above, but it makes most sense`,
        `for it to be a separate "database" in the same postgres "cluster".`,
      ],
    },

    maxConns: {
      type: v.number().default(30),
      desc: [
        `The maximum number of connections to open to the CVR database.`,
        `This is divided evenly amongst sync workers.`,
        ``,
        `Note that this number must allow for at least one connection per`,
        `sync worker, or zero-cache will fail to start. See {bold --numSyncWorkers}`,
      ],
    },

    maxConnsPerWorker: {
      type: v.number().optional(),
      hidden: true, // Passed from main thread to sync workers
    },
  },

  change: {
    db: {
      type: v.string(),
      desc: [`Yet another Postgres database, used to store a replication log.`],
    },

    maxConns: {
      type: v.number().default(1),
      desc: [
        `The maximum number of connections to open to the change database.`,
        `This is used by the {bold change-streamer} for catching up`,
        `{bold zero-cache} replication subscriptions.`,
      ],
    },
  },

  replicaFile: {
    type: v.string(),
    desc: [
      `File path to the SQLite replica that zero-cache maintains.`,
      `This can be lost, but if it is, zero-cache will have to re-replicate next`,
      `time it starts up.`,
    ],
  },

  log: logOptions,

  shard: shardOptions,

  port: {
    type: v.number().default(4848),
    desc: [
      `The main port for client connections.`,
      `Internally, zero-cache will also listen on the 2 ports after {bold --port}.`,
    ],
  },

  changeStreamerPort: {
    type: v.number().optional(),
    desc: [
      `The port on which the {bold change-streamer} runs. This is an internal`,
      `protocol between the {bold replication-manager} and {bold zero-cache}, which`,
      `runs in the same process in local development.`,
      ``,
      `If unspecified, defaults to {bold --port} + 1.`,
    ],
  },

  heartbeatMonitorPort: {
    type: v.number().optional(),
    desc: [
      `The port on which the heartbeat monitor listens for heartbeat`,
      `health checks. Once health checks are received at this port,`,
      `the monitor considers it a keepalive signal and triggers a drain`,
      `if health checks stop for more than 15 seconds. If health checks`,
      `never arrive on this port, the monitor does nothing (i.e. opt-in).`,
      ``,
      `If unspecified, defaults to {bold --port} + 2.`,
    ],
  },

  jwtSecret: {
    type: v.string().optional(),
    desc: [`JWT secret for verifying authentication tokens.`],
  },

  perUserMutationLimit,

  numSyncWorkers: {
    type: v.number().optional(),
    desc: [
      `The number of processes to use for view syncing.`,
      `Leave this unset to use the maximum available parallelism.`,
      `If set to 0, the server runs without sync workers, which is the`,
      `configuration for running the {bold replication-manager}.`,
    ],
  },

  changeStreamerURI: {
    type: v.string().optional(),
    desc: [
      `When unset, the zero-cache runs its own {bold replication-manager}`,
      `(i.e. {bold change-streamer}). In production, this should be set to`,
      `the {bold replication-manager} URI, which runs a {bold change-streamer}`,
      `on port 4849.`,
    ],
  },

  litestream: {
    type: v.boolean().optional(),
    desc: [
      `Indicates that a {bold litestream replicate} process is backing up the`,
      `{bold replicaDBFile}. This should be the production configuration for the`,
      `{bold replication-manager}. It is okay to run this in development too.`,
      ``,
      `Note that this flag does actually run {bold litestream}; rather, it `,
      `configures the internal replication logic to operate on the DB file in `,
      `a manner that is compatible with {bold litestream}.`,
    ],
  },

  storageDBTmpDir: {
    type: v.string().optional(),
    desc: [
      `tmp directory for IVM operator storage. Leave unset to use os.tmpdir()`,
    ],
  },
  warmWebsocket: {
    type: v.number().optional(),
    hidden: true, // for internal experimentation
  },
};

export type ZeroConfig = Config<typeof zeroOptions>;

export type Authorization = {authorization?: AuthorizationConfig | undefined};

// TODO: Remove when auth is moved to schema.
export type ZeroConfigWithAuthorization = ZeroConfig & Authorization;

const ENV_VAR_PREFIX = 'ZERO_';

let loadedConfig: Promise<ZeroConfigWithAuthorization> | undefined;

export function getZeroConfig(
  argv = process.argv.slice(2),
): Promise<ZeroConfigWithAuthorization> {
  if (loadedConfig) {
    return loadedConfig;
  }

  const dirname = path.dirname(fileURLToPath(import.meta.url));
  const configFile = process.env['ZERO_CONFIG_PATH'] ?? './zero.config.ts';
  const absoluteConfigPath = path.resolve(configFile);
  const relativePath = path.join(
    path.relative(dirname, path.dirname(absoluteConfigPath)),
    path.basename(absoluteConfigPath),
  );

  loadedConfig = tsImport(relativePath, import.meta.url)
    .then(async module => (await module.default) as Authorization)
    .then(authorization => ({
      ...authorization,
      ...parseOptions(zeroOptions, argv, ENV_VAR_PREFIX),
    }))
    .catch(e => {
      console.error(
        `Failed to load zero config from ${absoluteConfigPath}: ${e}`,
      );
      throw e;
    });
  return loadedConfig;
}
