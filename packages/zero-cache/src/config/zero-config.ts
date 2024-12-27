/**
 * These types represent the _compiled_ config whereas `define-config` types represent the _source_ config.
 */

import {parseOptions, type Config} from '../../../shared/src/options.js';
import * as v from '../../../shared/src/valita.js';
import {runtimeDebugFlags} from '../../../zqlite/src/runtime-debug.js';
import {singleProcessMode} from '../types/processes.js';

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
      '',
      'Due to constraints on replication slot names, a shard ID may only consist of',
      'lower-case letters, numbers, and the underscore character.',
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
      `If unspecified, zero-cache will create and use an internal publication that`,
      `publishes all tables in the {bold public} schema, i.e.:`,
      ``,
      `CREATE PUBLICATION _zero_public_0 FOR TABLES IN SCHEMA public;`,
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

  traceCollector: {
    type: v.string().optional(),
    desc: [
      `The URL of the trace collector to which to send trace data. Traces are sent over http.`,
      `Port defaults to 4318 for most collectors.`,
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

const authOptions = {
  jwk: {
    type: v.string().optional(),
    desc: [
      `A public key in JWK format used to verify JWTs. Only one of {bold jwk}, {bold jwksUrl} and {bold secret} may be set.`,
    ],
  },
  jwksUrl: {
    type: v.string().optional(),
    desc: [
      `A URL that returns a JWK set used to verify JWTs. Only one of {bold jwk}, {bold jwksUrl} and {bold secret} may be set.`,
    ],
  },
  secret: {
    type: v.string().optional(),
    desc: [
      `A symmetric key used to verify JWTs. Only one of {bold jwk}, {bold jwksUrl} and {bold secret} may be set.`,
    ],
  },
};

export type AuthConfig = Config<typeof authOptions>;

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

  queryHydrationStats: {
    type: v.boolean().optional(),
    desc: [
      `Track and log the number of rows considered by each query in the system.`,
      `This is useful for debugging and performance tuning.`,
    ],
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

  schema: {
    file: {
      type: v.string().default('zero-schema.json'),
      desc: [
        `File path to the JSON schema file that defines the database structure`,
        `and access control rules.`,
      ],
    },
    json: {
      type: v.string().optional(),
      desc: [
        `The JSON schema as a string, containing the same database structure`,
        `and access control rules as would be in the schema file.`,
      ],
    },
  },

  log: logOptions,

  shard: shardOptions,

  auth: authOptions,

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

  taskID: {
    type: v.string().optional(),
    desc: [
      `Globally unique identifier for the zero-cache instance.`,
      ``,
      `Setting this to a platform specific task identifier can be useful for debugging.`,
      `If unspecified, zero-cache will attempt to extract the TaskARN if run from within`,
      `an AWS ECS container, and otherwise use a random string.`,
    ],
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

  autoReset: {
    type: v.boolean().optional(),
    desc: [
      `Automatically wipe and resync the replica when replication is halted.`,
      `This situation can occur for configurations in which the upstream database`,
      `provider prohibits event trigger creation, preventing the zero-cache from`,
      `being able to correctly replicate schema changes. For such configurations,`,
      `an upstream schema change will instead result in halting replication with an`,
      `error indicating that the replica needs to be reset.`,
      ``,
      `When {bold auto-reset} is enabled, zero-cache will respond to such situations`,
      `by shutting down, and when restarted, resetting the replica and all synced `,
      `clients. This is a heavy-weight operation and can result in user-visible`,
      `slowness or downtime if compute resources are scarce.`,
      ``,
      `Moreover, {bold auto-reset} is only supported for single-node configurations`,
      `with a permanent volume for the replica. Specifically, it is incompatible`,
      `with the {bold litestream} option, and will be ignored with a warning if`,
      `set in combination with {bold litestream}.`,
    ],
  },

  litestream: {
    type: v.boolean().optional(),
    desc: [
      `Indicates that a {bold litestream replicate} process is backing up the`,
      `{bold replica-file}. This should be the production configuration for the`,
      `{bold replication-manager}. It is okay to run this in development too.`,
      ``,
      `Note that this flag does not actually run {bold litestream}; rather, it `,
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

  tenantID: {
    type: v.string().optional(),
    desc: ['Passed by multi/main.ts to tag the LogContext of zero-caches'],
    hidden: true,
  },
};

const debugOptions = {
  ...zeroOptions,
  debug: {
    ast: {
      type: v.string().optional(),
      desc: ['AST for the query to be transformed or timed.'],
    },
    hash: {
      type: v.string().optional(),
      desc: ['Hash of the query to fetch the AST for.'],
    },
  },
};

export type ZeroConfig = Config<typeof zeroOptions>;

export const ZERO_ENV_VAR_PREFIX = 'ZERO_';

let loadedConfig: ZeroConfig | undefined;

export function getZeroConfig(
  env: NodeJS.ProcessEnv = process.env,
  argv = process.argv.slice(2),
): ZeroConfig {
  if (!loadedConfig || singleProcessMode()) {
    loadedConfig = parseOptions(zeroOptions, argv, ZERO_ENV_VAR_PREFIX, env);

    if (loadedConfig.queryHydrationStats) {
      runtimeDebugFlags.trackRowsVended = true;
    }
  }

  return loadedConfig;
}

export function getDebugConfig(
  env: NodeJS.ProcessEnv = process.env,
  argv = process.argv.slice(2),
) {
  return parseOptions(debugOptions, argv, ZERO_ENV_VAR_PREFIX, env);
}
