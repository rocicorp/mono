/**
 * These types represent the _compiled_ config whereas `define-config` types represent the _source_ config.
 */

import {logOptions} from '../../../otel/src/log-options.ts';
import {parseOptions, type Config} from '../../../shared/src/options.ts';
import * as v from '../../../shared/src/valita.ts';
import {runtimeDebugFlags} from '../../../zqlite/src/runtime-debug.ts';
import {singleProcessMode} from '../types/processes.ts';
import {
  ALLOWED_APP_ID_CHARACTERS,
  INVALID_APP_ID_MESSAGE,
} from '../types/shards.ts';
export type {LogConfig} from '../../../otel/src/log-options.ts';

export const appOptions = {
  id: {
    type: v
      .string()
      .default('zero')
      .assert(id => ALLOWED_APP_ID_CHARACTERS.test(id), INVALID_APP_ID_MESSAGE),
    desc: [
      'Unique identifier for the app.',
      '',
      'Multiple zero-cache apps can run on a single upstream database, each of which',
      'is isolated from the others, with its own permissions, sharding (future feature),',
      'and change/cvr databases.',
      '',
      'The metadata of an app is stored in an upstream schema with the same name,',
      'e.g. "zero", and the metadata for each app shard, e.g. client and mutation',
      'ids, is stored in the "\\{app-id\\}_\\{#\\}" schema. (Currently there is only a single',
      '"0" shard, but this will change with sharding).',
      '',
      'The CVR and Change data are managed in schemas named "\\{app-id\\}_\\{shard-num\\}/cvr"',
      'and "\\{app-id\\}_\\{shard-num\\}/cdc", respectively, allowing multiple apps and shards',
      'to share the same database instance (e.g. a Postgres "cluster") for CVR and Change management.',
      '',
      'Due to constraints on replication slot names, an App ID may only consist of',
      'lower-case letters, numbers, and the underscore character.',
      '',
      'Note that this option is used by both {bold zero-cache} and {bold zero-deploy-permissions}.',
    ],
  },

  publications: {
    type: v.array(v.string()).optional(() => []),
    desc: [
      `Postgres {bold PUBLICATION}s that define the tables and columns to`,
      `replicate. Publication names may not begin with an underscore,`,
      `as zero reserves that prefix for internal use.`,
      ``,
      `If unspecified, zero-cache will create and use an internal publication that`,
      `publishes all tables in the {bold public} schema, i.e.:`,
      ``,
      `CREATE PUBLICATION _\\{app-id\\}_public_0 FOR TABLES IN SCHEMA public;`,
      ``,
      `Note that changing the set of publications will result in resyncing the replica,`,
      `which may involve downtime (replication lag) while the new replica is initializing.`,
      `To change the set of publications without disrupting an existing app, a new app`,
      `should be created.`,
    ],
  },
};

export const shardOptions = {
  id: {
    type: v
      .string()
      .assert(() => {
        throw new Error(
          `ZERO_SHARD_ID is deprecated. Please use ZERO_APP_ID instead.`,
          // TODO: Link to release / migration notes?
        );
      })
      .optional(),
    hidden: true,
  },

  num: {
    type: v.number().default(0),
    desc: [
      `The shard number (from 0 to NUM_SHARDS) of the App. zero will eventually`,
      `support data sharding as a first-class primitive; until then, deploying`,
      `multiple shard-nums creates functionally identical shards. Until sharding is`,
      `actually meaningful, this flag is hidden but available for testing.`,
    ],
    hidden: true,
  },
};

const replicaOptions = {
  file: {
    type: v.string(),
    desc: [
      `File path to the SQLite replica that zero-cache maintains.`,
      `This can be lost, but if it is, zero-cache will have to re-replicate next`,
      `time it starts up.`,
    ],
  },

  vacuumIntervalHours: {
    type: v.number().optional(),
    desc: [
      `Performs a VACUUM at server startup if the specified number of hours has elapsed`,
      `since the last VACUUM (or initial-sync). The VACUUM operation is heavyweight`,
      `and requires double the size of the db in disk space. If unspecified, VACUUM`,
      `operations are not performed.`,
    ],
  },
};

export type ReplicaOptions = Config<typeof replicaOptions>;

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

    type: {
      type: v.literalUnion('pg', 'custom').default('pg'),
      desc: [
        `The meaning of the {bold upstream-db} depends on the upstream type:`,
        `* {bold pg}: The connection database string, e.g. "postgres://..."`,
        `* {bold custom}: The base URI of the change source "endpoint, e.g.`,
        `          "https://my-change-source.dev/changes/v0/stream?apiKey=..."`,
      ],
      hidden: true, // TODO: Unhide when ready to officially support.
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
        `sync worker, or zero-cache will fail to start. See {bold num-sync-workers}`,
      ],
    },

    maxConnsPerWorker: {
      type: v.number().optional(),
      hidden: true, // Passed from main thread to sync workers
    },
  },

  push: {
    url: {
      type: v.string().optional(), // optional until we remove CRUD mutations
      desc: [
        `The URL of the API server to which zero-cache will push mutations.`,
      ],
    },
    apiKey: {
      type: v.string().optional(),
      desc: [
        `An optional secret used to authorize zero-cache to call the API server.`,
      ],
    },
  },

  cvr: {
    db: {
      type: v.string().optional(),
      desc: [
        `The Postgres database used to store CVRs. CVRs (client view records) keep track`,
        `of the data synced to clients in order to determine the diff to send on reconnect.`,
        `If unspecified, the {bold upstream-db} will be used.`,
      ],
    },

    maxConns: {
      type: v.number().default(30),
      desc: [
        `The maximum number of connections to open to the CVR database.`,
        `This is divided evenly amongst sync workers.`,
        ``,
        `Note that this number must allow for at least one connection per`,
        `sync worker, or zero-cache will fail to start. See {bold num-sync-workers}`,
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
      type: v.string().optional(),
      desc: [
        `The Postgres database used to store recent replication log entries, in order`,
        `to sync multiple view-syncers without requiring multiple replication slots on`,
        `the upstream database. If unspecified, the {bold upstream-db} will be used.`,
      ],
    },

    maxConns: {
      type: v.number().default(5),
      desc: [
        `The maximum number of connections to open to the change database.`,
        `This is used by the {bold change-streamer} for catching up`,
        `{bold zero-cache} replication subscriptions.`,
      ],
    },
  },

  replica: replicaOptions,

  log: logOptions,

  app: appOptions,

  shard: shardOptions,

  auth: authOptions,

  port: {
    type: v.number().default(4848),
    desc: [`The port for sync connections.`],
  },

  changeStreamer: {
    mode: {
      type: v.literalUnion('dedicated', 'discover').default('dedicated'),
      desc: [
        `The mode for running or connecting to the change-streamer:`,
        `* {bold dedicated}: runs the change-streamer and shuts down when another`,
        `      change-streamer takes over the replication slot. This is appropriate in a `,
        `      single-node configuration, or for the {bold replication-manager} in a `,
        `      multi-node configuration.`,
        `* {bold discover}: connects to the change-streamer as internally advertised in the`,
        `      change-db. This is appropriate for the {bold view-syncers} in a multi-node `,
        `      configuration.`,
      ],
    },

    port: {
      type: v.number().optional(),
      desc: [
        `The port on which the {bold change-streamer} runs. This is an internal`,
        `protocol between the {bold replication-manager} and {bold view-syncers}, which`,
        `runs in the same process tree in local development or a single-node configuration.`,
        ``,
        `If unspecified, defaults to {bold --port} + 1.`,
      ],
    },

    address: {
      type: v.string().optional(),
      desc: [
        `The {bold host:port} for other processes to use when connecting to this `,
        `change-streamer. When unspecified, the machine's IP address and the`,
        `{bold --change-streamer-port} will be advertised for discovery.`,
        ``,
        `In most cases, the default behavior (unspecified) is sufficient, including in a`,
        `single-node configuration or a multi-node configuration with host/awsvpc networking`,
        `(e.g. Fargate).`,
        ``,
        `For a multi-node configuration in which the process is unable to determine the`,
        `externally addressable port (e.g. a container running with {bold bridge} mode networking),`,
        `the {bold --change-streamer-address} must be specified manually (e.g. a load balancer or`,
        `service discovery address).`,
      ],
    },

    discoveryInterfacePreferences: {
      type: v.array(v.string()).default([
        'eth', // linux
        'en', // macbooks
      ]),
      desc: [
        `The name prefixes to prefer when introspecting the network interfaces to determine`,
        `the externally reachable IP address for change-streamer discovery. This defaults`,
        `to commonly used names for standard ethernet interfaces in order to prevent selecting`,
        `special interfaces such as those for VPNs.`,
      ],
      // More confusing than it's worth to advertise this. The default list should be
      // adjusted to make things work for all environments; it is controlled as a
      // hidden flag as an emergency to unblock people with outlier network configs.
      hidden: true,
    },

    uri: {
      type: v
        .string()
        .assert(() => {
          throw new Error(
            `ZERO_CHANGE_STREAMER_URI is deprecated. Please see notes for ` +
              `ZERO_CHANGE_STREAMER_MODE: https://github.com/rocicorp/mono/pull/4335`,
          );
        })
        .optional(),
      hidden: true,
    },
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

  autoReset: {
    type: v.boolean().default(true),
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
    ],
  },

  adminPassword: {
    type: v.string().optional(),
    desc: [
      `A password used to administer zero-cache server, for example to access the`,
      `/statz endpoint.`,
    ],
  },

  litestream: {
    executable: {
      type: v.string().optional(),
      desc: [`Path to the {bold litestream} executable.`],
    },

    configPath: {
      type: v.string().default('./src/services/litestream/config.yml'),
      desc: [
        `Path to the litestream yaml config file. zero-cache will run this with its`,
        `environment variables, which can be referenced in the file via $\\{ENV\\}`,
        `substitution, for example:`,
        `* {bold ZERO_REPLICA_FILE} for the db path`,
        `* {bold ZERO_LITESTREAM_BACKUP_LOCATION} for the db replica url`,
        `* {bold ZERO_LITESTREAM_LOG_LEVEL} for the log level`,
        `* {bold ZERO_LOG_FORMAT} for the log type`,
      ],
    },

    logLevel: {
      type: v.literalUnion('debug', 'info', 'warn', 'error').default('warn'),
    },

    backupURL: {
      type: v.string().optional(),
      desc: [
        `The location of the litestream backup, usually an {bold s3://} URL.`,
        `This is only consulted by the {bold replication-manager}.`,
        `{bold view-syncers} receive this information from the {bold replication-manager}.`,
      ],
    },

    port: {
      type: v.number().optional(),
      desc: [
        `Port on which litestream exports metrics, used to determine the replication`,
        `watermark up to which it is safe to purge change log records.`,
        ``,
        `If unspecified, defaults to {bold --port} + 2.`,
      ],
    },

    checkpointThresholdMB: {
      type: v.number().default(40),
      desc: [
        `The size of the WAL file at which to perform an SQlite checkpoint to apply`,
        `the writes in the WAL to the main database file. Each checkpoint creates`,
        `a new WAL segment file that will be backed up by litestream. Smaller thresholds`,
        `may improve read performance, at the expense of creating more files to download`,
        `when restoring the replica from the backup.`,
      ],
    },

    incrementalBackupIntervalMinutes: {
      type: v.number().default(15),
      desc: [
        `The interval between incremental backups of the replica. Shorter intervals`,
        `reduce the amount of change history that needs to be replayed when catching`,
        `up a new view-syncer, at the expense of increasing the number of files needed`,
        `to download for the initial litestream restore.`,
      ],
    },

    snapshotBackupIntervalHours: {
      type: v.number().default(12),
      desc: [
        `The interval between snapshot backups of the replica. Snapshot backups`,
        `make a full copy of the database to a new litestream generation. This`,
        `improves restore time at the expense of bandwidth. Applications with a`,
        `large database and low write rate can increase this interval to reduce`,
        `network usage for backups (litestream defaults to 24 hours).`,
      ],
    },

    restoreParallelism: {
      type: v.number().default(48),
      desc: [
        `The number of WAL files to download in parallel when performing the`,
        `initial restore of the replica from the backup.`,
      ],
    },

    multipartConcurrency: {
      type: v.number().default(48),
      desc: [
        `The number of parts (of size {bold --litestream-multipart-size} bytes)`,
        `to download in parallel when restoring the snapshot from the backup.`,
        ``,
        `This requires a custom build of litestream (version 0.3.13+z0.0.1+).`,
        `Set to 0 to disable.`,
      ],
    },

    multipartSize: {
      type: v.number().default(16 * 1024 * 1024),
      desc: [
        `The size of each part when downloading the snapshot with {bold --multipart-concurrency}.`,
        `Multipart downloads require {bold concurrency * size} bytes of memory when restoring`,
        `the snapshot from the backup.`,
        ``,
        `This requires a custom build of litestream (version 0.3.13+z0.0.1+).`,
      ],
    },
  },

  storageDBTmpDir: {
    type: v.string().optional(),
    desc: [
      `tmp directory for IVM operator storage. Leave unset to use os.tmpdir()`,
    ],
  },

  initialSync: {
    tableCopyWorkers: {
      type: v.number().default(5),
      desc: [
        `The number of parallel workers used to copy tables during initial sync.`,
        `Each worker uses a database connection and will buffer up to (approximately)`,
        `10 MB of table data in memory during initial sync. Increasing the number of`,
        `workers may improve initial sync speed; however, note that local disk throughput`,
        `(i.e. IOPS), upstream CPU, and network bandwidth may also be bottlenecks.`,
      ],
    },
  },

  tenantID: {
    type: v.string().optional(),
    desc: ['Passed by runner/main.ts to tag the LogContext of zero-caches'],
    hidden: true,
  },

  targetClientRowCount: {
    type: v.number().default(20_000),
    desc: [
      'The target number of rows to keep per client in the client side cache.',
      'This limit is a soft limit. When the number of rows in the cache exceeds',
      'this limit, zero-cache will evict inactive queries in order of ttl-based expiration.',
      'Active queries, on the other hand, are never evicted and are allowed to use more',
      'rows than the limit.',
    ],
  },

  lazyStartup: {
    type: v.boolean().default(false),
    desc: [
      'Delay starting the majority of zero-cache until first request.',
      '',
      'This is mainly intended to avoid connecting to Postgres replication stream',
      'until the first request is received, which can be useful i.e., for preview instances.',
      '',
      'Currently only supported in single-node mode.',
    ],
  },

  serverVersion: {
    type: v.string().optional(),
    desc: [`The version string outputted to logs when the server starts up.`],
  },
};

export type ZeroConfig = Config<typeof zeroOptions>;

export const ZERO_ENV_VAR_PREFIX = 'ZERO_';

let loadedConfig: Config<typeof zeroOptions> | undefined;

export function getZeroConfig(
  env: NodeJS.ProcessEnv = process.env,
  argv = process.argv.slice(2),
) {
  if (!loadedConfig || singleProcessMode()) {
    loadedConfig = parseOptions(zeroOptions, argv, ZERO_ENV_VAR_PREFIX, env);

    if (loadedConfig.queryHydrationStats) {
      runtimeDebugFlags.trackRowsVended = true;
    }
  }
  return loadedConfig;
}
