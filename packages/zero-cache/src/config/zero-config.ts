/**
 * These types represent the _compiled_ config whereas `define-config` types represent the _source_ config.
 */

import type {LogContext} from '@rocicorp/logger';
import {logOptions} from '../../../otel/src/log-options.ts';
import {
  parseOptions,
  type Config,
  type ParseOptions,
} from '../../../shared/src/options.ts';
import * as v from '../../../shared/src/valita.ts';
// @circular-dep-ignore - importing package.json for version info only
import packageJson from '../../../zero/package.json' with {type: 'json'};
import {runtimeDebugFlags} from '../../../zql/src/builder/debug-delegate.ts';
import {singleProcessMode} from '../types/processes.ts';
import {
  ALLOWED_APP_ID_CHARACTERS,
  INVALID_APP_ID_MESSAGE,
} from '../types/shards.ts';
import {
  assertNormalized,
  isDevelopmentMode,
  type NormalizedZeroConfig,
} from './normalize.ts';
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
          `ZERO_SHARD_ID is no longer an option. Please use ZERO_APP_ID instead.`,
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
    type: v.string().default('zero.db'),
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

  pageCacheSizeKib: {
    type: v.number().optional(),
    desc: [
      `The SQLite page cache size in kibibytes (KiB) for view-syncer connections.`,
      `The page cache stores recently accessed database pages in memory to reduce disk I/O.`,
      `Larger cache sizes improve performance for databases that fit in cache.`,
      `If unspecified, SQLite's default (~2 MB) is used.`,
      `Note that the effective memory use of this setting will be:`,
      `2 * cache_size * num_cores as each connection the the replica uses this cache`,
      `size and each core maintains 2 connections.`,
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

const makeMutatorQueryOptions = (
  replacement: string | undefined,
  suffix: string,
) => ({
  url: {
    type: v.array(v.string()).optional(), // optional until we remove CRUD mutations
    desc: [
      replacement ? `DEPRECATED. Use ${replacement} instead.` : ``,
      `The URL of the API server to which zero-cache will ${suffix}.`,
      ``,
      `{bold IMPORTANT:} URLs are matched using {bold URLPattern}, a standard Web API.`,
      ``,
      `{bold Pattern Syntax:}`,
      `  URLPattern uses a simple and intuitive syntax similar to Express routes.`,
      `  Wildcards and named parameters make it easy to match multiple URLs.`,
      ``,
      `{bold Basic Examples:}`,
      `  Exact URL match:`,
      `    "https://api.example.com/mutate"`,
      `  `,
      `  Any subdomain using wildcard:`,
      `    "https://*.example.com/mutate"`,
      `  `,
      `  Multiple subdomain levels:`,
      `    "https://*.*.example.com/mutate"`,
      `  `,
      `  Any path under a domain:`,
      `    "https://api.example.com/*"`,
      `  `,
      `  Named path parameters:`,
      `    "https://api.example.com/:version/mutate"`,
      `    ↳ Matches "https://api.example.com/v1/mutate", "https://api.example.com/v2/mutate", etc.`,
      ``,
      `{bold Advanced Patterns:}`,
      `  Optional path segments:`,
      `    "https://api.example.com/:path?"`,
      `  `,
      `  Regex in segments (for specific patterns):`,
      `    "https://api.example.com/:version(v\\\\d+)/mutate"`,
      `    ↳ Matches only "v" followed by digits`,
      ``,
      `{bold Multiple patterns:}`,
      `  ["https://api1.example.com/mutate", "https://api2.example.com/mutate"]`,
      ``,
      `{bold Note:} Query parameters and URL fragments (#) are automatically ignored during matching.`,
      ``,
      `For full URLPattern syntax, see: https://developer.mozilla.org/en-US/docs/Web/API/URLPattern`,
    ],
  },
  apiKey: {
    type: v.string().optional(),
    desc: [
      `An optional secret used to authorize zero-cache to call the API server handling writes.`,
    ],
  },
  forwardCookies: {
    type: v.boolean().default(false),
    desc: [
      `If true, zero-cache will forward cookies from the request.`,
      `This is useful for passing authentication cookies to the API server.`,
      `If false, cookies are not forwarded.`,
    ],
  },
});

const mutateOptions = makeMutatorQueryOptions(undefined, 'push mutations');
const pushOptions = makeMutatorQueryOptions('mutate-url', 'push mutations');
const queryOptions = makeMutatorQueryOptions(undefined, 'send synced queries');
const getQueriesOptions = makeMutatorQueryOptions(
  'query-url',
  'send synced queries',
);

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

  /** @deprecated */
  push: pushOptions,
  mutate: mutateOptions,
  /** @deprecated */
  getQueries: getQueriesOptions,
  query: queryOptions,

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

    garbageCollectionInactivityThresholdHours: {
      type: v.number().default(48),
      desc: [
        `The duration after which an inactive CVR is eligible for garbage collection.`,
        `Note that garbage collection is an incremental, periodic process which does not`,
        `necessarily purge all eligible CVRs immediately.`,
      ],
    },

    garbageCollectionInitialIntervalSeconds: {
      type: v.number().default(60),
      desc: [
        `The initial interval at which to check and garbage collect inactive CVRs.`,
        `This interval is increased exponentially (up to 16 minutes) when there is`,
        `nothing to purge.`,
      ],
    },

    garbageCollectionInitialBatchSize: {
      type: v.number().default(25),
      desc: [
        `The initial number of CVRs to purge per garbage collection interval.`,
        `This number is increased linearly if the rate of new CVRs exceeds the rate of`,
        `purged CVRs, in order to reach a steady state.`,
        ``,
        `Setting this to 0 effectively disables CVR garbage collection.`,
      ],
    },
  },

  queryHydrationStats: {
    type: v.boolean().optional(),
    desc: [
      `Track and log the number of rows considered by query hydrations which`,
      `take longer than {bold log-slow-hydrate-threshold} milliseconds.`,
      `This is useful for debugging and performance tuning.`,
    ],
  },

  enableQueryPlanner: {
    type: v.boolean().default(true),
    desc: [
      `Enable the query planner for optimizing ZQL queries.`,
      ``,
      `The query planner analyzes and optimizes query execution by determining`,
      `the most efficient join strategies.`,
      ``,
      `You can disable the planner if it is picking bad strategies.`,
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
    uri: {
      type: v.string().optional(),
      desc: [
        `When set, connects to the {bold change-streamer} at the given URI.`,
        `In a multi-node setup, this should be specified in {bold view-syncer} options,`,
        `pointing to the {bold replication-manager} URI, which runs a {bold change-streamer}`,
        `on port 4849.`,
      ],
    },

    mode: {
      type: v.literalUnion('dedicated', 'discover').default('dedicated'),
      desc: [
        `As an alternative to {bold ZERO_CHANGE_STREAMER_URI}, the {bold ZERO_CHANGE_STREAMER_MODE}`,
        `can be set to "{bold discover}" to instruct the {bold view-syncer} to connect to the `,
        `ip address registered by the {bold replication-manager} upon startup.`,
        ``,
        `This may not work in all networking configurations, e.g. certain private `,
        `networking or port forwarding configurations. Using the {bold ZERO_CHANGE_STREAMER_URI}`,
        `with an explicit routable hostname is recommended instead.`,
        ``,
        `Note: This option is ignored if the {bold ZERO_CHANGE_STREAMER_URI} is set.`,
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

    /** @deprecated */
    address: {
      type: v.string().optional(),
      deprecated: [
        `Set the {bold ZERO_CHANGE_STREAMER_URI} on view-syncers instead.`,
      ],
      hidden: true,
    },

    /** @deprecated */
    protocol: {
      type: v.literalUnion('ws', 'wss').default('ws'),
      deprecated: [
        `Set the {bold ZERO_CHANGE_STREAMER_URI} on view-syncers instead.`,
      ],
      hidden: true,
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

    startupDelayMs: {
      type: v.number().default(15000),
      desc: [
        `The delay to wait before the change-streamer takes over the replication stream`,
        `(i.e. the handoff during replication-manager updates), to allow loadbalancers to register`,
        `the task as healthy based on healthcheck parameters. Note that if a change stream request`,
        `is received during this interval, the delay will be canceled and the takeover will happen`,
        `immediately, since the incoming request indicates that the task is registered as a target.`,
      ],
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
      '',
      'A password is optional in development mode but {bold required in production} mode.',
    ],
  },

  websocketCompression: {
    type: v.boolean().default(false),
    desc: [
      'Enable WebSocket per-message deflate compression.',
      '',
      'Compression can reduce bandwidth usage for sync traffic but',
      'increases CPU usage on both client and server. Disabled by default.',
      '',
      'See: https://github.com/websockets/ws#websocket-compression',
    ],
  },

  websocketCompressionOptions: {
    type: v.string().optional(),
    desc: [
      'JSON string containing WebSocket compression options.',
      '',
      'Only used if websocketCompression is enabled.',
      '',
      'Example: \\{"zlibDeflateOptions":\\{"level":3\\},"threshold":1024\\}',
      '',
      'See https://github.com/websockets/ws/blob/master/doc/ws.md#new-websocketserveroptions-callback for available options.',
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
        `to upload or download in parallel when backing up or restoring the snapshot.`,
      ],
    },

    multipartSize: {
      type: v.number().default(16 * 1024 * 1024),
      desc: [
        `The size of each part when uploading or downloading the snapshot with`,
        `{bold --multipart-concurrency}. Note that up to {bold concurrency * size}`,
        `bytes of memory are used when backing up or restoring the snapshot.`,
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

    profileCopy: {
      type: v.boolean().optional(),
      hidden: true,
      desc: [
        `Takes a cpu profile during the copy phase initial-sync, storing it as a JSON file`,
        `initial-copy.cpuprofile in the tmp directory.`,
      ],
    },
  },

  /** @deprecated */
  targetClientRowCount: {
    type: v.number().default(20_000),
    deprecated: [
      'This option is no longer used and will be removed in a future version.',
      'The client-side cache no longer enforces a row limit. Instead, TTL-based expiration',
      'automatically manages cache size to prevent unbounded growth.',
    ],
    hidden: true,
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

  enableTelemetry: {
    type: v.boolean().default(true),
    desc: [
      `Set to false to opt out of telemetry collection.`,
      ``,
      `This helps us improve Zero by collecting anonymous usage data.`,
      `Setting the DO_NOT_TRACK environment variable also disables telemetry.`,
    ],
  },

  cloudEvent: {
    sinkEnv: {
      type: v.string().optional(),
      desc: [
        `ENV variable containing a URI to a CloudEvents sink. When set, ZeroEvents`,
        `will be published to the sink as the {bold data} field of CloudEvents.`,
        `The {bold source} field of the CloudEvents will be set to the {bold ZERO_TASK_ID},`,
        `along with any extension attributes specified by the {bold ZERO_CLOUD_EVENT_EXTENSION_OVERRIDES_ENV}.`,
        ``,
        `This configuration is modeled to easily integrate with a knative K_SINK binding,`,
        `(i.e. https://github.com/knative/eventing/blob/main/docs/spec/sources.md#sinkbinding).`,
        `However, any CloudEvents sink can be used.`,
      ],
    },

    extensionOverridesEnv: {
      type: v.string().optional(),
      desc: [
        `ENV variable containing a JSON stringified object with an {bold extensions} field`,
        `containing attributes that should be added or overridden on outbound CloudEvents.`,
        ``,
        `This configuration is modeled to easily integrate with a knative K_CE_OVERRIDES binding,`,
        `(i.e. https://github.com/knative/eventing/blob/main/docs/spec/sources.md#sinkbinding).`,
      ],
    },
  },
};

export type ZeroConfig = Config<typeof zeroOptions>;

export const ZERO_ENV_VAR_PREFIX = 'ZERO_';

let loadedConfig: Config<typeof zeroOptions> | undefined;

export function getZeroConfig(
  opts: Omit<ParseOptions, 'envNamePrefix'> = {},
): ZeroConfig {
  if (!loadedConfig || singleProcessMode()) {
    loadedConfig = parseOptions(zeroOptions, {
      envNamePrefix: ZERO_ENV_VAR_PREFIX,
      emitDeprecationWarnings: false, // overridden at the top level parse
      ...opts,
    });

    if (loadedConfig.queryHydrationStats) {
      runtimeDebugFlags.trackRowCountsVended = true;
    }
  }
  return loadedConfig;
}

/**
 * Same as {@link getZeroConfig}, with an additional check that the
 * config has already been normalized (i.e. by the top level server/runner).
 */
export function getNormalizedZeroConfig(
  opts: Omit<ParseOptions, 'envNamePrefix'> = {},
): NormalizedZeroConfig {
  const config = getZeroConfig(opts);
  assertNormalized(config);
  return config;
}

/**
 * Gets the server version from the config if provided. Otherwise it gets it
 * from the Zero package.json.
 */
export function getServerVersion(
  config: Pick<ZeroConfig, 'serverVersion'> | undefined,
): string {
  return config?.serverVersion ?? packageJson.version;
}

export function isAdminPasswordValid(
  lc: LogContext,
  config: Pick<NormalizedZeroConfig, 'adminPassword'>,
  password: string | undefined,
) {
  // If development mode, password is optional
  // We use process.env.NODE_ENV === 'development' as a sign that we're in
  // development mode, rather than a custom env var like ZERO_DEVELOPMENT_MODE,
  // because NODE_ENV is more standard and is already used by many tools.
  // Note that if NODE_ENV is not set, we assume production mode.

  if (!password && !config.adminPassword && isDevelopmentMode()) {
    warnOnce(
      lc,
      'No admin password set; allowing access in development mode only',
    );
    return true;
  }

  if (!config.adminPassword) {
    lc.warn?.('No admin password set; denying access');
    return false;
  }

  if (password !== config.adminPassword) {
    lc.warn?.('Invalid admin password');
    return false;
  }

  lc.debug?.('Admin password accepted');
  return true;
}

let hasWarned = false;

function warnOnce(lc: LogContext, msg: string) {
  if (!hasWarned) {
    lc.warn?.(msg);
    hasWarned = true;
  }
}

// For testing purposes - reset the warning state
export function resetWarnOnceState() {
  hasWarned = false;
}
