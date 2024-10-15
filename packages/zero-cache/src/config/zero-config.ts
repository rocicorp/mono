/**
 * These types represent the _compiled_ config whereas `define-config` types represent the _source_ config.
 */

import * as v from '../../../shared/src/valita.js';
import {astSchema} from '../../../zero-protocol/src/mod.js';
import {tsImport} from 'tsx/esm/api';

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

export type AuthorizationConfig = v.Infer<typeof authorizationConfigSchema>;

const envRefSchema = v.object({
  tag: v.literal('env'),
  name: v.string(),
});
export type EnvRef = v.Infer<typeof envRefSchema>;
const stringLiteral = v.string();
const numberLiteral = v.number();
const booleanLiteral = v.boolean();

const configStringValueSchema = stringLiteral;

/**
 * Configures the view of the upstream database replicated to this zero-cache.
 */
const shardConfigSchema = v.object({
  /**
   * Unique identifier for the zero-cache shard. This is used to partition
   * shardable tables such as `zero.clients`, as well as reserve a name for
   * the replication slot.
   *
   * The shard `id` value is written to the `shardID` column when updating
   * the `lastMutationID` for clients in the `zero.clients` table.
   *
   * Defaults to "0".
   */
  id: configStringValueSchema.optional(),

  /**
   * Optional (comma-separated) list of of Postgres `PUBLICATION`s that the
   * shard subscribes to. All publication names must begin with the prefix
   * `"zero_"`, and all tables must be in the `"public"` Postgres schema.
   *
   * If unspecified, zero will create and use a `"zero_public"` publication that
   * publishes all tables in the `"public"` schema.
   *
   * ```sql
   * CREATE PUBLICATION zero_public FOR TABLES IN SCHEMA public;
   * ```
   *
   * Note that once a shard has begun syncing data, this list of publications
   * cannot be changed, and zero-cache will refuse to start if a specified
   * value differs from what it originally synced.
   *
   * To use a different set of publications, a new shard should be created.
   */
  publications: v.array(stringLiteral).optional(),
});

const logConfigSchema = v.object({
  /**
   * `debug`, `info`, `warn`, or `error`.
   * Defaults to `info`.
   */
  level: v
    .union(
      envRefSchema,
      v.union(
        v.literal('debug'),
        v.literal('info'),
        v.literal('warn'),
        v.literal('error'),
      ),
    )
    .optional(),

  /**
   * Defaults to `text` for developer-friendly console logging.
   * Also supports `json` for consumption by structured-logging services.
   */
  format: v
    .union(envRefSchema, v.union(v.literal('text'), v.literal('json')))
    .optional(),

  datadogLogsApiKey: configStringValueSchema.optional(),
  datadogServiceLabel: configStringValueSchema.optional(),
});

const rateLimitConfigSchema = v.object({
  // Limits to `max` transactions per `windowMs` milliseconds.
  // This uses a sliding window algorithm to track number of transactions in the current window.
  mutationTransactions: v.object({
    algorithm: v.literal('sliding-window'),
    windowMs: v.union(envRefSchema, numberLiteral),
    maxTransactions: v.union(envRefSchema, numberLiteral),
  }),
});

const zeroConfigSchemaSansAuthorization = v.object({
  upstreamDBConnStr: configStringValueSchema,
  cvrDBConnStr: configStringValueSchema,
  changeDBConnStr: configStringValueSchema,
  taskId: configStringValueSchema.optional(),
  replicaDBFile: configStringValueSchema,
  storageDbTmpDir: configStringValueSchema.optional(),
  warmWebsocket: numberLiteral.optional(),

  // The number of sync workers defaults to available-cores - 1.
  // It should be set to 0 for the `replication-manager`.
  numSyncWorkers: v.union(envRefSchema, numberLiteral).optional(),

  // In development, the `zero-cache` runs its own `replication-manager`
  // (i.e. `change-streamer`). In production, this URI should point to
  // to the `replication-manager`, which runs a `change-streamer`
  // on port 4849.
  changeStreamerConnStr: configStringValueSchema.optional(),

  // Indicates that a `litestream replicate` process is backing up
  // the `replicatDbFile`. This should be the production configuration
  // for the `replication-manager`. It is okay to run this in
  // development too.
  litestream: v.union(envRefSchema, booleanLiteral).optional(),

  jwtSecret: configStringValueSchema.optional(),

  log: logConfigSchema.optional(),

  shard: shardConfigSchema.optional(),
  rateLimit: rateLimitConfigSchema.optional(),
});

export type ZeroConfigSansAuthorization = v.Infer<
  typeof zeroConfigSchemaSansAuthorization
>;

export const zeroConfigSchema = zeroConfigSchemaSansAuthorization.extend({
  authorization: authorizationConfigSchema.optional(),
});

export type ZeroConfig = v.Infer<typeof zeroConfigSchema>;

let loadedConfig: Promise<ZeroConfig> | undefined;

export function getZeroConfig(): Promise<ZeroConfig> {
  if (loadedConfig) {
    return loadedConfig;
  }
  const zeroConfigPath = process.env['ZERO_CONFIG_PATH'] ?? './zero.config.ts';
  loadedConfig = tsImport(zeroConfigPath, import.meta.url)
    .then(module => module.default as ZeroConfig)
    .catch(e => {
      console.error(`Failed to load zero config from ${zeroConfigPath}: ${e}`);
      throw e;
    });
  return loadedConfig;
}
