/**
 * These types represent the _compiled_ config whereas `define-config` types represent the _source_ config.
 */

import fs from 'node:fs/promises';
import {must} from 'shared/src/must.js';
import * as v from 'shared/src/valita.js';
import {astSchema} from 'zero-protocol/src/mod.js';

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

const configStringValueSchema = v.union(envRefSchema, stringLiteral);

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
  id: configStringValueSchema,

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
  publications: configStringValueSchema,
});
type ShardConfigType = v.Infer<typeof shardConfigSchema>;

const logConfigSchema = v.object({
  level: v.union(
    envRefSchema,
    v.union(v.literal('debug'), v.literal('info'), v.literal('error')),
  ),
  datadogLogsApiKey: configStringValueSchema.optional(),
  datadogServiceLabel: configStringValueSchema.optional(),
});
type LogConfigType = v.Infer<typeof logConfigSchema>;

const configValueSchema = v.union(
  configStringValueSchema,
  booleanLiteral,
  numberLiteral,
);
type ConfigValue = v.Infer<typeof configValueSchema>;

const rateLimitConfigSchema = v.object({
  // Limits to `max` transactions per `windowMs` milliseconds.
  // This uses a sliding window algorithm to track number of transactions in the current window.
  mutationTransactions: v.object({
    algorithm: v.literal('sliding-window'),
    windowMs: v.union(envRefSchema, numberLiteral),
    maxTransactions: v.union(envRefSchema, numberLiteral),
  }),
});
type RateLimitConfigType = v.Infer<typeof rateLimitConfigSchema>;

const zeroConfigSchemaSansAuthorization = v.object({
  upstreamDBConnStr: configStringValueSchema,
  cvrDBConnStr: configStringValueSchema,
  changeDBConnStr: configStringValueSchema,
  taskId: configStringValueSchema.optional(),
  replicaDBFile: configStringValueSchema,
  storageDbTmpDir: configStringValueSchema.optional(),

  // The number of sync workers defaults to available-cores - 1.
  // It should be set to 0 for the `replication-manager`.
  numSyncWorkers: v.union(envRefSchema, numberLiteral).optional(),

  // In development, the `zero-cache` runs its own `replication-manager`
  // (i.e. `change-streamer`). In production, this URI should point to
  // to the `replication-manager`, which runs a `change-streamer`
  // on port 2999.
  changeStreamerConnStr: configStringValueSchema.optional(),

  // Indicates that a `litestream replicate` process is backing up
  // the `replicatDbFile`. This should be the production configuration
  // for the `replication-manager`. It is okay to run this in
  // development too.
  litestream: v.union(envRefSchema, booleanLiteral).optional(),

  jwtSecret: configStringValueSchema.optional(),

  log: logConfigSchema,

  shard: shardConfigSchema.optional(),
  rateLimit: rateLimitConfigSchema.optional(),
});

export type ZeroConfigSansAuthorization = v.Infer<
  typeof zeroConfigSchemaSansAuthorization
>;

export const zeroConfigSchema = zeroConfigSchemaSansAuthorization.extend({
  authorization: authorizationConfigSchema.optional(),
});

export type ZeroConfigType = v.Infer<typeof zeroConfigSchema>;

let loadedConfig: Promise<ZeroConfig> | undefined;

export function getZeroConfig(): Promise<ZeroConfig> {
  if (loadedConfig) {
    return loadedConfig;
  }
  const zeroConfigPath = process.env['ZERO_CONFIG_PATH'];
  if (!zeroConfigPath) {
    // TODO: Use a specific error type and report it to the user in a nicer way.
    return Promise.reject(new Error('ZERO_CONFIG_PATH is not set'));
  }
  loadedConfig = fs
    .readFile(zeroConfigPath, 'utf-8')
    .then(
      rawContent =>
        new ZeroConfig(v.parse(JSON.parse(rawContent), zeroConfigSchema)),
    );
  return loadedConfig;
}

export class ZeroConfig {
  readonly #config: ZeroConfigType;
  readonly #log: LogConfig;
  readonly #shard: ShardConfig;
  readonly #rateLimit: RateLimitConfig | undefined;

  constructor(config: ZeroConfigType) {
    this.#config = config;
    this.#log = new LogConfig(config.log);
    this.#shard = new ShardConfig(config.shard);
    if (config.rateLimit) {
      this.#rateLimit = new RateLimitConfig(config.rateLimit);
    }
  }

  get upstreamDBConnStr() {
    return mustResolveValue(this.#config.upstreamDBConnStr);
  }

  get cvrDBConnStr() {
    return mustResolveValue(this.#config.cvrDBConnStr);
  }

  get changeDBConnStr() {
    return mustResolveValue(this.#config.changeDBConnStr);
  }

  get taskID() {
    return resolveValue(this.#config.taskId);
  }

  get replicaDBFile() {
    return mustResolveValue(this.#config.replicaDBFile);
  }

  get storageDBTmpDir() {
    return resolveValue(this.#config.storageDbTmpDir);
  }

  get numSyncWorkers() {
    return resolveValue(this.#config.numSyncWorkers);
  }

  get changeStreamerConnStr() {
    return resolveValue(this.#config.changeStreamerConnStr);
  }

  get litestream() {
    return resolveValue(this.#config.litestream);
  }

  get jwtSecret() {
    return resolveValue(this.#config.jwtSecret);
  }

  get shard() {
    return this.#shard;
  }

  get log() {
    return this.#log;
  }

  get rateLimit() {
    return this.#rateLimit;
  }

  get authorization() {
    return this.#config.authorization;
  }
}

export class RateLimitConfig {
  readonly #mutationTransactions: MutationTransactionLimits;
  constructor(config: RateLimitConfigType) {
    this.#mutationTransactions = new MutationTransactionLimits(
      config.mutationTransactions,
    );
  }

  get mutationTransactions() {
    return this.#mutationTransactions;
  }
}

export class MutationTransactionLimits {
  readonly #config: RateLimitConfigType['mutationTransactions'];
  constructor(config: RateLimitConfigType['mutationTransactions']) {
    this.#config = config;
  }

  get windowMs() {
    return mustResolveValue(this.#config.windowMs);
  }

  get maxTransactions() {
    return mustResolveValue(this.#config.maxTransactions);
  }
}

export class LogConfig {
  readonly #config: LogConfigType;
  constructor(config: LogConfigType) {
    this.#config = config;
  }

  get level() {
    return mustResolveValue(this.#config.level);
  }

  get datadogLogsApiKey() {
    return resolveValue(this.#config.datadogLogsApiKey);
  }

  get datadogServiceLabel() {
    return resolveValue(this.#config.datadogServiceLabel);
  }
}

const DEFAULT_SHARD_ID = '0';

export class ShardConfig {
  readonly id: string;
  readonly publications: readonly string[];

  constructor(config: ShardConfigType | undefined) {
    this.id = resolveValue(config?.id) ?? DEFAULT_SHARD_ID;
    const p = resolveValue(config?.publications);
    this.publications = p ? p.split(',') : [];
  }
}

function resolveValue<T extends ConfigValue>(
  value: T | undefined,
): Exclude<T, EnvRef> | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (typeof value === 'object' && value.tag === 'env') {
    return process.env[value.name] as Exclude<T, EnvRef>;
  }
  return value as Exclude<T, EnvRef>;
}

function mustResolveValue<T extends ConfigValue>(value: T | undefined) {
  return must(resolveValue(value));
}
