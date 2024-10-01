/**
 * These types represent the _compiled_ config whereas `define-config` types represent the _source_ config.
 */

import fs from 'node:fs/promises';
import {must} from 'shared/src/must.js';
import * as v from 'shared/src/valita.js';
import {astSchema} from 'zero-protocol';

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

const logConfigSchema = v.object({
  level: v.union(
    envRefSchema,
    v.union(v.literal('debug'), v.literal('info'), v.literal('error')),
  ),
  datadogLogsApiKey: v.union(envRefSchema, stringLiteral).optional(),
  datadogServiceLabel: v.union(envRefSchema, stringLiteral).optional(),
});
type LogConfigType = v.Infer<typeof logConfigSchema>;
const configValueSchema = v.union(
  envRefSchema,
  stringLiteral,
  booleanLiteral,
  numberLiteral,
);
type ConfigValue = v.Infer<typeof configValueSchema>;

const zeroConfigSchemaSansAuthorization = v.object({
  upstreamUri: v.union(envRefSchema, stringLiteral),
  cvrDbUri: v.union(envRefSchema, stringLiteral),
  changeDbUri: v.union(envRefSchema, stringLiteral),
  replicaId: v.union(envRefSchema, stringLiteral),
  taskId: v.union(envRefSchema, stringLiteral).optional(),
  replicaDbFile: v.union(envRefSchema, stringLiteral),
  storageDbTmpDir: v.union(envRefSchema, stringLiteral).optional(),
  numSyncWorkers: v.union(envRefSchema, numberLiteral).optional(),
  changeStreamerUri: v.union(envRefSchema, stringLiteral).optional(),
  litestream: v.union(envRefSchema, booleanLiteral).optional(),
  jwtSecret: v.union(envRefSchema, stringLiteral).optional(),

  log: logConfigSchema,
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
  constructor(config: ZeroConfigType) {
    this.#config = config;
    this.#log = new LogConfig(config.log);
  }

  get upstreamUri() {
    return mustResolveValue(this.#config.upstreamUri);
  }

  get cvrDbUri() {
    return mustResolveValue(this.#config.cvrDbUri);
  }

  get changeDbUri() {
    return mustResolveValue(this.#config.changeDbUri);
  }

  get replicaId() {
    return mustResolveValue(this.#config.replicaId);
  }

  get taskId() {
    return resolveValue(this.#config.taskId);
  }

  get replicaDbFile() {
    return mustResolveValue(this.#config.replicaDbFile);
  }

  get storageDbTmpDir() {
    return resolveValue(this.#config.storageDbTmpDir);
  }

  get numSyncWorkers() {
    return resolveValue(this.#config.numSyncWorkers);
  }

  get changeStreamerUri() {
    return resolveValue(this.#config.changeStreamerUri);
  }

  get litestream() {
    return resolveValue(this.#config.litestream);
  }

  get jwtSecret() {
    return resolveValue(this.#config.jwtSecret);
  }

  get log() {
    return this.#log;
  }

  get authorization() {
    return this.#config.authorization;
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
