/**
 * These types represent the _compiled_ config whereas `define-config` types represent the _source_ config.
 */

import * as v from 'shared/src/valita.js';
import {astSchema} from 'zero-protocol';
import fs from 'node:fs/promises';
import {must} from 'shared/src/must.js';

export type Action = 'read' | 'insert' | 'update' | 'delete';

const policySchema = v.array(v.tuple([v.literal('allow'), astSchema]));

const assetSchema = v.object({
  read: policySchema.optional(),
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

const logConfigSchema = v.object({
  level: v.union(v.literal('debug'), v.literal('info'), v.literal('error')),
  datadogLogsApiKey: v.string().optional(),
  datadogServiceLabel: v.string().optional(),
});
export type LogConfig = v.Infer<typeof logConfigSchema>;

const zeroConfigSchemaSansAuthorization = v.object({
  upstreamUri: v.string(),
  cvrDbUri: v.string(),
  changeDbUri: v.string(),
  replicaId: v.string(),
  taskId: v.string().optional(),
  replicaDbFile: v.string(),
  storageDbTmpDir: v.string().optional(),
  numSyncWorkers: v.number().optional(),
  changeStreamerUri: v.string().optional(),
  litestream: v.boolean().optional(),

  log: logConfigSchema,
});

export type ZeroConfigSansAuthorization = v.Infer<
  typeof zeroConfigSchemaSansAuthorization
>;

export const zeroConfigSchema = zeroConfigSchemaSansAuthorization.extend({
  authorization: authorizationConfigSchema.optional(),
});

export type ZeroConfig = v.Infer<typeof zeroConfigSchema>;

let loadedConfig: Promise<ZeroConfig> | undefined;
export function getZeroConfig() {
  if (loadedConfig) {
    return loadedConfig;
  }
  loadedConfig = fs
    .readFile(must(process.env['ZERO_CONFIG_PATH']), 'utf-8')
    .then(rawContent => v.parse(JSON.parse(rawContent), zeroConfigSchema));
  return loadedConfig;
}
