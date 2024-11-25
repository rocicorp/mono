import { authorizationConfigSchema, type AuthorizationConfig } from "./compiled-authorization.js";
import type { Schema } from "./schema.js";
import * as v from '../../shared/src/valita.js';
import { primaryKeySchema } from '../../zero-protocol/src/primary-key.js';

export type SchemaConfig = {
  schema: Schema;
  authorization: AuthorizationConfig;
}

const relationshipSchema = v.object({
  source: v.string(),
  junction: v.object({
    schema: v.unknown(), 
    sourceField: v.string(),
    destField: v.string(),
  }).optional(),
  dest: v.object({
    field: v.string(),
    schema: v.unknown(), 
  }),
});

export const schemaConfigSchema = v.object({
  schema: v.object({
    version: v.number(),
    tables: v.record(v.object({
      tableName: v.string(),
      columns: v.record(v.object({
        type: v.union(
          v.literal('string'),
          v.literal('number'),
          v.literal('boolean'),
          v.literal('null'),
          v.literal('json')
        ),
        optional: v.boolean().optional()
      })),
      primaryKey: primaryKeySchema,
      relationships: v.record(relationshipSchema)
    }))
  }),
  authorization: authorizationConfigSchema,
});

export function isSchemaConfig(value: unknown): value is SchemaConfig {
  return v.is(value, schemaConfigSchema);
}