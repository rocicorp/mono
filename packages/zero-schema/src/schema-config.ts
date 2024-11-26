import {type PermissionsConfig} from './compiled-permissions.js';
import type {Schema} from './schema.js';
import * as v from '../../shared/src/valita.js';
import {compoundKeySchema} from '../../zero-protocol/src/ast.js';
import {primaryKeySchema} from '../../zero-protocol/src/primary-key.js';
import type {TableSchema} from './table-schema.js';

export type SchemaConfig = {
  schema: Schema;
  permissions: PermissionsConfig;
};

const fieldRelationshipSchema = v.object({
  sourceField: compoundKeySchema,
  destField: compoundKeySchema,
  destSchema: v.lazy(() => tableSchemaSchema),
});

const junctionRelationshipSchema = v.readonly(
  v.tuple([fieldRelationshipSchema, fieldRelationshipSchema]),
);

export const relationshipSchema = v.union(
  fieldRelationshipSchema,
  junctionRelationshipSchema,
);

export const schemaValueSchema = v.object({
  type: v.union(
    v.literal('string'),
    v.literal('number'),
    v.literal('boolean'),
    v.literal('null'),
    v.literal('json'),
  ),
  optional: v.boolean().optional(),
});

export const sourceOrTableSchemaSchema = v.object({
  tableName: v.string(),
  columns: v.record(schemaValueSchema),
  primaryKey: primaryKeySchema,
});

export const tableSchemaSchema: v.Type<TableSchema> =
  sourceOrTableSchemaSchema.extend({
    relationships: v.record(relationshipSchema),
  });

export const schemaSchema = v.object({
  version: v.number(),
  tables: v.record(tableSchemaSchema),
});

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function isSchemaConfig(value: any): value is SchemaConfig {
  // eslint-disable-next-line eqeqeq
  return value != null && 'schema' in value && 'permissions' in value;
}
