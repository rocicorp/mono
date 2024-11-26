import * as v from '../../shared/src/valita.js';
import {compoundKeySchema} from '../../zero-protocol/src/ast.js';
import {primaryKeySchema} from '../../zero-protocol/src/primary-key.js';
import {type PermissionsConfig} from './compiled-permissions.js';
import type {Schema} from './schema.js';
import type {
  FieldRelationship,
  JunctionRelationship,
  Relationship,
  TableSchema,
  ValueType,
} from './table-schema.js';

export type SchemaConfig = {
  schema: Schema;
  permissions: PermissionsConfig;
};

const fieldRelationshipSchema: v.Type<FieldRelationship> = v.readonlyObject({
  sourceField: v.union(compoundKeySchema, v.string()),
  destField: v.union(compoundKeySchema, v.string()),
  destSchema: v.lazy(() => tableSchemaSchema),
});

const junctionRelationshipSchema: v.Type<JunctionRelationship> = v.readonly(
  v.tuple([fieldRelationshipSchema, fieldRelationshipSchema]),
);

export const relationshipSchema: v.Type<Relationship> = v.union(
  fieldRelationshipSchema,
  junctionRelationshipSchema,
);

export const valueTypeSchema: v.Type<ValueType> = v.union(
  v.literal('string'),
  v.literal('number'),
  v.literal('boolean'),
  v.literal('null'),
  v.literal('json'),
);

export const schemaValueSchema = v.readonlyObject({
  type: valueTypeSchema,
  optional: v.boolean().optional(),
});

export const tableSchemaSchema: v.Type<TableSchema> = v.readonlyObject({
  tableName: v.string(),
  columns: v.record(v.union(schemaValueSchema, valueTypeSchema)),
  primaryKey: v.union(primaryKeySchema, v.string()),
  relationships: v.record(relationshipSchema).optional(),
});

export const schemaSchema = v.readonlyObject({
  version: v.number(),
  tables: v.record(tableSchemaSchema),
});

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function isSchemaConfig(value: any): value is SchemaConfig {
  // eslint-disable-next-line eqeqeq
  return value != null && 'schema' in value && 'permissions' in value;
}
