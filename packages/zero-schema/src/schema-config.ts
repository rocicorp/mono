import * as v from '../../shared/src/valita.ts';
import {compoundKeySchema} from '../../zero-protocol/src/ast.ts';
import {valueTypeSchema} from '../../zero-protocol/src/client-schema.ts';
import {primaryKeySchema} from '../../zero-protocol/src/primary-key.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import {type PermissionsConfig} from './compiled-permissions.ts';
import type {Relationship, TableSchema} from './table-schema.ts';

export type SchemaConfig = {
  schema: Schema;
  permissions: PermissionsConfig;
};

const relationshipPart = v.readonlyObject({
  sourceField: compoundKeySchema,
  destField: compoundKeySchema,
  destSchema: v.string(),
  cardinality: v.literalUnion('one', 'many'),
});

export const relationshipSchema: v.Type<Relationship> = v.union(
  v.readonly(v.tuple([relationshipPart])),
  v.readonly(v.tuple([relationshipPart, relationshipPart])),
);

export const schemaValueSchema = v.readonlyObject({
  type: valueTypeSchema,
  serverName: v.string().optional(),
  optional: v.boolean().optional(),
});

export const tableSchemaSchema: v.Type<TableSchema> = v.readonlyObject({
  name: v.string(),
  serverName: v.string().optional(),
  columns: v.record(schemaValueSchema),
  primaryKey: primaryKeySchema,
});

export const schemaSchema = v.readonlyObject({
  tables: v.record(tableSchemaSchema),
  relationships: v.record(v.record(relationshipSchema)),
  enableLegacyQueries: v.boolean().optional(),
  enableLegacyMutators: v.boolean().optional(),
});

// oxlint-disable-next-line @typescript-eslint/no-explicit-any
export function isSchemaConfig(value: any): value is SchemaConfig {
  // oxlint-disable-next-line eqeqeq
  return value != null && 'schema' in value;
}
