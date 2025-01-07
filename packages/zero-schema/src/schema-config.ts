import * as v from '../../shared/src/valita.js';
import {compoundKeySchema} from '../../zero-protocol/src/ast.js';
import {primaryKeySchema} from '../../zero-protocol/src/primary-key.js';
import {
  permissionsConfigSchema,
  type PermissionsConfig,
} from './compiled-permissions.js';
import type {Schema} from './builder/schema-builder.js';
import type {Relationship, TableSchema, ValueType} from './table-schema.js';

export type SchemaConfig = {
  schema: Schema;
  permissions: PermissionsConfig;
};

const relationshipPart = v.readonlyObject({
  sourceField: compoundKeySchema,
  destField: compoundKeySchema,
  destSchema: v.string(),
});

export const relationshipSchema: v.Type<Relationship> = v.union(
  v.readonly(v.tuple([relationshipPart])),
  v.readonly(v.tuple([relationshipPart, relationshipPart])),
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
  name: v.string(),
  columns: v.record(schemaValueSchema),
  primaryKey: primaryKeySchema,
});

export const schemaSchema = v.readonlyObject({
  version: v.number(),
  tables: v.record(tableSchemaSchema),
  relationships: v.record(v.record(relationshipSchema)),
});

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function isSchemaConfig(value: any): value is SchemaConfig {
  // eslint-disable-next-line eqeqeq
  return value != null && 'schema' in value && 'permissions' in value;
}

export async function stringifySchema(module: unknown) {
  if (!isSchemaConfig(module)) {
    throw new Error(
      'Schema file must have a export `schema` and `permissions`.',
    );
  }
  const schemaConfig = module;
  const permissions = v.parse(
    await schemaConfig.permissions,
    permissionsConfigSchema,
  );

  return JSON.stringify(
    {
      permissions,
      schema: schemaConfig.schema,
    },
    undefined,
    2,
  );
}

export function parseSchema(
  input: string,
  source: string,
): {
  schema: Schema;
  permissions: PermissionsConfig;
} {
  try {
    const config = JSON.parse(input);
    const permissions = v.parse(config.permissions, permissionsConfigSchema);
    return {
      permissions,
      schema: config.schema,
    };
  } catch (e) {
    throw new Error(`Failed to parse schema config from ${source}: ${e}`);
  }
}
