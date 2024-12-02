import * as v from '../../shared/src/valita.js';
import {compoundKeySchema} from '../../zero-protocol/src/ast.js';
import {primaryKeySchema} from '../../zero-protocol/src/primary-key.js';
import {
  permissionsConfigSchema,
  type PermissionsConfig,
} from './compiled-permissions.js';
import {
  normalizeSchema,
  type DecycledNormalizedSchema,
  type NormalizedSchema,
} from './normalized-schema.js';
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

export function replacePointersWithSchemaNames(
  schema: NormalizedSchema,
): DecycledNormalizedSchema {
  const tables = Object.fromEntries(
    Object.entries(schema.tables).map(([name, table]) => {
      const relationships = Object.fromEntries(
        Object.entries(table.relationships).map(([name, relationship]) => {
          if ('sourceField' in relationship) {
            return [
              name,
              {...relationship, destSchema: relationship.destSchema.tableName},
            ];
          }
          return [
            name,
            [
              {
                ...relationship[0],
                destSchema: relationship[0].destSchema.tableName,
              },
              {
                ...relationship[1],
                destSchema: relationship[1].destSchema.tableName,
              },
            ],
          ];
        }),
      );
      return [name, {...table, relationships}];
    }),
  );
  return {...schema, tables};
}

export function replaceSchemaNamesWithPointers(
  schema: DecycledNormalizedSchema,
): NormalizedSchema {
  schema = structuredClone(schema) as DecycledNormalizedSchema;
  Object.values(schema.tables).forEach(table => {
    Object.values(table.relationships).forEach(relationship => {
      if ('sourceField' in relationship) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (relationship.destSchema as any) =
          schema.tables[relationship.destSchema];
      } else {
        relationship.forEach(r => {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          (r.destSchema as any) = schema.tables[r.destSchema];
        });
      }
    });
  });

  return schema as unknown as NormalizedSchema;
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

  const cycleFreeNormalizedSchema = replacePointersWithSchemaNames(
    normalizeSchema(schemaConfig.schema),
  );

  return JSON.stringify(
    {
      permissions,
      schema: cycleFreeNormalizedSchema,
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
    const normalizedSchema = normalizeSchema(
      replaceSchemaNamesWithPointers(config.schema),
    );
    return {
      permissions,
      schema: normalizedSchema,
    };
  } catch (e) {
    throw new Error(`Failed to parse schema config from ${source}: ${e}`);
  }
}
