/* eslint-disable @typescript-eslint/no-explicit-any */
import type {
  Relationship,
  RelationshipsSchema,
  TableSchema,
} from '../table-schema.js';
import type {Relationships} from './relationship-builder.js';
import {type TableBuilderWithColumns} from './table-builder.js';

export type Schema = {
  readonly version: number;
  readonly tables: {readonly [table: string]: TableSchema};
  readonly relationships: {readonly [table: string]: RelationshipsSchema};
};

/**
 * Note: the keys of the `tables` and `relationships` parameters do not matter.
 * You can assign them to any value you like. E.g.,
 *
 * ```ts
 * createSchema(1, {rsdfgafg: table('users')...}, {sdfd: relationships(users, ...)})
 * ```
 *
 * @param version The version of the schema. Only needs to be incremented
 * when the backend Postgres schema moves forward in a way that is not
 * compatible with the frontend. As in, if:
 * 1. Columns are removed
 * 2. Optional columns are made required
 *
 * Adding columns, adding tables, adding relationships, making
 * required columns optional are all backwards compatible changes and
 * do not require bumping the schema version.
 */
export function createSchema<
  TTables extends Record<string, TableBuilderWithColumns<TableSchema>>,
  TRelationships extends Record<string, Relationships>,
>(
  version: number,
  tables: TTables,
  relationships?: TRelationships | undefined,
): {
  version: number;
  tables: {
    [K in keyof TTables as TTables[K]['schema']['name']]: TTables[K]['schema'];
  };
  relationships: {
    [K in keyof TRelationships as TRelationships[K]['name']]: TRelationships[K]['relationships'];
  };
} {
  const retTables: Record<string, TableSchema> = {};
  const retRelationships: Record<string, Record<string, Relationship>> = {};

  Object.values(tables).forEach(table => {
    retTables[table.schema.name] = table.build();
  });
  Object.values(relationships ?? {}).forEach(relationship => {
    retRelationships[relationship.name] = relationship.relationships;
    checkRelationship(relationship.relationships, relationship.name, retTables);
  });

  return {
    version,
    tables: retTables,
    relationships: retRelationships,
  } as any;
}

function checkRelationship(
  relationships: Record<string, Relationship>,
  tableName: string,
  tables: Record<string, TableSchema>,
) {
  // TS should be able to check this for us but something is preventing it from happening.
  Object.entries(relationships).forEach(([name, rel]) => {
    let source = tables[tableName];
    rel.forEach(connection => {
      if (!tables[connection.destSchema]) {
        throw new Error(
          `For relationship "${tableName}"."${name}", destination table "${connection.destSchema}" is missing in the schema`,
        );
      }
      if (!source.columns[connection.sourceField[0]]) {
        throw new Error(
          `For relationship "${tableName}"."${name}", the source field "${connection.sourceField[0]}" is missing in the table schema "${source.name}"`,
        );
      }
      source = tables[connection.destSchema];
    });
  });
}
