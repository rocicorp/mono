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

export function createSchema<
  TTables extends Record<string, TableBuilderWithColumns<TableSchema>>,
  TRelationships extends Record<string, Relationships>,
>(
  version: number,
  tables: TTables,
  relationships: TRelationships,
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
  Object.values(relationships).forEach(relationship => {
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
