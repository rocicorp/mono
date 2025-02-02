/* eslint-disable @typescript-eslint/no-explicit-any */
import type {
  Relationship,
  RelationshipsSchema,
  TableSchema,
} from '../table-schema.ts';
import type {Relationships} from './relationship-builder.ts';
import {type TableBuilderWithColumns} from './table-builder.ts';

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
  const TTables extends readonly TableBuilderWithColumns<TableSchema>[],
  const TRelationships extends readonly Relationships[],
>(
  version: number,
  options: {
    readonly tables: TTables;
    readonly relationships?: TRelationships | undefined;
  },
): {
  version: number;
  tables: {
    readonly [K in TTables[number]['schema']['name']]: Extract<
      TTables[number]['schema'],
      {name: K}
    >;
  };
  relationships: {
    readonly [K in TRelationships[number]['name']]: Extract<
      TRelationships[number],
      {name: K}
    >['relationships'];
  };
} {
  const retTables: Record<string, TableSchema> = {};
  const retRelationships: Record<string, Record<string, Relationship>> = {};
  const serverNames = new Set<string>();

  options.tables.forEach(table => {
    const {serverName = table.schema.name} = table.schema;
    if (serverNames.has(serverName)) {
      throw new Error(`Multiple tables reference the name "${serverName}"`);
    }
    serverNames.add(serverName);
    if (retTables[table.schema.name]) {
      throw new Error(
        `Table "${table.schema.name}" is defined more than once in the schema`,
      );
    }
    retTables[table.schema.name] = table.build();
  });
  options.relationships?.forEach(relationships => {
    if (retRelationships[relationships.name]) {
      throw new Error(
        `Relationships for table "${relationships.name}" are defined more than once in the schema`,
      );
    }
    retRelationships[relationships.name] = relationships.relationships;
    checkRelationship(
      relationships.relationships,
      relationships.name,
      retTables,
    );
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
