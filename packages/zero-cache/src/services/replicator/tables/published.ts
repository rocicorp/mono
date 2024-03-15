import type postgres from 'postgres';
import {assert} from 'shared/src/asserts.js';
import * as v from 'shared/src/valita.js';
import type {TableSpec} from './specs.js';

const publishedColumnsSchema = v.array(
  v.object({
    tableSchema: v.string(),
    tableName: v.string(),
    ordinalPosition: v.number(),
    columnName: v.string(),
    dataType: v.string(),
    arrayType: v.string().nullable(),
    characterMaximumLength: v.number().nullable(),
    columnDefault: v.string().nullable(),
    isNullable: v.union(v.literal('YES'), v.literal('NO')),
    constraintType: v.string().nullable(),
    keyPosition: v.number().nullable(),
  }),
);

/**
 * Retrieves all tables and columns published under any PUBLICATION
 * whose name starts with the specified `pubPrefix` (e.g. "zero_").
 */
export async function getPublishedTables(
  sql: postgres.Sql,
  pubPrefix: string,
): Promise<Record<string, TableSpec>> {
  const result = await sql`
  SELECT c.table_schema, 
         c.table_name, 
         c.ordinal_position, 
         c.column_name, 
         c.data_type, 
         et.data_type as array_type, 
         c.character_maximum_length, 
         c.column_default, 
         c.is_nullable, 
         tc.constraint_type, 
         kcu.ordinal_position as key_position
  FROM information_schema.columns c
  JOIN pg_publication_tables AS p 
    ON p.schemaname = c.table_schema AND 
    p.tablename = c.table_name AND 
    c.column_name = ANY(p.attnames)
  LEFT JOIN information_schema.element_types et 
    ON c.table_schema = et.object_schema AND
      c.table_name = et.object_name AND 
      et.object_type = 'TABLE' AND 
      c.ordinal_position::integer = et.collection_type_identifier::integer
  LEFT JOIN (
    information_schema.table_constraints tc
    JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
    JOIN information_schema.key_column_usage AS kcu USING (constraint_schema, constraint_name, column_name)
  ) ON c.table_schema = ccu.table_schema and c.table_name = ccu.table_name and c.column_name = ccu.column_name
  WHERE starts_with(p.pubname, ${pubPrefix})
  ORDER BY c.table_schema, c.table_name, c.ordinal_position;
  `;

  const columns = v.parse(result, publishedColumnsSchema);
  const tables: Record<string, TableSpec> = {};
  let table: TableSpec | undefined;

  columns.forEach(col => {
    if (col.tableSchema !== table?.schema || col.tableName !== table?.name) {
      // New table
      table = {
        schema: col.tableSchema,
        name: col.tableName,
        columns: {},
        primaryKey: [],
      };
      tables[`${table.schema}.${table.name}`] = table;
    }

    table.columns[col.columnName] = {
      dataType: col.arrayType ? `${col.arrayType}[]` : col.dataType,
      characterMaximumLength: col.characterMaximumLength,
      columnDefault: col.columnDefault,
    };
    if (col.keyPosition) {
      while (table.primaryKey.length < col.keyPosition) {
        table.primaryKey.push('');
      }
      table.primaryKey[col.keyPosition - 1] = col.columnName;
    }
  });

  // Sanity check that the primary keys are filled in.
  Object.values(tables).forEach(table => {
    assert(
      table.primaryKey.indexOf('') < 0,
      `Invalid primary key for ${JSON.stringify(table)}`,
    );
  });

  return tables;
}
