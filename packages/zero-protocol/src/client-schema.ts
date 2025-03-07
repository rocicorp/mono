import {h64} from '../../shared/src/hash.ts';
import {mapAllEntries, mapEntries} from '../../shared/src/objects.ts';
import * as v from '../../shared/src/valita.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import {valueTypeSchema} from '../../zero-schema/src/schema-config.ts';

export const columnSchemaSchema = v.object({
  type: valueTypeSchema,
});

export type ColumnSchema = v.Infer<typeof columnSchemaSchema>;

export const tableSchemaSchema = v.object({
  columns: v.record(columnSchemaSchema),
});

export type TableSchema = v.Infer<typeof tableSchemaSchema>;

export const clientSchemaSchema = v.object({
  tables: v.record(tableSchemaSchema),
});

export type ClientSchema = v.Infer<typeof clientSchemaSchema>;

const keyCmp = ([a]: [a: string, _: unknown], [b]: [b: string, _: unknown]) =>
  a < b ? -1 : a > b ? 1 : 0;

/**
 * Returns a normalized schema (with the tables and columns sorted)
 * suitable for hashing.
 */
// exported for testing.
export function normalize(schema: ClientSchema): ClientSchema {
  return {
    tables: mapAllEntries(schema.tables, tables =>
      tables
        .sort(keyCmp)
        .map(([name, table]) => [
          name,
          {columns: mapAllEntries(table.columns, e => e.sort(keyCmp))},
        ]),
    ),
  };
}

export function clientSchemaFrom(schema: Schema): {
  clientSchema: ClientSchema;
  hash: string;
} {
  const client = {
    tables: mapEntries(schema.tables, (name, {serverName, columns}) => [
      serverName ?? name,
      {
        columns: mapEntries(columns, (name, {serverName, type}) => [
          serverName ?? name,
          {type},
        ]),
      },
    ]),
  };
  const clientSchema = normalize(client);
  const hash = h64(JSON.stringify(clientSchema)).toString(36);
  return {clientSchema, hash};
}
