import {mapAllEntries} from '../../shared/src/objects.ts';
import * as v from '../../shared/src/valita.ts';

export type ValueType =
  | 'string'
  | 'number'
  | 'boolean'
  | 'null'
  | 'json'
  | 'date'
  | 'timestamp';

export const valueTypeSchema: v.Type<ValueType> = v.union(
  v.literal('string'),
  v.literal('number'),
  v.literal('boolean'),
  v.literal('null'),
  v.literal('json'),
  v.literal('date'),
  v.literal('timestamp'),
);

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
export function normalizeClientSchema(schema: ClientSchema): ClientSchema {
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
