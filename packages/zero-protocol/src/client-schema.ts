import {must} from '../../shared/src/must.ts';
import {mapAllEntries} from '../../shared/src/objects.ts';
import * as v from '../../shared/src/valita.ts';

export type ValueType = 'string' | 'number' | 'boolean' | 'null' | 'json';

export const valueTypeSchema: v.Type<ValueType> = v.literalUnion(
  'string',
  'number',
  'boolean',
  'null',
  'json',
);

export const columnSchemaSchema = v.object({
  type: valueTypeSchema,
});

export type ColumnSchema = v.Infer<typeof columnSchemaSchema>;

export const tableSchemaSchema = v.object({
  columns: v.record(columnSchemaSchema),
  primaryKey: v.array(v.string()),
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
      tables.sort(keyCmp).map(([name, table]) => [
        name,
        {
          columns: mapAllEntries(table.columns, e => e.sort(keyCmp)),
          primaryKey: must(
            table.primaryKey,
            `new clients always specify a primaryKey`,
          ).sort(),
        },
      ]),
    ),
  };
}
