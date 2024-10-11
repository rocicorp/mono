import {sortedEntries} from '../../../shared/src/sorted-entries.js';
import type {Writable} from '../../../shared/src/writable.js';
import {
  NormalizedTableSchema,
  normalizeTableSchemaWithCache,
  type TableSchemaCache,
} from '../../../zql/src/zql/query/normalize-table-schema.js';
import type {Schema} from './zero.js';

/**
 * Creates a normalized schema from a schema.
 *
 * A normalized schema has all the keys sorted and the primary key and the
 * primary key columns are checked to be valid.
 */
export function normalizeSchema(schema: Schema): NormalizedSchema {
  if (schema instanceof NormalizedSchema) {
    return schema;
  }
  return new NormalizedSchema(schema);
}

export class NormalizedSchema {
  readonly version: number;
  readonly tables: {
    readonly [table: string]: NormalizedTableSchema;
  };

  constructor(schema: Schema) {
    this.version = schema.version;
    this.tables = normalizeTables(schema.tables);
  }
}

function normalizeTables(tables: Schema['tables']): {
  readonly [table: string]: NormalizedTableSchema;
} {
  const rv: Writable<{
    readonly [table: string]: NormalizedTableSchema;
  }> = {};
  const tableSchemaCache: TableSchemaCache = new Map();
  for (const [name, table] of sortedEntries(tables)) {
    rv[name] = normalizeTableSchemaWithCache(table, name, tableSchemaCache);
  }
  return rv;
}
