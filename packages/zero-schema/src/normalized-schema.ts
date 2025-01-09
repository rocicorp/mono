import {assert} from '../../shared/src/asserts.js';
import {sortedEntries} from '../../shared/src/sorted-entries.js';
import type {Writable} from '../../shared/src/writable.js';
import type {CompoundKey} from '../../zero-protocol/src/ast.js';
import {
  normalizeTableSchemaWithCache,
  type DecycledNormalizedTableSchema,
  type NormalizedFieldRelationship,
  type NormalizedTableSchema,
  type TableSchemaCache,
} from './normalize-table-schema.js';
import type {Schema} from './schema.js';
import type {TableSchema} from './table-schema.js';

const normalizedCache = new WeakMap<Schema, NormalizedSchema>();

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

  let s;
  if (!(s = normalizedCache.get(schema))) {
    normalizedCache.set(schema, (s = new NormalizedSchema(schema)));
  }
  return s;
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

export type DecycledNormalizedSchema = Omit<NormalizedSchema, 'tables'> & {
  readonly tables: {
    readonly [table: string]: DecycledNormalizedTableSchema;
  };
};

function normalizeTables(tables: Schema['tables']): {
  readonly [table: string]: NormalizedTableSchema;
} {
  const rv: Writable<{
    readonly [table: string]: NormalizedTableSchema;
  }> = {};
  const tableSchemaCache: TableSchemaCache = new Map();

  function assertFieldRelation(
    tableName: string,
    relationShipName: string,
    relation: NormalizedFieldRelationship,
  ) {
    const destTableName = relation.destSchema.tableName;
    assert(
      destTableName in tables,
      `Relationship "${tableName}"."${relationShipName}" destination "${destTableName}" is missing in schema`,
    );
    assertColumns(relation.sourceField, tables[tableName]);
    assertColumns(relation.destField, tables[destTableName]);
  }

  function assertColumns(columnNames: CompoundKey, table: TableSchema) {
    for (const columnName of columnNames) {
      assert(
        columnName in table.columns,
        `Column "${columnName}" is missing in table "${table.tableName}"`,
      );
    }
  }

  for (const [name, table] of sortedEntries(tables)) {
    rv[name] = normalizeTableSchemaWithCache(
      table,
      name,
      tableSchemaCache,
      assertFieldRelation,
    );
  }
  return rv;
}
